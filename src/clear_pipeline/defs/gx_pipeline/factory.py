"""Generic GX-gated bronze -> silver -> gold asset factory.
``build_gx_source_assets(source)`` produces one source's full pipeline: 9
assets + 6 GX checks + 1 job. Nothing writes to clear-api until
``<source>_push``. Bronze/silver stay S3 JSON; gold signals are Iceberg,
Type-1 (`iceberg_signals.py`). Gold events persistence is STUBBED for now
(`iceberg_events.py`) pending a decision on keeping it synchronous with
clear-api's own Event table — see that module's docstring and
docs/data-quality-medallion-implementation.md §6. ``_push`` only pushes
signals until that lands.

**Add a data source = add a ``GXSource`` to ``sources.py``** — this
module needs no change, mirroring ``defs/signals/factory.py``'s
``build_source_assets(connector)``. One exception: ``_reconcile`` probes
for the optional ``group_member``/``resolve_group`` pair via ``getattr``
(not part of the ``GXSource`` Protocol) — a source whose rows don't
compete with each other (i.e. every source except IDMC today) simply
doesn't define them and passes straight through; see ``IDMCGXSource``.

Simplifications, each with an upgrade path (details in the doc §5):

  - ``<source>_geo`` clusters on a heuristic district key (geoparser
    `display_name`, not a real admin-2 lookup — that needs a
    clear-api-resolved location, which only exists post-push). The
    authoritative admin-2 is resolved in ``<source>_push`` instead.
  - No LLM rewrite of merged event title/description — bootstrap only.
  - Single-writer, no cross-run locking (production uses `redis_lock`
    here); fine for one Dagster run at a time. ``_reconcile`` makes this
    assumption load-bearing rather than merely tidy: it is a
    read-modify-write over a whole supersession group, so two overlapping
    runs can compute verdicts from the same stale snapshot and silently
    revert each other's retraction. It does not self-heal, because a group
    is only ever revisited when one of its rows turns up in a batch. The
    ``reconcile -> gold -> push`` span wants wrapping in `redis_lock`
    before this runs concurrently.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import dagster as dg
import great_expectations as gx
import pandas as pd

from clear_pipeline.defs.gx_pipeline import iceberg_events, iceberg_signals
from clear_pipeline.defs.gx_pipeline.gx_utils import validate_dataframe
from clear_pipeline.defs.gx_pipeline.sources import GXSource
from clear_pipeline.defs.signals import lake
from clear_pipeline.providers.classify import classify_locally
from clear_pipeline.providers.clear_api import create_signal
from clear_pipeline.providers.event import ACTIVE_EVENTS_WINDOW_DAYS
from clear_pipeline.signals.config import settings

_BRONZE_COLUMNS = ["externalId", "publishedAt", "s3Key"]
_SILVER_COLUMNS = ["externalId", "publishedAt", "title", "description", "severity"]

# The one word `_reconcile` needs from a source's `resolve_group` hook.
# Anything else a hook returns (conventionally "keep") means the row lives,
# so a source can't accidentally retract rows by returning an unexpected
# value. See sources.py's IDMCGXSource for the hook contract.
_RETRACT = "retract"


def _s3():
    return lake.s3_client(), settings.s3_bucket


def _district_key(geoparsed: dict | None) -> str | None:
    """Heuristic district proxy from the geoparser's display_name — see the
    module docstring's simplifications note. None when nothing geoparsed."""
    if not geoparsed or not geoparsed.get("display_name"):
        return None
    parts = [p.strip() for p in geoparsed["display_name"].split(",") if p.strip()]
    return parts[1] if len(parts) > 1 else (parts[0] if parts else None)


def build_gx_source_assets(source: GXSource) -> list:
    """Return one source's full GX-gated defs: [8 assets, 6 checks, 1 job]."""
    src = source.source
    group = f"{src}_gx"

    # ══════════════════════════════════════════════════════════════════════
    # Bronze: raw records, untouched. GX-gated on shape before anything reads it.
    # ══════════════════════════════════════════════════════════════════════
    @dg.asset(
        name=f"{src}_bronze",
        group_name=group,
        description=f"Poll {src}, write each raw record to S3 bronze (unchanged from production's raw_{src}).",
    )
    def _bronze(context: dg.AssetExecutionContext) -> pd.DataFrame:
        poll_started = datetime.now(UTC)
        since = source.last_synced()
        records = source.poll(since)
        context.log.info("[%s bronze] %d records fetched", src, len(records))

        if not records:
            context.add_output_metadata({"records_fetched": 0})
            return pd.DataFrame(columns=_BRONZE_COLUMNS)

        s3, bucket = _s3()
        rows: list[dict] = []
        written = failed = 0
        for record in records:
            try:
                ext_id = source.external_id(record)
                pub_at = source.published_at(record)
                key = lake.raw_key(src, pub_at, ext_id)
                lake.write_raw(s3, bucket, key, source.raw_bytes(record))
                rows.append({"externalId": ext_id, "publishedAt": pub_at, "s3Key": key})
                written += 1
            except Exception:
                context.log.exception("[%s bronze] failed to write a record", src)
                failed += 1

        # Same clean-batch-only watermark advance as production's factory.py:
        # a partial failure holds it so the next poll retries the window.
        if failed == 0:
            source.set_watermark(poll_started)
        else:
            context.log.warning("[%s bronze] %d record(s) failed — watermark held for retry", src, failed)

        context.add_output_metadata({"records_fetched": len(records), "written": written, "failed": failed})
        return pd.DataFrame(rows)

    # ══════════════════════════════════════════════════════════════════════
    # Silver: cleansed, normalized, ONE row per record. No clear-api write.
    # ══════════════════════════════════════════════════════════════════════
    @dg.asset(
        name=f"{src}_silver",
        group_name=group,
        ins={"bronze_df": dg.AssetIn(key=f"{src}_bronze")},
        description=f"Normalize + geo-enrich {src} bronze rows (no clear-api write); write cleansed records to S3 silver.",
    )
    def _silver(context: dg.AssetExecutionContext, bronze_df: pd.DataFrame) -> pd.DataFrame:
        if bronze_df.empty:
            return pd.DataFrame(columns=_SILVER_COLUMNS)

        source_id = source.api_source_id()
        s3, bucket = _s3()

        # Parse every bronze row first, keeping it paired with its bronze
        # metadata (externalId/publishedAt) for the second pass below.
        parsed: list[tuple[dict, Any]] = []
        for bronze_row in bronze_df.to_dict("records"):
            raw = s3.get_object(Bucket=bucket, Key=bronze_row["s3Key"])["Body"].read()
            parsed.append((bronze_row, source.parse(raw)))

        rows: list[dict] = []
        for bronze_row, record in parsed:
            signal_input = source.to_silver_input(record, source_id)

            key = lake.raw_key(src, bronze_row["publishedAt"], bronze_row["externalId"], layer="silver")
            lake.write_json(s3, bucket, key, signal_input)

            rows.append({
                "externalId": bronze_row["externalId"],
                "publishedAt": bronze_row["publishedAt"],
                "title": signal_input.get("title"),
                "description": signal_input.get("description"),
                "severity": signal_input.get("severity"),
                "lat": signal_input.get("lat"),
                "lng": signal_input.get("lng"),
                "geoparsedData": signal_input.get("geoparsedData"),
                "signalInput": signal_input,
            })

        context.add_output_metadata({"rows": len(rows)})
        return pd.DataFrame(rows)

    # ══════════════════════════════════════════════════════════════════════
    # Reconcile: decide which rows in each supersession group survive, across
    # the WHOLE gold table rather than just this batch. Optional per source.
    # ══════════════════════════════════════════════════════════════════════
    def _group_hooks():
        """The optional supersession hooks, or None if the source doesn't do
        group supersession. All-or-nothing on purpose: one without the other
        can't produce a verdict, and silently half-running is worse than not
        running. See IDMCGXSource for the only implementation today."""
        hooks = tuple(getattr(source, name, None)
                      for name in ("group_member", "resolve_group"))
        return hooks if all(hooks) else None

    @dg.asset(
        name=f"{src}_reconcile",
        group_name=group,
        ins={"silver_df": dg.AssetIn(key=f"{src}_silver")},
        description=(
            "Resolve supersession groups against the whole gold table (not just this "
            "batch); drop superseded rows and flag retractions. Pass-through if the "
            "source defines no group hooks."
        ),
    )
    def _reconcile(context: dg.AssetExecutionContext, silver_df: pd.DataFrame) -> pd.DataFrame:
        """Sits between `_silver` and `_classify`, and must stay there.

        Not earlier: it reads `signalInput`, which `_silver` builds. Not
        later: a retracted row must never reach `_match`, whose severity and
        casualties aggregation would fold a row that's about to be retracted
        into the event totals.

        The gold table is read with a plain function call rather than a
        Dagster `AssetIn` on `<source>_gold`. An `AssetIn` would be a cycle
        — gold already depends on this asset transitively. Same pattern and
        same reason as `_load_open_gold_event_ids` above.
        """
        df = silver_df.copy()
        hooks = _group_hooks()
        if df.empty or hooks is None:
            # No group semantics for this source: every row survives, and
            # `groupKey` still has to exist as a column so `_match` can read
            # it off every row uniformly.
            df["groupKey"] = [None] * len(df)
            df["retracted"] = [False] * len(df)
            return df
        group_member, resolve_group = hooks

        # ── Batch side: one member per silver row that's in a group ───────
        batch_members: dict[str, dict] = {}
        # Every batch row's fresh signalInput, keyed by externalId (not just
        # grouped ones — a RETRACT verdict can apply to any externalId).
        # Needed below: a row whose verdict is RETRACT never reaches
        # `_match`/`_gold`'s normal full-row overwrite (keep_mask drops it),
        # so this is the ONLY place that can keep gold's stored content from
        # freezing at whatever it was the last time the row WAS kept — see
        # the gold-side loop below for what breaks if it doesn't.
        batch_signal_inputs: dict[str, Any] = {row.externalId: row.signalInput for row in df.itertuples()}
        for row in df.itertuples():
            member = group_member(row.externalId, (row.signalInput or {}).get("rawData"))
            if member is not None:
                batch_members[row.externalId] = member

        if not batch_members:
            df["groupKey"] = [None] * len(df)
            df["retracted"] = [False] * len(df)
            context.add_output_metadata({"rows_in": len(df), "rows_out": len(df), "groups": 0})
            return df

        # ── Gold side: every row already stored in a group this poll touched.
        # This is the whole point of the asset — without it the verdict is
        # computed against the batch alone, and a row superseded by a LATER
        # poll is never revisited.
        signals_table = iceberg_signals.get_signals_table(src)
        touched = sorted({m["groupKey"] for m in batch_members.values()})
        gold_rows = iceberg_signals.signals_in_groups(signals_table, touched)

        members_by_group: dict[str, list[dict]] = {}
        for member in batch_members.values():
            members_by_group.setdefault(member["groupKey"], []).append(member)
        for gold_row in gold_rows:
            # A row present in both sides is represented by its batch copy —
            # same externalId, but the freshly polled version, which may
            # carry a revised role.
            if gold_row["externalId"] in batch_members:
                continue
            member = group_member(gold_row["externalId"], (gold_row["signalInput"] or {}).get("rawData"))
            if member is not None:
                members_by_group.setdefault(member["groupKey"], []).append(member)

        verdicts: dict[str, str] = {}
        for members in members_by_group.values():
            verdicts.update(resolve_group(members))

        # ── Apply to gold: flip retracted, and refresh content whenever a
        # fresh batch copy exists — independent of each other.
        #
        # A RETRACT-verdict row never reaches `_match`/`_gold`'s normal
        # full-row overwrite (keep_mask drops it below), so if this loop
        # only flipped `retracted`, `signalInput`/`rawData` would freeze at
        # whatever it was the last time the row WAS kept — even though the
        # verdict above was computed from its fresh batch role. A LATER poll
        # that doesn't re-send this row would then read that frozen content
        # back out of gold (line ~247's fallback) and resolve the group
        # against a role that no longer exists — a wrong verdict computed
        # from stale truth, not a decaying display field. Content gets
        # refreshed whenever the row showed up in this poll's batch at all,
        # not only when `retracted` happens to flip, because an
        # already-retracted row can still be revised upstream.
        changed: list[dict] = []
        retracted_after_push = 0
        for gold_row in gold_rows:
            ext_id = gold_row["externalId"]
            should_retract = verdicts.get(ext_id) == _RETRACT
            fresh_signal_input = batch_signal_inputs.get(ext_id)
            retracted_changed = bool(gold_row.get("retracted")) != should_retract
            if not retracted_changed and fresh_signal_input is None:
                continue
            # upsert_signals replaces the ENTIRE matched row, so this writes
            # back the full row read from gold with these fields changed —
            # not a sparse patch, which would blank every other column.
            if fresh_signal_input is not None:
                gold_row["signalInput"] = fresh_signal_input
            gold_row["retracted"] = should_retract
            changed.append(gold_row)
            if should_retract and gold_row.get("pushedAt"):
                retracted_after_push += 1
        if changed:
            iceberg_signals.upsert_signals(signals_table, changed)
        if retracted_after_push:
            # Known gap until clear-api accepts a retraction (step 4 of the
            # design): these are correctly marked dead in gold but still
            # live in Postgres. Logged rather than silently dropped.
            context.log.warning(
                "[%s reconcile] %d already-pushed signal(s) retracted in gold — "
                "clear-api propagation not implemented yet, they remain live downstream",
                src, retracted_after_push,
            )

        # ── Apply to the batch: superseded rows leave the pipeline here.
        # They're dropped rather than written to gold as retracted: a row
        # that never reached gold has nothing to correct downstream, and
        # re-dropping it on every poll is idempotent.
        keep_mask = [verdicts.get(ext_id) != _RETRACT for ext_id in df["externalId"]]
        df["groupKey"] = [
            (batch_members[ext_id]["groupKey"] if ext_id in batch_members else None)
            for ext_id in df["externalId"]
        ]
        df["retracted"] = [False] * len(df)
        kept = df[keep_mask].reset_index(drop=True)

        context.add_output_metadata({
            "rows_in": len(df),
            "rows_out": len(kept),
            "groups": len(members_by_group),
            "batch_rows_superseded": int(len(df) - len(kept)),
            "gold_rows_retracted": sum(1 for r in changed if r["retracted"]),
            "gold_rows_unretracted": sum(1 for r in changed if not r["retracted"]),
        })
        return kept

    # ══════════════════════════════════════════════════════════════════════
    # Silver -> Gold business logic. Pure transforms, source-agnostic from
    # here on — nothing below reads or writes clear-api except `_push`.
    # ══════════════════════════════════════════════════════════════════════
    @dg.asset(
        name=f"{src}_classify",
        group_name=group,
        ins={"silver_df": dg.AssetIn(key=f"{src}_reconcile")},
        description="Relevance + event type (classify_locally, unchanged) — pure transform over reconciled silver.",
    )
    def _classify(context: dg.AssetExecutionContext, silver_df: pd.DataFrame) -> pd.DataFrame:
        df = silver_df.copy()
        if df.empty:
            df["relevanceScore"] = pd.Series(dtype=float)
            df["eventType"] = pd.Series(dtype=object)
            df["glideCode"] = pd.Series(dtype=object)
            return df

        relevances, types, glides = [], [], []
        for row in df.itertuples():
            c = classify_locally(title=row.title, description=row.description, source_severity=row.severity)
            relevances.append(c.relevance)
            types.append(c.type_level_2)
            glides.append(c.disaster_types[0] if c.disaster_types else "ot")
        df["relevanceScore"] = relevances
        df["eventType"] = types
        df["glideCode"] = glides
        context.add_output_metadata({"rows": len(df)})
        return df

    @dg.asset(
        name=f"{src}_geo",
        group_name=group,
        ins={"classify_df": dg.AssetIn(key=f"{src}_classify")},
        description="Heuristic district key from the geoparser's display_name — see factory.py's module docstring.",
    )
    def _geo(context: dg.AssetExecutionContext, classify_df: pd.DataFrame) -> pd.DataFrame:
        df = classify_df.copy()
        if df.empty:
            df["districtKey"] = pd.Series(dtype=object)
            return df
        df["districtKey"] = df["geoparsedData"].map(_district_key)
        unresolved = int(df["districtKey"].isna().sum())
        context.add_output_metadata({"rows": len(df), "unresolved_district": unresolved})
        return df

    def _load_open_gold_event_ids(now: datetime) -> dict[tuple[str, str], str]:
        """eventId of the current-version gold event (Iceberg, §6) still
        inside the active window, keyed by (districtKey, eventType). Rows
        outside the window are excluded — a new signal in that district+type
        starts a fresh event, same semantics as production's active-events
        cache. Only the id is needed here; `_match` re-reads the full
        current row itself when it actually merges into one."""
        cutoff = now - timedelta(days=ACTIVE_EVENTS_WINDOW_DAYS)
        events_table = iceberg_events.get_events_table(src)
        current_df = iceberg_events.current_events_df(events_table)
        open_event_ids: dict[tuple[str, str], str] = {}
        for row in current_df.to_dict("records"):
            try:
                last_touched = datetime.fromisoformat(str(row["lastSignalCreatedAt"]).replace("Z", "+00:00"))
                if last_touched.tzinfo is None:
                    # Some sources (e.g. ACLED's event_date) are date-only,
                    # no offset — treat as UTC like the rest of the pipeline.
                    last_touched = last_touched.replace(tzinfo=UTC)
            except (KeyError, ValueError, AttributeError, TypeError):
                continue
            if last_touched < cutoff:
                continue
            district, event_type = row.get("districtKey"), row.get("eventType")
            if district and event_type:
                open_event_ids[(district, event_type)] = row["eventId"]
        return open_event_ids

    @dg.asset(
        name=f"{src}_temporal",
        group_name=group,
        ins={"geo_df": dg.AssetIn(key=f"{src}_geo")},
        description="Tag new-vs-merged against gold events still inside the active window (S3, not clear-api).",
    )
    def _temporal(context: dg.AssetExecutionContext, geo_df: pd.DataFrame) -> pd.DataFrame:
        df = geo_df.copy()
        if df.empty:
            df["eventId"] = pd.Series(dtype=object)
            df["matchOutcome"] = pd.Series(dtype=object)
            return df

        now = datetime.now(UTC)
        open_event_ids = _load_open_gold_event_ids(now)
        # Batch-local: two new signals landing in the same (district, type)
        # this run join the same fresh event, not two separate ones.
        batch_new: dict[tuple[str, str], str] = {}

        event_ids, outcomes = [], []
        for row in df.itertuples():
            key = (row.districtKey, row.eventType)
            if row.districtKey is None or row.eventType is None:
                event_ids.append(str(uuid.uuid4()))
                outcomes.append("new_event")
                continue
            existing_id = open_event_ids.get(key)
            if existing_id:
                event_ids.append(existing_id)
                outcomes.append("merged")
            elif key in batch_new:
                event_ids.append(batch_new[key])
                outcomes.append("merged")
            else:
                new_id = str(uuid.uuid4())
                batch_new[key] = new_id
                event_ids.append(new_id)
                outcomes.append("new_event")
        df["eventId"] = event_ids
        df["matchOutcome"] = outcomes
        merged_ratio = (df["matchOutcome"] == "merged").mean() if len(df) else 0.0
        context.add_output_metadata({"rows": len(df), "merged_ratio": round(float(merged_ratio), 3)})
        return df

    @dg.asset(
        name=f"{src}_match",
        group_name=group,
        ins={"temporal_df": dg.AssetIn(key=f"{src}_temporal")},
        description="Create-or-merge into a gold-shaped Event (in-memory) — no S3/clear-api write yet.",
    )
    def _match(context: dg.AssetExecutionContext, temporal_df: pd.DataFrame) -> list[dict]:
        df = temporal_df
        if df.empty:
            return []

        now_iso = datetime.now(UTC).isoformat()
        events_table = iceberg_events.get_events_table(src)
        bundles: dict[str, dict] = {}

        for row in df.sort_values("publishedAt").itertuples():
            bundle = bundles.get(row.eventId)
            if bundle is None:
                existing = (
                    iceberg_events.current_event(events_table, row.eventId)
                    if row.matchOutcome == "merged" else None
                )
                bundle = existing or {
                    "eventId": row.eventId,
                    "districtKey": row.districtKey,
                    "eventType": row.eventType,
                    "glideCode": row.glideCode,
                    "title": row.title,
                    "description": row.description,
                    "severity": row.severity or 1,
                    "casualties": None,
                    "signalIds": [],
                    "startedAt": row.publishedAt,
                    "firstSignalCreatedAt": row.publishedAt,
                }
                bundles[row.eventId] = bundle
            else:
                # Merge-in-batch: latest signal's title/description wins (no
                # LLM rewrite this pass — see module docstring).
                bundle["title"] = row.title or bundle["title"]
                bundle["description"] = row.description or bundle["description"]

            bundle["signalIds"] = list({*bundle["signalIds"], row.externalId})
            bundle["severity"] = max(bundle["severity"] or 1, row.severity or 1)
            bundle["lastSignalCreatedAt"] = row.publishedAt
            bundle.setdefault("newSignalRows", []).append({
                "externalId": row.externalId,
                "eventId": row.eventId,
                "relevanceScore": row.relevanceScore,
                "eventType": row.eventType,
                "districtKey": row.districtKey,
                "matchOutcome": row.matchOutcome,
                "createdAt": now_iso,
                "pushedAt": None,
                # Carried from `_reconcile` so the row can be found by group
                # on a LATER poll — without it, gold holds no way to tell
                # which rows compete with an incoming one.
                "groupKey": row.groupKey,
                "retracted": row.retracted,
                "signalInput": row.signalInput,
            })
            casualties = (row.signalInput or {}).get("casualties")
            if casualties is not None:
                bundle["casualties"] = (bundle.get("casualties") or 0) + casualties

        context.add_output_metadata({"events": len(bundles), "signals": len(df)})
        return list(bundles.values())

    # ══════════════════════════════════════════════════════════════════════
    # Gold: finished signal + event rows, GX-gated before they're eligible to push.
    # ══════════════════════════════════════════════════════════════════════
    @dg.asset(
        name=f"{src}_gold",
        group_name=group,
        ins={"bundles": dg.AssetIn(key=f"{src}_match")},
        description="Upsert signal rows (Type-1) into Iceberg (§6). Event persistence is stubbed (iceberg_events.py).",
    )
    def _gold(context: dg.AssetExecutionContext, bundles: list[dict]) -> pd.DataFrame:
        if not bundles:
            return pd.DataFrame(columns=["externalId", "eventId", "severity", "populationAffectedContribution"])

        events_table = iceberg_events.get_events_table(src)
        signals_table = iceberg_signals.get_signals_table(src)
        signal_rows: list[dict] = []
        new_versions = no_op_versions = 0
        for bundle in bundles:
            new_signals = bundle.pop("newSignalRows", [])
            merged = iceberg_events.merge_event(events_table, bundle)
            if merged["version"] != bundle.get("version"):
                new_versions += 1
            else:
                no_op_versions += 1
            for sig_row in new_signals:
                sig_row["populationAffectedContribution"] = None
                sig_row["casualtiesContribution"] = (sig_row["signalInput"] or {}).get("casualties")
                sig_row["severity"] = bundle["severity"]
                signal_rows.append(sig_row)

        # A re-merged signal (matchOutcome="merged") already has a gold row
        # — preserve its pushedAt rather than letting the Type-1 upsert
        # reset an already-pushed signal back to NULL (re-push loop).
        already_pushed = iceberg_signals.existing_pushed_at(
            signals_table, [r["externalId"] for r in signal_rows]
        )
        for sig_row in signal_rows:
            existing = already_pushed.get(sig_row["externalId"])
            if existing is not None:
                sig_row["pushedAt"] = existing
        iceberg_signals.upsert_signals(signals_table, signal_rows)

        context.add_output_metadata({
            "events_written": len(bundles), "signals_written": len(signal_rows),
            "event_versions_new": new_versions, "event_versions_unchanged": no_op_versions,
        })
        return pd.DataFrame(signal_rows)

    # ══════════════════════════════════════════════════════════════════════
    # Push: the ONLY stage that writes to clear-api. Incremental — only rows
    # with pushedAt IS NULL. Signals only — event push is stubbed, see
    # iceberg_events.py's module docstring.
    # ══════════════════════════════════════════════════════════════════════
    @dg.asset(
        name=f"{src}_push",
        group_name=group,
        deps=[f"{src}_gold"],
        description="Push unpushed gold signals (pushedAt IS NULL) to clear-api. Event push is stubbed (iceberg_events.py).",
    )
    def _push(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        signals_table = iceberg_signals.get_signals_table(src)
        unpushed = iceberg_signals.unpushed_signals(signals_table)

        if not unpushed:
            context.log.info("[%s push] nothing to push", src)
            return dg.MaterializeResult(metadata={"pushed_signals": 0})

        pushed_signals = failed = 0
        to_upsert: list[dict] = []
        now_iso = datetime.now(UTC).isoformat()
        for row in unpushed:
            try:
                create_signal(row["signalInput"])
                source.mark_seen(row["externalId"])
                row["pushedAt"] = now_iso
                to_upsert.append(row)
                pushed_signals += 1
            except Exception:
                context.log.exception("[%s push] signal %s failed — stays unpushed for retry", src, row["externalId"])
                failed += 1

        iceberg_signals.upsert_signals(signals_table, to_upsert)

        context.log.info("[%s push] pushed_signals=%d failed=%d", src, pushed_signals, failed)
        return dg.MaterializeResult(metadata={"pushed_signals": pushed_signals, "failed_signals": failed})

    # ══════════════════════════════════════════════════════════════════════
    # GX asset checks — blocking at bronze/silver/gold, observational at
    # classify/geo/temporal. See gx_utils.py's docstring for the split.
    # ══════════════════════════════════════════════════════════════════════
    def _blocking_result(result, **extra) -> dg.AssetCheckResult:
        metadata = {**result.check_metadata(), **extra}
        if result.blocked:
            return dg.AssetCheckResult(passed=False, severity=dg.AssetCheckSeverity.ERROR, metadata=metadata)
        if not result.success:
            return dg.AssetCheckResult(passed=False, severity=dg.AssetCheckSeverity.WARN, metadata=metadata)
        return dg.AssetCheckResult(passed=True, metadata=metadata)

    def _observational_result(result, **extra) -> dg.AssetCheckResult:
        return dg.AssetCheckResult(
            passed=result.success, severity=dg.AssetCheckSeverity.WARN,
            metadata={**result.check_metadata(), **extra},
        )

    @dg.asset_check(asset=_bronze, blocking=True, name="bronze_shape")
    def _bronze_check(df: pd.DataFrame) -> dg.AssetCheckResult:
        if df.empty:
            # An empty poll is the normal steady state, not a shape defect —
            # skip the row-count gate so a quiet run doesn't fail the job.
            return dg.AssetCheckResult(passed=True, metadata={"row_count": 0, "skipped": "empty poll"})
        result = validate_dataframe(
            df, suite_name=f"{src}_bronze",
            expectations=[
                gx.expectations.ExpectColumnValuesToNotBeNull(column="externalId"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="publishedAt"),
                gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
            ],
        )
        return _blocking_result(result)

    @dg.asset_check(asset=_silver, blocking=True, name="silver_completeness")
    def _silver_check(df: pd.DataFrame) -> dg.AssetCheckResult:
        result = validate_dataframe(
            df, suite_name=f"{src}_silver",
            expectations=[
                gx.expectations.ExpectColumnValuesToNotBeNull(column="title", mostly=0.95),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="description", mostly=0.95),
                gx.expectations.ExpectColumnValuesToBeBetween(column="severity", min_value=1, max_value=5),
                gx.expectations.ExpectColumnValuesToBeUnique(column="externalId"),
                gx.expectations.ExpectColumnValuesToBeBetween(column="lat", min_value=-90, max_value=90, mostly=0.99),
                gx.expectations.ExpectColumnValuesToBeBetween(column="lng", min_value=-180, max_value=180, mostly=0.99),
            ],
        )
        return _blocking_result(result)

    @dg.asset_check(asset=_classify, name="classify_populated")
    def _classify_check(df: pd.DataFrame) -> dg.AssetCheckResult:
        result = validate_dataframe(
            df, suite_name=f"{src}_classify",
            expectations=[gx.expectations.ExpectColumnValuesToNotBeNull(column="relevanceScore")],
        )
        return _observational_result(result)

    @dg.asset_check(asset=_geo, name="geo_resolution_rate")
    def _geo_check(df: pd.DataFrame) -> dg.AssetCheckResult:
        result = validate_dataframe(
            df, suite_name=f"{src}_geo",
            expectations=[gx.expectations.ExpectColumnValuesToNotBeNull(column="districtKey", mostly=0.7)],
        )
        return _observational_result(result)

    @dg.asset_check(asset=_temporal, name="temporal_match_ratio")
    def _temporal_check(df: pd.DataFrame) -> dg.AssetCheckResult:
        result = validate_dataframe(
            df, suite_name=f"{src}_temporal",
            expectations=[gx.expectations.ExpectColumnValuesToBeInSet(column="matchOutcome", value_set=["new_event", "merged"])],
        )
        merged_ratio = (df["matchOutcome"] == "merged").mean() if len(df) else 0.0
        return _observational_result(result, merged_ratio=round(float(merged_ratio), 3))

    @dg.asset_check(asset=_gold, blocking=True, name="gold_integrity")
    def _gold_check(df: pd.DataFrame) -> dg.AssetCheckResult:
        result = validate_dataframe(
            df, suite_name=f"{src}_gold",
            expectations=[
                gx.expectations.ExpectColumnValuesToBeBetween(column="severity", min_value=1, max_value=5),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="eventId"),
            ],
        )
        return _blocking_result(result)

    job = dg.define_asset_job(
        name=f"{src}_gx",
        selection=[_bronze, _silver, _reconcile, _classify, _geo, _temporal, _match, _gold, _push],
    )

    return [
        _bronze, _silver, _reconcile, _classify, _geo, _temporal, _match, _gold, _push,
        _bronze_check, _silver_check, _classify_check, _geo_check, _temporal_check, _gold_check,
        job,
    ]
