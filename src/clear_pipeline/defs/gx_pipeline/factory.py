"""Generic GX-gated bronze -> silver -> gold asset factory.
``build_gx_source_assets(source)`` produces one source's full pipeline: 8
assets + 6 GX checks + 1 job. Nothing writes to clear-api until
``<source>_push``. Bronze/silver stay S3 JSON; gold signals are Iceberg,
Type-1 (`iceberg_signals.py`). Gold events persistence is STUBBED for now
(`iceberg_events.py`) pending a decision on keeping it synchronous with
clear-api's own Event table — see that module's docstring and
docs/data-quality-medallion-implementation.md §6. ``_push`` only pushes
signals until that lands.

**Add a data source = add a ``GXSource`` to ``sources.py``** — this
module needs no change, mirroring ``defs/signals/factory.py``'s
``build_source_assets(connector)``.

Simplifications, each with an upgrade path (details in the doc §5):

  - ``<source>_geo`` clusters on a heuristic district key (geoparser
    `display_name`, not a real admin-2 lookup — that needs a
    clear-api-resolved location, which only exists post-push). The
    authoritative admin-2 is resolved in ``<source>_push`` instead.
  - No LLM rewrite of merged event title/description — bootstrap only.
  - Single-writer, no cross-run locking (production uses `redis_lock`
    here); fine for one Dagster run at a time.
"""

import uuid
from datetime import UTC, datetime, timedelta

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
            except Exception:  # noqa: BLE001 — one bad record shouldn't drop the batch
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
        rows: list[dict] = []
        for row in bronze_df.to_dict("records"):
            raw = s3.get_object(Bucket=bucket, Key=row["s3Key"])["Body"].read()
            record = source.parse(raw)
            signal_input = source.to_silver_input(record, source_id)

            key = lake.raw_key(src, row["publishedAt"], row["externalId"], layer="silver")
            lake.write_json(s3, bucket, key, signal_input)

            rows.append({
                "externalId": row["externalId"],
                "publishedAt": row["publishedAt"],
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
    # Silver -> Gold business logic. Pure transforms, source-agnostic from
    # here on — nothing below reads or writes clear-api except `_push`.
    # ══════════════════════════════════════════════════════════════════════
    @dg.asset(
        name=f"{src}_classify",
        group_name=group,
        ins={"silver_df": dg.AssetIn(key=f"{src}_silver")},
        description="Relevance + event type (classify_locally, unchanged) — pure transform over silver.",
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
                "signalInput": row.signalInput,
            })
            casualties = (row.signalInput or {}).get("casualties")
            if casualties is not None:
                bundle["casualties"] = (bundle.get("casualties") or 0) + casualties

        context.add_output_metadata({"events": len(bundles), "signals": int(len(df))})
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
            except Exception:  # noqa: BLE001 — isolate one signal's push failure; it stays unpushed for retry
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
        selection=[_bronze, _silver, _classify, _geo, _temporal, _match, _gold, _push],
    )

    return [
        _bronze, _silver, _classify, _geo, _temporal, _match, _gold, _push,
        _bronze_check, _silver_check, _classify_check, _geo_check, _temporal_check, _gold_check,
        job,
    ]
