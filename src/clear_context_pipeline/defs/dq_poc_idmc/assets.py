"""Bronze -> silver -> gold medallion POC for IDMC IDU, GX-gated at every
promotion. Matches the next-state architecture in
docs/data-quality-ingestion-design.md §2a and docs/data-quality-great-expectations.pptx:
silver and gold are Dagster-native artifacts, not clear-api writes: nothing
in this module touches clear-api directly.

Deliberately isolated from `defs/signals/`: this is a proof of concept for
the *architecture* (Dagster-native silver/gold, GX gates instead of
clear-api round trips), not a replacement for the production IDMC
connector. Two simplifications from a production build, called out where
they happen:

  - Geographic consolidation resolves a synthetic district key from IDMC's
    own iso3 + locations_name, not a real admin-2 lookup against clear-api's
    location tree (§3a substep 2 in the doc). Avoids a live clear-api read
    dependency for something meant to run standalone.
  - Classification is a small heuristic (displacement_type + figure
    magnitude), not the production `classify_locally` classifier. Same
    reasoning: keep the POC runnable with zero external dependencies beyond
    IDMC's own public API and S3.

The gold-layer push to clear-api is stubbed: it serializes what *would* be
pushed to S3 instead of calling clear-api, so the POC never writes into a
shared database and needs no clear-api credentials to run end to end.

Substeps 1-4 (classify / geo / temporal / match) are separate Dagster assets
here, one per §3a phase, so each gets its own visible node and its own GX
check in the asset graph. The doc's own recommendation (§3a) is to collapse
these into named functions inside one asset for production, to avoid
per-asset scheduling overhead. The POC keeps them apart because the whole
point here is to make each quality gate visible, not to ship the
production shape.
"""

import json
import logging
import os
from datetime import UTC, datetime

import dagster as dg
import pandas as pd

from clear_context_pipeline.defs.signals import lake
from clear_context_pipeline.providers import idmc
from clear_context_pipeline.signals.config import settings as signals_settings

logger = logging.getLogger(__name__)

# ── POC scope: env-overridable, deliberately not coupled to signals/config.py ──
IDMC_COUNTRIES = frozenset(
    c.strip().upper()
    for c in os.environ.get("IDMC_POC_COUNTRIES", "SDN,AFG,VEN").split(",")
    if c.strip()
)
IDMC_ALLOWED_TYPES = frozenset(
    t.strip()
    for t in os.environ.get("IDMC_POC_ALLOWED_TYPES", "Conflict,Disaster").split(",")
    if t.strip()
)

# Doc §3a substep 3: how far apart two records can be and still be
# considered part of the same active window.
ACTIVE_WINDOW_DAYS = 30

GROUP = "dq_poc_idmc"


def _write_json(prefix: str, name: str, payload: object) -> str:
    """Persist a medallion-layer snapshot to S3 for inspection/replay.
    Silver and gold are Dagster-native artifacts (no clear-api write), but
    still worth landing somewhere durable and human-readable, mirroring
    bronze's existing raw/<source>/... convention. Returns the S3 key."""
    s3 = lake.s3_client()
    bucket = signals_settings.s3_bucket
    day = datetime.now(UTC).strftime("%Y-%m-%d")
    key = f"{prefix}/idmc_poc/{day}/{name}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    return key


# ══════════════════════════════════════════════════════════════════════════
# Bronze: raw IDU rows, scoped to configured countries/types, untouched
# otherwise. GX-gated on shape before anything downstream reads it.
# ══════════════════════════════════════════════════════════════════════════
@dg.asset(
    name="idmc_poc_bronze",
    group_name=GROUP,
    description="Fetch IDU records for the configured countries/types; write raw blobs to S3.",
)
def idmc_poc_bronze(context: dg.AssetExecutionContext) -> pd.DataFrame:
    raw_rows = idmc.fetch_idu_records()
    scoped = [
        r
        for r in raw_rows
        if (r.get("iso3") or "").upper() in IDMC_COUNTRIES
        and (r.get("displacement_type") or "") in IDMC_ALLOWED_TYPES
    ]
    context.log.info("[bronze] %d/%d raw rows in scope", len(scoped), len(raw_rows))

    s3 = lake.s3_client()
    bucket = signals_settings.s3_bucket
    written = failed = 0
    for row in scoped:
        key = lake.raw_key("idmc_poc", row.get("created_at") or "", str(row.get("id")))
        try:
            lake.write_raw(s3, bucket, key, json.dumps(row, default=str).encode("utf-8"))
            written += 1
        except Exception:
            context.log.exception("[bronze] failed to write raw blob for id=%s", row.get("id"))
            failed += 1

    context.add_output_metadata(
        {"raw_rows_fetched": len(raw_rows), "raw_rows_in_scope": len(scoped),
         "written_to_s3": written, "write_failed": failed}
    )
    return pd.DataFrame(scoped)


# ══════════════════════════════════════════════════════════════════════════
# Silver: cleansed, normalized, ONE row per source record. Dagster-native:
# no clear-api write. GX-gated on completeness/range before it feeds
# business logic.
# ══════════════════════════════════════════════════════════════════════════
@dg.asset(
    name="idmc_poc_silver",
    group_name=GROUP,
    deps=["idmc_poc_bronze"],
    description="Normalize bronze rows into the silver schema; no clear-api write.",
)
def idmc_poc_silver(context: dg.AssetExecutionContext, idmc_poc_bronze: pd.DataFrame) -> pd.DataFrame:
    parsed = [
        row
        for raw in idmc_poc_bronze.to_dict("records")
        if (row := idmc.parse_idu_record(raw)) is not None
    ]
    context.log.info("[silver] %d/%d bronze rows parsed", len(parsed), len(idmc_poc_bronze))

    df = pd.DataFrame(parsed).drop(columns=["raw"], errors="ignore")
    key = _write_json("silver", "idmc_poc", parsed)
    context.add_output_metadata({"silver_rows": len(df), "s3_key": key})
    return df


# ══════════════════════════════════════════════════════════════════════════
# Silver -> Gold business logic, §3a substeps 1-4. Pure transformations over
# the silver artifact. Nothing here reads or writes clear-api.
# ══════════════════════════════════════════════════════════════════════════

# Doc §3a substep 1: relevance + event type. POC heuristic, production
# would call providers/classify.py's classify_locally instead.
_EVENT_TYPE_MAP = {"Conflict": "conflict", "Disaster": "natural_hazard"}


def _classify_one(row: dict) -> tuple[float, str | None]:
    event_type = _EVENT_TYPE_MAP.get(row["displacement_type"])
    if event_type is None:
        return 0.0, None
    # Bigger displacement events are unambiguously relevant; small ones
    # still count but score lower. A stand-in for a real relevance model.
    figure = row.get("figure") or 0
    relevance = min(1.0, 0.4 + (figure / 50_000))
    return round(relevance, 3), event_type


@dg.asset(
    name="idmc_poc_classify",
    group_name=GROUP,
    deps=["idmc_poc_silver"],
    description="Substep 1: relevance score + event type (POC heuristic, not classify_locally).",
)
def idmc_poc_classify(context: dg.AssetExecutionContext, idmc_poc_silver: pd.DataFrame) -> pd.DataFrame:
    df = idmc_poc_silver.copy()
    scores, types = zip(*df.apply(lambda r: _classify_one(r.to_dict()), axis=1)) if len(df) else ((), ())
    df["relevance_score"] = list(scores)
    df["event_type"] = list(types)
    context.add_output_metadata({"rows": len(df)})
    return df


# Doc §3a substep 2: geographic consolidation. POC stand-in: a synthetic
# district key from IDMC's own iso3 + locations_name, not a real admin-2
# lookup against clear-api's location tree.
def _district_key(row: dict) -> str | None:
    name = row.get("locations_name")
    iso3 = row.get("iso3")
    if not name or not iso3:
        return None
    return f"{iso3}:{name}"


@dg.asset(
    name="idmc_poc_geo",
    group_name=GROUP,
    deps=["idmc_poc_classify"],
    description="Substep 2: synthetic district key from iso3 + locations_name (POC stand-in for admin-2 resolution).",
)
def idmc_poc_geo(context: dg.AssetExecutionContext, idmc_poc_classify: pd.DataFrame) -> pd.DataFrame:
    df = idmc_poc_classify.copy()
    df["district_id"] = df.apply(lambda r: _district_key(r.to_dict()), axis=1)
    unresolved = int(df["district_id"].isna().sum())
    context.add_output_metadata({"rows": len(df), "unresolved_district": unresolved})
    return df


# Doc §3a substep 3: temporal consolidation, statistical not per-record.
def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).replace(tzinfo=UTC)
    except ValueError:
        return None


@dg.asset(
    name="idmc_poc_temporal",
    group_name=GROUP,
    deps=["idmc_poc_geo"],
    description="Substep 3: tag new-vs-merged against records sharing (district, event_type) within the active window.",
)
def idmc_poc_temporal(context: dg.AssetExecutionContext, idmc_poc_geo: pd.DataFrame) -> pd.DataFrame:
    df = idmc_poc_geo.copy()
    df["start_dt"] = df["displacement_start_date"].map(_parse_date)
    df = df.sort_values("start_dt", na_position="last").reset_index(drop=True)

    seen_until: dict[tuple[str, str], datetime] = {}
    outcomes: list[str] = []
    for _, row in df.iterrows():
        key = (row["district_id"], row["event_type"])
        start = row["start_dt"]
        prior = seen_until.get(key)
        if start is None or prior is None:
            outcomes.append("new")
        else:
            outcomes.append("merged" if (start - prior).days <= ACTIVE_WINDOW_DAYS else "new")
        if start is not None:
            seen_until[key] = max(prior, start) if prior else start
    df["match_outcome"] = outcomes
    df = df.drop(columns=["start_dt"])

    merged_ratio = (df["match_outcome"] == "merged").mean() if len(df) else 0.0
    context.add_output_metadata({"rows": len(df), "merged_ratio": round(float(merged_ratio), 3)})
    return df


@dg.asset(
    name="idmc_poc_match",
    group_name=GROUP,
    deps=["idmc_poc_temporal"],
    description="Substep 4: group by (district, event_type, window) into Gold-shaped Event objects.",
)
def idmc_poc_match(context: dg.AssetExecutionContext, idmc_poc_temporal: pd.DataFrame) -> pd.DataFrame:
    df = idmc_poc_temporal.dropna(subset=["district_id", "event_type"])
    events = []
    for (district_id, event_type), group in df.groupby(["district_id", "event_type"]):
        events.append(
            {
                "district_id": district_id,
                "event_type": event_type,
                "population_affected": int(group["figure"].sum()),
                "signal_count": len(group),
                "signal_ids": list(group["idu_id"]),
                "severity": min(5, max(1, int(group["figure"].sum() // 10_000) + 1)),
                "earliest_start": group["displacement_start_date"].min(),
                "latest_end": group["displacement_end_date"].max(),
                "titles": list(group["title"].unique())[:3],
            }
        )
    out = pd.DataFrame(events)
    context.add_output_metadata({"events": len(out), "signals_matched": int(len(df))})
    return out


# ══════════════════════════════════════════════════════════════════════════
# Gold: finished Event-shaped objects, GX-gated before the (stubbed) push.
# ══════════════════════════════════════════════════════════════════════════
@dg.asset(
    name="idmc_poc_gold",
    group_name=GROUP,
    deps=["idmc_poc_match"],
    description="Finished Event objects: referential integrity + aggregate bounds gated before the push.",
)
def idmc_poc_gold(context: dg.AssetExecutionContext, idmc_poc_match: pd.DataFrame) -> pd.DataFrame:
    key = _write_json("gold", "idmc_poc", idmc_poc_match.to_dict("records"))
    context.add_output_metadata({"events": len(idmc_poc_match), "s3_key": key})
    return idmc_poc_match


@dg.asset(
    name="idmc_poc_push_stub",
    group_name=GROUP,
    deps=["idmc_poc_gold"],
    description="STUBBED single push: serializes what would go to clear-api instead of calling it.",
)
def idmc_poc_push_stub(context: dg.AssetExecutionContext, idmc_poc_gold: pd.DataFrame) -> dict:
    """The doc's next-state §2a push, stubbed for the POC: no clear-api
    credentials, no write into a shared database. One JSON manifest per run
    listing exactly what a real push would have sent, so the "single
    validated push" step is visible without depending on live clear-api."""
    manifest = {
        "would_push_count": len(idmc_poc_gold),
        "pushed_at": datetime.now(UTC).isoformat(),
        "events": idmc_poc_gold.to_dict("records"),
    }
    key = _write_json("gold-pushed", "idmc_poc", manifest)
    context.log.info(
        "[push_stub] would push %d finished Event objects to clear-api (stubbed, wrote %s instead)",
        len(idmc_poc_gold), key,
    )
    context.add_output_metadata({"would_push_count": len(idmc_poc_gold), "s3_key": key})
    return manifest


idmc_poc_job = dg.define_asset_job(
    name="idmc_poc_medallion",
    selection=[
        idmc_poc_bronze,
        idmc_poc_silver,
        idmc_poc_classify,
        idmc_poc_geo,
        idmc_poc_temporal,
        idmc_poc_match,
        idmc_poc_gold,
        idmc_poc_push_stub,
    ],
)
