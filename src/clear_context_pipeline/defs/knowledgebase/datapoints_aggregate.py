"""Trigger the four-tier aggregation refresh in clear-api.

Phase 2 of the datapoint pipeline (docs/humanitarian-datapoint-extraction.md).
Aggregation math lives in clear-api's TypeScript service, not here — a
Dagster asset would have to reimplement it in Python otherwise, and
keeping parity across two languages is a maintenance tax we don't
need. This asset just calls the `refreshAggregatedDatapoints`
mutation on a schedule.

The mutation walks the four tiers internally:
  weekly × A2 (atomic) → monthly × A1 → yearly × country → all-time × country

Each computed bucket gets a fresh `validFrom = now()` row; the
previous "current" row for the same bucket key has its `validTo`
stamped in the same transaction. No history is lost.

Window scope: this asset refreshes reports whose `reportingPeriodEnd`
falls inside a rolling window. The window widens automatically on
the first run so a fresh pipeline picks up any historical
`report_datapoints` already accumulated:

  - First run (no current `aggregated_datapoints` for this schema
    version): use ``KB_AGGREGATION_INITIAL_LOOKBACK_DAYS`` (default 90).
  - Subsequent runs: use ``KB_AGGREGATION_LOOKBACK_DAYS`` (default 7).

"First run" is detected by asking clear-api whether any current
aggregation rows exist. Superseded history rows don't count — a
schema-version bump correctly triggers another backfill run for the
new version even when the old version has fully-populated caches.

The narrower weekly window keeps subsequent runs cheap; the all-time
tier can lose accuracy for very old reports over long horizons if
new reports about mid-past events land — a monthly full-refresh cron
can be added later if it becomes a real issue.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dotenv import load_dotenv

from clear_context_pipeline.defs.knowledgebase.datapoints_schemas import (
    SCHEMA_VERSION,
)
from clear_context_pipeline.providers import clear_api

load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

logger = logging.getLogger(__name__)

# Window (in days) used on subsequent runs — the rolling weekly delta.
# Reports whose `reportingPeriodEnd` falls outside this window are
# still queryable via `reportDatapoint(reportId)` and the resolver's
# on-demand rollup path — but their contribution to pre-computed
# higher-tier caches won't refresh until a wider window's run picks
# them up.
_DEFAULT_LOOKBACK_DAYS = 7

# Window (in days) used on the first run of a fresh pipeline. Wide
# enough that any historical `report_datapoints` accumulated before
# Layer 1 was turned on get folded into the caches on the first pass.
_DEFAULT_INITIAL_LOOKBACK_DAYS = 90

# Floor on how far a retrospective report may widen the refresh window. Guards
# against an LLM-emitted `reportingPeriodEnd` with a wrong year dragging the
# refresh across a decade of buckets. Override via KB_AGGREGATION_MAX_RETRO_DAYS.
_DEFAULT_MAX_RETRO_DAYS = 400


def _earliest_reporting_period_end(summaries: list[dict]) -> datetime | None:
    """Earliest ``reporting_period_end`` across a batch's per-report summaries,
    as an aware UTC datetime, or None when none carry one (all reports reused,
    or none stated a period). Tolerates date-only and datetime ISO forms."""
    earliest: datetime | None = None
    for s in summaries:
        raw = s.get("reporting_period_end")
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw)
        except (ValueError, TypeError):
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if earliest is None or dt < earliest:
            earliest = dt
    return earliest


@dg.asset(group_name="reliefweb_kb")
def reliefweb_weekly_datapoint_aggregations(
    context: AssetExecutionContext,
    reliefweb_weekly_datapoints: list[dict],
) -> dict:
    """Trigger clear-api's aggregation refresh for the rolling window.

    Depends on ``reliefweb_weekly_datapoints`` so it always runs AFTER
    per-report extraction — a Dagster upstream constraint that mirrors
    the doc's Layer 2 → Layer 1 flow. The mutation is idempotent (uses
    bitemporal supersede semantics), so a re-run against unchanged
    data produces no observable change.
    """
    if not reliefweb_weekly_datapoints:
        context.log.info(
            "no per-report datapoints landed this week — skipping aggregation refresh",
        )
        return {"computed_buckets": 0, "superseded_buckets": 0, "skipped": True}

    # First-run detection: does any current aggregation row exist for
    # this schema version? If not, treat this as a backfill and use
    # the wider initial window; else use the routine weekly delta.
    # A schema-version bump correctly triggers another backfill for
    # the new version because superseded (validTo NOT NULL) history
    # rows from the old version don't count towards this check.
    try:
        already_populated = clear_api.has_aggregated_datapoints(SCHEMA_VERSION)
    except Exception as exc:  # noqa: BLE001
        # Existence check is advisory — if clear-api is momentarily
        # unhealthy, fall back to the safer wider window rather than
        # silently under-refreshing.
        context.log.warning(
            "has_aggregated_datapoints check failed (%s) — falling back to initial-window lookback",
            exc,
        )
        already_populated = False

    if already_populated:
        lookback_days = int(
            os.environ.get("KB_AGGREGATION_LOOKBACK_DAYS", str(_DEFAULT_LOOKBACK_DAYS)),
        )
        window_label = "weekly"
    else:
        lookback_days = int(
            os.environ.get(
                "KB_AGGREGATION_INITIAL_LOOKBACK_DAYS",
                str(_DEFAULT_INITIAL_LOOKBACK_DAYS),
            ),
        )
        window_label = "initial-backfill"

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=lookback_days)

    # Retrospective trigger (ADR-0005 §5): a report published now about an OLD
    # period has a `reportingPeriodEnd` before the rolling window, so keying the
    # refresh on the rolling window alone would never recompute its correct old
    # bucket. Widen the start to also cover this batch's earliest period end —
    # the union of the rolling window and the batch's [min…max] period span. The
    # refresh is idempotent, so the buckets in between with no new reports are
    # recomputed to an unchanged value.
    batch_start = _earliest_reporting_period_end(reliefweb_weekly_datapoints)
    if batch_start is not None and batch_start < window_start:
        # Clamp the widening (#27): `batch_start` is an LLM-emitted
        # `reportingPeriodEnd`, so one hallucinated/typo'd year ("2016" for
        # "2026") would make clear-api recompute every weekly×A2 → yearly →
        # all-time bucket across a decade, blow the 60s client timeout, and
        # redden the asset until the offending report ages out. A genuine
        # deep-retrospective bucket can be refreshed manually.
        max_retro_days = int(
            os.environ.get("KB_AGGREGATION_MAX_RETRO_DAYS", str(_DEFAULT_MAX_RETRO_DAYS)),
        )
        floor = now - timedelta(days=max_retro_days)
        if batch_start < floor:
            context.log.warning(
                "batch period_end %s predates the %d-day retrospective floor — "
                "clamping to %s; refresh that bucket manually if it is genuine",
                batch_start.isoformat(), max_retro_days, floor.isoformat(),
            )
            batch_start = floor
        context.log.info(
            "retrospective report(s) in batch: widening refresh start %s → %s",
            window_start.isoformat(), batch_start.isoformat(),
        )
        window_start = batch_start

    context.log.info(
        "refreshing aggregated datapoints: mode=%s window=[%s, %s] schema_version=%s",
        window_label, window_start.isoformat(), now.isoformat(), SCHEMA_VERSION,
    )

    try:
        result = clear_api.refresh_aggregated_datapoints(
            from_iso=window_start.isoformat(),
            to_iso=now.isoformat(),
            schema_version=SCHEMA_VERSION,
        )
    except clear_api.ClearApiError as exc:
        # 4xx from clear-api — mutation shape or config is wrong. Fail
        # the asset so the operator sees the run go red rather than
        # silently skipping the refresh.
        raise dg.Failure(
            description=f"clear-api rejected refresh: {exc}",
        ) from exc

    # `situationAnalysesInvalidated` — cascade count from the yearly-
    # country bucket writes. Non-zero means the situation-analysis
    # asset (which runs downstream in the same job) has stale rows
    # waiting to be regenerated.
    situation_invalidated = int(result.get("situationAnalysesInvalidated") or 0)
    context.log.info(
        "aggregation refresh complete: computed=%d superseded=%d situation_analyses_invalidated=%d",
        result["computedBuckets"], result["supersededBuckets"], situation_invalidated,
    )
    context.add_output_metadata({
        "computed_buckets": dg.MetadataValue.int(result["computedBuckets"]),
        "superseded_buckets": dg.MetadataValue.int(result["supersededBuckets"]),
        "situation_analyses_invalidated": dg.MetadataValue.int(situation_invalidated),
        "schema_version": dg.MetadataValue.text(result["schemaVersion"]),
        "lookback_days": dg.MetadataValue.int(lookback_days),
        "mode": dg.MetadataValue.text(window_label),
    })
    return {
        "computed_buckets": result["computedBuckets"],
        "superseded_buckets": result["supersededBuckets"],
        "situation_analyses_invalidated": situation_invalidated,
        "schema_version": result["schemaVersion"],
    }
