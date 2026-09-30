"""On-demand analysis drain — the pendingAnalyses queue consumer (ADR-0007 §4).

No ``from __future__ import annotations`` — Dagster inspects the ``context``
annotation on the asset.
"""

import logging
from datetime import datetime, timezone

import dagster as dg

from clear_pipeline.defs.analysis.combine import (
    combine_aggregated_buckets,
    dedupe_nested_locations,
)
from clear_pipeline.defs.knowledgebase.datapoints_schemas import (
    SCHEMA_VERSION as AGGREGATION_SCHEMA_VERSION,
)
from clear_pipeline.defs.signals.poll_sensor import build_poll_sensor
from clear_pipeline.defs.situation.frame import FALLBACK_SCOPE_LABEL, Frame, build_rag_filters, scope_label
from clear_pipeline.defs.situation.generate import generate_and_upsert_for_frame
from clear_pipeline.providers import clear_api
from clear_pipeline.providers.redis_lock import redis_lock
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

_DRAIN_LOCK_TTL_SECONDS = 3600  # generous single-flight guard
_BATCH_SIZE = 20
_MAX_BATCHES = 20

# Per-request drain outcomes.
_GENERATED = "generated"
_FAILED = "failed"
_REQUEUE = "requeue"  # couldn't mark the request done — leave PENDING, retry next run


def _frame_from_request(req: dict) -> Frame:
    return Frame.build(
        window_start=req["windowStart"],
        window_end=req.get("windowEnd"),
        location_ids=req.get("locationIds") or [],
        event_types=req.get("eventTypes") or [],
        need_sectors=req.get("needSectors") or [],
    )


def _period_label(frame: Frame) -> str:
    """Human window label for the LLM prompts (a custom frame has no calendar
    period name)."""
    end = (frame.window_end or "present")[:10]
    return f"{frame.window_start[:10]} to {end}"


def _scope_label(frame: Frame) -> str:
    """The frame's area by name ("Sheikan and Um Rawaba, North Kordofan,
    Sudan") so every prompt names the place it is analysing. Best-effort: a
    lookup failure falls back to a generic label rather than failing the run."""
    if not frame.location_ids:
        return FALLBACK_SCOPE_LABEL
    try:
        locations = {loc["id"]: loc for loc in clear_api.get_locations() if loc.get("id")}
    except Exception:  # noqa: BLE001 — naming is cosmetic; generation must still run
        logger.warning("[drain_analysis] location names unavailable for %s", frame.location_ids, exc_info=True)
        return FALLBACK_SCOPE_LABEL
    return scope_label(frame.location_ids, locations)


def _fetch_one_bucket(location_id: str, window_start: str, window_end: str) -> dict | None:
    """One location's aggregated bucket, or None on miss/error (best-effort —
    the KB narrative fills in when structured datapoints are unavailable)."""
    try:
        return clear_api.get_aggregated_datapoint(
            location_id=location_id,
            window_start=window_start,
            window_end=window_end,
            # A custom window won't hit a precomputed tier bucket; clear-api then
            # rolls up report_datapoints in the window on demand.
            window_kind="custom",
            schema_version=AGGREGATION_SCHEMA_VERSION,
        )
    except Exception:  # noqa: BLE001 — datapoints are best-effort; KB narrative fills in
        logger.warning(
            "[drain_analysis] aggregated fetch failed for %s — proceeding without it",
            location_id, exc_info=True,
        )
        return None


def _resolve_frame_aggregated(frame: Frame, *, effective_end: str | None = None) -> dict | None:
    """Leverage structured datapoints when available (decision #2). Each frame
    location gets its own aggregated bucket (clear-api returns a precomputed
    bucket if one matches, else an on-demand roll-up over ``report_datapoints``
    in the window scoped to that location's subtree). A single-location frame
    returns that one bucket unchanged; a multi-location frame de-nests (drops any
    location that is a descendant of another it also lists, so subtree roll-ups
    don't double-count) then sums the buckets into one. A location-less frame
    falls back to KB-only (None).

    ``effective_end`` materialises the window end for a rolling frame (an
    automation's "to present"), which otherwise has no stored ``window_end``."""
    end = effective_end or frame.window_end
    if not frame.location_ids or not end:
        return None

    location_ids = list(frame.location_ids)
    if len(location_ids) > 1:
        try:
            parent_of = clear_api.get_location_parents()
            location_ids = dedupe_nested_locations(location_ids, parent_of)
        except Exception:  # noqa: BLE001 — if the hierarchy lookup fails, sum as-is
            logger.warning(
                "[drain_analysis] location de-nest failed for %s — summing all listed locations",
                frame.location_ids, exc_info=True,
            )

    buckets = [
        b for lid in location_ids
        if (b := _fetch_one_bucket(lid, frame.window_start, end))
    ]
    if not buckets:
        return None
    if len(buckets) == 1:
        return buckets[0]
    return combine_aggregated_buckets(buckets)


def _run_frame_generation(context, frame: Frame, *, effective_end: str | None = None):
    """Shared core of both analysis drains (on-demand + automation): scope
    retrieval + structured datapoints to the frame and (re)generate its analysis,
    returning the ``generate_and_upsert_for_frame`` result (``None`` = all-empty).

    Retrieval time-filters to the window and leverages structured datapoints when
    present (decisions #1/#2). ``effective_end`` materialises a rolling frame's
    window end ("to present") for the automation path; the on-demand path leaves
    it ``None`` (the frame carries a fixed ``window_end``). The human period label
    is derived from the frame itself, so a rolling frame reads "… to present".

    Raises on generation failure — the caller owns the outcome/marking policy
    (terminal vs. retry), which differs between the two drains.
    """
    rag_filters = build_rag_filters(frame, include_time_range=True, effective_end=effective_end)
    aggregated = _resolve_frame_aggregated(frame, effective_end=effective_end)
    return generate_and_upsert_for_frame(
        frame=frame,
        scope_label=_scope_label(frame),
        period_label=_period_label(frame),
        aggregated=aggregated,
        rag_filters=rag_filters,
        log_context=context.log,
    )


def _mark(context, fn, request_id: str, *args) -> bool:
    """Run a completion mutation, swallowing clear-api errors so a mark failure
    leaves the request PENDING for the next tick rather than crashing the drain."""
    try:
        fn(request_id, *args)
        return True
    except Exception:  # noqa: BLE001
        context.log.warning(
            "[drain_analysis] could not mark request %s (%s) — leaving PENDING",
            request_id, fn.__name__, exc_info=True,
        )
        return False


def _process_one_request(context, req: dict) -> str:
    request_id = req["id"]
    frame = _frame_from_request(req)
    # A rolling request (no window_end) generates an automation's frame ahead
    # of its hourly poll: materialise "now" as the end, as the automation drain
    # does, so datapoints aggregate over [window_start, now] instead of being
    # skipped. A fixed window needs no effective_end.
    effective_end = None if frame.window_end else datetime.now(timezone.utc).isoformat()
    try:
        result = _run_frame_generation(context, frame, effective_end=effective_end)
    except clear_api.ClearApiError as exc:
        # Non-retryable (bad frame / rejected payload) — fail terminally.
        context.log.error("[drain_analysis] request %s rejected (non-retryable): %s", request_id, exc)
        return _FAILED if _mark(context, clear_api.mark_analysis_request_failed, request_id, str(exc)) else _REQUEUE
    except Exception as exc:  # noqa: BLE001 — transient (LLM / clear-api blip): leave PENDING to retry
        context.log.warning(
            "[drain_analysis] request %s failed transiently — leaving PENDING for retry: %s", request_id, exc,
        )
        return _REQUEUE

    if result is None:
        # All-empty generation — no evidence for this frame; terminal.
        ok = _mark(
            context, clear_api.mark_analysis_request_failed, request_id,
            "generation produced no content",
        )
        return _FAILED if ok else _REQUEUE

    return _GENERATED if _mark(context, clear_api.mark_analysis_request_generated, request_id) else _REQUEUE


def _drain(context) -> dg.MaterializeResult:
    """Drain PENDING analysis requests under a single-flight lock. Concurrent
    ticks are safe: the lock serialises runs, and every request is marked
    GENERATED / FAILED so it leaves the queue (a mark failure leaves it PENDING
    for the next run)."""
    with redis_lock("drain_analysis_requests:drain", ttl_seconds=_DRAIN_LOCK_TTL_SECONDS, wait_seconds=0) as acquired:
        if not acquired:
            context.log.info("[drain_analysis] another drain holds the lock — skipping this run")
            return dg.MaterializeResult(metadata={"skipped_concurrent": True})

        generated = failed = requeued = 0
        for _ in range(_MAX_BATCHES):
            batch = clear_api.get_pending_analyses(limit=_BATCH_SIZE)  # oldest-first
            if not batch:
                break
            made_progress = False
            for req in batch:
                outcome = _process_one_request(context, req)
                if outcome == _GENERATED:
                    generated += 1
                    made_progress = True
                elif outcome == _FAILED:
                    failed += 1
                    made_progress = True
                else:  # _REQUEUE — mark failed; stays PENDING for the next run
                    requeued += 1
            # Stop when a batch advanced nothing (every request requeued — clear-api
            # likely down); avoids re-fetching the same PENDING head _MAX_BATCHES×.
            if not made_progress:
                break

        context.log.info(
            "[drain_analysis] generated=%d failed=%d requeued=%d", generated, failed, requeued,
        )
        return dg.MaterializeResult(
            metadata={"generated": generated, "failed": failed, "requeued": requeued}
        )


@dg.asset(
    name="drain_analysis_requests",
    group_name="analysis",
    description="Drain PENDING on-demand analysis requests → generate + upsert each frame's analysis → mark GENERATED/FAILED.",
)
def drain_analysis_requests(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return _drain(context)


# ── drain trigger ──────────────────────────────────────────────────────────
# On-demand requests have no upstream ingest asset, so eager automation never
# fires. This sensor ticks the drain on an interval (ships STOPPED; enabled at
# cutover). Concurrent runs are safe — see `_drain`.
drain_analysis_requests_job = dg.define_asset_job(
    name="drain_analysis_requests_job", selection=[drain_analysis_requests]
)
analysis_request_sensor = build_poll_sensor(
    name="analysis_request_sensor",
    job=drain_analysis_requests_job,
    default_interval_minutes=settings.manual_poll_interval_minutes,
)
