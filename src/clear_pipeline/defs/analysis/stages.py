"""On-demand analysis drain — the pendingAnalyses queue consumer (ADR-0007 §4).

No ``from __future__ import annotations`` — Dagster inspects the ``context``
annotation on the asset.
"""

import logging

import dagster as dg

from clear_pipeline.defs.knowledgebase.datapoints_schemas import (
    SCHEMA_VERSION as AGGREGATION_SCHEMA_VERSION,
)
from clear_pipeline.defs.signals.poll_sensor import build_poll_sensor
from clear_pipeline.defs.situation.frame import Frame, build_rag_filters
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


def _resolve_frame_aggregated(frame: Frame, *, effective_end: str | None = None) -> dict | None:
    """Leverage structured datapoints when available (decision #2). For a
    single-location frame clear-api returns the precomputed bucket if one matches
    or an on-demand roll-up over ``report_datapoints`` in the window scoped to
    that location; a multi-location / location-less frame falls back to KB-only
    (None) — a combined-location roll-up is a later refinement.

    ``effective_end`` materialises the window end for a rolling frame (an
    automation's "to present"), which otherwise has no stored ``window_end``."""
    end = effective_end or frame.window_end
    if len(frame.location_ids) != 1 or not end:
        return None
    try:
        return clear_api.get_aggregated_datapoint(
            location_id=frame.location_ids[0],
            window_start=frame.window_start,
            window_end=end,
            # A custom window won't hit a precomputed tier bucket; clear-api then
            # rolls up report_datapoints in the window on demand.
            window_kind="custom",
            schema_version=AGGREGATION_SCHEMA_VERSION,
        )
    except Exception:  # noqa: BLE001 — datapoints are best-effort; KB narrative fills in
        logger.warning(
            "[drain_analysis] aggregated fetch failed for %s — proceeding KB-only",
            frame.location_ids, exc_info=True,
        )
        return None


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
    # Custom frames match locations literally and time-filter retrieval to the
    # window (decision #1); structured datapoints are leveraged when present.
    rag_filters = build_rag_filters(frame, include_time_range=True)
    aggregated = _resolve_frame_aggregated(frame)

    try:
        result = generate_and_upsert_for_frame(
            frame=frame,
            # TODO: resolve the frame's location names for richer prompt framing;
            # retrieval is already correctly scoped by the location filter.
            scope_label="the selected area",
            period_label=_period_label(frame),
            aggregated=aggregated,
            rag_filters=rag_filters,
            log_context=context.log,
        )
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
