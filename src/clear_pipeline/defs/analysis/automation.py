"""Scheduled analysis automation drain (ADR-0007 §5) — regenerate DUE frames.

A sensor ticks this on an interval; it drains clear-api's
``dueAnalysisAutomations``, groups them by FRAME, regenerates each frame once,
and marks all of that frame's automations ran (lastRunAt + nextRunAt from each
cadence). Because the finest-cadence subscriber always comes due first, the
frame effectively refreshes at the MINIMUM cadence across its subscribers, and
the coarser subscribers read the same fresh row.

Automations are rolling ("to present"): the frame's identity keeps
``window_end = None`` (so every run supersedes the same row), while retrieval +
structured datapoints are resolved over ``[window_start, now]`` via the
materialised ``effective_end``.

No ``from __future__ import annotations`` — Dagster inspects the ``context``
annotation on the asset.
"""

import logging
from datetime import datetime, timezone

import dagster as dg

from clear_pipeline.defs.analysis.stages import _resolve_frame_aggregated
from clear_pipeline.defs.signals.poll_sensor import build_poll_sensor
from clear_pipeline.defs.situation.frame import Frame, build_rag_filters
from clear_pipeline.defs.situation.generate import generate_and_upsert_for_frame
from clear_pipeline.providers import clear_api
from clear_pipeline.providers.redis_lock import redis_lock
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

_DRAIN_LOCK_TTL_SECONDS = 3600
_BATCH_SIZE = 100


def _frame_from_automation(auto: dict) -> Frame:
    # Rolling: window_end None = "to present" (the frame's stable identity).
    return Frame.build(
        window_start=auto["windowStart"],
        window_end=None,
        location_ids=auto.get("locationIds") or [],
        event_types=auto.get("eventTypes") or [],
        need_sectors=auto.get("needSectors") or [],
    )


def _frame_group_key(frame: Frame) -> tuple:
    return (frame.location_ids, frame.event_types, frame.need_sectors, frame.window_start)


def _process_frame(context, frame: Frame, automation_ids: list[str], now_iso: str) -> bool:
    """Regenerate one frame over [window_start, now] and mark its automations
    ran. Returns True on a successful generation. Best-effort — one frame's
    failure doesn't stop the others."""
    rag_filters = build_rag_filters(frame, include_time_range=True, effective_end=now_iso)
    aggregated = _resolve_frame_aggregated(frame, effective_end=now_iso)
    try:
        result = generate_and_upsert_for_frame(
            frame=frame,
            # TODO: resolve location names for richer prompt framing (as for the
            # on-demand drain); retrieval is already scoped by the location filter.
            scope_label="the selected area",
            period_label=f"{frame.window_start[:10]} to present",
            aggregated=aggregated,
            rag_filters=rag_filters,
            log_context=context.log,
        )
    except Exception:  # noqa: BLE001 — isolate one frame's failure
        context.log.exception("[drain_automations] frame %s generation raised", frame.location_ids)
        return False
    # Stamp the run on every subscriber of this frame regardless of whether the
    # generation produced content — a None result (all-empty) still means "we
    # tried this tick"; leaving nextRunAt unset would hot-loop the frame.
    try:
        clear_api.mark_analysis_automations_ran(automation_ids)
    except Exception:  # noqa: BLE001 — a mark failure just re-runs the frame next tick
        context.log.warning(
            "[drain_automations] could not mark automations ran for frame %s", frame.location_ids, exc_info=True,
        )
    return result is not None


def _drain(context) -> dg.MaterializeResult:
    with redis_lock("drain_analysis_automations:drain", ttl_seconds=_DRAIN_LOCK_TTL_SECONDS, wait_seconds=0) as acquired:
        if not acquired:
            context.log.info("[drain_automations] another drain holds the lock — skipping this run")
            return dg.MaterializeResult(metadata={"skipped_concurrent": True})

        due = clear_api.get_due_analysis_automations(limit=_BATCH_SIZE)
        if not due:
            return dg.MaterializeResult(metadata={"due": 0})

        # Group due automations by canonical frame — run each frame once (the
        # minimum cadence across its subscribers subsumes the coarser ones).
        by_frame: dict[tuple, list[dict]] = {}
        for auto in due:
            by_frame.setdefault(_frame_group_key(_frame_from_automation(auto)), []).append(auto)

        now_iso = datetime.now(timezone.utc).isoformat()
        generated = empty = 0
        for autos in by_frame.values():
            frame = _frame_from_automation(autos[0])
            ids = [a["id"] for a in autos]
            if _process_frame(context, frame, ids, now_iso):
                generated += 1
            else:
                empty += 1

        context.log.info(
            "[drain_automations] frames=%d generated=%d empty/failed=%d (from %d due automations)",
            len(by_frame), generated, empty, len(due),
        )
        return dg.MaterializeResult(
            metadata={"frames": len(by_frame), "generated": generated, "empty_or_failed": empty}
        )


@dg.asset(
    name="drain_analysis_automations",
    group_name="analysis",
    description="Regenerate DUE analysis automations, grouped by frame at the minimum cadence across subscribers.",
)
def drain_analysis_automations(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return _drain(context)


# Ships STOPPED; enabled at cutover. The interval only bounds how often we CHECK
# for due frames — the per-automation nextRunAt is what actually paces each frame.
drain_analysis_automations_job = dg.define_asset_job(
    name="drain_analysis_automations_job", selection=[drain_analysis_automations]
)
analysis_automation_sensor = build_poll_sensor(
    name="analysis_automation_sensor",
    job=drain_analysis_automations_job,
    default_interval_minutes=settings.manual_poll_interval_minutes,
)
