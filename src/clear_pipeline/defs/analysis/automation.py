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

from clear_pipeline.defs.analysis.stages import _run_frame_generation, evidence_gate, touch_synced
from clear_pipeline.defs.signals.poll_sensor import build_poll_sensor
from clear_pipeline.defs.situation.frame import Frame
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


def _mark_automations_ran(context, frame: Frame, automation_ids: list[str]) -> None:
    """Advance lastRunAt + nextRunAt for a frame's subscribers so the cadence
    stays on its fixed slots (ADR-0007 §5). Best-effort — a mark failure just
    re-runs the frame next tick."""
    try:
        clear_api.mark_analysis_automations_ran(automation_ids)
    except Exception:  # noqa: BLE001
        context.log.warning(
            "[drain_automations] could not mark automations ran for frame %s", frame.location_ids, exc_info=True,
        )


def _process_frame(context, frame: Frame, automation_ids: list[str], now_iso: str) -> bool:
    """Regenerate one frame over [window_start, now] and mark its automations
    ran. Returns True on a successful generation. Best-effort — one frame's
    failure doesn't stop the others.

    Regeneration gate (ADR-0008): when the frame is within the 24h floor or has
    no new evidence, SKIP the LLM generation — bump lastSyncedAt and still
    advance the schedule (nextRunAt stays on its cadence slots; the run just
    no-ops). Automations are never forced."""
    now = datetime.fromisoformat(now_iso)
    decision = evidence_gate(context, frame, force=False, now=now)
    if not decision.generate:
        context.log.info(
            "[drain_automations] frame %s skipped (%s) — bumping lastSyncedAt, schedule advances",
            frame.location_ids, decision.reason,
        )
        touch_synced(context, frame)
        _mark_automations_ran(context, frame, automation_ids)
        return False

    try:
        # Rolling frame: materialise "now" as the window end for retrieval +
        # datapoint aggregation (the frame's stored window_end is None).
        result = _run_frame_generation(context, frame, effective_end=now_iso)
    except Exception:  # noqa: BLE001 — isolate one frame's failure
        context.log.exception("[drain_automations] frame %s generation raised", frame.location_ids)
        return False
    # Stamp the run on every subscriber of this frame regardless of whether the
    # generation produced content — a None result (all-empty) still means "we
    # tried this tick"; leaving nextRunAt unset would hot-loop the frame.
    _mark_automations_ran(context, frame, automation_ids)
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
        # Frame is a frozen, canonicalised dataclass, so it keys the group map
        # directly (rolling automations all share window_end=None).
        by_frame: dict[Frame, list[dict]] = {}
        for auto in due:
            by_frame.setdefault(_frame_from_automation(auto), []).append(auto)

        now_iso = datetime.now(timezone.utc).isoformat()
        generated = empty = 0
        for frame, autos in by_frame.items():
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
