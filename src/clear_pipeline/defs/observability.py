"""Run/queue health alerting → GlitchTip. Auto-discovered by load_from_defs_folder.

The OOM incident (2026-10-12) was a run stuck in STARTED for days, found only by a
manual dmesg dig. These sensors make that page instead: a run stuck past a
threshold, and any run failure, are reported to GlitchTip."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import dagster as dg

from clear_pipeline.providers.observability import capture_message, init_sentry
from clear_pipeline.signals.config import settings

# The daemon process (which runs these sensors) imports this module — make sure
# GlitchTip is initialised here too, not only via definitions.py.
init_sentry()


def _run_age_start(rec) -> float | None:
    """Timestamp the current in-progress phase began, for a run record.

    A STARTED run uses ``start_time`` (execution start). A run that hasn't
    started executing yet (STARTING) has no ``start_time``, so we fall back to
    ``update_timestamp`` — the last state change, i.e. the transition INTO the
    current status. We deliberately do NOT use ``create_timestamp``: a run can
    sit QUEUED for a long time before it's launched, and that queue wait must
    not count as "stuck" (that would false-alert every run when a backlog
    drains). Returns None when no timestamp is available."""
    if rec.start_time is not None:
        return rec.start_time
    if rec.update_timestamp is not None:
        return rec.update_timestamp.timestamp()
    return None


def stuck_run_alerts(
    records, now_ts: float, threshold_s: int, already_alerted: set[str]
) -> tuple[list[dict], list[str]]:
    """Pure core of the stuck-run sensor, so its behaviour is unit-testable
    without a live Dagster instance.

    Returns ``(alerts, still_stuck)``:
      * ``alerts`` — one message+tags dict per run newly over the threshold
        (i.e. over threshold AND not in ``already_alerted``), ready to send.
      * ``still_stuck`` — every run_id currently over the threshold, to persist
        as the next cursor. A run that has since cleared drops out, so it can
        alert again if it ever gets stuck a second time."""
    alerts: list[dict] = []
    still_stuck: list[str] = []
    for rec in records:
        run = rec.dagster_run
        start = _run_age_start(rec)
        if start is None:
            continue
        age_min = int((now_ts - start) / 60)
        if age_min * 60 < threshold_s:
            continue
        still_stuck.append(run.run_id)
        if run.run_id in already_alerted:
            continue  # already paged for this run; don't spam every tick
        alerts.append(
            {
                "message": (
                    f"Dagster run stuck: {run.job_name} in {run.status.value} "
                    f"for {age_min}m (run {run.run_id})"
                ),
                "tags": {
                    "run_id": run.run_id,
                    "job": run.job_name or "unknown",
                    "status": run.status.value,
                    "stuck_minutes": str(age_min),
                },
            }
        )
    return alerts, still_stuck


def run_failure_alert(run, failure_message: str) -> dict:
    """Pure core of the run-failure sensor: the GlitchTip message + tags for a
    failed run. Separated so its wording and tags are unit-testable."""
    return {
        "message": f"Dagster run failed: {run.job_name} — {failure_message}",
        "tags": {"run_id": run.run_id, "job": run.job_name or "unknown"},
    }


@dg.sensor(
    name="stuck_run_monitor_sensor",
    minimum_interval_seconds=300,
    # Without this Dagster ships the sensor STOPPED and the deploy never starts
    # it, so no stuck-run alerts ever fire — RUNNING makes it live on deploy.
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def stuck_run_monitor_sensor(context: dg.SensorEvaluationContext) -> dg.SkipReason:
    """Alert when a run has been STARTED/STARTING longer than
    ``settings.stuck_run_alert_minutes`` — catches OOM-orphaned runs, launch
    stalls, and hangs that would otherwise pin the queue unnoticed.

    Alerts once per run (deduped via the cursor), and re-arms a run_id only after
    it clears, so a genuinely long run doesn't re-page every tick."""
    threshold_s = settings.stuck_run_alert_minutes * 60
    now = datetime.now(UTC).timestamp()
    already_alerted: set[str] = set(json.loads(context.cursor) if context.cursor else [])

    records = context.instance.get_run_records(
        filters=dg.RunsFilter(
            statuses=[dg.DagsterRunStatus.STARTED, dg.DagsterRunStatus.STARTING]
        ),
        limit=500,
    )

    alerts, still_stuck = stuck_run_alerts(records, now, threshold_s, already_alerted)
    for alert in alerts:
        capture_message(alert["message"], level="error", **alert["tags"])

    # Keep only currently-still-stuck ids so a resolved run can re-alert if it
    # ever gets stuck again.
    context.update_cursor(json.dumps(still_stuck))
    return dg.SkipReason(
        f"{len(records)} in-progress; {len(still_stuck)} stuck > "
        f"{settings.stuck_run_alert_minutes}m, {len(alerts)} newly alerted"
    )


@dg.run_failure_sensor(
    name="run_failure_glitchtip_sensor",
    # As above: RUNNING so failures are reported from the moment of deploy.
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def run_failure_glitchtip_sensor(context: dg.RunFailureSensorContext) -> None:
    """Report every run failure to GlitchTip with run id + job + reason."""
    alert = run_failure_alert(context.dagster_run, context.failure_event.message)
    capture_message(alert["message"], level="error", **alert["tags"])
