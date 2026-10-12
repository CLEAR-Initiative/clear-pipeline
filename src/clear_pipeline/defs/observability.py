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


@dg.sensor(name="stuck_run_monitor_sensor", minimum_interval_seconds=300)
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

    still_stuck: list[str] = []
    newly_alerted = 0
    for rec in records:
        run = rec.dagster_run
        start = rec.start_time or (rec.create_timestamp.timestamp() if rec.create_timestamp else None)
        if start is None:
            continue
        age_min = int((now - start) / 60)
        if age_min * 60 < threshold_s:
            continue
        still_stuck.append(run.run_id)
        if run.run_id in already_alerted:
            continue  # already paged for this run; don't spam every tick
        capture_message(
            f"Dagster run stuck: {run.job_name} in {run.status.value} for {age_min}m "
            f"(run {run.run_id})",
            level="error",
            run_id=run.run_id,
            job=run.job_name or "unknown",
            status=run.status.value,
            stuck_minutes=str(age_min),
        )
        newly_alerted += 1

    # Keep only currently-still-stuck ids so a resolved run can re-alert if it
    # ever gets stuck again.
    context.update_cursor(json.dumps(still_stuck))
    return dg.SkipReason(
        f"{len(records)} in-progress; {len(still_stuck)} stuck > "
        f"{settings.stuck_run_alert_minutes}m, {newly_alerted} newly alerted"
    )


@dg.run_failure_sensor(name="run_failure_glitchtip_sensor")
def run_failure_glitchtip_sensor(context: dg.RunFailureSensorContext) -> None:
    """Report every run failure to GlitchTip with run id + job + reason."""
    run = context.dagster_run
    capture_message(
        f"Dagster run failed: {run.job_name} — {context.failure_event.message}",
        level="error",
        run_id=run.run_id,
        job=run.job_name or "unknown",
    )
