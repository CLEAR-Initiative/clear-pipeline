"""GlitchTip/Sentry reporting must be a safe no-op without a DSN, and the
run-health sensors must be registered so a stuck/failed run pages next time."""

from datetime import UTC, datetime
from types import SimpleNamespace

import dagster as dg

import clear_pipeline.definitions as definitions
from clear_pipeline.defs import observability as observability_defs
from clear_pipeline.providers import observability

NOW = 1_000_000.0  # fixed "now" for deterministic ages
THRESHOLD_S = 30 * 60


def _record(run_id, *, started_min_ago=None, starting_min_ago=None, queued_min_ago=None, job="translate_job"):
    """A fake RunRecord. `started_min_ago` sets start_time (an executing run);
    `starting_min_ago` sets update_timestamp with no start_time (a run still
    launching); `queued_min_ago` sets create_timestamp (the long queue wait the
    sensor must ignore)."""
    start_time = NOW - started_min_ago * 60 if started_min_ago is not None else None
    update_ts = (
        datetime.fromtimestamp(NOW - starting_min_ago * 60, UTC) if starting_min_ago is not None else None
    )
    create_ts = (
        datetime.fromtimestamp(NOW - queued_min_ago * 60, UTC) if queued_min_ago is not None else None
    )
    status = dg.DagsterRunStatus.STARTED if started_min_ago is not None else dg.DagsterRunStatus.STARTING
    run = SimpleNamespace(run_id=run_id, job_name=job, status=status)
    return SimpleNamespace(
        dagster_run=run, start_time=start_time, update_timestamp=update_ts, create_timestamp=create_ts
    )


def test_init_sentry_is_noop_without_dsn(monkeypatch):
    monkeypatch.setattr(observability.settings, "sentry_dsn", "")
    monkeypatch.setattr(observability, "_initialized", False)
    # Must not raise, and must not create a sentry client.
    observability.init_sentry()
    assert observability._initialized is True


def test_capture_message_never_raises_without_init(monkeypatch):
    # Even if sentry isn't initialised, a capture call must be a quiet no-op.
    monkeypatch.setattr(observability, "_initialized", False)
    observability.capture_message("x", level="error", run_id="r1", job="translate_job")


def test_run_health_sensors_are_registered():
    defs = definitions.defs()
    names = {s.name for s in defs.sensors}
    assert "stuck_run_monitor_sensor" in names
    assert "run_failure_glitchtip_sensor" in names


def test_global_executor_is_in_process():
    # The OOM fix: one process per run (no per-step subprocess).
    defs = definitions.defs()
    assert "in_process" in defs.executor.name


def test_all_job_snapshots_build():
    # Building a job snapshot validates each job's default run config against
    # the active executor — the exact step that failed when a job still pinned
    # `execution.multiprocess` after the switch to the in_process executor
    # (in_process has no `multiprocess` config key). Guards against any job
    # shipping run config the global executor can't accept.
    defs = definitions.defs()
    jobs = defs.resolve_all_job_defs()
    assert jobs
    for job in jobs:
        job.get_job_snapshot_id()


def test_sensors_run_on_deploy():
    # Both health sensors must ship RUNNING, or no alert ever fires.
    defs = definitions.defs()
    by_name = {s.name: s for s in defs.sensors}
    for name in ("stuck_run_monitor_sensor", "run_failure_glitchtip_sensor"):
        assert by_name[name].default_status is dg.DefaultSensorStatus.RUNNING


def test_stuck_alerts_fire_only_past_the_threshold():
    records = [
        _record("fresh", started_min_ago=29),  # just under → no alert
        _record("stuck", started_min_ago=31),  # just over → alert
    ]
    alerts, still_stuck = observability_defs.stuck_run_alerts(records, NOW, THRESHOLD_S, set())
    assert still_stuck == ["stuck"]
    assert [a["tags"]["run_id"] for a in alerts] == ["stuck"]
    tags = alerts[0]["tags"]
    assert tags == {"run_id": "stuck", "job": "translate_job", "status": "STARTED", "stuck_minutes": "31"}
    assert "stuck" in alerts[0]["message"] and "31m" in alerts[0]["message"]


def test_stuck_alerts_dedupe_on_repeated_ticks():
    records = [_record("stuck", started_min_ago=45)]
    # First tick alerts and records the run in the cursor.
    alerts1, cursor1 = observability_defs.stuck_run_alerts(records, NOW, THRESHOLD_S, set())
    assert [a["tags"]["run_id"] for a in alerts1] == ["stuck"]
    # Second tick with that cursor: still stuck, but no new alert (no spam).
    alerts2, cursor2 = observability_defs.stuck_run_alerts(records, NOW, THRESHOLD_S, set(cursor1))
    assert alerts2 == []
    assert cursor2 == ["stuck"]


def test_cleared_runs_drop_out_of_the_cursor():
    # A run previously alerted that is no longer in-progress isn't in `records`,
    # so it must drop from the cursor — letting it alert again if it re-sticks.
    records = [_record("stuck", started_min_ago=45)]
    _, still_stuck = observability_defs.stuck_run_alerts(records, NOW, THRESHOLD_S, {"gone", "stuck"})
    assert "gone" not in still_stuck
    assert still_stuck == ["stuck"]


def test_queue_wait_is_not_counted_as_stuck():
    # A run that sat QUEUED for an hour and only just transitioned to STARTING
    # must not alert: age is measured from the STARTING transition, not creation.
    records = [_record("draining", starting_min_ago=2, queued_min_ago=90)]
    alerts, still_stuck = observability_defs.stuck_run_alerts(records, NOW, THRESHOLD_S, set())
    assert alerts == []
    assert still_stuck == []


def test_a_genuinely_stalled_launch_still_alerts():
    # But a run stuck IN STARTING past the threshold (no start_time) does alert.
    records = [_record("stalled_launch", starting_min_ago=40)]
    alerts, still_stuck = observability_defs.stuck_run_alerts(records, NOW, THRESHOLD_S, set())
    assert [a["tags"]["run_id"] for a in alerts] == ["stalled_launch"]
    assert alerts[0]["tags"]["status"] == "STARTING"
    assert still_stuck == ["stalled_launch"]


def test_run_failure_alert_message_and_tags():
    run = SimpleNamespace(run_id="r-42", job_name="reliefweb_weekly_knowledgebase")
    alert = observability_defs.run_failure_alert(run, "step crashed: OOM")
    assert alert["tags"] == {"run_id": "r-42", "job": "reliefweb_weekly_knowledgebase"}
    assert alert["message"] == "Dagster run failed: reliefweb_weekly_knowledgebase — step crashed: OOM"


def test_run_failure_alert_falls_back_when_job_name_missing():
    run = SimpleNamespace(run_id="r-1", job_name=None)
    alert = observability_defs.run_failure_alert(run, "boom")
    assert alert["tags"]["job"] == "unknown"
