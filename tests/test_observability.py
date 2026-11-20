"""GlitchTip/Sentry reporting must be a safe no-op without a DSN, and the
run-health sensors must be registered so a stuck/failed run pages next time."""

import http.server
import threading
from datetime import UTC, datetime
from types import SimpleNamespace

import dagster as dg
import sentry_sdk
import sentry_sdk.transport

import clear_pipeline.definitions as definitions
from clear_pipeline.defs import observability as observability_defs
from clear_pipeline.providers import idmc, metrics, observability

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
    assert {"run_success_metrics_sensor", "run_failure_metrics_sensor"} <= names


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


def _meta(**overrides):
    meta = {
        "run_id": "e209a4cf-7912-4877-b4e8-93b9e18cf29b",
        "job_name": "iom_dtm_flash_alerts_gx",
        "step_key": "iom_dtm_flash_alerts_silver",
        "resource_name": None,
        "dagster_event": SimpleNamespace(event_type=dg.DagsterEventType.STEP_FAILURE),
        "orig_message": 'Execution of step "iom_dtm_flash_alerts_silver" failed.',
    }
    meta.update(overrides)
    return meta


def test_dagster_log_fingerprint_ignores_the_run_id():
    a = observability.dagster_log_fingerprint(_meta())
    b = observability.dagster_log_fingerprint(_meta(run_id="2efaa9b2-0383-4bda-a790-dd7e8e6bc9ed"))
    assert a == b
    assert "e209a4cf" not in "".join(a)
    assert a[1:4] == ["job=iom_dtm_flash_alerts_gx|", "step=iom_dtm_flash_alerts_silver|", "event=STEP_FAILURE|"]


def test_dagster_log_fingerprint_strips_ids_from_the_message_and_splits_by_step():
    a = observability.dagster_log_fingerprint(_meta(orig_message="Run 42064586-759f-4ed8-a1ba-b10dc4bfdea8 wrote 41 rows"))
    b = observability.dagster_log_fingerprint(_meta(orig_message="Run 9e59b22c-f8de-4d41-9938-0f7789c33025 wrote 41 rows"))
    assert a == b
    other_step = observability.dagster_log_fingerprint(_meta(step_key="iom_dtm_flash_alerts_push"))
    assert other_step != a


def test_non_dagster_events_pass_through_unchanged():
    event = {"message": "plain"}
    assert observability.group_dagster_log_events(event, {}) == {"message": "plain"}
    record = SimpleNamespace()  # a log record with no dagster_meta
    assert "fingerprint" not in observability.group_dagster_log_events({"m": 1}, {"log_record": record})


def test_dagster_log_events_get_a_fingerprint_and_tags():
    record = SimpleNamespace(dagster_meta=_meta())
    event = observability.group_dagster_log_events({"message": "x"}, {"log_record": record})
    assert event["fingerprint"][0] == "dagster-log|"
    assert event["tags"] == {
        "job": "iom_dtm_flash_alerts_gx",
        "step": "iom_dtm_flash_alerts_silver",
        "run_id": "e209a4cf-7912-4877-b4e8-93b9e18cf29b",
    }


def test_run_failure_alert_is_grouped_per_job():
    run = SimpleNamespace(run_id="r-42", job_name="idmc_gx")
    assert observability_defs.run_failure_alert(run, "a")["fingerprint"] == ["dagster-run-failed|", "idmc_gx"]


def _finished(run_id, failed):
    status = dg.DagsterRunStatus.FAILURE if failed else dg.DagsterRunStatus.SUCCESS
    return SimpleNamespace(dagster_run=SimpleNamespace(run_id=run_id, status=status))


def test_failure_streak_counts_leading_failures_newest_first():
    records = [_finished("r3", True), _finished("r2", True), _finished("r1", False), _finished("r0", True)]
    assert observability_defs.failure_streak(records) == (2, "r2")
    assert observability_defs.failure_streak([_finished("r1", True)]) == (1, "r1")
    assert observability_defs.failure_streak([]) == (0, None)


def test_failure_streak_past_the_lookback_has_no_start(monkeypatch):
    monkeypatch.setattr(observability_defs, "FAILURE_STREAK_LOOKBACK", 3)
    records = [_finished(f"r{i}", True) for i in range(3)]
    assert observability_defs.failure_streak(records) == (3, None)


def test_repeated_failure_alert_fires_from_the_second_failure_once_per_streak():
    run = SimpleNamespace(run_id="r3", job_name="idmc_gx")
    assert observability_defs.repeated_failure_alert(run, 1, "r3", "boom") is None
    second = observability_defs.repeated_failure_alert(run, 2, "r2", "boom")
    third = observability_defs.repeated_failure_alert(SimpleNamespace(run_id="r4", job_name="idmc_gx"), 3, "r2", "boom")
    assert second["fingerprint"] == third["fingerprint"] == ["dagster-repeated-failure|", "idmc_gx", "|", "r2"]
    assert second["message"].startswith("Dagster job failing repeatedly: idmc_gx failed its last 2 runs")
    assert second["tags"]["failure_streak"] == "2"
    # Past the lookback the start is unknown: no new fingerprint, so no second issue.
    assert observability_defs.repeated_failure_alert(run, 50, None, "boom") is None


def test_failure_streak_counts_from_the_triggering_run_not_a_later_success():
    # r3 and r2 failed, then r4 succeeded before the daemon handled r3's failure.
    records = [_finished("r4", False), _finished("r3", True), _finished("r2", True), _finished("r1", False)]
    assert observability_defs.failure_streak(records, "r3") == (2, "r2")
    assert observability_defs.failure_streak(records) == (0, None)
    assert observability_defs.failure_streak(records, "unknown") == (0, None)


def test_long_streak_keeps_one_issue(monkeypatch):
    monkeypatch.setattr(observability_defs, "FAILURE_STREAK_LOOKBACK", 3)
    run = SimpleNamespace(run_id="r2", job_name="idmc_gx")
    fingerprints = set()
    for n in range(1, 6):  # streak grows 1..5 with a lookback of 3
        records = [_finished(f"r{i}", True) for i in range(n, 0, -1)]
        streak, start = observability_defs.failure_streak(records, f"r{n}")
        alert = observability_defs.repeated_failure_alert(run, streak, start, "boom")
        if alert:
            fingerprints.add(tuple(alert["fingerprint"]))
    assert fingerprints == {("dagster-repeated-failure|", "idmc_gx", "|", "r1")}


def test_run_failure_sensor_alerts_on_a_streak_a_later_success_came_after(monkeypatch):
    sent = []
    monkeypatch.setattr(
        observability_defs, "capture_message", lambda msg, **kw: sent.append((msg, kw.get("level"), kw.get("fingerprint")))
    )
    run = SimpleNamespace(run_id="r3", job_name="idmc_gx")
    records = [_finished("r4", False), _finished("r3", True), _finished("r2", True), _finished("r1", False)]
    instance = SimpleNamespace(get_run_records=lambda **kw: records)
    context = SimpleNamespace(dagster_run=run, failure_event=SimpleNamespace(message="boom"), instance=instance)
    observability_defs.run_failure_glitchtip_sensor._run_status_sensor_fn(context)
    assert [level for _, level, _ in sent] == ["error", "fatal"]
    assert sent[1][2] == ["dagster-repeated-failure|", "idmc_gx", "|", "r2"]


def test_run_failure_sensor_sends_the_repeated_alert_when_the_last_run_failed(monkeypatch):
    sent = []
    monkeypatch.setattr(
        observability_defs, "capture_message", lambda msg, **kw: sent.append((msg, kw.get("level"), kw.get("fingerprint")))
    )
    run = SimpleNamespace(run_id="r3", job_name="idmc_gx")
    records = [_finished("r3", True), _finished("r2", True), _finished("r1", False)]
    instance = SimpleNamespace(get_run_records=lambda **kw: records)
    context = SimpleNamespace(dagster_run=run, failure_event=SimpleNamespace(message="boom"), instance=instance)
    observability_defs.run_failure_glitchtip_sensor._run_status_sensor_fn(context)
    assert [level for _, level, _ in sent] == ["error", "fatal"]
    assert sent[1][2] == ["dagster-repeated-failure|", "idmc_gx", "|", "r2"]


class _CapturingTransport(sentry_sdk.transport.Transport):
    def __init__(self, options=None):
        super().__init__(options)
        self.envelopes = []

    def capture_envelope(self, envelope):
        self.envelopes.append(envelope.serialize().decode())


class _Forbidden(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(403)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


def test_glitchtip_events_never_carry_the_idmc_client_id(monkeypatch):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Forbidden)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    transport = _CapturingTransport()
    real_init = sentry_sdk.init
    monkeypatch.setattr(sentry_sdk, "init", lambda **kw: real_init(transport=transport, **kw))
    monkeypatch.setattr(observability.settings, "sentry_dsn", "http://key@127.0.0.1:1/1")
    monkeypatch.setattr(observability, "_initialized", False)
    monkeypatch.setattr(idmc.settings, "idmc_client_id", "s3cret")
    monkeypatch.setattr(idmc, "IDU_URL", f"http://127.0.0.1:{server.server_address[1]}/idus/")
    try:
        observability.init_sentry()
        assert idmc._fetch_all() == []
        sentry_sdk.flush()
    finally:
        server.shutdown()
        real_init()  # detach the capturing client

    assert any("API request failed" in e for e in transport.envelopes)
    assert not any("s3cret" in e for e in transport.envelopes)


def _check_event(asset, check, passed, severity=dg.AssetCheckSeverity.WARN):
    data = SimpleNamespace(asset_key=dg.AssetKey(asset), check_name=check, passed=passed, severity=severity)
    ev = SimpleNamespace(event_type=dg.DagsterEventType.ASSET_CHECK_EVALUATION, event_specific_data=data)
    return SimpleNamespace(dagster_event=ev)


def test_check_outcomes_extracts_gx_gates_and_skips_other_events():
    other = SimpleNamespace(dagster_event=SimpleNamespace(event_type=dg.DagsterEventType.STEP_SUCCESS))
    events = [_check_event(["acled", "bronze"], "bronze_shape", False, dg.AssetCheckSeverity.ERROR), other]
    out = observability_defs.check_outcomes(events + [SimpleNamespace(dagster_event=None)])
    assert out == [metrics.CheckOutcome("acled/bronze", "bronze_shape", False, "ERROR")]


def test_build_registry_exposes_run_and_gate_gauges():
    reg = metrics.build_registry("acled_job", False, 12.5, [metrics.CheckOutcome("a/b", "c", True, "WARN")])
    labels = {"job": "acled_job", "service": "clear-context-pipeline"}
    assert reg.get_sample_value("dagster_run_last_success", labels) == 0
    assert reg.get_sample_value("dagster_run_last_duration_seconds", labels) == 12.5
    gate = {**labels, "asset": "a/b", "check": "c", "severity": "WARN"}
    assert reg.get_sample_value("dagster_asset_check_last_passed", gate) == 1


def test_registry_uses_run_end_time_not_now():
    reg = metrics.build_registry("j", True, 1.0, [], finished_at=123.0)
    labels = {"job": "j", "service": "clear-context-pipeline"}
    assert reg.get_sample_value("dagster_run_last_timestamp_seconds", labels) == 123.0


def test_push_sends_job_and_registry_when_configured_and_nothing_when_not(monkeypatch):
    calls = []
    monkeypatch.setattr("prometheus_client.push_to_gateway", lambda *a, **k: calls.append((a, k)))
    monkeypatch.setattr(metrics.settings, "pushgateway_url", "")
    metrics.push_run_metrics("j", True, 1.0)
    assert calls == []
    monkeypatch.setattr(metrics.settings, "pushgateway_url", "http://pg:9091")
    metrics.push_run_metrics("j", False, 2.0, finished_at=5.0)
    (args, kwargs), = calls
    assert args == ("http://pg:9091",) and kwargs["job"] == "j"
    labels = {"job": "j", "service": "clear-context-pipeline"}
    assert kwargs["registry"].get_sample_value("dagster_run_last_success", labels) == 0


def test_push_failure_is_swallowed(monkeypatch):
    def boom(*a, **k):
        raise OSError("down")

    monkeypatch.setattr("prometheus_client.push_to_gateway", boom)
    monkeypatch.setattr(metrics.settings, "pushgateway_url", "http://pg:9091")
    metrics.push_run_metrics("j", True, 1.0)  # logged, not raised


def test_older_success_does_not_replace_newer_failure():
    newer = SimpleNamespace(dagster_run=SimpleNamespace(run_id="new"))
    assert not observability_defs.is_latest_finished([newer], "old")
    assert observability_defs.is_latest_finished([newer], "new")
    assert observability_defs.is_latest_finished([], "any")


def test_oldest_run_age_is_the_longest_in_progress_run_or_zero():
    records = [_record("a", started_min_ago=5), _record("b", started_min_ago=90), _record("c", starting_min_ago=1)]
    assert observability_defs.oldest_run_age_s(records, NOW) == 90 * 60
    assert observability_defs.oldest_run_age_s([], NOW) == 0.0


def test_heartbeat_registry_has_liveness_gauges():
    reg = metrics.build_heartbeat_registry(2, 600.0, now=99.0)
    labels = {"service": "clear-context-pipeline"}
    assert reg.get_sample_value("dagster_heartbeat_timestamp_seconds", labels) == 99.0
    assert reg.get_sample_value("dagster_running_runs", labels) == 2
    assert reg.get_sample_value("dagster_oldest_running_run_age_seconds", labels) == 600.0


def test_heartbeat_pushes_under_its_own_job(monkeypatch):
    calls = []
    monkeypatch.setattr("prometheus_client.push_to_gateway", lambda *a, **k: calls.append((a, k)))
    monkeypatch.setattr(metrics.settings, "pushgateway_url", "http://pg:9091")
    metrics.push_heartbeat(0, 0.0)
    (args, kwargs), = calls
    assert kwargs["job"] == "dagster_heartbeat"


def test_stuck_run_sensor_pushes_a_heartbeat_every_tick(monkeypatch):
    beats = []
    monkeypatch.setattr(observability_defs, "push_heartbeat", lambda running, age: beats.append((running, age)))
    instance = dg.DagsterInstance.ephemeral()
    ctx = dg.build_sensor_context(instance=instance)
    observability_defs.stuck_run_monitor_sensor(ctx)
    assert beats == [(0, 0.0)]  # idle: still reports, so a silent daemon is detectable


def test_dagster_log_lines_reach_glitchtip_with_the_job_step_fingerprint(monkeypatch):
    import json
    import logging

    transport = _CapturingTransport()
    real_init = sentry_sdk.init
    monkeypatch.setattr(sentry_sdk, "init", lambda **kw: real_init(transport=transport, **kw))
    monkeypatch.setattr(observability.settings, "sentry_dsn", "http://key@127.0.0.1:1/1")
    monkeypatch.setattr(observability, "_initialized", False)
    try:
        observability.init_sentry()
        logging.getLogger("dagster").error("iom_dtm_flash_alerts_gx - e209a4cf - silver failed", extra={"dagster_meta": _meta()})
        sentry_sdk.flush()
    finally:
        real_init()

    events = [json.loads(line) for env in transport.envelopes for line in env.splitlines() if '"fingerprint"' in line]
    assert events, transport.envelopes
    assert events[0]["fingerprint"][:2] == ["dagster-log|", "job=iom_dtm_flash_alerts_gx|"]
    assert events[0]["tags"]["run_id"] == "e209a4cf-7912-4877-b4e8-93b9e18cf29b"


def test_delayed_callback_on_a_cut_off_history_opens_no_second_issue(monkeypatch):
    # 100 failures in a row (r100 newest). The query returns only the 70 newest
    # (r100..r31). A delayed callback for r70 sees r70..r31, all failures, and the
    # history is cut off: the streak's start is unknown, so no new fingerprint.
    monkeypatch.setattr(observability_defs, "FAILURE_STREAK_LOOKBACK", 50)
    records = [_finished(f"r{i}", True) for i in range(100, 30, -1)]
    assert len(records) == 70
    assert observability_defs.failure_streak(records, "r70", 70) == (40, None)
    run = SimpleNamespace(run_id="r70", job_name="idmc_gx")
    assert observability_defs.repeated_failure_alert(run, 40, None, "boom") is None


def test_a_short_history_is_not_cut_off():
    # Fewer rows than the query limit: the oldest run is the job's first, so the
    # streak's start is known.
    records = [_finished("r3", True), _finished("r2", True)]
    assert observability_defs.failure_streak(records, "r3", 70) == (2, "r2")


def test_a_streak_that_ends_inside_a_full_history_keeps_its_start():
    records = [_finished(f"r{i}", True) for i in range(70, 60, -1)] + [
        _finished(f"r{i}", False) for i in range(60, 0, -1)
    ]
    assert len(records) == 70
    assert observability_defs.failure_streak(records, "r70", 70) == (10, "r61")


def test_run_failure_sensor_sends_no_repeated_alert_for_a_delayed_callback_on_a_cut_off_history(monkeypatch):
    sent = []
    monkeypatch.setattr(
        observability_defs, "capture_message", lambda msg, **kw: sent.append((msg, kw.get("level"), kw.get("fingerprint")))
    )
    fetch = getattr(observability_defs, "FAILURE_STREAK_FETCH", observability_defs.FAILURE_STREAK_LOOKBACK + 20)
    # The job has failed far longer than the query reaches; r70 is handled late.
    records = [_finished(f"r{i}", True) for i in range(fetch + 30, 30, -1)][:fetch]
    run = SimpleNamespace(run_id="r70", job_name="idmc_gx")
    instance = SimpleNamespace(get_run_records=lambda **kw: records)
    context = SimpleNamespace(dagster_run=run, failure_event=SimpleNamespace(message="boom"), instance=instance)
    observability_defs.run_failure_glitchtip_sensor._run_status_sensor_fn(context)
    assert [level for _, level, _ in sent] == ["error"]
