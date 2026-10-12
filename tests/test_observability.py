"""GlitchTip/Sentry reporting must be a safe no-op without a DSN, and the
run-health sensors must be registered so a stuck/failed run pages next time."""

import clear_pipeline.definitions as definitions
from clear_pipeline.providers import observability


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
