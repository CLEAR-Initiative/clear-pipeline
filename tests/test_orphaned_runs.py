"""Orphaned-run reaper (defs/orphaned_runs.py): fails STARTED runs whose worker
died with a previous dagster-code container."""

import os
import time
from datetime import UTC, datetime

import dagster as dg
import pytest
from dagster._core.storage.dagster_run import RunRecord

from clear_pipeline.defs import orphaned_runs as reaper

STARTED = dg.DagsterRunStatus.STARTED
STARTING = dg.DagsterRunStatus.STARTING


def _record(status, *, start_time=None, created=1_000.0, run_id="run-1") -> RunRecord:
    run = dg.DagsterRun(job_name="translate_job", run_id=run_id, status=status)
    ts = datetime.fromtimestamp(created, tz=UTC)
    return RunRecord(
        storage_id=1, dagster_run=run, create_timestamp=ts, update_timestamp=ts, start_time=start_time
    )


class TestOrphanedRuns:
    def test_started_before_the_container_is_orphaned(self):
        old = _record(STARTED, start_time=1_500.0)
        assert reaper.orphaned_runs([old], container_started=2_000.0) == [old]

    def test_started_after_the_container_is_alive(self):
        live = _record(STARTED, start_time=2_500.0)
        assert reaper.orphaned_runs([live], container_started=2_000.0) == []

    def test_only_started_runs_are_judged(self):
        # STARTING is the daemon's start_timeout's problem; SUCCESS is finished.
        rows = [_record(STARTING, start_time=1_500.0), _record(dg.DagsterRunStatus.SUCCESS, start_time=1_500.0)]
        assert reaper.orphaned_runs(rows, container_started=2_000.0) == []

    def test_falls_back_to_create_time_without_a_start_time(self):
        no_start = _record(STARTED, start_time=None, created=1_500.0)
        assert reaper.orphaned_runs([no_start], container_started=2_000.0) == [no_start]
        assert reaper.orphaned_runs([no_start], container_started=1_000.0) == []

    def test_message_names_both_timestamps_and_the_sensor(self):
        msg = reaper.failure_message(_record(STARTED, start_time=1_500.0), 2_000.0)
        assert "1970-01-01T00:25:00+00:00" in msg
        assert "1970-01-01T00:33:20+00:00" in msg
        assert reaper.SENSOR_NAME in msg


class TestContainerStartTime:
    def test_reads_pid1_start_from_proc(self, tmp_path):
        (tmp_path / "1").mkdir()
        # `comm` with spaces, as `dagster api grpc` shows up; field 22 = 12345 ticks.
        (tmp_path / "1" / "stat").write_text(
            "1 (dagster api grpc) S 0 1 1 0 -1 4194560 100 0 0 0 5 3 0 0 20 0 1 0 12345 1000 200 "
            "18446744073709551615 1 1 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
        )
        (tmp_path / "stat").write_text("cpu  1 2 3 4\nbtime 1700000000\nprocesses 10\n")
        expected = 1700000000 + 12345 / os.sysconf("SC_CLK_TCK")
        assert reaper.container_start_time(str(tmp_path)) == pytest.approx(expected)

    def test_none_without_proc(self, tmp_path):
        assert reaper.container_start_time(str(tmp_path / "missing")) is None

    def test_none_on_garbage(self, tmp_path):
        (tmp_path / "1").mkdir()
        (tmp_path / "1" / "stat").write_text("not a stat line\n")
        (tmp_path / "stat").write_text("btime nope\n")
        assert reaper.container_start_time(str(tmp_path)) is None


@dg.op
def _noop():
    pass


@dg.job
def _probe_job():
    _noop()


class TestReapOnInstance:
    def test_fails_only_runs_older_than_the_container(self):
        with dg.instance_for_test() as instance:
            stranded = instance.create_run_for_job(_probe_job, status=STARTED)
            finished = instance.create_run_for_job(_probe_job, status=dg.DagsterRunStatus.SUCCESS)
            # A container that came up *after* these runs were created.
            failed = reaper.reap_orphaned_runs(instance, container_started=time.time() + 60)

            assert failed == [stranded.run_id]
            assert instance.get_run_by_id(stranded.run_id).status == dg.DagsterRunStatus.FAILURE
            assert instance.get_run_by_id(finished.run_id).status == dg.DagsterRunStatus.SUCCESS
            messages = [e.message for e in instance.all_logs(stranded.run_id)]
            assert any(reaper.SENSOR_NAME in m for m in messages)

    def test_leaves_runs_started_in_this_container(self):
        with dg.instance_for_test() as instance:
            live = instance.create_run_for_job(_probe_job, status=STARTED)
            failed = reaper.reap_orphaned_runs(instance, container_started=time.time() - 60)
            assert failed == []
            assert instance.get_run_by_id(live.run_id).status == STARTED


class TestSensor:
    def _tick(self, instance):
        return reaper.orphaned_run_reaper_sensor(dg.build_sensor_context(instance=instance))

    def test_skips_outside_a_linux_container(self, monkeypatch):
        monkeypatch.setattr(reaper, "container_start_time", lambda: None)
        with dg.instance_for_test() as instance:
            tick = self._tick(instance)
        assert isinstance(tick, dg.SkipReason)
        assert "/proc" in tick.skip_message

    def test_reaps_and_reports_the_count(self, monkeypatch):
        monkeypatch.setattr(reaper, "container_start_time", lambda: time.time() + 60)
        with dg.instance_for_test() as instance:
            stranded = instance.create_run_for_job(_probe_job, status=STARTED)
            tick = self._tick(instance)
            assert instance.get_run_by_id(stranded.run_id).status == dg.DagsterRunStatus.FAILURE
        assert "1 orphaned run(s) failed" in tick.skip_message

    def test_ships_running_and_ticks_every_minute(self):
        assert reaper.orphaned_run_reaper_sensor.default_status == dg.DefaultSensorStatus.RUNNING
        assert reaper.orphaned_run_reaper_sensor.minimum_interval_seconds == 60
