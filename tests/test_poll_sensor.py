"""build_poll_sensor: one waiting run per job (#1388).

The polling sensors used to launch a run every interval whatever had become of
the last one. When prod fell behind in November 2026 they kept adding a run a
minute per sensor to a queue that was not moving: 19,500 queued runs, each a
no-op by the time it started, and a run coordinator that slowed with every one.

A due poll now launches nothing while a run of the same job is still waiting to
start. A run that has started does not block a new one.
"""

import json
import time

import dagster as dg
import pytest
from dagster._core.remote_origin import (
    InProcessCodeLocationOrigin,
    RemoteJobOrigin,
    RemoteRepositoryOrigin,
)
from dagster._core.test_utils import create_run_for_test as _create_run_for_test
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin

from clear_pipeline.defs.signals import poll_sensor as ps
from clear_pipeline.defs.signals.poll_sensor import build_poll_sensor


@dg.asset
def _thing():
    return 1


@dg.asset
def _other_thing():
    return 1


JOB = dg.define_asset_job(name="drain_job", selection=[_thing])
OTHER_JOB = dg.define_asset_job(name="other_job", selection=[_other_thing])


def create_run_for_test(instance, *, job_name, status):
    """A run of ``job_name`` in ``status``. Dagster refuses a QUEUED run without
    the origin the daemon would launch it from, so every run gets one."""
    origin = RemoteJobOrigin(
        repository_origin=RemoteRepositoryOrigin(
            code_location_origin=InProcessCodeLocationOrigin(LoadableTargetOrigin(python_file=__file__)),
            repository_name="__repository__",
        ),
        job_name=job_name,
    )
    return _create_run_for_test(instance, job_name=job_name, status=status, remote_job_origin=origin)


def _sensor(**kwargs):
    return build_poll_sensor(name="drain_sensor", job=JOB, default_interval_minutes=1, **kwargs)


def _tick(sensor, instance, cursor=None):
    """Evaluate the sensor once. Returns (run requests, skip message, cursor)."""
    context = dg.build_sensor_context(instance=instance, cursor=cursor)
    result = sensor.evaluate_tick(context)
    skip = result.skip_message
    return list(result.run_requests or []), skip, context.cursor


@pytest.fixture
def instance():
    with dg.instance_for_test() as inst:
        yield inst


def test_due_poll_launches_when_nothing_is_waiting(instance):
    requests, skip, cursor = _tick(_sensor(), instance)

    assert len(requests) == 1 and skip is None
    assert json.loads(cursor)["last_run"] == pytest.approx(time.time(), abs=5)


@pytest.mark.parametrize("status", ps._WAITING_STATUSES)
def test_due_poll_is_skipped_while_a_run_of_the_job_waits(instance, status):
    run = create_run_for_test(instance, job_name="drain_job", status=status)
    before = json.dumps({"interval_minutes": 1, "last_run": 0.0})

    requests, skip, cursor = _tick(_sensor(), instance, cursor=before)

    assert requests == []
    assert run.run_id[:8] in skip and "drain_job" in skip and "not queueing another" in skip
    # The cursor is untouched, so the poll is still due on the next tick.
    assert cursor == before


def test_the_queue_holds_one_run_per_job_however_long_it_is_stuck(instance):
    # What prod did: the queue stops moving and the sensor keeps ticking.
    sensor = _sensor()
    create_run_for_test(instance, job_name="drain_job", status=dg.DagsterRunStatus.QUEUED)

    launched = 0
    cursor = None
    for _ in range(50):
        requests, _skip, cursor = _tick(sensor, instance, cursor=cursor)
        launched += len(requests)

    assert launched == 0
    assert len(instance.get_runs(dg.RunsFilter(job_name="drain_job"))) == 1


def test_polling_resumes_on_the_first_tick_after_the_waiting_run_starts(instance):
    sensor = _sensor()
    run = create_run_for_test(instance, job_name="drain_job", status=dg.DagsterRunStatus.QUEUED)
    requests, _skip, cursor = _tick(sensor, instance)
    assert requests == []

    instance.delete_run(run.run_id)
    create_run_for_test(instance, job_name="drain_job", status=dg.DagsterRunStatus.STARTED)

    requests, skip, _cursor = _tick(sensor, instance, cursor=cursor)
    assert len(requests) == 1 and skip is None


@pytest.mark.parametrize("status", [
    dg.DagsterRunStatus.STARTED,   # busy for minutes: a fresh run serves new requests
    dg.DagsterRunStatus.SUCCESS,
    dg.DagsterRunStatus.FAILURE,
    dg.DagsterRunStatus.CANCELED,
])
def test_a_run_that_has_started_or_ended_does_not_block_a_new_one(instance, status):
    create_run_for_test(instance, job_name="drain_job", status=status)

    requests, skip, _cursor = _tick(_sensor(), instance)

    assert len(requests) == 1 and skip is None


def test_a_waiting_run_of_another_job_does_not_block(instance):
    create_run_for_test(instance, job_name="other_job", status=dg.DagsterRunStatus.QUEUED)

    requests, _skip, _cursor = _tick(_sensor(), instance)

    assert len(requests) == 1


def test_not_due_is_reported_before_the_queue_is_looked_at(instance):
    create_run_for_test(instance, job_name="drain_job", status=dg.DagsterRunStatus.QUEUED)
    recent = json.dumps({"interval_minutes": 5, "last_run": time.time()})

    requests, skip, cursor = _tick(_sensor(), instance, cursor=recent)

    assert requests == [] and "until next poll (interval=5m)" in skip
    assert cursor == recent


def test_skip_when_still_wins(instance):
    requests, skip, cursor = _tick(_sensor(skip_when=lambda: "nothing registered"), instance)

    assert requests == [] and skip == "nothing registered" and cursor is None


def test_every_polling_sensor_in_the_code_location_has_the_check():
    # One shared builder: a sensor built any other way would bring the pile-up back.
    from clear_pipeline.definitions import defs

    loaded = defs()
    polling = [
        s for s in loaded.sensors
        if s.name.endswith(("_poll_sensor", "_drain_sensor", "_request_sensor", "_automation_sensor",
                            "_enrich_sensor", "_transcribe_sensor", "_worker_sensor"))
        and s.name != "default_automation_condition_sensor"
    ]
    assert len(polling) >= 9
    with dg.instance_for_test() as inst:
        for sensor in polling:
            create_run_for_test(inst, job_name=sensor.job_name, status=dg.DagsterRunStatus.QUEUED)
            context = dg.build_sensor_context(
                instance=inst, cursor=json.dumps({"interval_minutes": 1, "last_run": 0.0}),
            )
            result = sensor.evaluate_tick(context)
            assert not result.run_requests, sensor.name
            if sensor.name == "task_worker_sensor":
                continue  # its own skip_when (no Task kind registered) answers first
            assert "not queueing another" in (result.skip_message or ""), (sensor.name, result.skip_message)
