"""Per-job run-time caps (defs/run_limits.py) and their presence on every
sensor-driven job — a stranded drain must time out in minutes, not the 6 h
instance default."""

import pytest

from clear_pipeline.defs.analysis.automation import drain_analysis_automations_job
from clear_pipeline.defs.analysis.stages import drain_analysis_requests_job
from clear_pipeline.defs.ground.stages import ground_hotline_enrich_job
from clear_pipeline.defs.ground.transcribe import ground_transcribe_job
from clear_pipeline.defs.location_metadata.assets import (
    location_metadata_daily_job,
    location_metadata_monthly_job,
    location_metadata_usgs_job,
)
from clear_pipeline.defs.run_limits import MAX_RUNTIME_TAG, max_runtime_tags
from clear_pipeline.defs.signals import factory
from clear_pipeline.defs.signals.connectors import DataminrConnector
from clear_pipeline.defs.signals.stages import classify_group_job, translate_job
from clear_pipeline.defs.tasks.worker import drain_tasks_job


def test_tag_is_dagsters_run_monitoring_key():
    assert MAX_RUNTIME_TAG == "dagster/max_runtime"
    assert max_runtime_tags(900) == {"dagster/max_runtime": "900"}


def test_merges_over_existing_tags():
    assert max_runtime_tags(60, {"dagster/priority": "5"}) == {
        "dagster/priority": "5",
        "dagster/max_runtime": "60",
    }


def test_rejects_non_positive_caps():
    with pytest.raises(ValueError):
        max_runtime_tags(0)


@pytest.mark.parametrize(
    ("job", "cap"),
    [
        (translate_job, 1800),
        (classify_group_job, 10800),
        (ground_hotline_enrich_job, 900),
        (ground_transcribe_job, 900),
        (drain_tasks_job, 3600),
        (location_metadata_daily_job, 7200),
        (location_metadata_monthly_job, 7200),
        (location_metadata_usgs_job, 1800),
    ],
    ids=lambda v: getattr(v, "name", v),
)
def test_every_sensor_or_schedule_driven_job_is_capped(job, cap):
    assert job.tags[MAX_RUNTIME_TAG] == str(cap)


@pytest.mark.parametrize(
    "job",
    [drain_analysis_requests_job, drain_analysis_automations_job],
    ids=lambda v: getattr(v, "name", v),
)
def test_analysis_generation_jobs_are_not_auto_killed(job):
    # ADR-0012: these runs are dominated by legitimate, variable-length LLM
    # generation, so they are NOT auto-terminated by a tight per-job cap — they
    # run to the 6 h instance backstop and a long run is surfaced (not killed) by
    # stuck_run_monitor_sensor. They carry no per-job max_runtime tag.
    assert MAX_RUNTIME_TAG not in job.tags


def test_task_drain_keeps_its_queue_priority():
    assert "dagster/priority" in drain_tasks_job.tags


def test_per_source_ingest_jobs_are_capped_via_their_poll_sensor():
    # The ingest job is only reachable through its sensor (factory returns
    # [asset, sensor]), so check the cap on the sensor's target job.
    _asset, sensor = factory.build_source_assets(DataminrConnector())
    assert sensor.job.tags[MAX_RUNTIME_TAG] == "1800"
