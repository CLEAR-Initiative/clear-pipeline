"""The analysis module is its OWN automation, decoupled from the weekly KB
pipeline (PR-9): analysis_sub has no KB dependency, and analysis_job /
analysis_schedule are standalone."""

from datetime import datetime, timezone

import dagster as dg

from clear_pipeline.defs.analysis.assets_descriptive import analysis_sub
from clear_pipeline.defs.analysis.jobs import ANALYSIS_ASSET_KEYS, analysis_job
from clear_pipeline.defs.analysis.schedules import analysis_schedule, build_run_requests


def test_analysis_sub_has_no_kb_dependency():
    # Root of the analysis subgraph — not chained to the KB pipeline.
    dep_strings = {k.to_user_string() for k in analysis_sub.dependency_keys}
    assert not any("kb" in d for d in dep_strings), dep_strings
    assert dep_strings == set()  # no upstream asset deps at all


def test_job_selects_only_the_new_dag_assets():
    assert analysis_job.name == "analysis_job"
    assert set(ANALYSIS_ASSET_KEYS) == {
        "analysis_sub", "analysis_numbers_extract", "analysis_gaps",
        "analysis_evaluation", "analysis_web_gapfill", "analysis_overview",
    }
    # the queue drains are NOT part of this scheduled job
    assert "drain_analysis_requests" not in ANALYSIS_ASSET_KEYS
    assert "drain_analysis_automations" not in ANALYSIS_ASSET_KEYS


def test_schedule_is_standalone():
    # distinct schedule, own cron (not the KB Tuesday), default stopped (opt-in)
    assert analysis_schedule.name == "analysis_schedule"
    assert analysis_schedule.cron_schedule == "0 6 * * WED"
    assert analysis_schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert analysis_schedule.job_name == "analysis_job"


def test_fan_out_one_run_per_partition():
    at = datetime(2026, 11, 4, 6, 0, tzinfo=timezone.utc)
    reqs = build_run_requests(["SDN", "ETH"], at)
    assert {r.partition_key for r in reqs} == {"SDN", "ETH"}
    assert all(r.run_key.startswith("analysis-") for r in reqs)
    # run_key is per-country-per-week (dedupes a re-tick in the same week)
    assert reqs[0].run_key == f"analysis-SDN-{reqs[0].run_key.split('-', 2)[2]}"
    assert build_run_requests([], at) == []
