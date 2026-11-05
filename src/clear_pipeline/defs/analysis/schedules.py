"""Schedule for the analysis module — its OWN automation.

Independent of the weekly KB fetch schedule (Tuesday): the analysis DAG runs on
its own cadence and fans out one run per live country partition. Shares only the
country partition keyspace (the same countries, reconciled centrally by the KB
partition sensor) — that is data sharing, not a pipeline join.

Defaults to STOPPED: it is opt-in until the operator has applied the
``analysis_datapoints`` migration (clear-api) and provisioned
``BRAVE_SEARCH_API_KEY`` (for the conditional web gap-fill). Enable it in the
Dagster UI (or set the default) once those are in place.
"""

from datetime import datetime

import dagster as dg

from clear_pipeline.defs.analysis.jobs import analysis_job
from clear_pipeline.defs.reliefweb_partitions import country_partitions


def _week_tag(at: datetime) -> str:
    iso_year, iso_week, _ = at.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def build_run_requests(iso3s, at: datetime) -> list[dg.RunRequest]:
    """One RunRequest per country partition, deduped per-country-per-week
    (pure, so the fan-out is testable without a Dagster instance)."""
    week = _week_tag(at)
    return [
        dg.RunRequest(partition_key=iso3, run_key=f"analysis-{iso3}-{week}")
        for iso3 in iso3s
    ]


@dg.schedule(
    name="analysis_schedule",
    job=analysis_job,
    # Wednesday 06:00 UTC — staggered after the KB fetch (Tuesday) so a fresh
    # week of KB is settled, but a SEPARATE automation (not chained to it).
    cron_schedule="0 6 * * WED",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def analysis_schedule(context: dg.ScheduleEvaluationContext):
    iso3s = context.instance.get_dynamic_partitions(country_partitions.name)
    if not iso3s:
        return dg.SkipReason(
            "no country partitions registered yet — the partition sensor seeds "
            "them from clear-api pipelineCountries",
        )
    return build_run_requests(iso3s, context.scheduled_execution_time)
