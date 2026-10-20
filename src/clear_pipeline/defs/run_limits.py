"""Per-job run-time caps, enforced by Dagster's run-monitoring daemon.

The instance-wide ``run_monitoring.max_runtime_seconds`` (6 h in clear-infra's
dagster.yaml) is sized for the weekly knowledge-base build. The sensor-driven
drains finish in seconds to minutes, so a drain that is still "running" after
half an hour is not working — its worker hung, or died with its container
(see ``defs/orphaned_runs.py``). Until the daemon fails it, it holds one of
``max_concurrent_runs`` queue slots; four of those and nothing dequeues at all
(2026-10-20 incident: two deploys stranded 2 translate + 2 classify_group runs
in STARTED, 96 runs queued behind them for two hours).

``dagster/max_runtime`` on a job's tags overrides the global cap per run —
``define_asset_job(tags=...)`` appends to every run the job launches, whether
from a sensor, a schedule, or the UI. Caps are ~5–10× the job's observed p95
so a slow day is not cut short; see the table in README.md.
"""

from collections.abc import Mapping

# Dagster's run-monitoring tag (dagster._core.storage.tags.MAX_RUNTIME_SECONDS_TAG).
MAX_RUNTIME_TAG = "dagster/max_runtime"


def max_runtime_tags(seconds: int, extra: Mapping[str, str] | None = None) -> dict[str, str]:
    """Run tags capping a job at ``seconds`` of wall-clock time, merged over
    ``extra`` (e.g. a priority or concurrency-weight tag the job already sets).
    The cap only bites when the daemon's run monitoring is enabled — it is in
    every deployed env; locally there is simply no daemon to enforce it."""
    if seconds <= 0:
        raise ValueError(f"max runtime must be positive, got {seconds}")
    return {**(extra or {}), MAX_RUNTIME_TAG: str(int(seconds))}
