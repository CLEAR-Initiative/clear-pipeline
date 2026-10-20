"""Orphaned-run reaper: fail runs whose worker died with a previous container.

Why this exists
---------------
Runs execute as child processes of the ``dagster-code`` gRPC server
(DefaultRunLauncher). That server reconciles a child that dies while the server
itself stays up (``_check_for_orphaned_runs``: an OOM-killed worker gets a
RunFailure within seconds). It cannot reconcile what it never saw: when the
*container* is replaced — Watchtower rolling a new image, a compose
``--force-recreate``, a host reboot — every in-flight worker dies with it and
the fresh server starts with an empty execution table. Those runs stay
``STARTED`` forever. The daemon's run monitoring can't tell either
(DefaultRunLauncher has no ``check_run_worker_health``), so it only fails them
on ``max_runtime_seconds`` (6 h). Meanwhile each one holds a
``max_concurrent_runs`` slot, and once the slots are full the queue is dead.

What it does
------------
Sensors are evaluated *inside* the code-server container, so this sensor can
read that container's own start time (PID 1 in ``/proc``). Any run in
``STARTED`` whose worker started before the container did ran in a previous
container and is gone — there is no other process tree it could live in. The
sensor marks each such run FAILED with an engine event saying why, which frees
the slot and lets ``run_failure_glitchtip_sensor`` page as usual. The poll
sensors re-enqueue the drains on their next tick; every drain is idempotent.

Not covered: runs that hang *inside* a live worker — those are bounded by the
per-job ``dagster/max_runtime`` tags (``defs/run_limits.py``).
"""

import os
from collections.abc import Sequence
from datetime import UTC, datetime

import dagster as dg
from dagster._core.storage.dagster_run import RunRecord

SENSOR_NAME = "orphaned_run_reaper_sensor"


def container_start_time(proc_root: str = "/proc") -> float | None:
    """Epoch seconds at which PID 1 of this container started, read from Linux
    ``/proc`` (``/proc/1/stat`` field 22, clock ticks since boot, plus the
    boot time from ``/proc/stat``). ``None`` where ``/proc`` is unavailable or
    unparseable — macOS, or a sandbox that hides it — in which case the reaper
    does nothing rather than guess."""
    try:
        with open(f"{proc_root}/1/stat", encoding="ascii", errors="replace") as fh:
            stat = fh.read()
        with open(f"{proc_root}/stat", encoding="ascii", errors="replace") as fh:
            lines = fh.read().splitlines()
        # `comm` (field 2) is parenthesised and may contain spaces; everything
        # after the last ')' is whitespace-split, so field N sits at index N-3.
        fields = stat[stat.rfind(")") + 2 :].split()
        start_ticks = int(fields[19])  # field 22: starttime
        btime = next(int(line.split()[1]) for line in lines if line.startswith("btime "))
        return btime + start_ticks / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError, StopIteration):
        return None


def _worker_started_at(record: RunRecord) -> float:
    # start_time is set by the run-start event the worker itself writes; a
    # STARTED run always has one in practice. Fall back to the create time so
    # a record without it is still judged (create <= start, so this can only
    # under-report, never flag a live run).
    if record.start_time is not None:
        return record.start_time
    return record.create_timestamp.timestamp()


def orphaned_runs(records: Sequence[RunRecord], container_started: float) -> list[RunRecord]:
    """The STARTED runs among ``records`` whose worker started before this
    container did — i.e. whose process tree no longer exists."""
    return [
        rec
        for rec in records
        if rec.dagster_run.status == dg.DagsterRunStatus.STARTED
        and _worker_started_at(rec) < container_started
    ]


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat(timespec="seconds")


def failure_message(record: RunRecord, container_started: float) -> str:
    return (
        f"Run worker lost: this run started at {_iso(_worker_started_at(record))} but the "
        f"dagster-code container it ran in was replaced at {_iso(container_started)} "
        "(deploy, restart, or host reboot), which killed every in-flight worker without a "
        f"failure event. Marked FAILED by {SENSOR_NAME} to free its concurrency slot; the "
        "drain sensors will re-enqueue any unfinished work."
    )


def reap_orphaned_runs(instance: dg.DagsterInstance, container_started: float) -> list[str]:
    """Fail every orphaned STARTED run on ``instance``; returns their run ids."""
    records = instance.get_run_records(
        filters=dg.RunsFilter(statuses=[dg.DagsterRunStatus.STARTED]), limit=500
    )
    failed: list[str] = []
    for rec in orphaned_runs(records, container_started):
        # Re-read: the run may have finished between the query and now.
        run = instance.get_run_by_id(rec.dagster_run.run_id)
        if run is None or run.is_finished:
            continue
        message = failure_message(rec, container_started)
        instance.report_engine_event(message, run)
        instance.report_run_failed(run, message)
        failed.append(run.run_id)
    return failed


@dg.sensor(
    name=SENSOR_NAME,
    minimum_interval_seconds=60,
    # RUNNING so a fresh deploy reaps on its first tick — the whole point is to
    # act right after the container that stranded the runs was replaced.
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def orphaned_run_reaper_sensor(context: dg.SensorEvaluationContext) -> dg.SkipReason:
    container_started = container_start_time()
    if container_started is None:
        return dg.SkipReason("no /proc — not in a Linux container; nothing to reap")
    failed = reap_orphaned_runs(context.instance, container_started)
    for run_id in failed:
        context.log.warning("[%s] failed orphaned run %s", SENSOR_NAME, run_id)
    return dg.SkipReason(
        f"container up since {_iso(container_started)}; {len(failed)} orphaned run(s) failed"
    )
