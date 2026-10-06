"""The Worker loop over clear-api's Task contract (ADR-0010).

No ``from __future__ import annotations`` — Dagster inspects the ``context``
annotation on the asset.
"""

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import dagster as dg

from clear_pipeline.defs.signals.poll_sensor import build_poll_sensor
from clear_pipeline.providers import clear_api
from clear_pipeline.providers.redis_lock import redis_lock
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

_DRAIN_LOCK_TTL_SECONDS = 3600
_BATCH_SIZE = 5
_MAX_BATCHES = 10

# Per-Task outcomes, for the run's metadata.
COMPLETED = "completed"
FAILED = "failed"
LOST = "lost"  # the lease was reclaimed or the Task ended elsewhere: nothing to write
CANCELLED = "cancelled"  # the requester withdrew it while we worked: result discarded


@dataclass
class TaskOutcome:
    """What a handler hands back: the raw output for audit, usage if it ran a
    model, and for ``event.impact_prior`` the proposal (``None`` = no prior
    found, which clear-api records as such and writes no row)."""

    result: dict[str, Any] = field(default_factory=dict)
    usage: dict[str, Any] | None = None
    impact_prior: dict[str, Any] | None = None


TaskHandler = Callable[[Any, dict[str, Any]], TaskOutcome]


class Lease:
    """A claimed Task's lease, kept alive by a background heartbeat while the
    handler runs. The heartbeat is also how the Worker learns it should stop:
    clear-api answers CANCELLED when the requester withdrew the Task, and
    NOT_LEASE_OWNER / NOT_LEASED when the lease lapsed and was reclaimed or
    the Task ended elsewhere. Either way `stopped` is set with the reason,
    and whatever the handler produces afterwards is discarded.
    """

    def __init__(self, task: dict[str, Any], *, interval_seconds: float, log) -> None:
        self.task_id = task["id"]
        self.token = task["leaseToken"]
        self.interval = interval_seconds
        self.log = log
        self.stopped: str | None = None  # CANCELLED | LOST
        self._halt = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"lease-{self.task_id}", daemon=True)

    def __enter__(self) -> "Lease":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._halt.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._halt.wait(self.interval):
            try:
                beat = clear_api.heartbeat_task(self.task_id, self.token)
            except clear_api.TaskLeaseError as exc:
                self.log.warning("[drain_tasks] task %s: lease lost at heartbeat: %s", self.task_id, exc)
                self.stopped = LOST
                return
            except Exception:  # noqa: BLE001 — a transient blip; the lease still has time
                self.log.warning("[drain_tasks] task %s: heartbeat failed, will retry", self.task_id, exc_info=True)
                continue
            if beat.get("status") == "CANCELLED":
                self.log.info("[drain_tasks] task %s cancelled by its requester — stopping", self.task_id)
                self.stopped = CANCELLED
                return

# kind → handler. Registering a kind is all it takes to drain it.
HANDLERS: dict[str, TaskHandler] = {}


def register_handler(kind: str):
    """Register the handler for a Task kind: ``handler(context, task) -> TaskOutcome``."""

    def _decorate(fn: TaskHandler) -> TaskHandler:
        HANDLERS[kind] = fn
        return fn

    return _decorate


def process_one_task(
    context, task: dict[str, Any], handler: TaskHandler, *, heartbeat_seconds: float | None = None,
) -> str:
    """Run one claimed Task through its handler, under a heartbeat, and report
    back to clear-api. A handler exception fails the Task with its message
    (clear-api retries while attempts remain, FAILED after maxAttempts); a
    cancel seen at a heartbeat discards the result; a lease error means the
    Task is no longer ours."""
    task_id, token = task["id"], task["leaseToken"]
    interval = heartbeat_seconds if heartbeat_seconds is not None else settings.task_heartbeat_minutes * 60
    try:
        with Lease(task, interval_seconds=interval, log=context.log) as lease:
            outcome = handler(context, task)
        if lease.stopped:
            context.log.info("[drain_tasks] task %s: result discarded (%s)", task_id, lease.stopped)
            return lease.stopped
    except clear_api.TaskLeaseError as exc:
        context.log.warning("[drain_tasks] task %s lost mid-work: %s", task_id, exc)
        return LOST
    except Exception as exc:  # noqa: BLE001 — the handler's failure is the Task's failure
        context.log.warning("[drain_tasks] task %s failed: %s", task_id, exc, exc_info=True)
        try:
            clear_api.fail_task(task_id, token, f"{type(exc).__name__}: {exc}")
        except clear_api.TaskLeaseError as lease_exc:
            context.log.warning("[drain_tasks] task %s lost before it could be failed: %s", task_id, lease_exc)
            return LOST
        return FAILED

    try:
        done = clear_api.complete_task(
            task_id, token, result=outcome.result, usage=outcome.usage, impact_prior=outcome.impact_prior,
        )
    except clear_api.TaskLeaseError as exc:
        context.log.warning("[drain_tasks] task %s lost before completion: %s", task_id, exc)
        return LOST
    except clear_api.ClearApiError as exc:
        # clear-api refused the proposal (BAD_USER_INPUT): the work is wrong,
        # not the queue. Fail with the reason so the requester sees it.
        context.log.error("[drain_tasks] task %s: completion rejected: %s", task_id, exc)
        try:
            clear_api.fail_task(task_id, token, f"completion rejected: {exc}")
        except clear_api.TaskLeaseError:
            return LOST
        return FAILED
    context.log.info("[drain_tasks] task %s %s (outcome=%s)", task_id, done.get("status"), done.get("outcome"))
    if done.get("status") == "COMPLETED":
        return COMPLETED
    # clear-api finished a cancellation requested while we worked: the result was discarded.
    return CANCELLED if done.get("status") == "CANCELLED" else LOST


def _drain_kind(context, kind: str, handler: TaskHandler) -> dict[str, int]:
    counts = {COMPLETED: 0, FAILED: 0, LOST: 0, CANCELLED: 0}
    for _ in range(_MAX_BATCHES):
        batch = clear_api.claim_tasks(kind, limit=_BATCH_SIZE)
        if not batch:
            break
        for task in batch:
            counts[process_one_task(context, task, handler)] += 1
    return counts


def _drain(context) -> dg.MaterializeResult:
    """Drain every registered kind under one single-flight lock. Concurrent
    ticks are safe twice over: the lock serialises runs, and clear-api's lease
    (per-claim token) means two Workers can never write the same Task."""
    with redis_lock("drain_tasks:drain", ttl_seconds=_DRAIN_LOCK_TTL_SECONDS, wait_seconds=0) as acquired:
        if not acquired:
            context.log.info("[drain_tasks] another drain holds the lock — skipping this run")
            return dg.MaterializeResult(metadata={"skipped_concurrent": True})

        metadata: dict[str, Any] = {}
        for kind, handler in HANDLERS.items():
            counts = _drain_kind(context, kind, handler)
            context.log.info("[drain_tasks] %s: %s", kind, counts)
            for outcome, n in counts.items():
                metadata[f"{kind}.{outcome}"] = n
        return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="drain_tasks",
    group_name="tasks",
    description="Task Worker: claim clear-api Tasks of every registered kind, run their handler, complete or fail each.",
)
def drain_tasks(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return _drain(context)


drain_tasks_job = dg.define_asset_job(name="drain_tasks_job", selection=[drain_tasks])
task_worker_sensor = build_poll_sensor(
    name="task_worker_sensor",
    job=drain_tasks_job,
    default_interval_minutes=settings.task_poll_interval_minutes,
)
