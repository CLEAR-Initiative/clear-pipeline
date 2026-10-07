"""The Worker loop over clear-api's Task contract (ADR-0010).

Run-queue placement: ``drain_tasks_job`` carries ``dagster/priority`` (see
``_RUN_PRIORITY``). The instance's QueuedRunCoordinator dequeues up to its
``max_concurrent_runs`` at a time (the instance's dagster.yaml decides: the
dev VM mounts its own) and, among the queued, the highest priority first
(default 0). The 1-minute hotline and translate sensors keep that queue busy,
and a Task is a person waiting on an Event page, so this job jumps the queue
rather than taking its turn behind an hour of ingest runs. Priority only
reorders the queue; it cannot free a slot two long runs already hold. A
priority tag needs no instance config, unlike a tag concurrency limit, which
would also only cap, not favour, the drain.

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
# One Task per claim: a lease starts at claim time, and only the Task being
# worked is heartbeated, so a batch would let later Tasks lapse while earlier
# ones run. The claim is a cheap SKIP LOCKED statement; loop instead.
_BATCH_SIZE = 1
_MAX_BATCHES = 50
# QueuedRunCoordinator dequeues higher `dagster/priority` first; 0 is every
# other run here. 10 leaves room for something more urgent later.
_RUN_PRIORITY = 10

# Per-Task outcomes, for the run's metadata.
COMPLETED = "completed"
FAILED = "failed"
LOST = "lost"  # the lease was reclaimed or the Task ended elsewhere: nothing to write
CANCELLED = "cancelled"  # the requester withdrew it while we worked: result discarded


@dataclass
class TaskOutcome:
    """What a handler hands back: the raw output for audit, usage if it ran a
    model, and for ``event.impact_prior.*`` the proposal (``None`` = no prior
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
        # Wait for an in-flight heartbeat (bounded by the HTTP timeout) so
        # `stopped` is final before the caller writes the result.
        self._thread.join()

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


def _fail(context, task_id: str, token: str, error: str) -> str:
    """Fail the Task with ``error``; if even that write is refused or fails,
    leave the lease to lapse (clear-api retries) rather than crash the run."""
    try:
        clear_api.fail_task(task_id, token, error)
    except clear_api.TaskLeaseError as exc:
        context.log.warning("[drain_tasks] task %s lost before it could be failed: %s", task_id, exc)
        return LOST
    except Exception:  # noqa: BLE001
        context.log.warning("[drain_tasks] task %s: could not fail — leaving it to lapse", task_id, exc_info=True)
        return LOST
    return FAILED


def process_one_task(
    context, task: dict[str, Any], handler: TaskHandler, *, heartbeat_seconds: float | None = None,
) -> str:
    """Run one claimed Task through its handler, under a heartbeat, and report
    back to clear-api. A handler exception fails the Task with its message
    (clear-api retries while attempts remain, FAILED after maxAttempts); a
    cancel seen at a heartbeat discards the result; a lease error means the
    Task is no longer ours."""
    task_id, token = task["id"], task["leaseToken"]
    context.log.info("[drain_tasks] task %s (%s) claimed", task_id, task.get("kind"))
    interval = heartbeat_seconds if heartbeat_seconds is not None else settings.task_heartbeat_minutes * 60
    lease = Lease(task, interval_seconds=interval, log=context.log)
    try:
        with lease:
            outcome = handler(context, task)
    except clear_api.TaskLeaseError as exc:
        context.log.warning("[drain_tasks] task %s lost mid-work: %s", task_id, exc)
        return LOST
    except Exception as exc:  # noqa: BLE001 — the handler's failure is the Task's failure
        if lease.stopped:
            # The heartbeat already learnt the Task is no longer ours (cancelled
            # or reclaimed): clear-api would refuse the write, so don't make it.
            context.log.info("[drain_tasks] task %s: handler failed after %s — nothing to write", task_id, lease.stopped)
            return lease.stopped
        context.log.warning("[drain_tasks] task %s failed: %s", task_id, exc, exc_info=True)
        return _fail(context, task_id, token, f"{type(exc).__name__}: {exc}")
    if lease.stopped:
        context.log.info("[drain_tasks] task %s: result discarded (%s)", task_id, lease.stopped)
        return lease.stopped

    try:
        done = clear_api.complete_task(
            task_id, token, result=outcome.result, usage=outcome.usage, impact_prior=outcome.impact_prior,
        )
    except clear_api.TaskLeaseError as exc:
        context.log.warning("[drain_tasks] task %s lost before completion: %s", task_id, exc)
        return LOST
    except clear_api.ClearApiError as exc:
        # clear-api refused the write (BAD_USER_INPUT, e.g. a proposal that
        # does not fit the Event): the work is wrong, not the queue. Fail
        # with the reason so the requester sees it.
        context.log.error("[drain_tasks] task %s: completion rejected: %s", task_id, exc)
        return _fail(context, task_id, token, f"completion rejected: {exc}")
    except Exception:  # noqa: BLE001 — transport blip on the terminal write: the lease lapses and clear-api retries
        context.log.warning("[drain_tasks] task %s: could not complete — leaving it to lapse", task_id, exc_info=True)
        return LOST
    context.log.info("[drain_tasks] task %s %s (outcome=%s)", task_id, done.get("status"), done.get("outcome"))
    if done.get("status") == "COMPLETED":
        return COMPLETED
    # clear-api finished a cancellation requested while we worked: the result was discarded.
    return CANCELLED if done.get("status") == "CANCELLED" else LOST


def _drain_kind(context, kind: str, handler: TaskHandler) -> dict[str, int]:
    """Claim and work Tasks of ``kind`` until the queue is empty or a Task
    fails. A failed Task goes straight back to PENDING at the head of the
    queue (clear-api retries with no delay of its own), so claiming again in
    the same run would re-lease it at once and burn its remaining attempts
    in seconds on what is usually a transient fault (the model or clear-api
    briefly down). Stopping leaves the retry to the next sensor tick, which
    is the backoff — the same rule as the analysis drain's no-progress stop.
    A LOST outcome stops too: it means clear-api is unhealthy or someone
    else holds our leases, and neither improves by claiming more."""
    counts = {COMPLETED: 0, FAILED: 0, LOST: 0, CANCELLED: 0}
    for _ in range(_MAX_BATCHES):
        batch = clear_api.claim_tasks(kind, limit=_BATCH_SIZE)
        if not batch:
            break
        stop = False
        for task in batch:
            outcome = process_one_task(context, task, handler)
            counts[outcome] += 1
            if outcome in (FAILED, LOST):
                context.log.info(
                    "[drain_tasks] %s: task %s %s — leaving the rest of the queue to the next run",
                    kind, task["id"], outcome,
                )
                stop = True
        if stop:
            break
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
        if not HANDLERS:
            context.log.warning("[drain_tasks] no Task kind is registered — nothing to claim")
        for kind, handler in HANDLERS.items():
            counts = _drain_kind(context, kind, handler)
            context.log.info("[drain_tasks] %s: %s", kind, counts)
            for outcome, n in counts.items():
                metadata[f"{kind}.{outcome}"] = n
            # A lost lease means clear-api is unhealthy or someone else holds
            # our Tasks; claiming the next kind straight away would not help.
            if counts[LOST]:
                context.log.info("[drain_tasks] %s lost a lease — leaving the other kinds to the next run", kind)
                break
        return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="drain_tasks",
    group_name="tasks",
    description="Task Worker: claim clear-api Tasks of every registered kind, run their handler, complete or fail each.",
)
def drain_tasks(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return _drain(context)


drain_tasks_job = dg.define_asset_job(
    name="drain_tasks_job",
    selection=[drain_tasks],
    tags={"dagster/priority": str(_RUN_PRIORITY)},
)
task_worker_sensor = build_poll_sensor(
    name="task_worker_sensor",
    job=drain_tasks_job,
    default_interval_minutes=settings.task_poll_interval_minutes,
)
