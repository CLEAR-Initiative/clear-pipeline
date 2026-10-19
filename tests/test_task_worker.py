"""Unit tests for the generic Task Worker drain (clear-api ADR-0010): the
handler registry (empty since the `.clear` ImpactPrior handler was retired),
how one Task's outcome reaches clear-api (complete / fail / lost), and the
claim loop. Handlers here are test-only stand-ins. Mocked clear_api."""

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from clear_pipeline.defs.tasks.worker import (
    CANCELLED,
    COMPLETED,
    FAILED,
    HANDLERS,
    LOST,
    Lease,
    TaskOutcome,
    _drain,
    _drain_kind,
    drain_tasks_job,
    process_one_task,
    register_handler,
    task_worker_sensor,
)
from clear_pipeline.providers import clear_api
from clear_pipeline.providers.clear_api import ClearApiError, GraphQLErrors, TaskLeaseError

# Test-only kinds: no production handler is registered.
KIND = "test.echo"
OTHER_KIND = "test.other"
TASK = {"id": "task-1", "kind": KIND, "subjectType": "event", "subjectId": "evt-1",
        "leaseToken": "tok-1", "payload": {}, "status": "LEASED"}
OTHER_TASK = dict(TASK, id="task-other", kind=OTHER_KIND, leaseToken="tok-other")


def _ctx():
    ctx = MagicMock()
    ctx.log = MagicMock()
    return ctx


class TestRegistry:
    def test_no_production_kind_is_registered(self):
        # The `.clear` ImpactPrior handler was retired (2026-10-08); nothing
        # should claim `event.impact_prior*` Tasks any more.
        assert not any(kind.startswith("event.impact_prior") for kind in HANDLERS)

    def test_drain_tasks_job_jumps_the_run_queue(self):
        # QueuedRunCoordinator dequeues the highest `dagster/priority` first;
        # every other job here is 0, so a Task never waits behind the ingest sensors.
        assert int(drain_tasks_job.tags["dagster/priority"]) > 0

    def test_register_handler_adds_a_kind(self):
        @register_handler("event.other")
        def other(context, task):  # noqa: ARG001
            return TaskOutcome()
        try:
            assert HANDLERS["event.other"] is other
        finally:
            del HANDLERS["event.other"]


class TestProcessOneTask:
    def test_completes_with_the_handler_outcome(self):
        outcome = TaskOutcome(result={"cases": 1}, usage={"model": "m", "inputTokens": 1, "outputTokens": 1, "costUsd": 0.0})
        with patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task",
                   return_value={"status": "COMPLETED", "outcome": "produced"}) as complete:
            assert process_one_task(_ctx(), TASK, lambda c, t: outcome, heartbeat_seconds=60) == COMPLETED
        complete.assert_called_once_with("task-1", "tok-1", result={"cases": 1}, usage=outcome.usage)

    def test_complete_task_sends_only_result_and_usage(self):
        # clear-api is removing `ImpactPriorInput`: a document that still
        # declared it would be rejected for every completion.
        assert "impactPrior" not in clear_api.COMPLETE_TASK
        assert "ImpactPriorInput" not in clear_api.COMPLETE_TASK
        with patch("clear_pipeline.providers.clear_api._worker_key", return_value="sk_live_w"), \
             patch("clear_pipeline.providers.clear_api._execute",
                   return_value={"completeTask": {"status": "COMPLETED"}}) as execute:
            clear_api.complete_task("task-1", "tok", result={"ok": True})
            clear_api.complete_task("task-1", "tok", result={}, usage={"model": "m"})
        assert execute.call_args_list[0].args[1] == {"id": "task-1", "leaseToken": "tok", "result": {"ok": True}}
        assert execute.call_args_list[1].args[1]["usage"] == {"model": "m"}

    def test_handler_exception_fails_the_task_with_its_message(self):
        def boom(context, task):  # noqa: ARG001
            raise ValueError("model exploded")
        with patch("clear_pipeline.defs.tasks.worker.clear_api.fail_task") as fail, \
             patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task") as complete:
            assert process_one_task(_ctx(), TASK, boom, heartbeat_seconds=60) == FAILED
        fail.assert_called_once_with("task-1", "tok-1", "ValueError: model exploded")
        complete.assert_not_called()

    def test_rejected_completion_fails_the_task_with_the_reason(self):
        # The real shape: HTTP 200 with a GraphQL error carrying code BAD_USER_INPUT,
        # which _task_write turns into a ClearApiError.
        rejected = GraphQLErrors([{"message": "result.caseId is not one of the Event's cases",
                                   "extensions": {"code": "BAD_USER_INPUT"}}])
        with patch("clear_pipeline.providers.clear_api._execute", side_effect=rejected), \
             patch("clear_pipeline.providers.clear_api._worker_key", return_value="sk_live_w"), \
             patch("clear_pipeline.defs.tasks.worker.clear_api.fail_task") as fail:
            assert process_one_task(_ctx(), TASK, lambda c, t: TaskOutcome(result={"caseId": "x"}), heartbeat_seconds=60) == FAILED
        assert "completion rejected" in fail.call_args.args[2]
        assert "not one of the Event's cases" in fail.call_args.args[2]

    def test_task_write_maps_graphql_errors_by_extensions(self):
        with patch("clear_pipeline.providers.clear_api._worker_key", return_value="sk_live_w"):
            with patch("clear_pipeline.providers.clear_api._execute",
                       side_effect=GraphQLErrors([{"message": "stale", "extensions": {"code": "FORBIDDEN", "subCode": "NOT_LEASE_OWNER"}}])):
                with pytest.raises(TaskLeaseError):
                    clear_api.heartbeat_task("task-1", "tok")
            with patch("clear_pipeline.providers.clear_api._execute",
                       side_effect=GraphQLErrors([{"message": "done", "extensions": {"code": "CONFLICT", "subCode": "NOT_LEASED"}}])):
                with pytest.raises(TaskLeaseError):
                    clear_api.fail_task("task-1", "tok", "x")
            with patch("clear_pipeline.providers.clear_api._execute",
                       side_effect=GraphQLErrors([{"message": "lock", "extensions": {"code": "INTERNAL_SERVER_ERROR"}}])):
                with pytest.raises(GraphQLErrors):
                    clear_api.complete_task("task-1", "tok", result={})

    def test_a_transport_failure_on_the_terminal_write_leaves_the_task_to_lapse(self):
        with patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task", side_effect=OSError("connection reset")):
            assert process_one_task(_ctx(), TASK, lambda c, t: TaskOutcome(), heartbeat_seconds=60) == LOST

        def boom(context, task):  # noqa: ARG001
            raise ValueError("x")
        with patch("clear_pipeline.defs.tasks.worker.clear_api.fail_task", side_effect=ClearApiError("401")):
            assert process_one_task(_ctx(), TASK, boom, heartbeat_seconds=60) == LOST

    @pytest.mark.parametrize("where", ["handler", "complete", "fail"])
    def test_a_lost_lease_is_never_retried(self, where):
        def handler(context, task):  # noqa: ARG001
            if where == "handler":
                raise TaskLeaseError("NOT_LEASE_OWNER")
            if where == "fail":
                raise ValueError("x")
            return TaskOutcome()
        with patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task",
                   side_effect=TaskLeaseError("NOT_LEASED") if where == "complete" else None), \
             patch("clear_pipeline.defs.tasks.worker.clear_api.fail_task",
                   side_effect=TaskLeaseError("NOT_LEASE_OWNER") if where == "fail" else None):
            assert process_one_task(_ctx(), TASK, handler, heartbeat_seconds=60) == LOST


class TestHeartbeatAndCancel:
    def test_heartbeats_while_the_handler_runs(self):
        import time

        def slow(context, task):  # noqa: ARG001
            time.sleep(0.35)
            return TaskOutcome(result={"ok": True})
        with patch("clear_pipeline.defs.tasks.worker.clear_api.heartbeat_task",
                   return_value={"status": "LEASED"}) as beat, \
             patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task", return_value={"status": "COMPLETED"}):
            assert process_one_task(_ctx(), TASK, slow, heartbeat_seconds=0.1) == COMPLETED
        assert beat.call_count >= 2
        assert beat.call_args.args == ("task-1", "tok-1")

    def test_a_cancel_seen_at_a_heartbeat_discards_the_result(self):
        import time

        def slow(context, task):  # noqa: ARG001
            time.sleep(0.3)
            return TaskOutcome(result={"late": True})
        with patch("clear_pipeline.defs.tasks.worker.clear_api.heartbeat_task",
                   return_value={"status": "CANCELLED"}), \
             patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task") as complete, \
             patch("clear_pipeline.defs.tasks.worker.clear_api.fail_task") as fail:
            assert process_one_task(_ctx(), TASK, slow, heartbeat_seconds=0.05) == CANCELLED
        complete.assert_not_called()
        fail.assert_not_called()

    def test_a_lease_lost_at_a_heartbeat_discards_the_result(self):
        import time

        def slow(context, task):  # noqa: ARG001
            time.sleep(0.3)
            return TaskOutcome()
        with patch("clear_pipeline.defs.tasks.worker.clear_api.heartbeat_task",
                   side_effect=TaskLeaseError("NOT_LEASE_OWNER")), \
             patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task") as complete:
            assert process_one_task(_ctx(), TASK, slow, heartbeat_seconds=0.05) == LOST
        complete.assert_not_called()

    def test_a_transient_heartbeat_failure_keeps_going(self):
        import time

        def slow(context, task):  # noqa: ARG001
            time.sleep(0.3)
            return TaskOutcome()
        with patch("clear_pipeline.defs.tasks.worker.clear_api.heartbeat_task",
                   side_effect=[RuntimeError("blip"), {"status": "LEASED"}, {"status": "LEASED"}, {"status": "LEASED"}]), \
             patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task", return_value={"status": "COMPLETED"}):
            assert process_one_task(_ctx(), TASK, slow, heartbeat_seconds=0.05) == COMPLETED

    def test_a_cancel_that_clear_api_finishes_at_completion_counts_as_cancelled(self):
        with patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task", return_value={"status": "CANCELLED"}):
            assert process_one_task(_ctx(), TASK, lambda c, t: TaskOutcome(), heartbeat_seconds=60) == CANCELLED

    def test_lease_stops_its_thread_on_exit(self):
        lease = Lease(TASK, interval_seconds=60, log=MagicMock())
        with lease:
            assert lease._thread.is_alive()
        assert not lease._thread.is_alive()

    def test_a_handler_failure_after_a_cancel_writes_nothing(self):
        # The heartbeat saw CANCELLED, then the handler raised: clear-api has
        # already closed the Task, so neither fail nor complete is attempted
        # and the outcome is the cancel, not a misreported loss.
        import time

        def slow_then_boom(context, task):  # noqa: ARG001
            time.sleep(0.3)
            raise ValueError("late failure")
        with patch("clear_pipeline.defs.tasks.worker.clear_api.heartbeat_task",
                   return_value={"status": "CANCELLED"}), \
             patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task") as complete, \
             patch("clear_pipeline.defs.tasks.worker.clear_api.fail_task") as fail:
            assert process_one_task(_ctx(), TASK, slow_then_boom, heartbeat_seconds=0.05) == CANCELLED
        complete.assert_not_called()
        fail.assert_not_called()


def _drain_with(handlers):
    with patch("clear_pipeline.defs.tasks.worker.redis_lock") as lock, \
         patch.dict(HANDLERS, handlers, clear=True):
        lock.return_value.__enter__.return_value = True
        return _drain(_ctx())


class TestDrain:
    def test_claims_every_registered_kind_in_order_and_passes_the_kind_unchanged(self):
        # One Task of each kind: the first-registered queue is drained first;
        # each Task is completed by id and lease token, so the kind reaches
        # clear-api exactly as claimed.
        queues = {KIND: [[TASK], []], OTHER_KIND: [[OTHER_TASK], []]}

        def claim(kind, *, limit):  # noqa: ARG001
            return queues[kind].pop(0)
        with patch("clear_pipeline.defs.tasks.worker.clear_api.claim_tasks", side_effect=claim) as claimed, \
             patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task",
                   return_value={"status": "COMPLETED", "outcome": "produced"}) as complete:
            result = _drain_with({KIND: lambda c, t: TaskOutcome(result={"kind": t["kind"]}),
                                  OTHER_KIND: lambda c, t: TaskOutcome(result={"kind": t["kind"]})})
        assert [c.args[0] for c in claimed.call_args_list] == [KIND, KIND, OTHER_KIND, OTHER_KIND]
        assert [(c.args[0], c.args[1], c.kwargs["result"]["kind"]) for c in complete.call_args_list] == [
            ("task-1", "tok-1", KIND),
            ("task-other", "tok-other", OTHER_KIND),
        ]
        assert result.metadata[f"{KIND}.completed"] == 1
        assert result.metadata[f"{OTHER_KIND}.completed"] == 1

    def test_a_lost_lease_stops_the_whole_drain_not_just_its_kind(self):
        claimed_kinds = []

        def claim(kind, *, limit):  # noqa: ARG001
            claimed_kinds.append(kind)
            return [TASK] if kind == KIND else [OTHER_TASK]
        with patch("clear_pipeline.defs.tasks.worker.clear_api.claim_tasks", side_effect=claim), \
             patch("clear_pipeline.defs.tasks.worker.process_one_task", return_value=LOST):
            result = _drain_with({KIND: lambda c, t: None, OTHER_KIND: lambda c, t: None})
        assert claimed_kinds == [KIND]
        assert result.metadata[f"{KIND}.lost"] == 1
        assert f"{OTHER_KIND}.lost" not in result.metadata

    def test_an_empty_registry_claims_nothing(self):
        with patch("clear_pipeline.defs.tasks.worker.clear_api.claim_tasks") as claim, \
             patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task") as complete:
            result = _drain_with({})
        claim.assert_not_called()
        complete.assert_not_called()
        assert result.metadata == {"registered_kinds": 0}


class TestSensor:
    def _tick(self):
        with dg.instance_for_test() as instance:
            return list(task_worker_sensor(dg.build_sensor_context(instance=instance)))

    def test_skips_every_tick_while_no_kind_is_registered(self):
        # No run is launched (it would jump the run queue to claim nothing).
        with patch.dict(HANDLERS, {}, clear=True):
            [tick] = self._tick()
        assert isinstance(tick, dg.SkipReason)
        assert "no Task kind is registered" in tick.skip_message

    def test_launches_a_run_once_a_kind_is_registered(self):
        with patch.dict(HANDLERS, {KIND: lambda c, t: TaskOutcome()}, clear=True):
            [tick] = self._tick()
        assert isinstance(tick, dg.RunRequest)


class TestDrainKind:
    """The claim loop. clear-api retries a failed Task with no delay of its
    own, so the loop must not re-claim it in the same run."""

    def test_drains_until_the_queue_is_empty(self):
        tasks = [dict(TASK, id="t1"), dict(TASK, id="t2")]
        with patch("clear_pipeline.defs.tasks.worker.clear_api.claim_tasks",
                   side_effect=[[tasks[0]], [tasks[1]], []]) as claim, \
             patch("clear_pipeline.defs.tasks.worker.process_one_task", return_value=COMPLETED):
            counts = _drain_kind(_ctx(), KIND, lambda c, t: TaskOutcome())
        assert claim.call_count == 3
        assert counts[COMPLETED] == 2

    @pytest.mark.parametrize("outcome", [FAILED, LOST])
    def test_stops_claiming_after_a_failed_or_lost_task(self, outcome):
        # Without the stop, the just-failed Task (PENDING again, oldest) would
        # be the very next claim and lose all its attempts within seconds.
        with patch("clear_pipeline.defs.tasks.worker.clear_api.claim_tasks", return_value=[TASK]) as claim, \
             patch("clear_pipeline.defs.tasks.worker.process_one_task", return_value=outcome):
            counts = _drain_kind(_ctx(), KIND, lambda c, t: TaskOutcome())
        assert claim.call_count == 1
        assert counts[outcome] == 1

    def test_a_cancelled_task_does_not_stop_the_drain(self):
        with patch("clear_pipeline.defs.tasks.worker.clear_api.claim_tasks",
                   side_effect=[[TASK], [dict(TASK, id="t2")], []]) as claim, \
             patch("clear_pipeline.defs.tasks.worker.process_one_task", side_effect=[CANCELLED, COMPLETED]):
            counts = _drain_kind(_ctx(), KIND, lambda c, t: TaskOutcome())
        assert claim.call_count == 3
        assert counts == {COMPLETED: 1, FAILED: 0, LOST: 0, CANCELLED: 1}
