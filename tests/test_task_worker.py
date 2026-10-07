"""Unit tests for the Task Worker drain (clear-api ADR-0010): the handler
registry, how one Task's outcome reaches clear-api (complete / fail / lost),
and the ImpactPrior tracer handler's case building. Mocked clear_api."""

from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.defs.tasks import impact_prior as ip
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
)
from clear_pipeline.providers import clear_api
from clear_pipeline.providers.clear_api import ClearApiError, GraphQLErrors, TaskLeaseError

TASK = {"id": "task-1", "kind": "event.impact_prior.clear", "subjectType": "event", "subjectId": "evt-1",
        "leaseToken": "tok-1", "payload": {"horizonYears": 10}, "status": "LEASED"}
# A Task opened before the fan-out rename (clear-api #727): still drained for one release.
LEGACY_TASK = dict(TASK, id="task-legacy", kind="event.impact_prior", leaseToken="tok-legacy")


def _ctx():
    ctx = MagicMock()
    ctx.log = MagicMock()
    return ctx


class TestRegistry:
    def test_impact_prior_handler_is_registered_under_every_configured_kind_in_order(self):
        # Every configured kind dispatches to the same handler, registered in
        # claim order: the drain works HANDLERS in insertion order. Asserted
        # against the configured kinds, not a literal, so an environment that
        # has already dropped the legacy kind still passes.
        assert ip.KIND == "event.impact_prior.clear"
        assert ip.LEGACY_KIND == "event.impact_prior"
        configured = ip.claim_kinds()
        assert configured and configured[0] == ip.KIND
        assert all(HANDLERS[kind] is ip.handle_impact_prior for kind in configured)
        assert [kind for kind in HANDLERS if kind in configured] == configured

    def test_default_claims_clear_first_then_the_bare_kind_for_one_release(self):
        # The shipped default (the field, not the env-driven instance).
        default = type(ip.settings).model_fields["task_drain_impact_prior_kinds"].default
        with patch.object(ip.settings, "task_drain_impact_prior_kinds", default):
            assert ip.claim_kinds() == [ip.KIND, ip.LEGACY_KIND]

    def test_claim_kinds_come_from_settings_in_order(self):
        with patch.object(ip.settings, "task_drain_impact_prior_kinds", "event.impact_prior.clear,event.impact_prior"):
            assert ip.claim_kinds() == ["event.impact_prior.clear", "event.impact_prior"]
        # Dropping the bare kind once the release has shipped is an env change, not a code change.
        with patch.object(ip.settings, "task_drain_impact_prior_kinds", "event.impact_prior.clear"):
            assert ip.claim_kinds() == ["event.impact_prior.clear"]
        with patch.object(ip.settings, "task_drain_impact_prior_kinds", " event.impact_prior.clear , ,event.impact_prior.clear,"):
            assert ip.claim_kinds() == ["event.impact_prior.clear"]

    def test_never_claims_a_kind_outside_its_own(self, caplog):
        # clear-api's fan-out default pasted here must not make this CLEAR-only
        # Worker claim (and mislabel) web Tasks; a typo must not be a silent no-op.
        with patch.object(ip.settings, "task_drain_impact_prior_kinds", "event.impact_prior.clear,event.impact_prior.web"):
            assert ip.claim_kinds() == ["event.impact_prior.clear"]
        assert "event.impact_prior.web" in caplog.text
        with patch.object(ip.settings, "task_drain_impact_prior_kinds", "event.impact_prior_clear"):
            assert ip.claim_kinds() == ["event.impact_prior.clear"]

    def test_an_empty_setting_claims_clear_rather_than_nothing(self, caplog):
        for value in ("", " , ,"):
            with patch.object(ip.settings, "task_drain_impact_prior_kinds", value):
                assert ip.claim_kinds() == ["event.impact_prior.clear"]
        assert "claiming event.impact_prior.clear" in caplog.text

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
        outcome = TaskOutcome(result={"cases": 1}, usage={"model": "m", "inputTokens": 1, "outputTokens": 1, "costUsd": 0.0},
                              impact_prior={"hazardType": "FL"})
        with patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task",
                   return_value={"status": "COMPLETED", "outcome": "produced"}) as complete:
            assert process_one_task(_ctx(), TASK, lambda c, t: outcome, heartbeat_seconds=60) == COMPLETED
        complete.assert_called_once_with("task-1", "tok-1", result={"cases": 1}, usage=outcome.usage,
                                         impact_prior={"hazardType": "FL"})


    def test_handler_exception_fails_the_task_with_its_message(self):
        def boom(context, task):  # noqa: ARG001
            raise ValueError("model exploded")
        with patch("clear_pipeline.defs.tasks.worker.clear_api.fail_task") as fail, \
             patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task") as complete:
            assert process_one_task(_ctx(), TASK, boom, heartbeat_seconds=60) == FAILED
        fail.assert_called_once_with("task-1", "tok-1", "ValueError: model exploded")
        complete.assert_not_called()

    def test_rejected_proposal_fails_the_task_with_the_reason(self):
        # The real shape: HTTP 200 with a GraphQL error carrying code BAD_USER_INPUT,
        # which _task_write turns into a ClearApiError.
        rejected = GraphQLErrors([{"message": "impactPrior.hazardType \"EQ\" is not one of the Event's types (FL)",
                                   "extensions": {"code": "BAD_USER_INPUT"}}])
        with patch("clear_pipeline.providers.clear_api._execute", side_effect=rejected), \
             patch("clear_pipeline.providers.clear_api._worker_key", return_value="sk_live_w"), \
             patch("clear_pipeline.defs.tasks.worker.clear_api.fail_task") as fail:
            assert process_one_task(_ctx(), TASK, lambda c, t: TaskOutcome(impact_prior={"hazardType": "EQ"}), heartbeat_seconds=60) == FAILED
        assert "completion rejected" in fail.call_args.args[2]
        assert "not one of the Event's types" in fail.call_args.args[2]

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


class TestDrain:
    def test_claims_every_registered_kind_in_order_and_passes_the_kind_unchanged(self):
        # One `.clear` Task and one legacy bare Task: the `.clear` queue is
        # drained first; each Task is completed by id and lease token, so the
        # kind reaches clear-api exactly as claimed (sourceKind is stamped there).
        queues = {"event.impact_prior.clear": [[TASK], []], "event.impact_prior": [[LEGACY_TASK], []]}

        def claim(kind, *, limit):  # noqa: ARG001
            return queues[kind].pop(0)
        with patch("clear_pipeline.defs.tasks.worker.redis_lock") as lock, \
             patch("clear_pipeline.defs.tasks.worker.clear_api.claim_tasks", side_effect=claim) as claimed, \
             patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task",
                   return_value={"status": "COMPLETED", "outcome": "produced"}) as complete, \
             patch.dict(HANDLERS, {"event.impact_prior.clear": lambda c, t: TaskOutcome(result={"kind": t["kind"]}),
                                   "event.impact_prior": lambda c, t: TaskOutcome(result={"kind": t["kind"]})}, clear=True):
            lock.return_value.__enter__.return_value = True
            result = _drain(_ctx())
        assert [c.args[0] for c in claimed.call_args_list] == [
            "event.impact_prior.clear", "event.impact_prior.clear", "event.impact_prior", "event.impact_prior",
        ]
        assert [(c.args[0], c.args[1], c.kwargs["result"]["kind"]) for c in complete.call_args_list] == [
            ("task-1", "tok-1", "event.impact_prior.clear"),
            ("task-legacy", "tok-legacy", "event.impact_prior"),
        ]
        assert result.metadata["event.impact_prior.clear.completed"] == 1
        assert result.metadata["event.impact_prior.completed"] == 1


    def test_a_lost_lease_stops_the_whole_drain_not_just_its_kind(self):
        claimed_kinds = []

        def claim(kind, *, limit):  # noqa: ARG001
            claimed_kinds.append(kind)
            return [TASK] if kind == "event.impact_prior.clear" else [LEGACY_TASK]
        with patch("clear_pipeline.defs.tasks.worker.redis_lock") as lock, \
             patch("clear_pipeline.defs.tasks.worker.clear_api.claim_tasks", side_effect=claim), \
             patch("clear_pipeline.defs.tasks.worker.process_one_task", return_value=LOST), \
             patch.dict(HANDLERS, {"event.impact_prior.clear": lambda c, t: None,
                                   "event.impact_prior": lambda c, t: None}, clear=True):
            lock.return_value.__enter__.return_value = True
            result = _drain(_ctx())
        assert claimed_kinds == ["event.impact_prior.clear"]
        assert result.metadata["event.impact_prior.clear.lost"] == 1
        assert "event.impact_prior.lost" not in result.metadata


class TestDrainKind:
    """The claim loop. clear-api retries a failed Task with no delay of its
    own, so the loop must not re-claim it in the same run."""

    def test_drains_until_the_queue_is_empty(self):
        tasks = [dict(TASK, id="t1"), dict(TASK, id="t2")]
        with patch("clear_pipeline.defs.tasks.worker.clear_api.claim_tasks",
                   side_effect=[[tasks[0]], [tasks[1]], []]) as claim, \
             patch("clear_pipeline.defs.tasks.worker.process_one_task", return_value=COMPLETED):
            counts = _drain_kind(_ctx(), "event.impact_prior", lambda c, t: TaskOutcome())
        assert claim.call_count == 3
        assert counts[COMPLETED] == 2

    @pytest.mark.parametrize("outcome", [FAILED, LOST])
    def test_stops_claiming_after_a_failed_or_lost_task(self, outcome):
        # Without the stop, the just-failed Task (PENDING again, oldest) would
        # be the very next claim and lose all its attempts within seconds.
        with patch("clear_pipeline.defs.tasks.worker.clear_api.claim_tasks", return_value=[TASK]) as claim, \
             patch("clear_pipeline.defs.tasks.worker.process_one_task", return_value=outcome):
            counts = _drain_kind(_ctx(), "event.impact_prior", lambda c, t: TaskOutcome())
        assert claim.call_count == 1
        assert counts[outcome] == 1

    def test_a_cancelled_task_does_not_stop_the_drain(self):
        with patch("clear_pipeline.defs.tasks.worker.clear_api.claim_tasks",
                   side_effect=[[TASK], [dict(TASK, id="t2")], []]) as claim, \
             patch("clear_pipeline.defs.tasks.worker.process_one_task", side_effect=[CANCELLED, COMPLETED]):
            counts = _drain_kind(_ctx(), "event.impact_prior", lambda c, t: TaskOutcome())
        assert claim.call_count == 3
        assert counts == {COMPLETED: 1, FAILED: 0, LOST: 0, CANCELLED: 1}


def _loc(id_, level, ancestors=(), name=None):
    return {"id": id_, "name": name or id_, "level": level, "ancestorIds": list(ancestors)}


EVENT = {"id": "evt-1", "title": "Flood in Testville", "types": ["FL"], "startedAt": "2026-08-01T00:00:00.000Z",
         "generalLocation": _loc("district-1", 2, ["state-1", "sdn"], "Testville")}


class _FakeLLM:
    """A provider stub: returns the given selection and reports usage."""
    model = "claude-sonnet-5-5"
    provider_name = "fake"
    role = "narrative"

    def __init__(self, selection):
        self._selection = selection
        self.last_usage = None
        self.calls = []

    def complete_structured(self, **kwargs):
        self.calls.append(kwargs)
        self.last_usage = {"input_tokens": 1000, "output_tokens": 200}
        return self._selection

    def complete_text(self, **kwargs):  # pragma: no cover
        raise NotImplementedError


class TestModelSelection:
    def test_cost_from_the_price_table_and_none_for_unknown_models(self):
        one_each = {"input_tokens": 1_000_000, "output_tokens": 1_000_000}
        assert ip.cost_usd("claude-sonnet-5-5", one_each) == 12.0
        assert ip.cost_usd("claude-sonnet-5", one_each) == 12.0
        assert ip.cost_usd("claude-opus-5-5", one_each) == 24.0
        assert ip.cost_usd("claude-opus-5", one_each) == 30.0
        assert ip.cost_usd("claude-opus-5-5-20260401", one_each) == 24.0   # dated id → exact prefix, not opus-5
        assert ip.cost_usd("claude-haiku-4-5", {"input_tokens": 500_000, "output_tokens": 0}) == 0.5
        assert ip.cost_usd("llama-whatever", {"input_tokens": 1, "output_tokens": 1}) is None
        assert ip.cost_usd("claude-sonnet-5-5", None) is None

    def test_select_cases_maps_the_model_choice_back_onto_candidates(self):
        candidates = [
            {"tier": "clear", "eventId": "evt-2021", "scope": "district", "quote": "Flood 2021", "occurredAt": "2021-08-10"},
            {"tier": "clear", "reportId": "rw-1", "sourceUrl": "https://rw/1", "scope": "country", "quote": "In 2019 floods displaced…"},
            {"tier": "clear", "eventId": "evt-dup", "scope": "country", "quote": "Flood 2021 again"},
        ]
        selection = ip.ImpactPriorSelection(
            cases=[
                ip.SelectedCase(candidate=1, scope="district", note="peak rainy season"),
                ip.SelectedCase(candidate=2, scope="country", occurred_at="2019-09-01"),
                ip.SelectedCase(candidate=2, scope="country"),   # duplicate choice is ignored
                ip.SelectedCase(candidate=9, scope="country"),   # out of range is ignored
            ],
            excluded=[ip.ExcludedCandidate(candidate=3, reason="same occurrence as 1")],
            reasoning="Two distinct floods.",
        )
        llm = _FakeLLM(selection)
        basis, decision = ip.select_cases(llm, event=EVENT, hazard="FL", country_name="Testland", horizon=10, candidates=candidates)
        assert [c.get("eventId") or c.get("reportId") for c in basis] == ["evt-2021", "rw-1"]
        assert basis[0]["note"] == "peak rainy season"
        assert basis[1]["occurredAt"] == "2019-09-01"
        assert decision["excluded"] == [{"candidate": 3, "reason": "same occurrence as 1"}]
        assert "Candidates:" in llm.calls[0]["user"] and "[IN THE INPUT EVENT'S DISTRICT]" in llm.calls[0]["user"]
        assert ip.usage_for_task(llm) == {"model": "claude-sonnet-5-5", "inputTokens": 1000, "outputTokens": 200, "costUsd": 0.004}

    def test_model_may_narrow_a_scope_but_never_promote_one(self):
        candidates = [
            {"tier": "clear", "eventId": "evt-country", "scope": "country", "quote": "Elsewhere in the country"},
            {"tier": "clear", "eventId": "evt-district", "scope": "district", "quote": "Same district"},
        ]
        selection = ip.ImpactPriorSelection(
            cases=[ip.SelectedCase(candidate=1, scope="district"),   # mislabelled: candidate is country-scope
                   ip.SelectedCase(candidate=2, scope="country")],   # narrowing a district case is allowed
            reasoning="x",
        )
        basis, _ = ip.select_cases(_FakeLLM(selection), event=EVENT, hazard="FL", country_name="T", horizon=10, candidates=candidates)
        assert [c["scope"] for c in basis] == ["country", "country"]

    def test_a_model_date_replaces_the_candidates_only_when_it_is_iso_8601(self):
        candidates = [
            {"tier": "clear", "eventId": "a", "scope": "country", "occurredAt": "2019-09-01", "quote": "a"},
            {"tier": "clear", "eventId": "b", "scope": "country", "occurredAt": "2021-08-10T00:00:00Z", "quote": "b"},
            {"tier": "clear", "eventId": "c", "scope": "country", "occurredAt": None, "quote": "c"},
        ]
        selection = ip.ImpactPriorSelection(
            cases=[ip.SelectedCase(candidate=1, scope="country", occurred_at="spring 2019"),
                   ip.SelectedCase(candidate=2, scope="country", occurred_at="2021-08-12T06:00:00Z"),
                   ip.SelectedCase(candidate=3, scope="country", occurred_at="2015-07-01")],
            reasoning="x",
        )
        basis, _ = ip.select_cases(_FakeLLM(selection), event=EVENT, hazard="FL", country_name="T", horizon=10, candidates=candidates)
        assert [c["occurredAt"] for c in basis] == ["2019-09-01", "2021-08-12T06:00:00Z", "2015-07-01"]

    def test_the_horizon_is_anchored_on_the_events_date_not_now(self):
        from datetime import datetime, timezone

        old_event = dict(EVENT, startedAt="2014-06-01T00:00:00.000Z")
        with patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_get_event", return_value=old_event), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_locations_by_level", return_value=[{"id": "sdn"}]), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_events_page",
                   return_value={"items": [], "hasMore": False}) as page, \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_search_knowledgebase", return_value=[]):
            ip.handle_impact_prior(_ctx(), TASK)
        sent = page.call_args.args[0]
        since, until = ip.parse_iso(sent["from"]), ip.parse_iso(sent["to"])
        assert until == datetime(2014, 6, 1, tzinfo=timezone.utc)
        assert since < until
        assert since.year == 2004

    def test_handler_uses_events_then_kb_then_the_model_and_reports_usage(self):
        priors = {"items": [
            {"id": "evt-2021", "title": "Flood 2021", "types": ["FL"], "startedAt": "2021-08-10T00:00:00Z",
             "generalLocation": _loc("district-1", 2, ["state-1", "sdn"], "Testville")},
        ], "hasMore": False}
        hits = [{"reportId": "rw-1", "reportTitle": "Sudan floods 2019", "sourceUrl": "https://rw/1",
                 "publishedAt": "2019-09-15", "chunkText": "In September 2019 floods displaced 400,000 people."},
                {"reportId": "rw-nourl", "reportTitle": "No link", "sourceUrl": None, "chunkText": "uncitable"}]
        selection = ip.ImpactPriorSelection(
            cases=[ip.SelectedCase(candidate=1, scope="district"), ip.SelectedCase(candidate=2, scope="country")],
            excluded=[], reasoning="Both are distinct prior floods in Sudan.",
        )
        llm = _FakeLLM(selection)
        with patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_get_event", return_value=EVENT), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_locations_by_level",
                   return_value=[{"id": "sdn", "name": "Sudan"}]), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_events_page", return_value=priors), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_search_knowledgebase", return_value=hits) as kb, \
             patch("clear_pipeline.defs.tasks.impact_prior.make_llm_provider", return_value=llm):
            outcome = ip.handle_impact_prior(_ctx(), TASK)
        assert kb.call_args.kwargs["filters"]["countryLocationId"] == "sdn"
        assert kb.call_args.kwargs["filters"]["eventTypes"] == ["FL"]
        prior = outcome.impact_prior
        assert prior["numberOfCases"] == 2
        assert outcome.result["candidates"] == 2  # the passage without a URL was never a candidate
        assert prior["basis"][0]["eventId"] == "evt-2021" and prior["basis"][0]["scope"] == "district"
        assert prior["basis"][1]["reportId"] == "rw-1" and prior["basis"][1]["sourceUrl"] == "https://rw/1"
        assert prior["geographicScope"] == "country"
        assert prior["methodVersion"] == ip.METHOD_VERSION
        assert outcome.usage == {"model": "claude-sonnet-5-5", "inputTokens": 1000, "outputTokens": 200, "costUsd": 0.004}
        assert outcome.result["selection"]["reasoning"] == "Both are distinct prior floods in Sudan."
        assert [s["tool"] for s in outcome.result["searched"]] == ["eventsPage", "searchKnowledgebase"]

    def test_model_can_find_no_case_among_candidates(self):
        priors = {"items": [{"id": "evt-x", "title": "Earlier phase", "types": ["FL"],
                             "generalLocation": EVENT["generalLocation"]}], "hasMore": False}
        llm = _FakeLLM(ip.ImpactPriorSelection(cases=[], excluded=[ip.ExcludedCandidate(candidate=1, reason="earlier phase")],
                                               reasoning="Nothing distinct."))
        with patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_get_event", return_value=EVENT), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_locations_by_level", return_value=[{"id": "sdn"}]), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_events_page", return_value=priors), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_search_knowledgebase", return_value=[]), \
             patch("clear_pipeline.defs.tasks.impact_prior.make_llm_provider", return_value=llm):
            outcome = ip.handle_impact_prior(_ctx(), TASK)
        assert outcome.impact_prior is None
        assert outcome.usage is not None and outcome.result["cases"] == 0

    def test_kb_outage_is_not_the_tasks_failure(self):
        with patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_get_event", return_value=EVENT), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_locations_by_level", return_value=[{"id": "sdn"}]), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_events_page", return_value={"items": [], "hasMore": False}), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_search_knowledgebase", side_effect=RuntimeError("kb down")), \
             patch("clear_pipeline.defs.tasks.impact_prior.make_llm_provider", side_effect=RuntimeError("no LLM env")):
            outcome = ip.handle_impact_prior(_ctx(), TASK)
        assert outcome.impact_prior is None and outcome.result["cases"] == 0


class TestImpactPriorTracer:
    def test_resolves_country_and_district(self):
        assert ip.resolve_country_id(EVENT, {"sdn", "eth"}) == "sdn"
        assert ip.district_id(EVENT) == "district-1"
        assert ip.resolve_country_id({"generalLocation": _loc("sdn", 0)}, {"sdn"}) == "sdn"
        assert ip.resolve_country_id({"originLocation": _loc("x", 3, ["nope"])}, {"sdn"}) is None

    def test_cases_from_clear_events_labelled_by_scope_excluding_the_input(self):
        # Rule-based selection: no model configured, no knowledge-base hits.
        priors = {
            "items": [
                {"id": "evt-1", "title": "self", "types": ["FL"], "generalLocation": EVENT["generalLocation"]},
                {"id": "evt-2021", "title": "Flood 2021", "types": ["FL"], "startedAt": "2021-08-10T00:00:00Z",
                 "generalLocation": _loc("district-1", 2, ["state-1", "sdn"], "Testville")},
                {"id": "evt-2019", "title": "Flood 2019", "types": ["FL"], "startedAt": "2019-09-01T00:00:00Z",
                 "originLocation": _loc("district-9", 2, ["state-2", "sdn"], "Elsewhere")},
            ],
            "hasMore": False,
        }
        with patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_get_event", return_value=EVENT), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_locations_by_level",
                   return_value=[{"id": "sdn"}, {"id": "eth"}]), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_events_page", return_value=priors) as page, \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_search_knowledgebase", return_value=[]), \
             patch("clear_pipeline.defs.tasks.impact_prior.make_llm_provider", side_effect=RuntimeError("no LLM env")):
            outcome = ip.handle_impact_prior(_ctx(), TASK)
        sent = page.call_args.args[0]
        assert sent["eventTypes"] == ["FL"] and sent["locationId"] == "sdn" and sent["to"] == EVENT["startedAt"]
        prior = outcome.impact_prior
        assert prior["hazardType"] == "FL" and prior["countryLocationId"] == "sdn"
        assert prior["numberOfCases"] == 2 and len(prior["basis"]) == 2
        assert [c["scope"] for c in prior["basis"]] == ["district", "country"]
        assert all(c["tier"] == "clear" for c in prior["basis"])
        assert prior["geographicScope"] == "country"
        assert outcome.usage is None

    @pytest.mark.parametrize("task", [TASK, LEGACY_TASK])
    def test_no_prior_event_means_no_proposal(self, task):
        # The handler does the same work whichever of its two kinds the Task carries.
        with patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_get_event", return_value=EVENT), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_locations_by_level", return_value=[{"id": "sdn"}]), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_events_page",
                   return_value={"items": [], "hasMore": False}), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_search_knowledgebase", return_value=[]):
            outcome = ip.handle_impact_prior(_ctx(), task)
        assert outcome.impact_prior is None
        assert outcome.result["cases"] == 0

    def test_an_event_without_a_country_completes_without_a_proposal(self):
        with patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_get_event",
                   return_value={"id": "evt-1", "types": ["FL"], "generalLocation": None}), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_locations_by_level", return_value=[{"id": "sdn"}]):
            outcome = ip.handle_impact_prior(_ctx(), TASK)
        assert outcome.impact_prior is None
        assert "country" in outcome.result["reason"]
