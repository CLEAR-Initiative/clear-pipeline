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
    process_one_task,
    register_handler,
)
from clear_pipeline.providers import clear_api
from clear_pipeline.providers.clear_api import ClearApiError, GraphQLErrors, TaskLeaseError

TASK = {"id": "task-1", "kind": "event.impact_prior", "subjectType": "event", "subjectId": "evt-1",
        "leaseToken": "tok-1", "payload": {"horizonYears": 10}, "status": "LEASED"}


def _ctx():
    ctx = MagicMock()
    ctx.log = MagicMock()
    return ctx


class TestRegistry:
    def test_impact_prior_handler_is_registered(self):
        assert HANDLERS["event.impact_prior"] is ip.handle_impact_prior

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

    def test_no_prior_event_means_no_proposal(self):
        with patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_get_event", return_value=EVENT), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_locations_by_level", return_value=[{"id": "sdn"}]), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_events_page",
                   return_value={"items": [], "hasMore": False}), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_search_knowledgebase", return_value=[]):
            outcome = ip.handle_impact_prior(_ctx(), TASK)
        assert outcome.impact_prior is None
        assert outcome.result["cases"] == 0

    def test_an_event_without_a_country_completes_without_a_proposal(self):
        with patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_get_event",
                   return_value={"id": "evt-1", "types": ["FL"], "generalLocation": None}), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_locations_by_level", return_value=[{"id": "sdn"}]):
            outcome = ip.handle_impact_prior(_ctx(), TASK)
        assert outcome.impact_prior is None
        assert "country" in outcome.result["reason"]
