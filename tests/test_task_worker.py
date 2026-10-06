"""Unit tests for the Task Worker drain (clear-api ADR-0010): the handler
registry, how one Task's outcome reaches clear-api (complete / fail / lost),
and the ImpactPrior tracer handler's case building. Mocked clear_api."""

from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.defs.tasks import impact_prior as ip
from clear_pipeline.defs.tasks.worker import (
    COMPLETED,
    FAILED,
    HANDLERS,
    LOST,
    TaskOutcome,
    process_one_task,
    register_handler,
)
from clear_pipeline.providers.clear_api import ClearApiError, TaskLeaseError

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
            assert process_one_task(_ctx(), TASK, lambda c, t: outcome) == COMPLETED
        complete.assert_called_once_with("task-1", "tok-1", result={"cases": 1}, usage=outcome.usage,
                                         impact_prior={"hazardType": "FL"})

    def test_a_cancelled_completion_counts_as_lost(self):
        with patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task", return_value={"status": "CANCELLED"}):
            assert process_one_task(_ctx(), TASK, lambda c, t: TaskOutcome()) == LOST

    def test_handler_exception_fails_the_task_with_its_message(self):
        def boom(context, task):  # noqa: ARG001
            raise ValueError("model exploded")
        with patch("clear_pipeline.defs.tasks.worker.clear_api.fail_task") as fail, \
             patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task") as complete:
            assert process_one_task(_ctx(), TASK, boom) == FAILED
        fail.assert_called_once_with("task-1", "tok-1", "ValueError: model exploded")
        complete.assert_not_called()

    def test_rejected_proposal_fails_the_task_with_the_reason(self):
        with patch("clear_pipeline.defs.tasks.worker.clear_api.complete_task",
                   side_effect=ClearApiError("clear-api 400: hazardType EQ is not one of the Event's types")), \
             patch("clear_pipeline.defs.tasks.worker.clear_api.fail_task") as fail:
            assert process_one_task(_ctx(), TASK, lambda c, t: TaskOutcome(impact_prior={"hazardType": "EQ"})) == FAILED
        assert "completion rejected" in fail.call_args.args[2]

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
            assert process_one_task(_ctx(), TASK, handler) == LOST


def _loc(id_, level, ancestors=(), name=None):
    return {"id": id_, "name": name or id_, "level": level, "ancestorIds": list(ancestors)}


EVENT = {"id": "evt-1", "title": "Flood in Testville", "types": ["FL"], "startedAt": "2026-08-01T00:00:00.000Z",
         "generalLocation": _loc("district-1", 2, ["state-1", "sdn"], "Testville")}


class TestImpactPriorTracer:
    def test_resolves_country_and_district(self):
        assert ip.resolve_country_id(EVENT, {"sdn", "eth"}) == "sdn"
        assert ip.district_id(EVENT) == "district-1"
        assert ip.resolve_country_id({"generalLocation": _loc("sdn", 0)}, {"sdn"}) == "sdn"
        assert ip.resolve_country_id({"originLocation": _loc("x", 3, ["nope"])}, {"sdn"}) is None

    def test_cases_from_clear_events_labelled_by_scope_excluding_the_input(self):
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
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_events_page", return_value=priors) as page:
            outcome = ip.handle_impact_prior(_ctx(), TASK)
        sent = page.call_args.args[0]
        assert sent["eventTypes"] == ["FL"] and sent["locationId"] == "sdn" and sent["to"] == EVENT["startedAt"]
        prior = outcome.impact_prior
        assert prior["hazardType"] == "FL" and prior["countryLocationId"] == "sdn"
        assert prior["numberOfCases"] == 2 and len(prior["basis"]) == 2
        assert [c["scope"] for c in prior["basis"]] == ["district", "country"]
        assert all(c["tier"] == "clear" for c in prior["basis"])
        assert prior["geographicScope"] == "country"
        assert outcome.result["excluded"] == [{"id": "evt-1", "reason": "the input Event"}]

    def test_no_prior_event_means_no_proposal(self):
        with patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_get_event", return_value=EVENT), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_locations_by_level", return_value=[{"id": "sdn"}]), \
             patch("clear_pipeline.defs.tasks.impact_prior.clear_api.worker_events_page",
                   return_value={"items": [], "hasMore": False}):
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
