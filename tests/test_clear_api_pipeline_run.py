"""The run inputs of clear-api's location Outcome log (clear-api ADR-0020):
which run kind, pipeline version and Dagster run id the client sends with
createSignal and markSignalsProcessed, and the fallback for a clear-api that
does not know them yet."""

from unittest.mock import patch

import pytest

from clear_pipeline.providers import clear_api
from clear_pipeline.providers.clear_api import (
    RUN_KIND_REPLAY,
    ClearApiError,
    create_signal,
    create_signal_for_sync,
    mark_signals_processed,
    pipeline_run,
)

ITEMS = [{"id": "s1", "revision": 0}]


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(clear_api, "_run_inputs_supported", True)
    monkeypatch.setenv("PIPELINE_VERSION", "abc123")


def _calls(execute):
    return [(c.args[0], c.args[1]) for c in execute.call_args_list]


def test_mark_sends_live_the_version_and_the_run_id():
    with patch.object(clear_api, "_execute", return_value={"markSignalsProcessed": 1}) as execute:
        with pipeline_run(run_id="run-1"):
            assert mark_signals_processed(ITEMS) == 1
    query, variables = _calls(execute)[0]
    assert query == clear_api.MARK_SIGNALS_PROCESSED
    assert variables == {
        "items": ITEMS, "status": "PROCESSED",
        "runKind": "LIVE", "pipelineVersion": "abc123", "runId": "run-1",
    }


def test_a_replay_block_sends_replay_and_keeps_the_outer_run_id():
    with patch.object(clear_api, "_execute", return_value={"markSignalsProcessed": 1}) as execute:
        with pipeline_run(run_id="run-1"):
            with pipeline_run(RUN_KIND_REPLAY):
                mark_signals_processed(ITEMS, "FAILED")
            mark_signals_processed(ITEMS)
    replay, live = (variables for _query, variables in _calls(execute))
    assert (replay["runKind"], replay["runId"], replay["status"]) == ("REPLAY", "run-1", "FAILED")
    assert (live["runKind"], live["runId"]) == ("LIVE", "run-1")


def test_outside_a_run_and_without_a_version_only_the_kind_is_sent(monkeypatch):
    monkeypatch.delenv("PIPELINE_VERSION")
    with patch.object(clear_api, "_execute", return_value={"markSignalsProcessed": 1}) as execute:
        mark_signals_processed(ITEMS)
    assert _calls(execute)[0][1] == {"items": ITEMS, "status": "PROCESSED", "runKind": "LIVE"}


@pytest.mark.parametrize("create, query", [
    (create_signal, clear_api.CREATE_SIGNAL),
    (create_signal_for_sync, clear_api.CREATE_SIGNAL_FOR_SYNC),
])
def test_create_signal_sends_the_version_and_the_run_id(create, query):
    sent = {"sourceId": "src", "rawData": {}}
    with patch.object(clear_api, "_execute", return_value={"createSignal": {"id": "s1"}}) as execute:
        with pipeline_run(run_id="run-1"):
            assert create(sent) == {"id": "s1"}
    assert _calls(execute) == [(query, {"input": {**sent, "pipelineVersion": "abc123", "runId": "run-1"}})]
    assert sent == {"sourceId": "src", "rawData": {}}  # the caller's dict is not changed


def test_an_older_clear_api_gets_the_old_mutations_from_then_on():
    unknown_argument = ClearApiError(
        'clear-api 400 (non-retryable): Unknown argument "runKind" on field "Mutation.markSignalsProcessed".'
    )
    unknown_field = ClearApiError(
        'clear-api 400 (non-retryable): Field "pipelineVersion" is not defined by type "CreateSignalInput".'
    )
    with patch.object(
        clear_api, "_execute",
        side_effect=[unknown_argument, {"markSignalsProcessed": 1}, {"markSignalsProcessed": 1}],
    ) as execute:
        assert mark_signals_processed(ITEMS) == 1
        assert mark_signals_processed(ITEMS) == 1
    legacy = (clear_api.MARK_SIGNALS_PROCESSED_LEGACY, {"items": ITEMS, "status": "PROCESSED"})
    assert _calls(execute)[1:] == [legacy, legacy]

    clear_api._run_inputs_supported = True
    with patch.object(clear_api, "_execute", side_effect=[unknown_field, {"createSignal": {"id": "s1"}}]) as execute:
        with pipeline_run(run_id="run-1"):
            assert create_signal({"sourceId": "src"}) == {"id": "s1"}
    assert _calls(execute)[1] == (clear_api.CREATE_SIGNAL, {"input": {"sourceId": "src"}})


class _Response:
    def __init__(self, status_code: int, body: dict):
        self.status_code = status_code
        self._body = body
        self.text = str(body)

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


@pytest.mark.parametrize("refusal", [
    # An unknown input field: HTTP 200 with a GraphQL error.
    _Response(200, {"errors": [{
        "message": 'Variable "$input" got invalid value { sourceId: "src", runId: "run-1" }; '
                   'Field "runId" is not defined by type "CreateSignalInput".',
        "extensions": {"code": "BAD_USER_INPUT"},
    }]}),
    # A validation failure: HTTP 400.
    _Response(400, {"errors": [{"message": 'Field "pipelineVersion" is not defined by type "CreateSignalInput".'}]}),
])
def test_the_fallback_reads_the_refusal_of_a_real_response(refusal, monkeypatch):
    monkeypatch.setenv("CLEAR_API_URL", "http://clear-api.test/graphql")
    monkeypatch.setenv("CLEAR_API_KEY", "key")
    monkeypatch.setattr(clear_api.time, "sleep", lambda _s: None)
    sent: list[dict] = []

    def post(_url, json, **_kwargs):
        sent.append(json["variables"]["input"])
        if "runId" in json["variables"]["input"]:
            return refusal
        return _Response(200, {"data": {"createSignal": {"id": "s1"}}})

    with patch.object(clear_api.httpx, "post", side_effect=post):
        with pipeline_run(run_id="run-1"):
            assert create_signal({"sourceId": "src"}) == {"id": "s1"}
            assert create_signal({"sourceId": "src"}) == {"id": "s1"}
    assert sent[-2:] == [{"sourceId": "src"}, {"sourceId": "src"}]
    assert clear_api._run_inputs_supported is False


def test_a_graphql_error_about_something_else_is_raised(monkeypatch):
    monkeypatch.setenv("CLEAR_API_URL", "http://clear-api.test/graphql")
    monkeypatch.setenv("CLEAR_API_KEY", "key")
    monkeypatch.setattr(clear_api.time, "sleep", lambda _s: None)
    not_found = _Response(200, {"errors": [{"message": "DataSource not found", "extensions": {"code": "NOT_FOUND"}}]})
    with patch.object(clear_api.httpx, "post", return_value=not_found):
        with pytest.raises(clear_api.ClearApiNotFound):
            create_signal({"sourceId": "src"})
    assert clear_api._run_inputs_supported is True


def test_another_clear_api_error_is_not_a_reason_to_fall_back():
    with patch.object(clear_api, "_execute", side_effect=ClearApiError("clear-api 401 (non-retryable)")):
        with pytest.raises(ClearApiError):
            mark_signals_processed(ITEMS)
    assert clear_api._run_inputs_supported is True
