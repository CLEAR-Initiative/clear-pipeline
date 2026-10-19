"""A deleted/superseded entity must not poison the translation queue: a
NOT_FOUND GraphQL error from clear-api resolves to None (canonical getters) and
is never retried (_execute). Regression for the translate_job 'Crisis not found'
incident — clear-api's crisis resolver throws NOT_FOUND where peers return null."""

from unittest.mock import patch

import pytest

from clear_pipeline.providers import clear_api
from clear_pipeline.providers.clear_api import GraphQLErrors


def _not_found(path: str = "crisis") -> GraphQLErrors:
    return GraphQLErrors([
        {"message": f"{path.title()} not found", "path": [path], "extensions": {"code": "NOT_FOUND"}},
    ])


class _Resp:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.text = ""

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def test_get_crisis_canonical_returns_none_on_not_found():
    # clear-api's crisis resolver throws NOT_FOUND → the getter must yield None
    # so the translate drain drops the stale queue rows (not re-fail forever).
    with patch.object(clear_api, "_execute", side_effect=_not_found("crisis")):
        assert clear_api.get_crisis_canonical("gone-id") is None


def test_every_canonical_getter_tolerates_not_found():
    getters = [
        (clear_api.get_crisis_canonical, "crisis"),
        (clear_api.get_event_canonical, "event"),
        (clear_api.get_location_canonical, "location"),
        (clear_api.get_situation_canonical, "situationAnalysisById"),
        (clear_api.get_analysis_canonical, "analysisById"),
        (clear_api.get_ground_message_canonical, "groundMessageForTranslation"),
    ]
    for fn, path in getters:
        with patch.object(clear_api, "_execute", side_effect=_not_found(path)):
            assert fn("gone-id") is None, f"{fn.__name__} should return None on NOT_FOUND"


def test_execute_allow_missing_reraises_other_graphql_errors():
    boom = GraphQLErrors([{"message": "boom", "extensions": {"code": "INTERNAL_SERVER_ERROR"}}])
    with patch.object(clear_api, "_execute", side_effect=boom):
        with pytest.raises(GraphQLErrors):
            clear_api.get_crisis_canonical("x")


def test_execute_does_not_retry_not_found(monkeypatch):
    # NOT_FOUND is permanent — one attempt, no backoff sleeps.
    calls = {"n": 0}

    def fake_post(*_a, **_k):
        calls["n"] += 1
        return _Resp({"errors": [{"message": "Crisis not found", "extensions": {"code": "NOT_FOUND"}}]})

    monkeypatch.setattr(clear_api.httpx, "post", fake_post)
    monkeypatch.setattr(clear_api.time, "sleep", lambda *_a, **_k: pytest.fail("NOT_FOUND must not be retried"))
    monkeypatch.setenv("CLEAR_API_URL", "http://x/graphql")
    monkeypatch.setenv("CLEAR_API_KEY", "k")

    with pytest.raises(GraphQLErrors):
        clear_api._execute("query { crisis { id } }", {"id": "gone"})
    assert calls["n"] == 1  # no retries


def test_execute_still_retries_transient_graphql_errors(monkeypatch):
    # A non-NOT_FOUND GraphQL error still retries (transient server state).
    calls = {"n": 0}

    def fake_post(*_a, **_k):
        calls["n"] += 1
        return _Resp({"errors": [{"message": "lock conflict", "extensions": {"code": "CONFLICT"}}]})

    monkeypatch.setattr(clear_api.httpx, "post", fake_post)
    monkeypatch.setattr(clear_api.time, "sleep", lambda *_a, **_k: None)
    monkeypatch.setenv("CLEAR_API_URL", "http://x/graphql")
    monkeypatch.setenv("CLEAR_API_KEY", "k")

    with pytest.raises(GraphQLErrors):
        clear_api._execute("query { crisis { id } }", {"id": "x"}, retries=3)
    assert calls["n"] == 3  # retried the full budget
