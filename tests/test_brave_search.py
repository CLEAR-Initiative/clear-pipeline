"""Brave LLM Context client (PR-6). httpx mocked."""

from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.providers import brave_search
from clear_pipeline.providers.brave_search import BraveSearchError, llm_context

_RESPONSE = {
    "grounding": {
        "generic": [
            {"url": "https://who.int/sudan", "title": "WHO Sudan", "snippets": ["10,000 displaced.", ""]},
            {"url": "https://example.com/x", "title": "X", "snippets": ["Some context."]},
            {"url": "", "title": "no-url", "snippets": ["dropped"]},
        ]
    },
    "sources": {
        "https://who.int/sudan": {"site_name": "World Health Organization"},
    },
}


def _resp():
    r = MagicMock()
    r.raise_for_status.return_value = None
    r.json.return_value = _RESPONSE
    return r


def test_missing_key_raises(monkeypatch):
    monkeypatch.delenv("BRAVE_SEARCH_API_KEY", raising=False)
    with pytest.raises(BraveSearchError):
        llm_context("floods in Sudan")


def test_parses_results_and_builds_request(monkeypatch):
    monkeypatch.setenv("BRAVE_SEARCH_API_KEY", "bk")
    with patch.object(brave_search.httpx, "get", return_value=_resp()) as get:
        results = llm_context("floods " * 300, count=80, freshness="py", max_urls=5, threshold_mode="balanced")

    # parsing: the empty-url item is dropped; snippets stripped of blanks
    assert [r.url for r in results] == ["https://who.int/sudan", "https://example.com/x"]
    assert results[0].snippets == ["10,000 displaced."]
    assert results[0].site_name == "World Health Organization"
    assert results[1].site_name is None

    # request: header + param hygiene
    _, kwargs = get.call_args
    assert kwargs["headers"]["x-subscription-token"] == "bk"
    params = kwargs["params"]
    assert len(params["q"]) <= 600          # query capped
    assert params["count"] == 50            # clamped to max
    assert params["enable_source_metadata"] == "true"
    assert params["freshness"] == "py"
    assert params["context_threshold_mode"] == "balanced"
    assert params["maximum_number_of_urls"] == 5


def test_http_error_wrapped(monkeypatch):
    import httpx

    monkeypatch.setenv("BRAVE_SEARCH_API_KEY", "bk")
    with patch.object(brave_search.httpx, "get", side_effect=httpx.ConnectError("boom")):
        with pytest.raises(BraveSearchError):
            llm_context("q")
