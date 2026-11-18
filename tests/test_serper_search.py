"""Serper (Google SERP) web-search provider: organic mapping, locale/freshness
params, errors — plus the WEB_SEARCH_PROVIDER resolver. httpx mocked."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from clear_pipeline.providers import serper_search as mod
from clear_pipeline.providers.serper_search import (
    SerperSearchError,
    serper_scrape,
    serper_search,
)
from clear_pipeline.providers.web_search import WebSearchError, resolve_web_search

_SAMPLE = {
    "organic": [
        {"title": "WHO Sudan", "link": "https://who.int/sudan", "snippet": "10,000 displaced", "position": 1},
        {"title": "MSF", "link": "https://www.msf.org/y", "snippet": "flooding hit the state", "position": 2},
        {"title": "no-snippet", "link": "https://example.org/z", "position": 3},
        {"title": "no-link", "snippet": "dropped", "position": 4},
    ]
}


def _resp(payload):
    r = MagicMock()
    r.raise_for_status = MagicMock()
    r.json = MagicMock(return_value=payload)
    return r


def test_missing_key_raises(monkeypatch):
    monkeypatch.delenv("SERPER_API_KEY", raising=False)
    with pytest.raises(SerperSearchError):
        serper_search("q")


def test_maps_organic_to_results_with_host_sitename(monkeypatch):
    monkeypatch.setenv("SERPER_API_KEY", "sk")
    with patch.object(mod.httpx, "post", return_value=_resp(_SAMPLE)) as post:
        results = serper_search("displacement sudan", max_urls=5)

    # last two dropped: missing snippet keeps the result (empty snippets), missing link dropped
    assert [r.url for r in results] == ["https://who.int/sudan", "https://www.msf.org/y", "https://example.org/z"]
    assert results[0].snippets == ["10,000 displaced"]
    assert results[0].site_name == "who.int"
    assert results[1].site_name == "msf.org"  # www. stripped
    assert results[2].snippets == []  # no snippet → empty list, still returned
    # request body
    body = post.call_args.kwargs["json"]
    assert body["q"] == "displacement sudan" and body["num"] == 5
    assert post.call_args.kwargs["headers"]["X-API-KEY"] == "sk"


def test_locale_and_freshness_params(monkeypatch):
    monkeypatch.setenv("SERPER_API_KEY", "sk")
    monkeypatch.setenv("SERPER_GL", "SA")
    monkeypatch.setenv("SERPER_HL", "AR")
    with patch.object(mod.httpx, "post", return_value=_resp(_SAMPLE)) as post:
        serper_search("الفيضانات", freshness="pm")
    body = post.call_args.kwargs["json"]
    assert body["gl"] == "sa" and body["hl"] == "ar"  # from env, lowercased
    assert body["tbs"] == "qdr:m"  # pm → month


def test_explicit_gl_hl_override_env(monkeypatch):
    monkeypatch.setenv("SERPER_API_KEY", "sk")
    monkeypatch.setenv("SERPER_GL", "us")
    with patch.object(mod.httpx, "post", return_value=_resp({"organic": []})) as post:
        serper_search("q", gl="ae", hl="ar")
    body = post.call_args.kwargs["json"]
    assert body["gl"] == "ae" and body["hl"] == "ar"


def test_http_error_raises_serper_error(monkeypatch):
    monkeypatch.setenv("SERPER_API_KEY", "sk")
    with patch.object(mod.httpx, "post", side_effect=httpx.ConnectError("boom")):
        with pytest.raises(SerperSearchError):
            serper_search("q")


def test_empty_organic_returns_empty(monkeypatch):
    monkeypatch.setenv("SERPER_API_KEY", "sk")
    with patch.object(mod.httpx, "post", return_value=_resp({})):
        assert serper_search("q") == []


# ── scrape enrichment ───────────────────────────────────────────────

def test_scrape_flag_replaces_snippet_with_full_content(monkeypatch):
    monkeypatch.setenv("SERPER_API_KEY", "sk")
    monkeypatch.setenv("SERPER_SCRAPE", "true")

    def _post(url, *, json, headers, timeout):
        if url.endswith("/search"):
            return _resp(_SAMPLE)
        return _resp({"markdown": "FULL PAGE for " + json["url"]})

    with patch.object(mod.httpx, "post", side_effect=_post):
        results = serper_search("q", max_urls=5)
    # each result's snippet replaced by scraped full content
    assert results[0].snippets == ["FULL PAGE for https://who.int/sudan"]
    assert results[2].snippets == ["FULL PAGE for https://example.org/z"]  # had no SERP snippet


def test_scrape_best_effort_keeps_snippet_on_failure(monkeypatch):
    monkeypatch.setenv("SERPER_API_KEY", "sk")

    def _post(url, *, json, headers, timeout):
        if url.endswith("/search"):
            return _resp(_SAMPLE)
        raise httpx.ConnectError("scrape down")

    with patch.object(mod.httpx, "post", side_effect=_post):
        results = serper_search("q", scrape=True, max_urls=5)
    # scrape failed → original SERP snippet retained, search still succeeds
    assert results[0].snippets == ["10,000 displaced"]


def test_scrape_off_by_default_makes_no_scrape_call(monkeypatch):
    monkeypatch.setenv("SERPER_API_KEY", "sk")
    monkeypatch.delenv("SERPER_SCRAPE", raising=False)
    with patch.object(mod.httpx, "post", return_value=_resp(_SAMPLE)) as post:
        serper_search("q")
    # only the search call, never scrape.serper.dev
    assert all(c.args[0].endswith("/search") for c in post.call_args_list)


def test_serper_scrape_defensive_parse_and_cap(monkeypatch):
    monkeypatch.setenv("SERPER_API_KEY", "sk")
    with patch.object(mod.httpx, "post", return_value=_resp({"text": "x" * 50})):
        assert serper_scrape("https://a.com", max_chars=10) == "x" * 10
    # unrecognised shape → None (best-effort)
    with patch.object(mod.httpx, "post", return_value=_resp({"weird": "nope"})):
        assert serper_scrape("https://a.com") is None


# ── resolver ────────────────────────────────────────────────────────

def test_resolver_defaults_to_serper(monkeypatch):
    monkeypatch.delenv("WEB_SEARCH_PROVIDER", raising=False)
    client, key_env, name = resolve_web_search()
    assert name == "serper" and key_env == "SERPER_API_KEY"
    assert client is serper_search


def test_resolver_selects_brave_explicitly(monkeypatch):
    monkeypatch.setenv("WEB_SEARCH_PROVIDER", "brave")
    _client, key_env, name = resolve_web_search()
    assert name == "brave" and key_env == "BRAVE_SEARCH_API_KEY"


def test_resolver_unknown_falls_back_to_serper(monkeypatch):
    monkeypatch.setenv("WEB_SEARCH_PROVIDER", "nope")
    _client, key_env, name = resolve_web_search()
    assert name == "serper" and key_env == "SERPER_API_KEY"


def test_serper_error_is_a_web_search_error():
    assert issubclass(SerperSearchError, WebSearchError)
