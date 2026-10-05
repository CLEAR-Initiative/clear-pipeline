"""Unit tests for the Jev disaster-type classifier (providers/jev.py). HTTP is
mocked — no network, no torch (the taxonomy maps are plain JSON reads)."""

import json

import pytest

from clear_pipeline.providers import jev
from clear_pipeline.providers.classify import DEFAULT_FALLBACK_SEVERITY


class _FakeResp:
    def __init__(self, payload: dict, status_ok: bool = True):
        self._payload = payload
        self._ok = status_ok

    def raise_for_status(self):
        if not self._ok:
            raise RuntimeError("HTTP 500")

    def json(self):
        return self._payload


def _answers(choice="fl", confidence=0.9, noul=0.95) -> dict:
    return {"answers": {
        "glide": {"type": "choice", "choice": choice, "confidence": confidence, "probabilities": {}},
        "relevant": {"type": "noul", "noul": noul},
    }}


@pytest.fixture(autouse=True)
def _key(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")


def _patch_post(monkeypatch, resp):
    captured = {}

    def fake_post(url, headers=None, json=None, timeout=None):  # noqa: A002
        captured["url"] = url
        captured["headers"] = headers
        captured["body"] = json
        return resp

    monkeypatch.setattr(jev.requests, "post", fake_post)
    return captured


def test_maps_choice_to_code_and_levels(monkeypatch):
    _patch_post(monkeypatch, _FakeResp(_answers(choice="fl", noul=0.95)))
    c = jev.classify_with_jev(title="Rivers burst their banks", description=None, source_severity=4)
    assert c.disaster_types == ["fl"]
    assert c.type_level_1 == "natural hazard"
    assert c.type_level_2 == "flood"
    assert c.type_level_3 == "flood"
    # relevance is the NOUL (is-incident), not the choice confidence.
    assert c.relevance == 0.95
    assert c.severity == 4  # source severity passes through


def test_default_severity_when_source_missing(monkeypatch):
    _patch_post(monkeypatch, _FakeResp(_answers()))
    c = jev.classify_with_jev(title="x", description=None, source_severity=None)
    assert c.severity == DEFAULT_FALLBACK_SEVERITY


def test_missing_choice_defaults_to_ot(monkeypatch):
    _patch_post(monkeypatch, _FakeResp(_answers(choice=None)))
    c = jev.classify_with_jev(title="unclear", description=None)
    assert c.disaster_types == ["ot"]


def test_sends_two_parallel_questions_over_all_codes(monkeypatch):
    captured = _patch_post(monkeypatch, _FakeResp(_answers()))
    jev.classify_with_jev(title="flood", description="villages submerged")
    body = captured["body"]
    assert body["model"] == jev.JEV_MODEL
    assert body["state"]["text"] == "flood villages submerged"
    qs = body["questions"]
    assert qs["glide"]["type"] == "choice" and qs["relevant"]["type"] == "noul"
    assert len(qs["glide"]["criteria"]) == 52  # all GLIDE codes offered
    assert captured["headers"]["Authorization"] == "Bearer sk-or-test"


def test_http_error_raises_jeverror(monkeypatch):
    _patch_post(monkeypatch, _FakeResp(_answers(), status_ok=False))
    with pytest.raises(jev.JevError):
        jev.classify_with_jev(title="x", description=None)


def test_malformed_response_raises_jeverror(monkeypatch):
    _patch_post(monkeypatch, _FakeResp({"answers": {}}))  # no glide/relevant keys
    with pytest.raises(jev.JevError):
        jev.classify_with_jev(title="x", description=None)


def test_missing_api_key_raises_jeverror(monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    # Shouldn't even reach the network.
    monkeypatch.setattr(jev.requests, "post", lambda *a, **k: pytest.fail("called network without key"))
    with pytest.raises(jev.JevError):
        jev.classify_with_jev(title="x", description=None)
