"""Unit tests for the Jev/MiniLM selector (providers/signal_classifier.py).
Both classifiers are mocked — no network, no torch."""

import pytest

from clear_pipeline.providers import signal_classifier as sc
from clear_pipeline.providers.classify import SignalClassification
from clear_pipeline.providers.jev import JevError


def _clf(code: str) -> SignalClassification:
    return SignalClassification(disaster_types=[code], relevance=0.9, severity=3, summary="s")


@pytest.fixture(autouse=True)
def _reset_stats():
    sc.reset_classifier_stats()
    yield
    sc.reset_classifier_stats()


def test_jev_primary_used_when_enabled(monkeypatch):
    monkeypatch.setattr(sc.settings, "signal_classifier", "jev")
    monkeypatch.setattr(sc, "classify_with_jev", lambda **k: _clf("jev"))
    monkeypatch.setattr(sc, "classify_locally", lambda **k: pytest.fail("MiniLM should not run"))
    out = sc.classify_signal("flood", None, 4)
    assert out.disaster_types == ["jev"]


def test_falls_back_to_minilm_on_jev_error(monkeypatch):
    monkeypatch.setattr(sc.settings, "signal_classifier", "jev")
    def boom(**k):
        raise JevError("openrouter down")
    monkeypatch.setattr(sc, "classify_with_jev", boom)
    called = {}
    def fake_local(**k):
        called["yes"] = True
        return _clf("minilm")
    monkeypatch.setattr(sc, "classify_locally", fake_local)
    out = sc.classify_signal("flood", None, 4)
    assert out.disaster_types == ["minilm"] and called.get("yes")


def test_minilm_forced_skips_jev(monkeypatch):
    monkeypatch.setattr(sc.settings, "signal_classifier", "minilm")
    monkeypatch.setattr(sc, "classify_with_jev", lambda **k: pytest.fail("Jev should not run when forced to minilm"))
    monkeypatch.setattr(sc, "classify_locally", lambda **k: _clf("minilm"))
    out = sc.classify_signal("flood", None, None)
    assert out.disaster_types == ["minilm"]


def test_stats_count_jev_success_and_fallback(monkeypatch):
    monkeypatch.setattr(sc.settings, "signal_classifier", "jev")
    monkeypatch.setattr(sc, "classify_locally", lambda **k: _clf("minilm"))
    calls = {"n": 0}

    def flaky(**k):
        calls["n"] += 1
        if calls["n"] == 1:
            return _clf("jev")
        raise JevError("down")

    monkeypatch.setattr(sc, "classify_with_jev", flaky)
    sc.classify_signal("flood", None, 4)   # jev ok
    sc.classify_signal("quake", None, 4)   # jev fails → fallback
    assert sc.classifier_stats_snapshot() == {"jev": 1, "fallback": 1}
