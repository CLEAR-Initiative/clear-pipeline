"""Unit tests for the signal classifier entry point (providers/signal_classifier.py).
Jev is the only classifier (mocked — no network); a Jev failure propagates as
JevError, there is no local fallback."""

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


def test_jev_result_is_returned(monkeypatch):
    monkeypatch.setattr(sc, "classify_with_jev", lambda **k: _clf("jev"))
    out = sc.classify_signal("flood", None, 4)
    assert out.disaster_types == ["jev"]


def test_jev_error_propagates_with_no_fallback(monkeypatch):
    # No local fallback: a Jev failure must surface as JevError so the caller
    # (the signals drain) can requeue the signal.
    def boom(**k):
        raise JevError("openrouter down")

    monkeypatch.setattr(sc, "classify_with_jev", boom)
    with pytest.raises(JevError):
        sc.classify_signal("flood", None, 4)


def test_stats_count_jev_success_and_failure(monkeypatch):
    calls = {"n": 0}

    def flaky(**k):
        calls["n"] += 1
        if calls["n"] == 1:
            return _clf("jev")
        raise JevError("down")

    monkeypatch.setattr(sc, "classify_with_jev", flaky)
    sc.classify_signal("flood", None, 4)   # jev ok
    with pytest.raises(JevError):
        sc.classify_signal("quake", None, 4)   # jev fails → counted, re-raised
    assert sc.classifier_stats_snapshot() == {"jev": 1, "jev_failed": 1}
