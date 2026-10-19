"""Unit tests for the Jev disaster-type classifier (providers/jev.py). HTTP is
mocked — no network, no torch (the taxonomy maps are plain JSON reads)."""

import httpx
import pytest

from clear_pipeline.providers import jev


class _FakeResp:
    def __init__(self, payload: dict | None = None, status_code: int = 200, text: str = ""):
        self._payload = payload or {}
        self.status_code = status_code
        self.text = text

    def json(self):
        return self._payload


def _answers(choice="fl", confidence=0.9, probabilities=None) -> dict:
    # Jev returns a single Choice answer: the primary code, its confidence, and the
    # full probability distribution (relevance = 1 - P("none") is derived from it).
    # Default distribution = all mass on the chosen code (no "none") → relevance 1.0.
    glide = {
        "type": "choice",
        "choice": choice,
        "confidence": confidence,
        "probabilities": {choice: confidence} if probabilities is None else probabilities,
    }
    return {"answers": {"glide": glide}}


@pytest.fixture(autouse=True)
def _key(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")


@pytest.fixture(autouse=True)
def _fresh_breaker():
    """The breaker is module-level; reset it so one test's failures don't open the
    circuit for the next."""
    jev._BREAKER.record_success()
    yield
    jev._BREAKER.record_success()


def _patch_post(monkeypatch, *responses):
    """Patch httpx.post to return the given responses in sequence (the last one
    repeats). A response may be an Exception instance to raise instead."""
    captured = {"calls": 0, "url": None, "headers": None, "body": None}
    seq = list(responses)

    def fake_post(url, headers=None, json=None, timeout=None):  # noqa: A002
        captured["calls"] += 1
        captured["url"] = url
        captured["headers"] = headers
        captured["body"] = json
        item = seq[min(captured["calls"] - 1, len(seq) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(jev.httpx, "post", fake_post)
    # Keep the tests fast — no real backoff sleeps.
    monkeypatch.setattr(jev.time, "sleep", lambda *_a, **_k: None)
    return captured


def test_maps_choice_to_code_and_levels(monkeypatch):
    _patch_post(monkeypatch, _FakeResp(_answers(choice="fl", probabilities={"fl": 1.0})))
    c = jev.classify_with_jev(title="Rivers burst their banks", description=None, source_severity=4)
    assert c.disaster_types == ["fl"]
    assert c.type_level_1 == "natural hazard"
    assert c.type_level_2 == "flood"
    assert c.type_level_3 == "flood"
    # relevance = 1 - P("none"); all mass on a real code → 1.0.
    assert c.relevance == 1.0
    assert c.severity == 4  # source severity passes through


def test_severity_is_null_when_source_missing(monkeypatch):
    # No source severity → severity is None (not an invented default).
    _patch_post(monkeypatch, _FakeResp(_answers()))
    c = jev.classify_with_jev(title="x", description=None, source_severity=None)
    assert c.severity is None


def test_missing_choice_raises_jeverror(monkeypatch):
    # A missing `choice` is a malformed answer — raise so the caller falls back,
    # rather than silently classifying a real "ot".
    _patch_post(monkeypatch, _FakeResp(_answers(choice=None)))
    with pytest.raises(jev.JevError):
        jev.classify_with_jev(title="unclear", description=None)


def test_model_returned_ot_is_accepted(monkeypatch):
    # A genuine "ot" from the model is a valid classification, distinct from a
    # missing choice.
    _patch_post(monkeypatch, _FakeResp(_answers(choice="ot")))
    c = jev.classify_with_jev(title="unclear emergency", description=None)
    assert c.disaster_types == ["ot"]


# ── none / low-confidence → "other" catch-all (anti-misclassification) ───────

def test_glide_criteria_includes_the_none_option():
    assert jev.NONE_CHOICE in jev._glide_criteria()


def test_none_choice_is_recorded_as_natural_other(monkeypatch):
    # Model declined (no specific type fits) → stored as "ot", not force-fit.
    _patch_post(monkeypatch, _FakeResp(_answers(choice=jev.NONE_CHOICE, confidence=0.9)))
    c = jev.classify_with_jev(title="some ambiguous incident", description=None)
    assert c.disaster_types == ["ot"]
    assert c.type_level_1 == "natural hazard"


def test_low_confidence_specific_code_demoted_to_other(monkeypatch):
    # A weakly-picked NATURAL code (fl, conf 0.1 < floor) → "ot", preserving L1.
    monkeypatch.setattr(jev.settings, "signal_jev_min_code_confidence", 0.5)
    _patch_post(monkeypatch, _FakeResp(_answers(choice="fl", confidence=0.1)))
    c = jev.classify_with_jev(title="maybe a flood?", description=None)
    assert c.disaster_types == ["ot"]
    assert c.type_level_1 == "natural hazard"


def test_low_confidence_conflict_code_demoted_to_po(monkeypatch):
    # A weakly-picked CONFLICT code (ba, conf 0.1 < floor) keeps its L1 → "po".
    monkeypatch.setattr(jev.settings, "signal_jev_min_code_confidence", 0.5)
    _patch_post(monkeypatch, _FakeResp(_answers(choice="ba", confidence=0.1)))
    c = jev.classify_with_jev(title="possible clash", description=None)
    assert c.disaster_types == ["po"]
    assert c.type_level_1 == "conflict"


def test_confident_specific_code_is_kept(monkeypatch):
    # Above the floor → the specific code stands.
    monkeypatch.setattr(jev.settings, "signal_jev_min_code_confidence", 0.5)
    _patch_post(monkeypatch, _FakeResp(_answers(choice="fl", confidence=0.9)))
    c = jev.classify_with_jev(title="rivers burst their banks", description=None)
    assert c.disaster_types == ["fl"]


def test_missing_glide_answer_raises_jeverror(monkeypatch):
    # The glide answer is required — a response without it is malformed.
    _patch_post(monkeypatch, _FakeResp({"answers": {"not_glide": {}}}))
    with pytest.raises(jev.JevError):
        jev.classify_with_jev(title="flood", description=None)


def test_sends_single_choice_question_over_all_codes(monkeypatch):
    captured = _patch_post(monkeypatch, _FakeResp(_answers()))
    jev.classify_with_jev(title="flood", description="villages submerged")
    body = captured["body"]
    assert body["model"] == jev.JEV_MODEL
    assert body["state"]["text"] == "flood villages submerged"
    qs = body["questions"]
    # Single Choice question now — the `relevant` Noul was dropped; relevance comes
    # from the glide distribution.
    assert set(qs) == {"glide"}
    assert qs["glide"]["type"] == "choice"
    criteria = qs["glide"]["criteria"]
    assert len(criteria) == 53  # 52 GLIDE codes + the explicit "none" no-match option
    assert jev.NONE_CHOICE in criteria
    assert captured["headers"]["Authorization"] == "Bearer sk-or-test"


# ── multilabel + distribution-derived relevance ──────────────────────────────

def test_multilabel_adds_secondary_codes_above_threshold(monkeypatch):
    # A flood that also triggers landslides: both clear the multilabel floor; a
    # trace earthquake probability does not. Primary (Choice) first, extras by prob.
    _patch_post(monkeypatch, _FakeResp(_answers(
        choice="fl", confidence=0.55,
        probabilities={"fl": 0.55, "ls": 0.30, "eq": 0.05, "none": 0.0},
    )))
    c = jev.classify_with_jev(title="Floods trigger landslides", description=None)
    assert c.disaster_types == ["fl", "ls"]  # eq (0.05) below the 0.15 floor
    # Levels reflect the PRIMARY code.
    assert c.type_level_2 == "flood"
    assert c.relevance == 1.0  # P("none") == 0


def test_relevance_is_one_minus_p_none(monkeypatch):
    _patch_post(monkeypatch, _FakeResp(_answers(
        choice="fl", confidence=0.4, probabilities={"fl": 0.4, "none": 0.6},
    )))
    c = jev.classify_with_jev(title="maybe about a flood", description=None)
    assert c.relevance == pytest.approx(0.4)


def test_relevance_falls_back_to_confidence_without_distribution(monkeypatch):
    # An older Jev build returns no probabilities → relevance = code confidence,
    # and disaster_types is just the primary.
    _patch_post(monkeypatch, _FakeResp(_answers(choice="fl", confidence=0.7, probabilities={})))
    c = jev.classify_with_jev(title="flood", description=None)
    assert c.disaster_types == ["fl"]
    assert c.relevance == pytest.approx(0.7)


def test_demoted_primary_keeps_single_other_even_with_extras(monkeypatch):
    # A weak primary (below the floor) is demoted to "other"; the specific codes in
    # the distribution are then untrustworthy too, so we keep a single label.
    monkeypatch.setattr(jev.settings, "signal_jev_min_code_confidence", 0.5)
    _patch_post(monkeypatch, _FakeResp(_answers(
        choice="fl", confidence=0.1, probabilities={"fl": 0.1, "ls": 0.3, "none": 0.0},
    )))
    c = jev.classify_with_jev(title="unclear hazard", description=None)
    assert c.disaster_types == ["ot"]


def test_non_retryable_4xx_raises_without_retry(monkeypatch):
    captured = _patch_post(monkeypatch, _FakeResp(status_code=400, text="bad request"))
    with pytest.raises(jev.JevError):
        jev.classify_with_jev(title="x", description=None)
    assert captured["calls"] == 1  # 400 is a caller error — no retry


def test_retries_on_429_then_succeeds(monkeypatch):
    captured = _patch_post(
        monkeypatch,
        _FakeResp(status_code=429, text="rate limited"),
        _FakeResp(_answers(choice="eq")),
    )
    c = jev.classify_with_jev(title="quake", description=None)
    assert c.disaster_types == ["eq"]
    assert captured["calls"] == 2  # retried once after the 429


def test_retries_on_5xx_then_exhausts(monkeypatch):
    captured = _patch_post(monkeypatch, _FakeResp(status_code=503, text="unavailable"))
    with pytest.raises(jev.JevError):
        jev.classify_with_jev(title="x", description=None)
    assert captured["calls"] == jev._RETRIES  # all attempts used


def test_timeout_is_retried(monkeypatch):
    captured = _patch_post(
        monkeypatch,
        httpx.TimeoutException("timed out"),
        _FakeResp(_answers(choice="dr")),
    )
    c = jev.classify_with_jev(title="drought", description=None)
    assert c.disaster_types == ["dr"]
    assert captured["calls"] == 2


def test_malformed_response_raises_jeverror(monkeypatch):
    _patch_post(monkeypatch, _FakeResp({"answers": {}}))  # no glide key
    with pytest.raises(jev.JevError):
        jev.classify_with_jev(title="x", description=None)


def test_missing_answers_key_raises_jeverror(monkeypatch):
    _patch_post(monkeypatch, _FakeResp({"not_answers": {}}))
    with pytest.raises(jev.JevError):
        jev.classify_with_jev(title="x", description=None)


def test_missing_api_key_raises_jeverror(monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    # Shouldn't even reach the network.
    monkeypatch.setattr(jev.httpx, "post", lambda *a, **k: pytest.fail("called network without key"))
    with pytest.raises(jev.JevError):
        jev.classify_with_jev(title="x", description=None)


def test_circuit_opens_after_repeated_failures(monkeypatch):
    captured = _patch_post(monkeypatch, _FakeResp(status_code=503, text="down"))
    threshold = jev._BREAKER._threshold
    # Drive enough consecutive failures to open the breaker.
    for _ in range(threshold):
        with pytest.raises(jev.JevError):
            jev.classify_with_jev(title="x", description=None)
    calls_before = captured["calls"]
    # Breaker is now open — the next call fast-fails without touching the network.
    with pytest.raises(jev.JevError, match="circuit"):
        jev.classify_with_jev(title="x", description=None)
    assert captured["calls"] == calls_before  # no new HTTP attempt


def test_success_resets_failure_count(monkeypatch):
    # A success midway keeps the breaker from opening on the next failure.
    _patch_post(monkeypatch, _FakeResp(status_code=503))
    with pytest.raises(jev.JevError):
        jev.classify_with_jev(title="x", description=None)
    _patch_post(monkeypatch, _FakeResp(_answers()))
    jev.classify_with_jev(title="x", description=None)
    assert jev._BREAKER._failures == 0
