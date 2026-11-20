"""The ingestion router cascade (C-1445 §4; providers/router.py). Gate 1 (source
kind) → Gate 2 (Jev occurrence/analysis) → Gate 3 (LLM fallback). Jev and the LLM
are stubbed; these tests pin the *cascade* logic and the abstain band, not Jev/LLM
quality."""

import types

import pytest

from clear_pipeline.providers.intake_record import IntakeRecord
from clear_pipeline.providers.jev import JevError, OccurrenceAnalysis
from clear_pipeline.providers import router
from clear_pipeline.providers.router import (
    ANALYSIS,
    OCCURRENCE,
    RouterMetrics,
    RoutingDecision,
    gate_jev,
    gate_source_kind,
    route,
)
from clear_pipeline.signals.config import settings


def _record(source: str, *, title="t", lead="l", start=None, end=None) -> IntakeRecord:
    si = {"externalId": f"{source}:1", "title": title, "description": lead}
    if start:
        si["startDate"] = start
    if end:
        si["endDate"] = end
    return IntakeRecord.from_signal_input(si, external_id="1", source=source)


class _FakeLLM:
    """Stands in for an LLMProvider: returns fixed labels, records it was called."""

    def __init__(self, occurrence: bool, analysis: bool):
        self._occ, self._ana = occurrence, analysis
        self.calls = 0

    def complete_structured(self, *, system, user, schema, max_tokens=1024, cache_key=None, images=None):
        self.calls += 1
        return types.SimpleNamespace(occurrence=self._occ, analysis=self._ana)


def _jev(occ_p: float, ana_p: float):
    """A jev_fn stub returning fixed probabilities."""
    def _fn(title, description, *, event_span=None):
        return OccurrenceAnalysis(occurrence_p=occ_p, analysis_p=ana_p)
    return _fn


def _jev_raises(*_a, **_k):
    raise JevError("boom")


def _never_called(*_a, **_k):
    raise AssertionError("gate should not have been reached")


# ── Gate 1: source kind ────────────────────────────────────────────────────────
def test_authored_feed_decides_without_touching_jev_or_llm():
    llm = _FakeLLM(True, True)
    d = route(_record("acled"), jev_fn=_never_called, llm=llm)
    assert d.decided_by == "source_kind"
    assert (d.occurrence, d.analysis) == (True, False)
    assert d.labels == frozenset({OCCURRENCE})
    assert llm.calls == 0


def test_idmc_is_pinned_to_analysis():
    d = gate_source_kind(_record("idmc"))
    assert d is not None
    assert (d.occurrence, d.analysis) == (False, True)
    assert d.routes_to_context and not d.routes_to_event


def test_relayed_source_abstains_at_gate_one():
    # dataminr is a relay (§4.1) — Gate 1 returns None so content gates decide.
    assert gate_source_kind(_record("dataminr")) is None


def test_unknown_source_abstains_at_gate_one():
    assert gate_source_kind(_record("darfur24")) is None


def test_mixed_output_source_abstains_so_both_stays_reachable():
    # INSO (and other mixed-output sources) must NOT be pinned: one report can carry
    # an incident AND analytics, so routing is per-item (Gate 2/3), where `both` is
    # reachable. Pinning it to one route would misroute the other half.
    assert gate_source_kind(_record("inso")) is None


def test_mixed_item_routes_to_both_pipelines():
    # A single item scoring high on both labels feeds the event AND context branch
    # off the one record (C-1445 §4.2).
    llm = _FakeLLM(occurrence=True, analysis=True)
    d = route(_record("inso"), jev_fn=_jev(0.5, 0.5), llm=llm)  # Jev abstains → LLM: both
    assert d.both and d.routes_to_event and d.routes_to_context


# ── Gate 2: Jev abstain band ───────────────────────────────────────────────────
def test_jev_decides_when_both_labels_confident():
    d = gate_jev(_record("dataminr"), jev_fn=_jev(0.95, 0.05))
    assert d is not None
    assert d.decided_by == "jev"
    assert (d.occurrence, d.analysis) == (True, False)
    assert d.occurrence_confidence == 0.95


def test_jev_can_decide_both():
    d = gate_jev(_record("dataminr"), jev_fn=_jev(0.9, 0.8))
    assert d is not None and d.both
    assert d.labels == frozenset({OCCURRENCE, ANALYSIS})


def test_jev_abstains_when_a_label_is_in_the_band():
    # occurrence_p 0.5 sits inside (deny 0.4, assert 0.6) → abstain → None (→ LLM).
    assert gate_jev(_record("dataminr"), jev_fn=_jev(0.5, 0.05)) is None


def test_jev_abstains_on_jev_error_never_raises():
    assert gate_jev(_record("dataminr"), jev_fn=_jev_raises) is None


def test_jev_confident_neither_is_a_decision_not_an_abstain():
    # both clearly below deny threshold → confident "neither", Jev decides.
    d = gate_jev(_record("dataminr"), jev_fn=_jev(0.1, 0.1))
    assert d is not None and d.neither and d.decided_by == "jev"


# ── Gate 3 + full cascade ──────────────────────────────────────────────────────
def test_cascade_falls_through_to_llm_when_jev_abstains():
    llm = _FakeLLM(occurrence=True, analysis=True)
    d = route(_record("dataminr"), jev_fn=_jev(0.5, 0.5), llm=llm)
    assert d.decided_by == "llm"
    assert d.both
    assert llm.calls == 1


def test_cascade_falls_through_to_llm_on_jev_error():
    llm = _FakeLLM(occurrence=False, analysis=True)
    d = route(_record("dataminr"), jev_fn=_jev_raises, llm=llm)
    assert d.decided_by == "llm"
    assert (d.occurrence, d.analysis) == (False, True)


def test_jev_decision_short_circuits_the_llm():
    llm = _FakeLLM(True, True)
    d = route(_record("dataminr"), jev_fn=_jev(0.95, 0.02), llm=llm)
    assert d.decided_by == "jev"
    assert llm.calls == 0


# ── Metrics ────────────────────────────────────────────────────────────────────
def test_router_metrics_track_fallback_rate():
    m = RouterMetrics()
    llm = _FakeLLM(True, False)
    route(_record("acled"), jev_fn=_never_called, llm=llm, metrics=m)        # source_kind
    route(_record("dataminr"), jev_fn=_jev(0.95, 0.02), llm=llm, metrics=m)  # jev
    route(_record("dataminr"), jev_fn=_jev(0.5, 0.5), llm=llm, metrics=m)    # llm
    route(_record("dataminr"), jev_fn=_jev_raises, llm=llm, metrics=m)       # llm
    assert m.total == 4
    assert m.counts["source_kind"] == 1 and m.counts["jev"] == 1 and m.counts["llm"] == 2
    assert m.llm_fallback_rate == 0.5
    assert m.as_dict()["by_gate"]["llm"] == 2


# ── RoutingDecision helpers ────────────────────────────────────────────────────
def test_routing_decision_label_helpers():
    both = RoutingDecision(occurrence=True, analysis=True, decided_by="llm")
    assert both.both and not both.neither
    assert both.routes_to_event and both.routes_to_context

    none = RoutingDecision(occurrence=False, analysis=False, decided_by="jev")
    assert none.neither and none.labels == frozenset()


def test_assert_threshold_exceeds_deny_threshold():
    # The band only makes sense if assert > deny; guard the config invariant.
    assert settings.router_assert_threshold > settings.router_deny_threshold
