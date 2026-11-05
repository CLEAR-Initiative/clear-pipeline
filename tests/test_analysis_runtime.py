"""Analysis bounded-agent runtime + per-run budget (PR-3).

Pure logic with a fake provider — no Dagster / DB / network.
"""

import pytest
from pydantic import BaseModel

from clear_pipeline.defs.analysis import budget as budget_mod
from clear_pipeline.defs.analysis.budget import analysis_run_budget, charge, usd_for
from clear_pipeline.defs.analysis.profiles import Caps
from clear_pipeline.defs.analysis.runtime import (
    BudgetExhaustedError,
    NodeOutcome,
    run_node,
)
from clear_pipeline.providers.guardrails import RunBudget


class Out(BaseModel):
    answer: str
    completeness: float | None = None
    questions_not_answered: list[str] = []


class FakeProvider:
    """Minimal LLMProvider stand-in: returns queued outputs, tracks usage."""

    def __init__(self, model="deepseek/deepseek-v3.2", outputs=None, per_call=(100, 20)):
        self.model = model
        self.role = "analysis"
        self.provider_name = "fake"
        self.last_usage = None
        self.total_usage = {"input_tokens": 0, "output_tokens": 0}
        self._outputs = list(outputs or [Out(answer="x", completeness=0.9)])
        self._in, self._out = per_call
        self.calls = 0

    def complete_structured(self, *, system, user, schema, max_tokens=1024, **kw):
        self.calls += 1
        self.last_usage = {"input_tokens": self._in, "output_tokens": self._out}
        self.total_usage = {
            "input_tokens": self.total_usage["input_tokens"] + self._in,
            "output_tokens": self.total_usage["output_tokens"] + self._out,
        }
        return self._outputs[min(self.calls - 1, len(self._outputs) - 1)]


# ── budget / pricing ────────────────────────────────────────────────

def test_usd_for_prices_deepseek_models():
    m = {"input_tokens": 1_000_000, "output_tokens": 1_000_000}
    assert usd_for("deepseek/deepseek-v3.2", m) == pytest.approx(0.27 + 0.40)
    assert usd_for("deepseek/deepseek-v4-flash", m) == pytest.approx(0.14 + 0.28)


def test_usd_for_unknown_model_or_no_usage_is_zero():
    assert usd_for("some/unpriced-model", {"input_tokens": 9, "output_tokens": 9}) == 0.0
    assert usd_for("deepseek/deepseek-v3.2", None) == 0.0


def test_analysis_run_budget_reads_env(monkeypatch):
    monkeypatch.setenv("ANALYSIS_MAX_COST_USD_PER_RUN", "2.5")
    b = analysis_run_budget()
    assert isinstance(b, RunBudget)
    assert b.allow_more_spend()  # nothing spent yet
    b.add_spend("analysis", 3.0)
    assert not b.allow_more_spend()  # over the $2.5 cap


def test_charge_converts_last_usage_to_usd():
    p = FakeProvider(model="deepseek/deepseek-v3.2", per_call=(1_000_000, 1_000_000))
    p.complete_structured(system="s", user="u", schema=Out)  # sets last_usage
    b = RunBudget(10.0)
    spent = charge(b, p)
    assert spent == pytest.approx(0.67)
    assert b.spent == pytest.approx(0.67)
    assert b.by_role["analysis"] == pytest.approx(0.67)


# ── run_node ────────────────────────────────────────────────────────

def _build_user(_evidence):
    return "user prompt"


def test_single_call_is_the_default():
    p = FakeProvider(outputs=[Out(answer="a", completeness=0.8, questions_not_answered=["q"])])
    out = run_node(provider=p, system="s", build_user=_build_user, schema=Out, caps=Caps(max_turns=1))
    assert isinstance(out, NodeOutcome)
    assert p.calls == 1
    assert out.status == "ok"
    assert out.turns == 1
    assert out.completeness == 0.8
    assert out.questions_not_answered == ["q"]
    assert out.usage == {"input_tokens": 100, "output_tokens": 20}
    assert out.provenance["model"] == "deepseek/deepseek-v3.2"
    assert out.cost_usd == pytest.approx(usd_for("deepseek/deepseek-v3.2", out.usage))


def test_escalates_to_cap_then_best_effort():
    p = FakeProvider(outputs=[Out(answer="a", completeness=0.1)])
    out = run_node(
        provider=p, system="s", build_user=_build_user, schema=Out,
        caps=Caps(max_turns=3), meets_bar=lambda o: False,  # never satisfied
    )
    assert p.calls == 3
    assert out.turns == 3
    assert out.status == "best_effort"


def test_escalation_stops_when_bar_met():
    outs = [Out(answer="a", completeness=0.2), Out(answer="b", completeness=0.95)]
    p = FakeProvider(outputs=outs)
    out = run_node(
        provider=p, system="s", build_user=_build_user, schema=Out,
        caps=Caps(max_turns=3), meets_bar=lambda o: (o.completeness or 0) >= 0.9,
    )
    assert p.calls == 2
    assert out.turns == 2
    assert out.status == "ok"
    assert out.output.answer == "b"


def test_budget_gate_stops_further_dispatch():
    # One call costs $0.67; cap $0.5 → the 2nd dispatch is gated off.
    p = FakeProvider(model="deepseek/deepseek-v3.2", per_call=(1_000_000, 1_000_000),
                     outputs=[Out(answer="a", completeness=0.1)])
    b = RunBudget(0.5)
    out = run_node(
        provider=p, system="s", build_user=_build_user, schema=Out,
        caps=Caps(max_turns=5), budget=b, meets_bar=lambda o: False,
    )
    assert p.calls == 1
    assert out.turns == 1
    assert out.status == "best_effort"
    assert b.spent == pytest.approx(0.67)


def test_budget_exhausted_before_first_call_raises():
    p = FakeProvider()
    b = RunBudget(0.5)
    b.add_spend("other", 1.0)  # already over cap
    with pytest.raises(BudgetExhaustedError):
        run_node(provider=p, system="s", build_user=_build_user, schema=Out,
                 caps=Caps(max_turns=3), budget=b)
    assert p.calls == 0


def test_retrieve_and_sanitise_feed_the_prompt():
    seen = {}

    def retrieve(turn):
        return [f"doc{turn}"]

    def sanitise(evidence):
        return [e.upper() for e in evidence]

    def build_user(evidence):
        seen["evidence"] = list(evidence)
        return "u"

    p = FakeProvider(outputs=[Out(answer="a", completeness=1.0)])
    run_node(provider=p, system="s", build_user=build_user, schema=Out,
             caps=Caps(max_turns=1), retrieve=retrieve, sanitise=sanitise)
    assert seen["evidence"] == ["DOC1"]


def test_unknown_model_warns_once(caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger=budget_mod.logger.name):
        usd_for("mystery/model", {"input_tokens": 1, "output_tokens": 1})
    assert any("no price for model" in r.message for r in caplog.records)
