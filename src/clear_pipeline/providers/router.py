"""The **ingestion router** — the cost-ordered occurrence/analysis cascade
(C-1445 §4; design: docs/ingestion-router-design.md).

Given an :class:`~clear_pipeline.providers.intake_record.IntakeRecord` (the
source-agnostic, per-item artifact silver now emits, C-1445 §3), decide — per
item — whether its content is an **occurrence** (→ event branch), **analysis**
(→ context branch), **both**, or **neither**. The decision is *multilabel*:
occurrence and analysis are independent, so an item can carry both.

The cascade is ordered cheapest-first; the first gate that *decides* wins, and a
gate that cannot decide *abstains* and passes the item on:

  1. **Source kind** (§4.1) — an authored, homogeneous feed implies a fixed route
     (ACLED/GDACS → occurrence, IDMC → analysis). A relayed/heterogeneous source
     (Dataminr social forwards, free text) implies nothing, so it **abstains** and
     lets the content gates decide. Only the sources §4.1 names are pinned; the
     rest default to abstain, which is the safe choice — we never force-fit a route.
  2. **Jev occurrence/analysis** (§4.2) — a cheap System One classifier (two
     independent Nouls). Decides only when BOTH labels are confidently resolved
     (outside the abstain band); otherwise abstains to the LLM.
  3. **LLM fallback** (§4.3) — one structured LLM call, the authority, run only on
     the minority Jev could not resolve. Also backstops no-extraction sources
     (ReliefWeb, X). Always decides.

**Scope (slice 2 of C-1445).** This module is the routing *decision* only. It does
not yet rewire the medallion DAG — moving materialization/grouping onto the event
branch behind the router is slice 3 (#1536). Freshness is deliberately NOT a gate
(it governs alerting on the event branch, not routing — §4).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Callable

from pydantic import BaseModel, Field

from clear_pipeline.providers import jev
from clear_pipeline.providers.intake_record import IntakeRecord
from clear_pipeline.providers.jev import JevError, OccurrenceAnalysis
from clear_pipeline.providers.llm import LLMProvider, make_llm_provider
from clear_pipeline.signals.config import settings

OCCURRENCE = "occurrence"
ANALYSIS = "analysis"

# Which gate produced a decision — for metrics (the LLM-fallback-rate the ticket
# asks us to measure) and for debugging a route.
BY_SOURCE_KIND = "source_kind"
BY_JEV = "jev"
BY_LLM = "llm"


@dataclass(frozen=True)
class RoutingDecision:
    """The per-item routing outcome (C-1445 §4). Multilabel: ``occurrence`` and
    ``analysis`` are independent, so ``both`` and ``neither`` are valid states.
    Confidences are populated only when a probabilistic gate (Jev) decided."""

    occurrence: bool
    analysis: bool
    decided_by: str                      # BY_SOURCE_KIND | BY_JEV | BY_LLM
    occurrence_confidence: float | None = None
    analysis_confidence: float | None = None

    @property
    def labels(self) -> frozenset[str]:
        out: set[str] = set()
        if self.occurrence:
            out.add(OCCURRENCE)
        if self.analysis:
            out.add(ANALYSIS)
        return frozenset(out)

    @property
    def both(self) -> bool:
        return self.occurrence and self.analysis

    @property
    def neither(self) -> bool:
        return not self.occurrence and not self.analysis

    @property
    def routes_to_event(self) -> bool:
        """Occurrence-labelled items feed the event branch (slice 3)."""
        return self.occurrence

    @property
    def routes_to_context(self) -> bool:
        """Analysis-labelled items feed the context branch (slice 4)."""
        return self.analysis


# ── Gate 1 — source kind (§4.1) ────────────────────────────────────────────────
# Pin ONLY a source whose every output is a single, homogeneous shape: ACLED (one
# discrete conflict event per row) and GDACS (discrete disaster alerts) → occurrence;
# IDMC (displacement figures / estimates) → analysis. A source whose single output
# can carry BOTH a discrete incident AND analytics — INSO weekly reports, Dataminr
# social forwards, darfur24 articles, sudan_witness, iom_dtm_flash_alerts — must
# NOT be pinned: it abstains here so the per-item gates (§4.2/§4.3) can assign
# `both` and route it to BOTH pipelines. Pinning such a source to one route is the
# exact failure mode §4.1 warns against. GDACS is listed ready for a future gx
# adapter; it does not flow through gx today.
AUTHORED_ROUTES: dict[str, frozenset[str]] = {
    "acled": frozenset({OCCURRENCE}),
    "gdacs": frozenset({OCCURRENCE}),
    "idmc": frozenset({ANALYSIS}),
}

JevFn = Callable[..., OccurrenceAnalysis]


def gate_source_kind(record: IntakeRecord) -> RoutingDecision | None:
    """Gate 1: a fixed route for authored homogeneous feeds, else abstain (None)."""
    route = AUTHORED_ROUTES.get(record.source)
    if route is None:
        return None
    return RoutingDecision(
        occurrence=OCCURRENCE in route,
        analysis=ANALYSIS in route,
        decided_by=BY_SOURCE_KIND,
    )


# ── Gate 2 — Jev occurrence/analysis (§4.2) ────────────────────────────────────
def gate_jev(
    record: IntakeRecord,
    *,
    jev_fn: JevFn = jev.classify_occurrence_analysis,
) -> RoutingDecision | None:
    """Gate 2: decide via Jev when BOTH labels are confidently resolved; else
    abstain (None → LLM). A Jev failure is an abstain, never a raise — routing
    always has the LLM backstop."""
    try:
        oa = jev_fn(record.title, record.lead, event_span=record.event_span)
    except JevError:
        return None

    hi = settings.router_assert_threshold
    lo = settings.router_deny_threshold
    # A label is "uncertain" when its probability sits in the middle band — neither
    # a confident yes (≥ hi) nor a confident no (≤ lo). Jev may only decide when
    # NEITHER label is uncertain; any uncertainty falls through to the LLM.
    if (lo < oa.occurrence_p < hi) or (lo < oa.analysis_p < hi):
        return None
    return RoutingDecision(
        occurrence=oa.occurrence_p >= hi,
        analysis=oa.analysis_p >= hi,
        decided_by=BY_JEV,
        occurrence_confidence=oa.occurrence_p,
        analysis_confidence=oa.analysis_p,
    )


# ── Gate 3 — LLM fallback (§4.3) ───────────────────────────────────────────────
class _RouteLabels(BaseModel):
    """Structured multilabel output of the LLM fallback — same contract as Jev."""

    occurrence: bool = Field(
        description="True iff the item reports a discrete event at an identifiable "
        "time and place (not commentary, a cumulative figure or a forecast)."
    )
    analysis: bool = Field(
        description="True iff the item provides analysis, context, a trend or a "
        "cumulative/total-to-date assessment (independent of occurrence)."
    )


_ROUTER_PROMPT_VERSION = "router-occ-analysis-v1"
_ROUTER_SYSTEM = (
    "You route humanitarian items. Judge the two labels INDEPENDENTLY — a single "
    "item can be BOTH:\n"
    "- occurrence=true if the item REPORTS A DISCRETE EVENT that happened at an "
    "identifiable time and place (an attack, a flood, a displacement wave, an "
    "outbreak), EVEN IF it also carries analysis. A bounded start ≈ end span is a "
    "strong cue.\n"
    "- analysis=true if the item provides CONTEXT, a TREND, a CUMULATIVE/total-to-"
    "date figure, an explainer or a forecast, EVEN IF it also reports a discrete "
    "event. An open-ended / cumulative span is a strong cue.\n"
    "Set BOTH true for an item that does both (a sitrep announcing a new "
    "displacement wave AND analysing the trend; many periodic source outputs, e.g. "
    "INSO reports, are both). Set both false only when neither holds. "
    "Recency/freshness is NOT a factor. High importance or severity does NOT make "
    "something an occurrence — a cumulative '500k displaced since January' is "
    "analysis, not an occurrence."
)


def _surface_text(record: IntakeRecord) -> str:
    """The bounded surface the content gates read — never the full document."""
    s = record.routing_surface()
    return "\n".join(
        [
            f"Source: {s['source']}",
            f"Title: {s['title'] or ''}",
            f"Lead: {s['lead'] or ''}",
            f"Event start: {s['event_start'] or 'unknown'}",
            f"Event end: {s['event_end'] or 'unknown'}",
        ]
    )


def gate_llm(record: IntakeRecord, *, llm: LLMProvider | None = None) -> RoutingDecision:
    """Gate 3: the authority. One structured LLM call; always decides."""
    provider = llm or make_llm_provider("signal")
    out = provider.complete_structured(
        system=_ROUTER_SYSTEM,
        user=_surface_text(record),
        schema=_RouteLabels,
        max_tokens=256,
        cache_key=_ROUTER_PROMPT_VERSION,
    )
    return RoutingDecision(
        occurrence=bool(out.occurrence),
        analysis=bool(out.analysis),
        decided_by=BY_LLM,
    )


# ── Metrics — the ticket asks us to measure the LLM fallback rate/cost ─────────
@dataclass
class RouterMetrics:
    """Accumulates which gate decided each item, so a batch run can report the
    LLM-fallback rate (and thus the added LLM cost) the router introduces (C-1445
    slice 2 acceptance criterion). Latency/token cost is the provider's own usage
    accounting (``LLMProvider.total_usage``); this tracks call *counts*."""

    counts: Counter = field(default_factory=Counter)
    total: int = 0

    def record(self, decision: RoutingDecision) -> None:
        self.counts[decision.decided_by] += 1
        self.total += 1

    @property
    def llm_fallback_rate(self) -> float:
        return self.counts[BY_LLM] / self.total if self.total else 0.0

    @property
    def jev_decide_rate(self) -> float:
        return self.counts[BY_JEV] / self.total if self.total else 0.0

    def as_dict(self) -> dict:
        return {
            "total": self.total,
            "by_gate": dict(self.counts),
            "llm_fallback_rate": round(self.llm_fallback_rate, 4),
        }


def route(
    record: IntakeRecord,
    *,
    jev_fn: JevFn = jev.classify_occurrence_analysis,
    llm: LLMProvider | None = None,
    metrics: RouterMetrics | None = None,
) -> RoutingDecision:
    """Run the full §4 cascade for one item. ``jev_fn`` and ``llm`` are injectable
    for tests and so the DAG asset (slice 3) can share one provider across a batch."""
    decision = gate_source_kind(record)
    if decision is None:
        decision = gate_jev(record, jev_fn=jev_fn)
    if decision is None:
        decision = gate_llm(record, llm=llm)
    if metrics is not None:
        metrics.record(decision)
    return decision
