"""Forward-looking scenarios for a frame (ADR-0007 §4) — analysis-native.

One retrieve-then-generate call over the frame's evidence producing the
most-likely / best-case / worst-case trajectories plus the hinge variables that
swing between them. Prose-only, coarse provenance (the RAG report union).

Native to the analysis package (``runtime`` + ``rag_helper`` + ``prompts``) — the
``analysis_scenarios`` asset fans this out per country. Pure function (provider +
RAG injected), so it unit-tests with a mocked provider and a mocked RAG fetch.
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel, Field

from clear_pipeline.defs.analysis.frame import Frame, build_rag_filters
from clear_pipeline.defs.analysis.profiles import Caps
from clear_pipeline.defs.analysis.prompts import build_evidence_user_prompt, build_system_prompt
from clear_pipeline.defs.analysis.rag_helper import fetch_rag_context
from clear_pipeline.defs.analysis.registry import provenance_stamp
from clear_pipeline.defs.analysis.runtime import run_node

_DEFAULT_CAPS = Caps(max_turns=1, max_tokens=2000)

_INSTRUCTION = (
    "Give the forward outlook as three trajectories — `most_likely`, `best_case`, "
    "`worst_case` (2–4 sentences each) — and `description` (the hinge variables "
    "that move the situation between them). Project from the evidence; do not "
    "invent events it does not support. These are scenarios, not current state."
)


class _ScenariosLLM(BaseModel):
    most_likely: str = Field(default="", description="2–4 sentences: the most likely trajectory.")
    best_case: str = Field(default="", description="2–4 sentences: a realistic best case.")
    worst_case: str = Field(default="", description="2–4 sentences: a realistic worst case.")
    description: str = Field(default="", description="The hinge variables that swing between cases.")


@dataclass(frozen=True)
class ScenarioResult:
    most_likely: str
    best_case: str
    worst_case: str
    description: str
    source_report_ids: list[str]
    status: str
    usage: dict
    cost_usd: float
    provenance: dict

    def to_payload(self) -> dict:
        """The v5 ``scenarios`` block clear-mvp reads."""
        return {
            "most_likely": self.most_likely,
            "best_case": self.best_case,
            "worst_case": self.worst_case,
            "description": self.description,
            "source_report_ids": list(self.source_report_ids),
        }


def _period_label(frame: Frame) -> str:
    start = (frame.window_start or "")[:10]
    end = (frame.window_end or "")[:10]
    return f"{start} to {end}" if end else f"since {start}" if start else "the period"


def generate_scenarios(
    *,
    provider,
    frame: Frame,
    scope_label: str,
    aggregated_context: str | None = None,
    budget=None,
    caps: Caps | None = None,
    rag_limit: int = 20,
) -> ScenarioResult:
    """Generate the frame's forward-looking scenarios. On empty retrieval the LLM
    is skipped and an empty result is returned (no spend on an unanswerable
    outlook) — matching the node analysts' thin-retrieval behaviour."""
    caps = caps or _DEFAULT_CAPS
    filters = build_rag_filters(
        frame,
        country_scope_id=(frame.location_ids[0] if len(frame.location_ids) == 1 else None),
    )
    rag = fetch_rag_context(
        query=f"{scope_label} humanitarian outlook, risks and likely trajectory",
        limit=rag_limit, filters=filters, mode="TOPICAL",
    )
    if rag.is_empty:
        return ScenarioResult(
            "", "", "", "", [], "best_effort",
            dict(getattr(provider, "total_usage", {}) or {}), 0.0,
            provenance_stamp(getattr(provider, "model", "")),
        )

    system = build_system_prompt(scope_label, _period_label(frame), aggregated_context or "")

    def build_user(_evidence):
        return build_evidence_user_prompt(
            f"OUTLOOK ({scope_label}):", _INSTRUCTION, rag.formatted_for_prompt,
            include_completeness=False,
        )

    outcome = run_node(
        provider=provider, system=system, build_user=build_user,
        schema=_ScenariosLLM, caps=caps, budget=budget,
    )
    o = outcome.output
    return ScenarioResult(
        most_likely=o.most_likely, best_case=o.best_case, worst_case=o.worst_case,
        description=o.description, source_report_ids=list(rag.contributing_report_ids),
        status=outcome.status, usage=outcome.usage, cost_usd=outcome.cost_usd,
        provenance=outcome.provenance,
    )
