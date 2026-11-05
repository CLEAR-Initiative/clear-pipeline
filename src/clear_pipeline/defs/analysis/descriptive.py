"""Descriptive analysis for one taxonomy node (doc §6).

A single parameterised analyst — not a bespoke generator per node. Given a
taxonomy node (its Analytical Question), a frame, and a provider, it runs one
structured RAG call through the bounded-agent runtime (:func:`runtime.run_node`),
grounding the answer in knowledge-base evidence and resolving ``[Rn]`` citations
back to report ids.

This is the unit of work the ``analysis_sub`` asset fans out over the 22 L1
nodes (a later PR); here it is a pure function (provider + RAG injected), so it
is unit-testable with a mocked provider and a mocked RAG fetch.

Single structured call by default (``caps.max_turns == 1``). On empty retrieval
the LLM is skipped entirely — an unanswerable node emits an empty, zero-
completeness finding rather than paying for a call with no evidence (matches the
situation generator's skip-on-empty and the §6 thin-retrieval trigger).
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel, Field

from .citations import resolve_bullets
from .frame import Frame, build_rag_filters
from .profiles import Caps
from .prompts import build_evidence_user_prompt, build_system_prompt
from .rag_helper import fetch_rag_context
from .registry import ProvenanceStamp, provenance_stamp
from .runtime import Status, run_node
from .schemas import CitationEnvelope, QuestionCoverage, SourcedBullet
from .taxonomy import TaxonomyNode

# Instruction appended to the evidence prompt: answer as findings, cite inline.
_MAP_INSTRUCTION = (
    "Answer the QUESTION as a list of short, standalone findings in `findings`. "
    "Ground every finding in the RETRIEVED EVIDENCE and cite inline with [Rn] "
    "markers at the end of the line. Omit anything the evidence does not support."
)

_DEFAULT_CAPS = Caps(max_turns=1, max_tokens=2000)


class DescriptiveLLM(BaseModel):
    """LLM output schema for one node's descriptive answer.

    Flat ``list[str]`` findings (nested {text, sources} objects make cheap
    models misbehave — see citations.py); ``[Rn]`` markers are resolved to
    report ids after generation. Carries the completeness self-rating (doc §8).
    """

    findings: list[str] = Field(
        default_factory=list,
        description="Bulleted findings answering the question; cite [Rn] at line end.",
    )
    completeness: float = Field(
        default=0.0, ge=0.0, le=1.0,
        description="How completely the question is answered from the evidence (0–1).",
    )
    questions_not_answered: list[str] = Field(
        default_factory=list,
        description="Parts of the question the evidence did not support.",
    )


class NodeDescriptive(QuestionCoverage, CitationEnvelope):
    """Resolved descriptive finding for one taxonomy node — the published form."""

    node_key: str
    node_name: str
    task: str
    level: int
    findings: list[SourcedBullet] = Field(default_factory=list)


@dataclass(frozen=True)
class NodeAnalysis:
    """What the fan-out asset persists per node: the finding + run accounting."""

    node_key: str
    descriptive: NodeDescriptive
    status: Status
    usage: dict
    cost_usd: float
    provenance: ProvenanceStamp
    rag_hit_count: int


def _period_label(frame: Frame) -> str:
    start = (frame.window_start or "")[:10]
    end = (frame.window_end or "")[:10]
    return f"{start} to {end}" if end else f"since {start}" if start else "the period"


def _question_block(node: TaxonomyNode) -> str:
    return f"QUESTION ({node.name}):\n{node.question}"


def _empty_result(node: TaxonomyNode) -> NodeDescriptive:
    return NodeDescriptive(
        node_key=node.key, node_name=node.name, task=node.task, level=node.level,
        findings=[], completeness=0.0, questions_not_answered=[node.question],
        source_report_ids=[], contributing_sources={},
    )


def analyse_node(
    *,
    node: TaxonomyNode,
    frame: Frame,
    scope_label: str,
    provider,
    aggregated_context: str | None = None,
    budget=None,
    caps: Caps | None = None,
    rag_limit: int = 20,
) -> NodeAnalysis:
    """Run the descriptive analyst for one taxonomy node and return the result.

    ``scope_label`` is the human area label for the frame (e.g. "Sudan"), used
    both to scope the RAG query and to frame the prompt. ``provider`` must be a
    provider dedicated to this call (one per concurrent unit — doc §7).
    """
    caps = caps or _DEFAULT_CAPS

    filters = build_rag_filters(
        frame,
        country_scope_id=(frame.location_ids[0] if len(frame.location_ids) == 1 else None),
    )
    rag = fetch_rag_context(
        query=f"{scope_label} {node.question}", limit=rag_limit, filters=filters, mode="TOPICAL",
    )

    # No evidence → skip the LLM; emit an empty, zero-completeness finding.
    if rag.is_empty:
        return NodeAnalysis(
            node_key=node.key, descriptive=_empty_result(node), status="best_effort",
            usage=dict(getattr(provider, "total_usage", {}) or {}), cost_usd=0.0,
            provenance=provenance_stamp(getattr(provider, "model", "")), rag_hit_count=0,
        )

    system = build_system_prompt(scope_label, _period_label(frame), aggregated_context or "")

    def build_user(_evidence):
        return build_evidence_user_prompt(
            _question_block(node), _MAP_INSTRUCTION, rag.formatted_for_prompt,
            include_completeness=True,
        )

    outcome = run_node(
        provider=provider, system=system, build_user=build_user,
        schema=DescriptiveLLM, caps=caps, budget=budget,
    )
    llm = outcome.output

    clean, per_bullet_ids, contributing = resolve_bullets(llm.findings, rag.hit_report_ids)
    bullets = [
        SourcedBullet(description=c, source_report_ids=ids)
        for c, ids in zip(clean, per_bullet_ids)
    ]
    source_ids = list(contributing.keys()) or rag.contributing_report_ids

    descriptive = NodeDescriptive(
        node_key=node.key, node_name=node.name, task=node.task, level=node.level,
        findings=bullets,
        completeness=llm.completeness,
        questions_not_answered=list(llm.questions_not_answered),
        source_report_ids=source_ids,
        contributing_sources=contributing,
    )
    return NodeAnalysis(
        node_key=node.key, descriptive=descriptive, status=outcome.status,
        usage=outcome.usage, cost_usd=outcome.cost_usd, provenance=outcome.provenance,
        rag_hit_count=rag.hit_count,
    )
