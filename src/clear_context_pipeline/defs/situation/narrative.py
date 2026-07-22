"""LLM-generated narrative components for the situation analysis.

Four components:
  2. AI summary                  (2–4 paragraph prose)
  3. context_risks               (8 sub-domains × bullets)
  4. hazards_and_vulnerabilities (bullets each)
  5. displacement                (push_factors + return_intention)

Component 6 (per-sector analysis) lives in `sectors.py` — it needs its
own prompt discipline per sector.

Each generator runs one RAG search and stamps that search's
`contributing_report_ids` onto every bullet or sub-domain it produces,
so a component's bullets always share one source set.

Prompt-cache strategy: every generator's system prompt is identical
across a single country-year generation cycle. The shared prefix
(instructions + aggregated-datapoints context) is cached by
Anthropic's prompt cache; only the per-component RAG hits + output
schema vary per call. Sequential invocation stays inside the 5-minute
cache TTL by design.

Failure isolation: any generator that raises returns its component's
empty default. One bad LLM call doesn't drop the other three.
"""

import json
import logging
from typing import Any

from pydantic import BaseModel, Field

from clear_context_pipeline.defs.situation.rag_helper import (
    RAGContext,
    fetch_rag_context,
)
from clear_context_pipeline.defs.situation.schemas import (
    AISummary,
    ContextRisks,
    DisplacementNarrative,
    HazardsAndVulnerabilities,
    RiskDomain,
    SourcedBullet,
)
from clear_context_pipeline.providers.llm import LLMProvider

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────
# LLM output schemas — decoupled from the DB-written schemas.
#
# The DB shapes (AISummary, RiskDomain, SourcedBullet …) carry
# `source_report_ids` because the dashboard needs them. But asking
# the LLM to emit report ids invites hallucination — it can't reason
# about which specific report supported which specific bullet
# reliably. Coarse-grained citation is what we ship today:
#   LLM emits bullets → we stamp all bullets with the union of
#   report ids that fed the RAG context.
# The Phase E refinement (per-bullet [R1]-style markers post-
# processed to specific ids) can layer on later without a schema
# change on the DB side.
# ────────────────────────────────────────────────────────────────────


class _AISummaryLLM(BaseModel):
    """2–4 paragraph narrative synthesis. Prose only, no bullets."""
    text: str = Field(
        description=(
            "Two to four paragraphs of prose synthesising the country's "
            "humanitarian situation for the target year. Weave the "
            "headline figures into the narrative naturally; avoid "
            "bullet lists and section headings — this reads as a "
            "briefing summary."
        ),
    )


def _risk_domain_field() -> Any:
    """Fresh Field for one context-risk domain — a flat list of bullets."""
    return Field(
        default_factory=list,
        description=(
            "Concise bulleted risks / observations for this domain. "
            "Each bullet is one sentence. Prefer 3–6 bullets. Return "
            "an empty list only when no evidence in the retrieved "
            "sources supports any claim in this domain."
        ),
    )


class _ContextRisksLLM(BaseModel):
    """Eight OCHA-style context-risk domains, each a FLAT list of bullet
    strings. Fixed order so the dashboard's tab layout stays stable.

    Flat on purpose: an earlier nested-object shape (one sub-model per
    domain) made the model intermittently return a domain as a
    JSON-encoded string instead of an object, which failed validation and
    blanked the whole component. The narrative components that never
    glitch — hazards, displacement — are all `list[str]`; this matches
    them. `source_report_ids` is attached later from the RAG set, not
    asked of the LLM."""
    demographics: list[str] = _risk_domain_field()
    political: list[str] = _risk_domain_field()
    economy: list[str] = _risk_domain_field()
    socio_culture: list[str] = _risk_domain_field()
    security: list[str] = _risk_domain_field()
    legal_policy: list[str] = _risk_domain_field()
    infrastructure: list[str] = _risk_domain_field()
    environment: list[str] = _risk_domain_field()


class _HazardsVulnerabilitiesLLM(BaseModel):
    hazards: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted list of natural / man-made hazards affecting the "
            "country during the target period. One sentence per bullet."
        ),
    )
    vulnerabilities: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted list of pre-crisis structural vulnerabilities "
            "(economic, institutional, environmental, social). One "
            "sentence per bullet."
        ),
    )


class _DisplacementLLM(BaseModel):
    push_factors: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted drivers pushing populations to displace — "
            "conflict, drought, insecurity, service collapse, etc. "
            "One sentence per bullet."
        ),
    )
    return_intention: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted signals about displaced populations' intent to "
            "return — willingness, blockers, conditions. One sentence "
            "per bullet."
        ),
    )


# ────────────────────────────────────────────────────────────────────
# Shared system-prompt scaffolding
# ────────────────────────────────────────────────────────────────────


_BASE_INSTRUCTIONS = (
    "You are a humanitarian analyst producing a situation summary for "
    "the Norwegian Refugee Council. Every claim you emit MUST be "
    "grounded in the retrieved evidence — do not invent numbers, "
    "dates, or attributions. When the evidence is thin, prefer "
    "returning fewer bullets over speculating.\n"
    "\n"
    "Rules:\n"
    "- Report every number the way the source cites it (with unit and "
    "  scale). Do not extrapolate.\n"
    "- Use the country name and admin-region names as they appear in "
    "  the sources. Preserve local naming (e.g. 'Kordofan', 'Darfur').\n"
    "- Prefer verified / reported sources over media when both cover "
    "  the same claim. If they conflict, prefer the more recent report.\n"
    "- Neutral, factual tone. No editorialising. No calls to action.\n"
)


def _build_system_prompt(country_name: str, period_label: str, agg_context: str) -> str:
    """One prompt reused across all four component calls in a run.

    Static prefix + country context: this is what Anthropic's prompt
    cache keys on. Keeping it byte-identical across the four calls
    means calls 2–4 read from cache at ~10% of the input cost.
    """
    return (
        f"{_BASE_INSTRUCTIONS}\n"
        f"---\n"
        f"COUNTRY: {country_name}\n"
        f"PERIOD: {period_label}\n"
        f"---\n"
        f"AGGREGATED HEADLINE FIGURES (cached; do not repeat back):\n"
        f"{agg_context}\n"
        f"---"
    )


def _format_aggregated_for_prompt(aggregated: dict[str, Any] | None) -> str:
    """Render the aggregated-datapoints blob in a compact form the
    LLM can reason over. We serialise the whole `data` map because
    the model may want to cross-reference sector PIN figures against
    narrative claims about needs."""
    if not aggregated:
        return "(no aggregated figures available for this period)"
    payload = {
        "reportCount": aggregated.get("reportCount"),
        "dataQualityScore": aggregated.get("dataQualityScore"),
        "newestSourceAt": aggregated.get("newestSourceAt"),
        "oldestSourceAt": aggregated.get("oldestSourceAt"),
        "data": aggregated.get("data") or {},
    }
    return json.dumps(payload, indent=2, ensure_ascii=False, default=str)


def _run_component(
    llm: LLMProvider,
    *,
    system_prompt: str,
    user_prompt: str,
    schema: type[BaseModel],
    cache_key: str,
    max_tokens: int,
) -> BaseModel:
    """Single component call — thin wrapper so all four narrative
    generators go through the same code path (retry policy, cache-key
    convention, max-tokens guard)."""
    return llm.complete_structured(
        system=system_prompt,
        user=user_prompt,
        schema=schema,
        max_tokens=max_tokens,
        cache_key=cache_key,
    )


# ────────────────────────────────────────────────────────────────────
# Component 2 — AI Summary
# ────────────────────────────────────────────────────────────────────


def generate_ai_summary(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
) -> AISummary:
    """2–4 paragraph narrative synthesis grounded in a broad RAG search."""
    rag = fetch_rag_context(
        query=(
            f"humanitarian situation overview {country_name} {period_label} "
            "conflict displacement needs response funding"
        ),
        limit=12,
    )
    if rag.is_empty:
        logger.info("[situation:ai_summary] no RAG hits — returning empty summary")
        return AISummary()

    system = _build_system_prompt(country_name, period_label, _format_aggregated_for_prompt(aggregated))
    user = (
        f"Produce the AI Summary component for {country_name}, {period_label}. "
        "Two to four paragraphs of narrative prose. Weave the headline "
        "figures and the retrieved evidence into a briefing that a "
        "humanitarian program manager could read in under a minute.\n"
        "\n"
        "RETRIEVED EVIDENCE:\n"
        f"{rag.formatted_for_prompt}"
    )
    try:
        result = _run_component(
            llm, system_prompt=system, user_prompt=user,
            schema=_AISummaryLLM, cache_key=cache_key, max_tokens=1500,
        )
    except Exception as exc:  # noqa: BLE001 — component-level isolation
        logger.warning("[situation:ai_summary] LLM call failed: %s", exc)
        return AISummary()
    return AISummary(
        text=result.text.strip(),
        source_report_ids=rag.contributing_report_ids,
    )


# ────────────────────────────────────────────────────────────────────
# Component 3 — Context Risks
# ────────────────────────────────────────────────────────────────────


def generate_context_risks(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
) -> ContextRisks:
    """Eight risk domains in one LLM call. Single broad RAG search
    covers cross-domain context — separate per-domain searches would
    ~8× the cost with marginal quality lift for the POC."""
    rag = fetch_rag_context(
        query=(
            f"{country_name} context risks demographics political economy "
            "society culture security legal policy infrastructure environment"
        ),
        limit=15,
    )
    if rag.is_empty:
        return ContextRisks()

    system = _build_system_prompt(country_name, period_label, _format_aggregated_for_prompt(aggregated))
    user = (
        f"Produce the Context Risks component for {country_name}, {period_label}. "
        "For each of the eight domains (demographics, political, economy, "
        "socio_culture, security, legal_policy, infrastructure, "
        "environment), emit 3–6 bulleted risks or observations. Skip a "
        "domain (empty list) only if the evidence doesn't support any "
        "claim in it.\n"
        "\n"
        "RETRIEVED EVIDENCE:\n"
        f"{rag.formatted_for_prompt}"
    )
    try:
        result = _run_component(
            llm, system_prompt=system, user_prompt=user,
            schema=_ContextRisksLLM, cache_key=cache_key, max_tokens=3000,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[situation:context_risks] LLM call failed: %s", exc)
        return ContextRisks()

    # Wrap each domain with the coarse-grained source list. Every
    # domain sharing the same RAG set means the dashboard's
    # per-domain "sources" panel looks identical — that's fine for
    # POC; per-domain search + attribution can layer on later.
    src_ids = rag.contributing_report_ids
    return ContextRisks(
        demographics=RiskDomain(bullets=result.demographics, source_report_ids=src_ids),
        political=RiskDomain(bullets=result.political, source_report_ids=src_ids),
        economy=RiskDomain(bullets=result.economy, source_report_ids=src_ids),
        socio_culture=RiskDomain(bullets=result.socio_culture, source_report_ids=src_ids),
        security=RiskDomain(bullets=result.security, source_report_ids=src_ids),
        legal_policy=RiskDomain(bullets=result.legal_policy, source_report_ids=src_ids),
        infrastructure=RiskDomain(bullets=result.infrastructure, source_report_ids=src_ids),
        environment=RiskDomain(bullets=result.environment, source_report_ids=src_ids),
    )


# ────────────────────────────────────────────────────────────────────
# Component 4 — Hazards & Pre-Crisis Vulnerabilities
# ────────────────────────────────────────────────────────────────────


def generate_hazards_and_vulnerabilities(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
) -> HazardsAndVulnerabilities:
    rag = fetch_rag_context(
        query=(
            f"{country_name} hazards vulnerabilities natural disasters "
            "conflict drought flood economic institutional structural"
        ),
        limit=10,
    )
    if rag.is_empty:
        return HazardsAndVulnerabilities()

    system = _build_system_prompt(country_name, period_label, _format_aggregated_for_prompt(aggregated))
    user = (
        f"Produce the Hazards & Pre-Crisis Vulnerabilities component for "
        f"{country_name}, {period_label}. Emit two lists: `hazards` (natural + "
        "man-made shocks) and `vulnerabilities` (structural conditions "
        "that make populations susceptible). One sentence per bullet.\n"
        "\n"
        "RETRIEVED EVIDENCE:\n"
        f"{rag.formatted_for_prompt}"
    )
    try:
        result = _run_component(
            llm, system_prompt=system, user_prompt=user,
            schema=_HazardsVulnerabilitiesLLM, cache_key=cache_key, max_tokens=2000,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[situation:hazards_and_vulnerabilities] LLM call failed: %s", exc)
        return HazardsAndVulnerabilities()

    return HazardsAndVulnerabilities(
        hazards=[SourcedBullet(description=b, source_report_ids=rag.contributing_report_ids) for b in result.hazards],
        vulnerabilities=[SourcedBullet(description=b, source_report_ids=rag.contributing_report_ids) for b in result.vulnerabilities],
    )


# ────────────────────────────────────────────────────────────────────
# Component 5 — Displacement Narrative
# ────────────────────────────────────────────────────────────────────


def generate_displacement_narrative(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
) -> DisplacementNarrative:
    rag = fetch_rag_context(
        query=(
            f"{country_name} displacement push factors return intention "
            "IDPs refugees returnees drivers barriers conditions"
        ),
        limit=10,
    )
    if rag.is_empty:
        return DisplacementNarrative()

    system = _build_system_prompt(country_name, period_label, _format_aggregated_for_prompt(aggregated))
    user = (
        f"Produce the Displacement Narrative component for {country_name}, "
        f"{period_label}. Emit two lists: `push_factors` (what's driving people to "
        "displace) and `return_intention` (signals about their intent to "
        "return, blockers, or conditions). One sentence per bullet.\n"
        "\n"
        "RETRIEVED EVIDENCE:\n"
        f"{rag.formatted_for_prompt}"
    )
    try:
        result = _run_component(
            llm, system_prompt=system, user_prompt=user,
            schema=_DisplacementLLM, cache_key=cache_key, max_tokens=2000,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[situation:displacement] LLM call failed: %s", exc)
        return DisplacementNarrative()

    return DisplacementNarrative(
        push_factors=[SourcedBullet(description=b, source_report_ids=rag.contributing_report_ids) for b in result.push_factors],
        return_intention=[SourcedBullet(description=b, source_report_ids=rag.contributing_report_ids) for b in result.return_intention],
    )
