"""Per-sector analysis generator — component 6.

One LLM call per first-level sector. Every call:
  1. Sector-scoped RAG search (filters knowledgebase hits by
     `needSectors = [<sector display name>]`)
  2. Prompt with retrieved chunks + aggregated headline figures
  3. Structured output: severity + impact / conditions / vulnerable
     sections / top needs / priority interventions / information
     coverage areas

Per-sector isolation: a failure in one sector returns that sector's
empty default; the others still ship. Sectors outside the six KB SAF
tags fall back to an unfiltered search when scoped hits are empty.

Prompt caching: the shared `BASE_INSTRUCTIONS` in `prompts.py` plus
aggregated figures keep the system-prompt prefix byte-identical across
narrative + sector calls in a country-year run (one shared `cache_key`).
"""

import logging
from typing import Any, Optional

from pydantic import BaseModel, Field

from clear_pipeline.defs.situation.citations import (
    merge_contributing,
    resolve_bullets,
)
from clear_pipeline.defs.situation.generate_component import (
    GuidedCoverageLLM,
    run_structured,
)
from clear_pipeline.defs.situation.prompts import (
    build_evidence_user_prompt,
    build_system_prompt,
    format_aggregated_for_prompt,
)
from clear_pipeline.defs.situation.questions import (
    SECTOR_KEYS,
    format_question_block,
    rag_query_for,
    sector_question_for,
)
from clear_pipeline.defs.situation.rag_helper import fetch_rag_context
from clear_pipeline.defs.situation.schemas import (
    InformationCoverageArea,
    SafSector,
    SectorAnalysis,
    Sectors,
    Severity,
)
from clear_pipeline.providers.llm import LLMProvider

logger = logging.getLogger(__name__)

# Re-export for tests that imported the old private name.
_SECTOR_KEYS = SECTOR_KEYS


# ────────────────────────────────────────────────────────────────────
# LLM output schemas
#
# Same decoupling as narrative.py — LLM emits shape, we add
# source_report_ids post-hoc. `_InformationCoverageAreaLLM` doesn't
# take `report_count` because that's a deterministic field we compute
# from the RAG hit count.
# ────────────────────────────────────────────────────────────────────


class _InformationCoverageAreaLLM(BaseModel):
    area: str = Field(
        description=(
            "A specific dimension of the sector where coverage matters "
            'e.g. "school attendance rates", "aid delivery logistics", '
            '"vaccination uptake". Short noun phrase.'
        ),
    )
    rating_out_of_10: int = Field(
        ge=0, le=10,
        description=(
            "Your judgement of how well this area is covered in the "
            "retrieved evidence: 0 = no evidence at all, 10 = "
            "comprehensively covered by multiple recent sources."
        ),
    )


class _SectorLLM(GuidedCoverageLLM):
    """LLM output shape for one sector call.

    Field descriptions stay short — the CSV analysis question in the
    user prompt is the content brief, not these labels.
    """
    severity: Optional[Severity] = Field(
        default=None,
        description=(
            "Overall severity. Use `critical` only for mortality, "
            "irreversible harm, or full-service collapse. Null when "
            "evidence is too thin — do not default to medium."
        ),
    )
    impact: list[str] = Field(
        default_factory=list,
        description="Shock impacts on people, assets, and services. Terse bullets (≤15 words).",
    )
    humanitarian_conditions: list[str] = Field(
        default_factory=list,
        description="Access, availability, quality, wellbeing, and coping. Terse bullets (≤15 words).",
    )
    vulnerable_sections: list[str] = Field(
        default_factory=list,
        description="Population groups highlighted as vulnerable. Terse bullets.",
    )
    top_needs: list[str] = Field(
        default_factory=list,
        description="Priority unmet needs (affected population and actors). Terse bullets (≤15 words).",
    )
    priority_interventions: list[str] = Field(
        default_factory=list,
        description="Preferred / recommended assistance types. Terse bullets (≤15 words).",
    )
    information_coverage: list[_InformationCoverageAreaLLM] = Field(
        default_factory=list,
        description="3–8 coverage areas rated 0–10 against the retrieved evidence.",
    )


_SECTOR_BULLET_FIELDS = (
    "impact",
    "humanitarian_conditions",
    "vulnerable_sections",
    "top_needs",
    "priority_interventions",
)

_SECTOR_MAP_INSTRUCTION = (
    "Map the answer into severity, impact, humanitarian_conditions, "
    "vulnerable_sections, top_needs, priority_interventions, and "
    "information_coverage. Empty list when evidence is thin."
)


# ────────────────────────────────────────────────────────────────────
# Per-sector generator
# ────────────────────────────────────────────────────────────────────


def _generate_one_sector(
    llm: LLMProvider,
    *,
    sector_key: SafSector,
    sector_display_name: str,
    country_name: str,
    period_label: str,
    aggregated_context: str,
    cache_key: str,
    rag_filters: dict[str, Any] | None = None,
) -> SectorAnalysis:
    """One LLM call, one sector. Returns the empty default on any
    failure so a single bad sector never drops the others."""

    # Sector-scoped RAG — filter to chunks tagged with this sector.
    # Query text is the CSV analysis question (not a hand-rolled keyword bag).
    question = sector_question_for(sector_key)
    query = rag_query_for(country_name, question, fallback=sector_display_name)
    rag = fetch_rag_context(
        query=query,
        limit=12,
        filters={**(rag_filters or {}), "needSectors": [sector_display_name]},
    )

    # If the sector-scoped search returned nothing, fall back to an
    # unfiltered search — better a broad set of hits than no evidence
    # at all. The LLM prompt says to prefer fewer claims over
    # speculation, so it'll degrade gracefully.
    used_fallback = False
    if rag.is_empty:
        logger.info(
            "[situation:sector:%s] no sector-scoped hits — falling back to unfiltered search",
            sector_key,
        )
        used_fallback = True
        # Drop the SECTOR filter to broaden, but keep the country scope — a
        # broader fallback must still not pull in another country's reports.
        rag = fetch_rag_context(
            query=query,
            limit=8,
            filters=rag_filters,
        )

    if rag.is_empty:
        logger.info(
            "[situation:sector:%s] no evidence available — returning empty analysis",
            sector_key,
        )
        return SectorAnalysis()

    system = build_system_prompt(
        country_name, period_label, aggregated_context, sector=sector_display_name,
    )
    user = build_evidence_user_prompt(
        format_question_block(question, fallback_topic=sector_display_name),
        _SECTOR_MAP_INSTRUCTION,
        rag.formatted_for_prompt,
    )

    try:
        result = run_structured(
            llm,
            system_prompt=system,
            user_prompt=user,
            schema=_SectorLLM,
            max_tokens=3000,
            cache_key=cache_key,
        )
    except Exception:  # noqa: BLE001 — per-sector isolation
        logger.exception(
            "[situation:sector:%s] LLM call failed — returning empty sector", sector_key,
        )
        return SectorAnalysis()

    # Deterministic report_count for every coverage area — the number
    # of unique reports feeding this sector's RAG search. Same across
    # areas in a given sector; per-area attribution would require
    # per-area RAG searches (over-engineering for POC).
    report_count = len(rag.contributing_report_ids)
    info_coverage = [
        InformationCoverageArea(
            area=area.area,
            rating_out_of_10=area.rating_out_of_10,
            report_count=report_count,
        )
        for area in result.information_coverage
    ]

    # Resolve inline [Rn] citations on every bulleted field; merge into one
    # report_id -> [lines] map for the sector. Each field's markers are stripped
    # from the rendered bullets.
    resolved: dict[str, list[str]] = {}
    contribs = []
    for field in _SECTOR_BULLET_FIELDS:
        clean, _, contributing = resolve_bullets(
            getattr(result, field), rag.hit_report_ids,
        )
        resolved[field] = clean
        contribs.append(contributing)
    contributing = merge_contributing(*contribs)

    return SectorAnalysis(
        severity=result.severity,
        impact=resolved["impact"],
        humanitarian_conditions=resolved["humanitarian_conditions"],
        vulnerable_sections=resolved["vulnerable_sections"],
        top_needs=resolved["top_needs"],
        priority_interventions=resolved["priority_interventions"],
        information_coverage=info_coverage,
        completeness=result.completeness,
        questions_not_answered=list(result.questions_not_answered),
        source_report_ids=list(contributing) or rag.contributing_report_ids,
        contributing_sources=contributing,
        evidence_scope="fallback" if used_fallback else "sector",
    )


# ────────────────────────────────────────────────────────────────────
# Orchestrator
# ────────────────────────────────────────────────────────────────────


def generate_all_sectors(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
    rag_filters: dict[str, Any] | None = None,
) -> Sectors:
    """Fan out one LLM call per sector; assemble into the `Sectors`
    payload. Order preserved from SECTOR_KEYS so the dashboard's tab
    layout stays stable across regenerations.

    Sequential (not parallel) because Anthropic prompt caching
    benefits most from back-to-back calls sharing a stable system
    prefix — the 5-minute cache TTL comfortably covers sequential
    Sonnet calls of typical size."""
    aggregated_context = format_aggregated_for_prompt(aggregated)

    logger.info(
        "[situation:sectors] generating %d sectors for %s (%s)",
        len(SECTOR_KEYS), country_name, period_label,
    )
    outputs: dict[str, SectorAnalysis] = {}
    for sector_key, sector_display in SECTOR_KEYS:
        logger.debug("[situation:sector:%s] generating", sector_key)
        sector = _generate_one_sector(
            llm,
            sector_key=sector_key,  # type: ignore[arg-type]
            sector_display_name=sector_display,
            country_name=country_name,
            period_label=period_label,
            aggregated_context=aggregated_context,
            cache_key=cache_key,
            rag_filters=rag_filters,
        )
        outputs[sector_key] = sector
        logger.debug(
            "[situation:sector:%s] done: severity=%s impact=%d conditions=%d needs=%d",
            sector_key, sector.severity, len(sector.impact),
            len(sector.humanitarian_conditions), len(sector.top_needs),
        )

    populated = [k for k, s in outputs.items() if _sector_has_content(s)]
    logger.info(
        "[situation:sectors] done for %s (%s): %d/%d sectors populated (%s)",
        country_name, period_label, len(populated), len(outputs),
        ", ".join(populated) or "none",
    )
    return Sectors(**outputs)


def _sector_has_content(s: SectorAnalysis) -> bool:
    """True when a sector carries any generated content — used only for the
    populated/empty summary log so an all-empty run is obvious at a glance."""
    return bool(
        s.severity is not None
        or s.impact or s.humanitarian_conditions or s.vulnerable_sections
        or s.top_needs or s.priority_interventions
    )
