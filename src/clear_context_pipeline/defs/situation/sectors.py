"""Per-sector analysis generator — component 6.

Six SAF sectors, one LLM call each. Every call:
  1. Sector-scoped RAG search (filters knowledgebase hits by
     `needSectors = [<sector display name>]`)
  2. Prompt with retrieved chunks + aggregated headline figures
  3. Structured output: severity + impact / conditions / vulnerable
     sections / top needs / priority interventions / information
     coverage areas

Per-sector isolation: a failure in one sector returns that sector's
empty default; the other five still ship.

Prompt caching: the system-prompt scaffolding is identical to
`narrative.py`'s so the same doc-level cache (aggregated figures)
gets reused across the 4 narrative calls + 6 sector calls. All 10
calls in a country-year run share one `cache_key`.
"""

import logging
from typing import Any, Optional

from pydantic import BaseModel, Field

from clear_context_pipeline.defs.situation.citations import (
    merge_contributing,
    resolve_bullets,
)
from clear_context_pipeline.defs.situation.rag_helper import fetch_rag_context
from clear_context_pipeline.defs.situation.schemas import (
    InformationCoverageArea,
    SafSector,
    SectorAnalysis,
    Sectors,
    Severity,
)
from clear_context_pipeline.providers.llm import LLMProvider

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────
# Sector taxonomy mapping
#
# Two representations for the same concept:
#   - Python / dashboard key: `food_security` (snake_case)
#   - Extractor / knowledgebase tag: `Food Security` (TitleCase, matches
#     the SafSector Literal used by the datapoint extractor)
# Keep both here so the RAG filter and the payload assembly stay in
# sync. Adding a new sector = one entry in this tuple.
# ────────────────────────────────────────────────────────────────────


_SECTOR_KEYS: tuple[tuple[SafSector, str], ...] = (
    ("education", "Education"),
    ("food_security", "Food Security"),
    ("health", "Health"),
    ("shelter", "Shelter"),
    ("wash", "WASH"),
    ("protection", "Protection"),
)


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


class _SectorLLM(BaseModel):
    """LLM output shape for one sector call. Every list has explicit
    prompt guidance in its description so tool_use output stays
    consistent across sectors."""
    severity: Optional[Severity] = Field(
        default=None,
        description=(
            "Overall severity of this sector's humanitarian situation. "
            "Use `critical` only when the evidence documents mortality, "
            "irreversible harm, or full-service collapse. Return null "
            "when the evidence is too thin to judge — do not default to "
            "medium as a hedge."
        ),
    )
    impact: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted list of concrete impacts on affected populations. "
            "Terse fragment per bullet (max 15 words). 3–6 bullets ideal."
        ),
    )
    humanitarian_conditions: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted description of current humanitarian conditions "
            "(access to services, coverage rates, service quality). "
            "Terse fragment per bullet."
        ),
    )
    vulnerable_sections: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted population groups whose vulnerability the "
            "evidence highlights (e.g. children under 5, pregnant "
            "women, IDPs, elderly). Terse fragment per bullet."
        ),
    )
    top_needs: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted top unmet needs the evidence identifies. Ordered "
            "roughly by scale / urgency. Terse fragment per bullet."
        ),
    )
    priority_interventions: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted interventions the evidence prioritises "
            "(what programs / activities should scale up). Terse "
            "fragment per bullet."
        ),
    )
    information_coverage: list[_InformationCoverageAreaLLM] = Field(
        default_factory=list,
        description=(
            "3–8 areas where sector information matters, each rated "
            "0–10 on how well the retrieved evidence covers it. Use "
            "this to flag gaps a follow-up assessment should target."
        ),
    )


# ────────────────────────────────────────────────────────────────────
# Prompts
#
# Kept in this module (not narrative.py) to avoid a circular import
# and because the sector prompts have their own scaffolding — the
# system prompt still shares the base instructions block but the
# per-sector user prompt is quite different from the narrative
# components' user prompts.
# ────────────────────────────────────────────────────────────────────


_BASE_INSTRUCTIONS = (
    "You are a humanitarian analyst producing a per-sector situation "
    "briefing for the Norwegian Refugee Council. Every claim MUST be "
    "grounded in the retrieved evidence — do not invent numbers or "
    "attributions. When the evidence is thin, prefer emitting fewer "
    "bullets over speculating.\n"
    "\n"
    "Rules:\n"
    "- Preserve number formats and units exactly as the source cites.\n"
    "- Use the local admin names as they appear in the sources.\n"
    "- Neutral factual tone. No editorialising, no calls to action.\n"
    "- Bullets are terse fragments (max 15 words), not sentences. Lead with the figure or fact; drop filler.\n"
    "- Cite evidence inline. A bullet's marker(s) MUST come at its very END, "
    "  NEVER at the start. Use the bracketed evidence numbers you drew from, "
    "  e.g. [R2] or [R1][R4], matching the [Rn] items in RETRIEVED EVIDENCE. "
    "  Cite only evidence you actually used; a bullet that uses none gets no "
    "  marker.\n"
)


def _build_system_prompt(
    country_name: str,
    period_label: str,
    sector_display_name: str,
    aggregated_context: str,
) -> str:
    """The system prompt is nearly identical across all 6 sector calls
    for one country-year — only the sector name changes. Anthropic's
    prompt cache tolerates this small variation; the country context
    block (which is the largest cached section) stays byte-identical."""
    return (
        f"{_BASE_INSTRUCTIONS}\n"
        f"---\n"
        f"COUNTRY: {country_name}\n"
        f"PERIOD: {period_label}\n"
        f"SECTOR: {sector_display_name}\n"
        f"---\n"
        f"AGGREGATED HEADLINE FIGURES (cached; do not repeat back):\n"
        f"{aggregated_context}\n"
        f"---"
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
) -> SectorAnalysis:
    """One LLM call, one sector. Returns the empty default on any
    failure so a single bad sector never drops the other five."""

    # Sector-scoped RAG — filter knowledgebase hits to chunks the
    # extractor tagged with this sector. Query text still uses the
    # display name so the semantic ranker also biases towards
    # sector-relevant content.
    rag = fetch_rag_context(
        query=(
            f"{country_name} {sector_display_name} needs response impact "
            "affected populations vulnerable groups interventions"
        ),
        limit=12,
        filters={"needSectors": [sector_display_name]},
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
        rag = fetch_rag_context(
            query=f"{country_name} {sector_display_name} humanitarian needs",
            limit=8,
        )

    if rag.is_empty:
        logger.info(
            "[situation:sector:%s] no evidence available — returning empty analysis",
            sector_key,
        )
        return SectorAnalysis()

    system = _build_system_prompt(country_name, period_label, sector_display_name, aggregated_context)
    user = (
        f"Produce the {sector_display_name} sector analysis for "
        f"{country_name}, {period_label}. Populate every field the evidence "
        "supports; skip fields (empty list) when the evidence is too "
        "thin to make a claim.\n"
        "\n"
        "RETRIEVED EVIDENCE:\n"
        f"{rag.formatted_for_prompt}"
    )

    try:
        result = llm.complete_structured(
            system=system,
            user=user,
            schema=_SectorLLM,
            max_tokens=3000,
            cache_key=cache_key,
        )
    except Exception as exc:  # noqa: BLE001 — per-sector isolation
        logger.warning(
            "[situation:sector:%s] LLM call failed: %s", sector_key, exc,
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
    impact, _, c_impact = resolve_bullets(result.impact, rag.hit_report_ids)
    conditions, _, c_cond = resolve_bullets(result.humanitarian_conditions, rag.hit_report_ids)
    vulnerable, _, c_vuln = resolve_bullets(result.vulnerable_sections, rag.hit_report_ids)
    needs, _, c_needs = resolve_bullets(result.top_needs, rag.hit_report_ids)
    interventions, _, c_interv = resolve_bullets(result.priority_interventions, rag.hit_report_ids)
    contributing = merge_contributing(c_impact, c_cond, c_vuln, c_needs, c_interv)

    return SectorAnalysis(
        severity=result.severity,
        impact=impact,
        humanitarian_conditions=conditions,
        vulnerable_sections=vulnerable,
        top_needs=needs,
        priority_interventions=interventions,
        information_coverage=info_coverage,
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
) -> Sectors:
    """Fan out one LLM call per sector; assemble into the `Sectors`
    payload. Order preserved from _SECTOR_KEYS so the dashboard's tab
    layout stays stable across regenerations.

    Sequential (not parallel) because Anthropic prompt caching
    benefits most from back-to-back calls sharing a stable system
    prefix — the 5-minute cache TTL comfortably covers 6 sequential
    Sonnet calls of typical size."""
    # Same aggregated-context format helper the narrative module uses.
    # Duplicated inline to avoid the circular import — a shared
    # `_format_aggregated_for_prompt` in a future `common.py` module
    # would be the cleanup path if we grow more narrative modules.
    aggregated_context = _format_aggregated(aggregated)

    outputs: dict[str, SectorAnalysis] = {}
    for sector_key, sector_display in _SECTOR_KEYS:
        outputs[sector_key] = _generate_one_sector(
            llm,
            sector_key=sector_key,
            sector_display_name=sector_display,
            country_name=country_name,
            period_label=period_label,
            aggregated_context=aggregated_context,
            cache_key=cache_key,
        )

    return Sectors(**outputs)


def _format_aggregated(aggregated: dict[str, Any] | None) -> str:
    """Render the aggregated-datapoints blob in a compact form for
    prompts. Kept here (duplicated with narrative.py's version) to
    avoid a circular import between the two modules."""
    import json
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
