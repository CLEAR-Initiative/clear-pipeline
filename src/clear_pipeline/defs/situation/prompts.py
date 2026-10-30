"""Shared LLM system-prompt scaffolding for situation analysis.

Used by both narrative components and per-sector generators so every
analysis call shares one base instruction block (and stays byte-identical
for Anthropic prompt caching across a country-year run).
"""

from __future__ import annotations

import json
from typing import Any

BASE_INSTRUCTIONS = (
    "You are a humanitarian analyst producing a situation analysis for "
    "the Norwegian Refugee Council. Every claim you emit MUST be "
    "grounded in the retrieved evidence — do not invent numbers, "
    "dates, or attributions. When the evidence is thin or off-topic "
    "for this component, leave that field empty rather than "
    "speculating.\n"
    "\n"
    "Rules:\n"
    "- Summarise the evidence; do not interpret it. Restate what "
    "  sources report — no causal claims, implications, or "
    "  conclusions beyond the text.\n"
    "- If you do not know, or the retrieved evidence / aggregated "
    "  figures do not address the topic you are asked to cover, "
    "  leave that field empty (empty string or empty list). Do not "
    "  invent, infer from general knowledge, or fill gaps with "
    "  plausible humanitarian boilerplate.\n"
    "- Bullets are terse fragments, NOT full sentences. Aim for 15 words "
    "  or fewer. Lead with the figure or the fact; drop filler openers "
    "  ('there is', 'it is reported that', 'continues to'). One claim per "
    "  bullet.\n"
    "- Report every number the way the source cites it (with unit and "
    "  scale). Do not extrapolate.\n"
    "- Use the country name and admin-region names as they appear in "
    "  the sources. Preserve local naming (e.g. 'Kordofan', 'Darfur').\n"
    "- Prefer verified / reported sources over media when both cover "
    "  the same claim. If they conflict, prefer the more recent report.\n"
    "- Neutral, factual tone. No editorialising. No calls to action.\n"
    "- Cite evidence inline. A bullet's or sentence's marker(s) MUST come at "
    "  its very END — after the full stop — NEVER at the start. Placing a "
    "  marker before the text it cites will mis-attribute it to the previous "
    "  line. Use the bracketed evidence numbers you drew from, e.g. [R2] or "
    "  [R1][R4], matching the [Rn] items in RETRIEVED EVIDENCE. Cite only "
    "  evidence you actually used; a line that uses none gets no marker.\n"
)

_COMPLETENESS_INSTRUCTION = (
    "Set completeness (0–1) and questions_not_answered."
)


def format_aggregated_for_prompt(aggregated: dict[str, Any] | None) -> str:
    """Render the aggregated-datapoints blob in a compact form the LLM
    can reason over. Serialises the whole ``data`` map so the model can
    cross-reference sector PIN figures against narrative claims."""
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


def build_system_prompt(
    country_name: str,
    period_label: str,
    aggregated_context: str,
    *,
    sector: str | None = None,
) -> str:
    """Shared system prompt for narrative + sector calls in one run.

    Static prefix + country context is what Anthropic's prompt cache
    keys on. Keeping it byte-identical across calls means later calls
    read from cache. Optional ``sector`` adds a ``SECTOR:`` line (small
    variation the cache tolerates; the country/aggregated block stays
    identical).
    """
    sector_line = f"SECTOR: {sector}\n" if sector else ""
    return (
        f"{BASE_INSTRUCTIONS}\n"
        f"---\n"
        f"AREA: {country_name}\n"
        f"PERIOD: {period_label}\n"
        f"{sector_line}"
        f"---\n"
        f"AGGREGATED HEADLINE FIGURES (cached; do not repeat back):\n"
        f"{aggregated_context}\n"
        f"---"
    )


def build_evidence_user_prompt(
    question_block: str,
    mapping_instruction: str,
    formatted_evidence: str,
    *,
    include_completeness: bool = True,
) -> str:
    """QUESTION block + field-mapping brief + RETRIEVED EVIDENCE."""
    parts = [question_block.strip(), "", mapping_instruction.strip()]
    if include_completeness:
        parts.append(_COMPLETENESS_INSTRUCTION)
    parts.extend(["", "RETRIEVED EVIDENCE:", formatted_evidence])
    return "\n".join(parts)
