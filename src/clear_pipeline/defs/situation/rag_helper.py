"""Shared RAG-context helpers for the narrative generators.

Every LLM component (AI summary, context risks, hazards,
displacement) follows the same three-step pattern:
  1. Formulate a component-specific search query
  2. Fetch top-K knowledgebase chunks via clear-api's searchKnowledgebase
  3. Format the hits into a prompt-ready context block

Grouping this in one place keeps prompts and citation logic
consistent — every narrative section carries the same source-id
provenance shape.

Citation model: per-line. Each hit is numbered `[Rn]`; the generators
ask the LLM to cite those numbers inline, and `situation/citations.py`
resolves each `[Rn]` back to its report via `hit_report_ids` below,
producing the `report_id -> [generated lines]` map the dashboard cites
from. The deduped `contributing_report_ids` union is still exposed as
each component's coarse `source_report_ids` fallback.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

from clear_pipeline.providers import clear_api

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RAGContext:
    """One search's worth of retrieved evidence + attribution data.

    `formatted_for_prompt` is the string the LLM sees — a numbered
    list of chunks with per-hit metadata. `contributing_report_ids`
    is the deduped union of source reports across every hit, used to
    populate the component's `source_report_ids` field.

    `hit_report_ids` is the report id of EACH hit, index-aligned to the
    `[Rn]` marker (hit_report_ids[0] is `[R1]`). It carries duplicates
    and preserves order — unlike `contributing_report_ids`, it lets the
    citation resolver map a `[Rn]` marker back to its report. An empty
    string marks a hit with no report id (unresolvable).
    """
    formatted_for_prompt: str
    contributing_report_ids: list[str] = field(default_factory=list)
    hit_report_ids: list[str] = field(default_factory=list)
    hit_count: int = 0

    @property
    def is_empty(self) -> bool:
        return self.hit_count == 0


def fetch_rag_context(
    *,
    query: str,
    limit: int = 10,
    filters: dict[str, Any] | None = None,
    country_id: str | None = None,
) -> RAGContext:
    """Run one hybrid dense+BM25 search and package the hits.

    ``country_id`` scopes the search to one country's subtree via clear-api's
    ``countryLocationId`` filter — so a country's situation analysis only cites
    knowledge-base chunks from reports about THAT country, not any other country
    in the shared KB. Merged into ``filters`` here so callers just pass their
    country id through; None leaves the search unscoped (used off the situation
    path).

    On network / server error we return an empty RAGContext instead
    of raising — the caller degrades gracefully to "no evidence
    available for this component" rather than failing the whole
    situation analysis for a transient search hiccup.

    The failure is LOGGED (``logger.exception``) rather than swallowed silently:
    an empty RAGContext nulls the whole narrative/sector component downstream, so
    a search error that used to vanish here — e.g. clear-api's embedding config
    missing, making every ``searchKnowledgebase`` throw — is now diagnosable from
    the pipeline logs instead of only surfacing as unexplained null fields.
    """
    if country_id:
        filters = {**(filters or {}), "countryLocationId": country_id}
    logger.debug("[situation:rag] searching knowledgebase: query=%r limit=%d filters=%s", query, limit, filters)
    try:
        hits = clear_api.search_knowledgebase(query=query, filters=filters, limit=limit)
    except Exception:  # noqa: BLE001 — degrade to empty on any search failure, but LOG it
        logger.exception(
            "[situation:rag] searchKnowledgebase FAILED for query=%r — returning empty context "
            "(this nulls the dependent narrative/sector component). Check clear-api logs + its "
            "EMBEDDING_* config.",
            query,
        )
        return RAGContext(formatted_for_prompt="", contributing_report_ids=[], hit_count=0)

    if not hits:
        logger.info(
            "[situation:rag] searchKnowledgebase returned 0 hits for query=%r — component will be empty. "
            "If unexpected, verify the knowledgebase is populated and clear-api's EMBEDDING_PROVIDER/"
            "EMBEDDING_MODEL match the rows' embedding_provider/embedding_model.",
            query,
        )
        return RAGContext(formatted_for_prompt="", contributing_report_ids=[], hit_count=0)

    logger.debug("[situation:rag] query=%r → %d hits", query, len(hits))

    # De-dupe report ids preserving first-seen order (which mirrors
    # the RRF-fused ranking — most relevant reports first).
    seen: set[str] = set()
    ordered_report_ids: list[str] = []
    for hit in hits:
        rid = hit.get("reportId")
        if rid and rid not in seen:
            seen.add(rid)
            ordered_report_ids.append(rid)

    # Per-hit report id, index-aligned to the [Rn] marker (empty string for a
    # hit with no report id, so the [Rn] numbering still lines up with the hits).
    hit_report_ids = [hit.get("reportId") or "" for hit in hits]

    return RAGContext(
        formatted_for_prompt=_format_hits_for_prompt(hits),
        contributing_report_ids=ordered_report_ids,
        hit_report_ids=hit_report_ids,
        hit_count=len(hits),
    )


def _format_hits_for_prompt(hits: list[dict[str, Any]]) -> str:
    """Render hits as a numbered evidence list. The `[Rn]` prefix is
    load-bearing: the generators ask the LLM to cite these numbers
    inline, and `citations.py` resolves each `[Rn]` back to its report
    via `RAGContext.hit_report_ids`. The numbering here MUST stay
    aligned with the hit order (hit i → `[R{i+1}]`).

    Each hit includes the report title, publication date, page range,
    and the chunk text itself — enough context for the LLM to reason
    about what's being cited and when.
    """
    lines: list[str] = []
    for i, hit in enumerate(hits, start=1):
        title = hit.get("reportTitle") or hit.get("reportId") or "(untitled)"
        published = hit.get("publishedAt") or "unknown date"
        pages = _pages_range(hit)
        chunk = (hit.get("chunkText") or "").strip()
        lines.append(
            f"[R{i}] {title} — {published}{pages}\n"
            f"{chunk}\n",
        )
    return "\n".join(lines)


def _pages_range(hit: dict[str, Any]) -> str:
    start = hit.get("pageStart")
    end = hit.get("pageEnd")
    if start is None:
        return ""
    if end is None or end == start:
        return f" (p.{start})"
    return f" (pp.{start}–{end})"
