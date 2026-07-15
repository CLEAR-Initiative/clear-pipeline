"""Shared RAG-context helpers for the narrative generators.

Every LLM component (AI summary, context risks, hazards,
displacement) follows the same three-step pattern:
  1. Formulate a component-specific search query
  2. Fetch top-K knowledgebase chunks via clear-api's searchKnowledgebase
  3. Format the hits into a prompt-ready context block

Grouping this in one place keeps prompts and citation logic
consistent — every narrative section carries the same source-id
provenance shape.

POC citation model: coarse-grained. Every bullet in a component
attributes to the union of contributing report ids for that
component's RAG hits. Per-bullet citation refinement (LLM emits
`[R1]` markers → post-processed to report ids) is a Phase E follow-up
if the dashboard actually wants that granularity.
"""

from dataclasses import dataclass, field
from typing import Any

from clear_context_pipeline.providers import clear_api


@dataclass(frozen=True)
class RAGContext:
    """One search's worth of retrieved evidence + attribution data.

    `formatted_for_prompt` is the string the LLM sees — a numbered
    list of chunks with per-hit metadata. `contributing_report_ids`
    is the deduped union of source reports across every hit, used to
    populate the component's `source_report_ids` field.
    """
    formatted_for_prompt: str
    contributing_report_ids: list[str] = field(default_factory=list)
    hit_count: int = 0

    @property
    def is_empty(self) -> bool:
        return self.hit_count == 0


def fetch_rag_context(
    *,
    query: str,
    limit: int = 10,
    filters: dict[str, Any] | None = None,
) -> RAGContext:
    """Run one hybrid dense+BM25 search and package the hits.

    On network / server error we return an empty RAGContext instead
    of raising — the caller degrades gracefully to "no evidence
    available for this component" rather than failing the whole
    situation analysis for a transient search hiccup.
    """
    try:
        hits = clear_api.search_knowledgebase(query=query, filters=filters, limit=limit)
    except Exception:  # noqa: BLE001 — degrade to empty on any search failure
        return RAGContext(formatted_for_prompt="", contributing_report_ids=[], hit_count=0)

    if not hits:
        return RAGContext(formatted_for_prompt="", contributing_report_ids=[], hit_count=0)

    # De-dupe report ids preserving first-seen order (which mirrors
    # the RRF-fused ranking — most relevant reports first).
    seen: set[str] = set()
    ordered_report_ids: list[str] = []
    for hit in hits:
        rid = hit.get("reportId")
        if rid and rid not in seen:
            seen.add(rid)
            ordered_report_ids.append(rid)

    return RAGContext(
        formatted_for_prompt=_format_hits_for_prompt(hits),
        contributing_report_ids=ordered_report_ids,
        hit_count=len(hits),
    )


def _format_hits_for_prompt(hits: list[dict[str, Any]]) -> str:
    """Render hits as a numbered evidence list. The `[Rn]` prefix
    lets a future citation-enabled prompt refer to specific hits
    (Phase E); for now, we just want a clean readable block.

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
