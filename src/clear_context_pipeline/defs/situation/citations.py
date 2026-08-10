"""Per-line source attribution for the RAG-grounded narrative components.

The narrative generators ask the LLM to append inline ``[Rn]`` markers to each
bullet / sentence, referring to the numbered ``[Rn]`` items in the RETRIEVED
EVIDENCE block (see ``rag_helper._format_hits_for_prompt``). This module turns
those markers into the ``contributing_sources`` map the dashboard renders:

    { report_id: [ generated lines that report contributed to ] }

Design notes:
  - **Inline markers, not nested LLM objects.** The LLM output schemas stay flat
    (``list[str]`` / prose) — a nested ``{text, sources}`` shape made the cheap
    models intermittently emit a JSON-encoded string and blank the component
    (see narrative.py). Markers are stripped and resolved deterministically here.
  - **Values are the GENERATED lines**, not the source chunks — the map answers
    "which report contributed to which line of the output".
  - **Hallucination-safe.** A marker outside the hit range (or pointing at a hit
    with no report id) is dropped. A line with no valid marker still renders in
    the text; it just contributes to no report's list.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence

# One evidence marker: [R1], [R12]. The number is 1-based over the RAG hits.
_REF_RE = re.compile(r"\[R(\d+)\]")
# Sentence boundary for prose: a ., !, or ? followed by whitespace. Decimals
# ("8.6M", "3.5 million") have no space after the dot, so they don't split.
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+")
# A marker run at the START of a piece — the author put it after the previous
# sentence's full stop ("…people. [R1] Funding…"), so it cites THAT sentence.
_LEADING_REFS_RE = re.compile(r"\s*((?:\[R\d+\]\s*)+)(.*)", re.DOTALL)


def _clean(text: str) -> str:
    """Strip ``[Rn]`` markers and tidy the whitespace/punctuation they leave."""
    out = _REF_RE.sub("", text)
    out = re.sub(r"\s+([.,;:!?])", r"\1", out)  # " ." -> "."
    out = re.sub(r"\s{2,}", " ", out)
    return out.strip()


def _resolve_refs(text: str, hit_report_ids: Sequence[str]) -> list[str]:
    """Report ids the ``[Rn]`` markers in ``text`` resolve to — 1-based, range-
    guarded, order-preserving, de-duplicated. Unresolvable markers are dropped."""
    ids: list[str] = []
    for n in (int(m) for m in _REF_RE.findall(text)):
        if 1 <= n <= len(hit_report_ids):
            rid = hit_report_ids[n - 1]
            if rid and rid not in ids:
                ids.append(rid)
    return ids


def resolve_bullets(
    bullets: Iterable[str],
    hit_report_ids: Sequence[str],
) -> tuple[list[str], list[list[str]], dict[str, list[str]]]:
    """Resolve a list of marked bullets.

    Returns ``(clean_bullets, per_bullet_report_ids, contributing_sources)`` —
    the marker-free bullets, each bullet's resolved report ids, and the inverted
    ``report_id -> [clean bullets]`` map (order-preserving, de-duplicated)."""
    clean_bullets: list[str] = []
    per_bullet_ids: list[list[str]] = []
    contributing: dict[str, list[str]] = {}
    for raw in bullets:
        clean = _clean(raw)
        ids = _resolve_refs(raw, hit_report_ids)
        clean_bullets.append(clean)
        per_bullet_ids.append(ids)
        for rid in ids:
            bucket = contributing.setdefault(rid, [])
            if clean and clean not in bucket:
                bucket.append(clean)
    return clean_bullets, per_bullet_ids, contributing


def resolve_prose(
    text: str,
    hit_report_ids: Sequence[str],
) -> tuple[str, dict[str, list[str]]]:
    """Resolve a prose block (the AI summary). Splits into sentences, attributes
    each, and returns ``(clean_text, contributing_sources)`` keyed by sentence."""
    if not text.strip():
        return "", {}
    pieces = _SENTENCE_RE.split(text.strip())
    # Markers trail the full stop, so a piece can start with the previous
    # sentence's citation. Reattach a leading marker run to the prior sentence.
    sentences: list[str] = []
    for piece in pieces:
        m = _LEADING_REFS_RE.match(piece)
        if m and sentences:
            sentences[-1] = f"{sentences[-1]} {m.group(1).strip()}"
            rest = m.group(2).strip()
            if rest:
                sentences.append(rest)
        else:
            sentences.append(piece)
    clean_sentences, _ids, contributing = resolve_bullets(sentences, hit_report_ids)
    clean_text = " ".join(s for s in clean_sentences if s)
    return clean_text, contributing


def merge_contributing(*maps: dict[str, list[str]]) -> dict[str, list[str]]:
    """Union several ``contributing_sources`` maps (e.g. a component's two lists),
    preserving order and de-duplicating lines per report id."""
    merged: dict[str, list[str]] = {}
    for m in maps:
        for rid, lines in m.items():
            bucket = merged.setdefault(rid, [])
            for line in lines:
                if line not in bucket:
                    bucket.append(line)
    return merged
