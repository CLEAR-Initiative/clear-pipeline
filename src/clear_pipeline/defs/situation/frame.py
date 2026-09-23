"""Frame — the scope of a unified analysis (ADR-0007, clear-api).

A *frame* is the set of dimensions the knowledgebase / datapoints / events are
indexed on: ``location_ids``, ``event_types``, ``need_sectors`` and a time
window (``window_start`` .. ``window_end``, where ``window_end`` None means a
rolling "to present" window). It generalises the situation analysis's fixed
``(country, calendar window)`` scope to an arbitrary selection.

The arrays are **canonicalised** — sorted + de-duplicated, empties dropped — so
a frame has a stable identity that matches clear-api's ``analyses`` partial
unique index (which the resolver canonicalises the same way). Keep the two in
lockstep: if this ordering/dedup rule changes, the identity of every existing
row shifts.

This module is pure (no I/O) so the frame logic is unit-testable without a live
clear-api / Dagster / LLM.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any


def _canon(xs: Iterable[str] | None) -> tuple[str, ...]:
    """Sort + de-duplicate, dropping falsy entries. Returns a tuple so a Frame
    stays hashable/immutable."""
    return tuple(sorted({x for x in (xs or []) if x}))


@dataclass(frozen=True)
class Frame:
    """A canonicalised analysis scope. Build via :meth:`build` so the arrays are
    normalised; the constructor assumes already-canonical inputs.

    ``window_start`` / ``window_end`` are ISO-8601 strings (matching the
    generator + clear-api). ``window_end=None`` is a rolling "to present"
    window — the automated-analysis case; a concrete end string is a fixed
    range (on-demand)."""

    window_start: str
    window_end: str | None = None
    location_ids: tuple[str, ...] = ()
    event_types: tuple[str, ...] = ()
    need_sectors: tuple[str, ...] = ()

    @staticmethod
    def build(
        *,
        window_start: str,
        window_end: str | None = None,
        location_ids: Iterable[str] | None = None,
        event_types: Iterable[str] | None = None,
        need_sectors: Iterable[str] | None = None,
    ) -> Frame:
        return Frame(
            window_start=window_start,
            window_end=window_end,
            location_ids=_canon(location_ids),
            event_types=_canon(event_types),
            need_sectors=_canon(need_sectors),
        )

    @property
    def is_country_default(self) -> bool:
        """A country-default frame is a single location (the A0) with no event
        or sector narrowing — the shape the weekly job produces. Derivable, so
        there is no ``kind`` flag (ADR-0007 §1)."""
        return (
            len(self.location_ids) == 1
            and not self.event_types
            and not self.need_sectors
        )

    def upsert_kwargs(self) -> dict[str, Any]:
        """The frame columns as ``clear_api.upsert_analysis`` keyword args."""
        return {
            "location_ids": list(self.location_ids),
            "event_types": list(self.event_types),
            "need_sectors": list(self.need_sectors),
            "window_start": self.window_start,
            "window_end": self.window_end,
        }


def country_frame(country_location_id: str, *, window_start: str, window_end: str | None) -> Frame:
    """The frame for a country-default analysis — a single A0 location over the
    given window. Matches what the weekly situation job scopes to today."""
    return Frame.build(
        window_start=window_start,
        window_end=window_end,
        location_ids=[country_location_id],
    )


def build_rag_filters(
    frame: Frame,
    *,
    country_scope_id: str | None = None,
    include_time_range: bool = False,
    effective_end: str | None = None,
) -> dict[str, Any] | None:
    """Turn a frame into a ``KnowledgebaseFilters`` dict for
    ``searchKnowledgebase``.

    ``country_scope_id`` — when set, scope by ``countryLocationId`` (which
    clear-api expands to the A0's whole admin subtree) instead of matching
    ``location_ids`` literally. The weekly country path passes this so its
    retrieval stays identical to today's behaviour: knowledgebase chunks are
    tagged at admin-2, so a literal ``locationIds=[A0]`` filter would match
    almost nothing, whereas the subtree expansion is what actually scopes to
    the country. A **single-location** custom frame likewise scopes by subtree
    (via ``countryLocationId``, below) — so a country-level on-demand analysis
    actually retrieves. A multi-location frame passes its ``location_ids``
    through as ``locationIds`` (literal overlap).

    ``include_time_range`` — off by default so the country path's retrieval is
    unchanged (it never time-filtered RAG; the window governs the numeric
    aggregation, not the narrative retrieval). A future refinement may turn it
    on for fixed-window custom frames so their narrative cites only in-window
    evidence.
    """
    filters: dict[str, Any] = {}
    if country_scope_id:
        filters["countryLocationId"] = country_scope_id
    elif len(frame.location_ids) == 1:
        # A single-location custom frame (e.g. a country-level on-demand analysis)
        # scopes by its SUBTREE, not a literal match: KB chunks are tagged at
        # admin-2, so a literal locationIds=[A0] would match almost nothing.
        # clear-api's countryLocationId expands ANY id to its descendants (a leaf
        # expands to itself, so this is a no-op for an admin-2 frame).
        filters["countryLocationId"] = frame.location_ids[0]
    elif frame.location_ids:
        filters["locationIds"] = list(frame.location_ids)
    if frame.event_types:
        filters["eventTypes"] = list(frame.event_types)
    if frame.need_sectors:
        filters["needSectors"] = list(frame.need_sectors)
    if include_time_range:
        # A rolling frame (window_end None) has no stored end; the caller passes
        # `effective_end` (materialised "now") so the retrieval window is
        # [window_start, now] rather than open-ended.
        end = effective_end or frame.window_end
        time_range: dict[str, str] = {"from": frame.window_start}
        if end:
            time_range["to"] = end
        filters["timeRange"] = time_range
    return filters or None
