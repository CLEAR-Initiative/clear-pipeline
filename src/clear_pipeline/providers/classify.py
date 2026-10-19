"""Signal classification types + taxonomy helpers.

Holds the ``SignalClassification`` result model and the ``disaster_types``
taxonomy maps (glide code ⇄ level_1/2/3, the level_2 event-type vocabulary, and
the LLM-tag coercion). The disaster-type classification itself is Jev
(``providers/jev.py``); the former local sentence-transformer classifier
(``EventClassifier`` / ``classify_locally``) has been removed — Jev is the only
classifier and a Jev failure requeues the signal (see ``providers/signal_classifier.py``).
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel


class SignalClassification(BaseModel):
    """Output from signal classification (disaster types, relevance, severity)."""

    disaster_types: list[str]  # glide codes, PRIMARY first, e.g. ["fl", "ls"] (multilabel)
    relevance: float  # 0.0-1.0 relevance gate: Jev's P(real disaster type) = 1 - P("none")
    severity: int | None  # 1-5, or None when the source supplied no severity
    summary: str
    # Full taxonomy prediction for the PRIMARY type (``disaster_types[0]``), carried
    # so the grouping stage can reuse it instead of running a second inference on the
    # same text. Secondary multilabel codes live in ``disaster_types`` only.
    type_level_1: str | None = None
    type_level_2: str | None = None
    type_level_3: str | None = None


# ── Taxonomy maps ────────────────────────────────────────────────────────────

# The local mirror of clear-api's `disaster_types` taxonomy (glide code, the
# level_1/2/3 path, key words/phrases). Public so other taxonomy consumers (e.g.
# defs/ground) can load the same file.
DEFAULT_TAXONOMY_PATH = Path(__file__).resolve().parent / "event_categories.json"

_taxonomy: list[dict] | None = None

_TAXONOMY_PATH = DEFAULT_TAXONOMY_PATH


def _load_taxonomy() -> list[dict]:
    global _taxonomy
    if _taxonomy is None:
        _taxonomy = json.loads(_TAXONOMY_PATH.read_text(encoding="utf-8"))
    return _taxonomy


def code_to_level2_map() -> dict[str, str]:
    """Glide code → level_2 group name (e.g. 'fl' → 'flood', 'pp' → 'protests')."""
    taxonomy = _load_taxonomy()
    out: dict[str, str] = {}
    for row in taxonomy:
        code = row.get("id")
        l2 = row.get("type_level_2")
        if code and l2:
            out[code] = l2
    return out


def level2_to_codes_map() -> dict[str, list[str]]:
    """Level_2 group name → list of glide codes (multiple when level_3 codes
    are distinct, e.g. 'protests' → ['pp','pi','pf'])."""
    taxonomy = _load_taxonomy()
    out: dict[str, list[str]] = {}
    for row in taxonomy:
        code = row.get("id")
        l2 = row.get("type_level_2")
        if not (code and l2):
            continue
        out.setdefault(l2, [])
        if code not in out[l2]:
            out[l2].append(code)
    return out


def code_to_level1_map() -> dict[str, str]:
    """Glide code → level_1 category name (e.g. 'ba' → 'conflict')."""
    taxonomy = _load_taxonomy()
    out: dict[str, str] = {}
    for row in taxonomy:
        code = row.get("id")
        l1 = row.get("type_level_1")
        if code and l1:
            out[code] = l1
    return out


def code_to_level3_map() -> dict[str, str]:
    """Glide code → level_3 sub-type name (e.g. 'ba' → 'armed clash',
    'pp' → 'peaceful protest'). Used for the per-event-type stats lookup
    (acled_event_type_stats.json), whose keys are the level_3 strings."""
    taxonomy = _load_taxonomy()
    out: dict[str, str] = {}
    for row in taxonomy:
        code = row.get("id")
        l3 = row.get("type_level_3")
        if code and l3:
            out[code] = l3
    return out


# ── Level-2 event-type vocabulary (shared with KB enrichment + datapoints) ────
#
# The `disaster_types` table's level_2 column is the ONE authoritative event-type
# vocabulary. `event_categories.json` is the pipeline's local mirror of it; the
# signal classifier already picks a `type_level_2` from it. These helpers expose
# the same level_2 set (+ a coercion) so the LLM extraction paths (enrich.py,
# datapoints) stop emitting free-text tags like 'displacement' / 'search-and-rescue'
# that aren't event types.

# Safe synonym map for LLM tags that unambiguously mean a level_2 label but aren't
# spelled exactly. Anything neither a level_2 value nor here is DROPPED.
_EVENT_TYPE_ALIASES: dict[str, str] = {
    "wildfire": "wild fire",
    "wildfires": "wild fire",
    "wild-fire": "wild fire",
    "bushfire": "wild fire",
    "landslide": "land slide",
    "mudslide": "mud slide",
    "flashflood": "flash flood",
    "flash-flood": "flash flood",
    "quake": "earthquake",
    "cyclone": "tropical cyclone",
    "hurricane": "tropical cyclone",
    "typhoon": "tropical cyclone",
    "outbreak": "epidemic",
    "disease outbreak": "epidemic",
    "disease-outbreak": "epidemic",
    "disease": "epidemic",
    "protest": "protests",
    "riot": "riots",
    "avalanche": "snow avalanche",
    "eruption": "volcano",
    "volcanic eruption": "volcano",
}


def level2_values() -> list[str]:
    """Sorted distinct disaster_types level_2 labels — the authoritative event-type
    vocabulary, mirrored from `event_categories.json` (== clear-api disaster_types
    level_2). Shared by signal classification, KB enrichment, and datapoint extraction."""
    return sorted({
        str(row["type_level_2"]).strip().lower()
        for row in _load_taxonomy()
        if row.get("type_level_2")
    })


def coerce_event_types(values: object) -> object:
    """Map LLM-emitted event-type tags onto the level_2 taxonomy, DROPPING anything
    off-taxonomy. Lowercased, aliased, deduped, order-preserving — so a stray tag
    ('displacement', 'search-and-rescue', …) never becomes a bogus event type.
    A non-list is returned unchanged so pydantic reports it as it would today."""
    if not isinstance(values, list):
        return values
    valid = set(level2_values())
    out: list[str] = []
    for item in values:
        key = str(item).strip().lower()
        canon = key if key in valid else _EVENT_TYPE_ALIASES.get(key)
        if canon and canon in valid and canon not in out:
            out.append(canon)
    return out
