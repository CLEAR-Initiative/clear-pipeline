"""
Process-wide singleton for the EventClassifier.

The classifier loads ~80MB of sentence-transformer weights plus computes
embeddings for every taxonomy row on construction — we only want to do
that once per worker child process.

Also exposes taxonomy-derived lookup dicts (`code_to_level2`, `level2_to_codes`)
so the grouping layer can translate between glide codes (what `events.types[]`
stores) and level_2 group names (what the classifier returns).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_classifier: Any | None = None
_taxonomy: list[dict] | None = None

_TAXONOMY_PATH = Path(__file__).resolve().parent / "event_categories.json"


def _load_taxonomy() -> list[dict]:
    global _taxonomy
    if _taxonomy is None:
        _taxonomy = json.loads(_TAXONOMY_PATH.read_text(encoding="utf-8"))
    return _taxonomy


def get_classifier():
    """Return the singleton EventClassifier instance (lazy init on first call)."""
    global _classifier
    if _classifier is None:
        logger.info("[CLASSIFIER] Loading event classifier (first call)…")
        # Imported lazily so workers that never call Claude-less classification
        # don't pay the torch / sentence-transformers import cost.
        from src.services.event_classifier import EventClassifier

        _classifier = EventClassifier()
        logger.info(
            "[CLASSIFIER] Loaded with %d categories",
            len(_classifier.categories),
        )
    return _classifier


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
