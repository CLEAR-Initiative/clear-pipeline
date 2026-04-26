"""Local (no-Claude) signal classification for v2 grouping mode.

The legacy v1 path calls Claude to derive `disaster_types`, `relevance`,
`severity`, and a short `summary` for every incoming signal. That's by far
the single biggest token-driver in the pipeline — ~3k input tokens per
signal, and every poll cycle can fire dozens of them.

In v2 mode the `EventClassifier` (local sentence-transformers model) already
produces the disaster-type label. This module bridges the gap so we can
keep the existing `SignalClassification` contract without calling Claude.

Returned `SignalClassification` has:
  - disaster_types  — list with the classifier's top glide code
  - relevance       — classifier's blended score (0.0–1.0); the
                      RELEVANCE_THRESHOLD applies exactly the same way
  - severity        — source-provided if present, else `default_severity`
                      (caller decides: 3 for unknown-but-relevant, for
                      example). The event-level calculator (mean or Claude
                      rewrite fallback) gives a more accurate number later.
  - summary         — brief summary derived from the signal text
"""

from __future__ import annotations

import logging

from src.models.clear import SignalClassification
from src.services.classifier_singleton import get_classifier

logger = logging.getLogger(__name__)

DEFAULT_FALLBACK_SEVERITY = 3  # used when source didn't supply one


def classify_locally(
    title: str | None,
    description: str | None,
    source_severity: int | None = None,
    default_severity: int = DEFAULT_FALLBACK_SEVERITY,
) -> SignalClassification:
    """Build a `SignalClassification` from the local EventClassifier. No
    network calls.

    `source_severity` — the 1-5 severity already attached to the signal by
    its source (Dataminr alertType mapping, GDACS alert level, ACLED
    fatalities). Pass through if present; otherwise fall back to
    `default_severity` so downstream gates (e.g. severity >= 4 alert check)
    still have something to look at.
    """
    classifier = get_classifier()
    text = " ".join(filter(None, [title, description])) or "unknown event"
    pred = classifier.predict(text, top_k=1)
    top = pred["top_k"][0] if pred.get("top_k") else {}

    glide_code: str | None = top.get("id")
    level_3: str | None = top.get("type_level_3")
    level_1: str | None = top.get("type_level_1")
    confidence: float = float(pred.get("confidence") or 0.0)

    # Summary is a cheap extraction from the title/description — no LLM
    summary_src = (title or description or "").strip()
    summary = summary_src[:200] if summary_src else (level_3 or "unknown event")

    classification = SignalClassification(
        disaster_types=[glide_code] if glide_code else ["ot"],
        relevance=confidence,
        severity=source_severity if source_severity is not None else default_severity,
        summary=summary,
    )
    logger.info(
        "[LOCAL CLASSIFY] l1=%s l3=%s code=%s confidence=%.3f severity=%d (source=%s)",
        level_1, level_3, glide_code, confidence,
        classification.severity, source_severity,
    )
    return classification
