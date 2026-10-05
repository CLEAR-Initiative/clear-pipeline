"""Signal disaster-type classifier selector — the single entry point used by
both the signals drain (``defs/signals/stages.py``) and the gx_pipeline
(``defs/gx_pipeline/factory.py``).

Runs Jev (TypeSafe System One via OpenRouter) by default and falls back to the
local MiniLM classifier on an OpenRouter outage, so a transient API failure never
drops classification to zero. ``SIGNAL_CLASSIFIER=minilm`` forces the local path
(instant rollback without a deploy)."""

from __future__ import annotations

import logging

from clear_pipeline.providers.classify import SignalClassification, classify_locally
from clear_pipeline.providers.jev import JevError, classify_with_jev
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)


def classify_signal(
    title: str | None,
    description: str | None,
    source_severity: int | None = None,
) -> SignalClassification:
    """Classify a signal's disaster type. Jev primary (unless
    ``SIGNAL_CLASSIFIER=minilm``), MiniLM fallback on any Jev/OpenRouter failure."""
    if settings.signal_classifier == "jev":
        try:
            return classify_with_jev(
                title=title, description=description, source_severity=source_severity,
            )
        except JevError as exc:
            logger.warning(
                "[classify] Jev classify failed (%s) — falling back to local MiniLM classifier", exc,
            )
    return classify_locally(title=title, description=description, source_severity=source_severity)
