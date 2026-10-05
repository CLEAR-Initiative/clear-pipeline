"""Signal disaster-type classifier selector — the single entry point used by
both the signals drain (``defs/signals/stages.py``) and the gx_pipeline
(``defs/gx_pipeline/factory.py``).

Runs Jev (TypeSafe System One via OpenRouter) by default and falls back to the
local MiniLM classifier on an OpenRouter outage, so a transient API failure never
drops classification to zero. ``SIGNAL_CLASSIFIER=minilm`` forces the local path
(instant rollback without a deploy).

Process-local counters (``jev``, ``fallback``, ``minilm``) let a caller surface
how often Jev is actually serving vs. silently falling back — a Jev outage would
otherwise be invisible (every signal still classifies, just via MiniLM). The
signals drain snapshots + resets these per run and emits a fallback rate into its
``MaterializeResult`` metadata (see ``defs/signals/stages.py``)."""

from __future__ import annotations

import logging
from collections import Counter

from clear_pipeline.providers.classify import SignalClassification, classify_locally
from clear_pipeline.providers.jev import JevError, classify_with_jev
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

# Process-local; a long-lived worker accumulates until a caller resets it.
_STATS: Counter[str] = Counter()


def classifier_stats_snapshot() -> dict[str, int]:
    """Current classifier outcome counts (``jev`` = Jev served, ``fallback`` =
    Jev failed → MiniLM, ``minilm`` = MiniLM forced by config)."""
    return dict(_STATS)


def reset_classifier_stats() -> None:
    """Reset the counters — call at the start of a bounded unit of work (a drain)
    so the emitted rate reflects that run, not the worker's whole lifetime."""
    _STATS.clear()


def classify_signal(
    title: str | None,
    description: str | None,
    source_severity: int | None = None,
) -> SignalClassification:
    """Classify a signal's disaster type. Jev primary (unless
    ``SIGNAL_CLASSIFIER=minilm``), MiniLM fallback on any Jev/OpenRouter failure."""
    if settings.signal_classifier == "jev":
        try:
            result = classify_with_jev(
                title=title, description=description, source_severity=source_severity,
            )
            _STATS["jev"] += 1
            return result
        except JevError as exc:
            _STATS["fallback"] += 1
            logger.warning(
                "[CLASSIFY] Jev classify failed (%s) — falling back to local MiniLM classifier", exc,
            )
    else:
        _STATS["minilm"] += 1
    return classify_locally(title=title, description=description, source_severity=source_severity)
