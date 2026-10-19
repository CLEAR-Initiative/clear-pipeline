"""Signal disaster-type classifier entry point — used by both the signals drain
(``defs/signals/stages.py``) and the gx_pipeline (``defs/gx_pipeline/factory.py``).

Jev (TypeSafe System One via OpenRouter) is the ONLY classifier. There is no
local fallback: a Jev/OpenRouter failure raises ``JevError`` so the caller can
leave the signal NEW and retry it on the next drain (the signals drain maps the
failure to a requeue; see ``defs/signals/stages.py``). A transient outage stalls
the oldest-first queue head until Jev recovers rather than silently classifying
on a lower-accuracy local model.

Process-local counters (``jev`` = served, ``jev_failed`` = raised → requeued)
let a caller surface how often Jev is failing. The signals drain snapshots +
resets these per run and emits a Jev failure rate into its ``MaterializeResult``
metadata (see ``defs/signals/stages.py``)."""

from __future__ import annotations

import logging
from collections import Counter

from clear_pipeline.providers.classify import SignalClassification
from clear_pipeline.providers.jev import JevError, classify_with_jev

logger = logging.getLogger(__name__)

# Process-local; a long-lived worker accumulates until a caller resets it.
_STATS: Counter[str] = Counter()


def classifier_stats_snapshot() -> dict[str, int]:
    """Current classifier outcome counts (``jev`` = Jev served, ``jev_failed`` =
    Jev failed and the signal was requeued)."""
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
    """Classify a signal's disaster type with Jev. Raises ``JevError`` on any
    Jev/OpenRouter failure (incl. a malformed answer or an open circuit) — there
    is no local fallback, so the caller must handle it (the signals drain leaves
    the signal NEW and retries next run)."""
    try:
        result = classify_with_jev(
            title=title, description=description, source_severity=source_severity,
        )
    except JevError:
        _STATS["jev_failed"] += 1
        raise
    _STATS["jev"] += 1
    return result
