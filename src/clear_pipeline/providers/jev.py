"""Jev (TypeSafe System One) disaster-type classifier, via OpenRouter's
Decisions API. Drop-in replacement for ``classify_locally``: returns the same
``SignalClassification`` so signal grouping + event classification are unchanged
downstream (events reuse this result — see ``providers/event.py``).

Jev reads text and returns typed answers with calibrated probabilities (not
generated prose). One Decisions call runs two parallel questions over the signal:

  * ``glide``    — a Choice over the 52 GLIDE codes → the disaster type (+ a
                   calibrated confidence in that code).
  * ``relevant`` — a Noul "is this an actual, current incident?" → the probability
                   used as the relevance gate.

Relevance (the event-creation gate) is the **Noul**, NOT the Choice confidence:
a signal can be clearly a real incident yet genuinely ambiguous between two codes
(low Choice confidence) — it must still create an event. Conflating the two (as
the MiniLM score did) would drop ambiguous-but-real signals.

Config (env):
  OPENROUTER_API_KEY   required — the OpenRouter key (same one the evals use).
  JEV_MODEL            default "typesafe/jev-1.13".
  JEV_URL              default the Decisions endpoint.
  JEV_TIMEOUT_SECONDS  default 30.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache

import requests

from clear_pipeline.providers.classify import (
    DEFAULT_FALLBACK_SEVERITY,
    SignalClassification,
    _load_taxonomy,
    code_to_level1_map,
    code_to_level2_map,
    code_to_level3_map,
)

logger = logging.getLogger(__name__)

JEV_MODEL = os.environ.get("JEV_MODEL", "typesafe/jev-1.13")
# OpenRouter serves Jev through the Decisions API (not chat-completions). The API
# reference also documents .../api/v1/systemone — override with JEV_URL if needed.
JEV_URL = os.environ.get("JEV_URL", "https://openrouter.ai/api/alpha/decisions")
_TIMEOUT = float(os.environ.get("JEV_TIMEOUT_SECONDS", "30"))

_GLIDE_INSTRUCTIONS = (
    "Classify the emergency/disaster signal in `text` into exactly one GLIDE "
    "disaster-type code. Pick the single code whose category best matches the "
    "PRIMARY hazard or event described. If several apply, choose the dominant one."
)
_RELEVANT_INSTRUCTIONS = (
    "Does `text` report an ACTUAL, CURRENT humanitarian, disaster, or security "
    "incident — a real event that has happened or is happening?"
)
_RELEVANT_CRITERIA = {
    "true": "A concrete incident/event is described (e.g. a flood, clash, outbreak, strike).",
    "false": "Routine news, analysis or opinion, a retrospective/anniversary, a "
    "forecast or warning with no event yet, or an unrelated topic.",
}


class JevError(RuntimeError):
    """Raised on any Jev/OpenRouter failure so the caller can fall back."""


@lru_cache(maxsize=1)
def _glide_criteria() -> dict[str, str]:
    """GLIDE code -> criterion the model chooses among: the L1 > L2 > L3 path
    plus a couple of canonical phrasings as cues. Cached — the taxonomy is static."""
    criteria: dict[str, str] = {}
    for row in _load_taxonomy():
        code = row.get("id")
        if not code:
            continue
        path = " > ".join(
            p for p in (row.get("type_level_1"), row.get("type_level_2"), row.get("type_level_3")) if p
        )
        cues = "; ".join((row.get("key_phrases") or [])[:3])
        criteria[code] = f"{path} (e.g. {cues})" if cues else path
    return criteria


def _api_key() -> str:
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        raise JevError("OPENROUTER_API_KEY is not set")
    return key


def classify_with_jev(
    title: str | None,
    description: str | None,
    source_severity: int | None = None,
    default_severity: int = DEFAULT_FALLBACK_SEVERITY,
) -> SignalClassification:
    """Classify one signal with Jev. Same contract as ``classify_locally``.

    ``source_severity`` (the 1-5 severity the source already attached) is passed
    through if present, else ``default_severity`` — Jev is not asked to score
    severity (sources supply it; that's out of scope for the type classifier).

    Raises ``JevError`` on any API/parse failure so the caller can fall back to
    the local classifier rather than drop the signal."""
    text = " ".join(filter(None, [title, description])) or "unknown event"
    body = {
        "model": JEV_MODEL,
        "state": {"text": text},
        "questions": {
            "glide": {"type": "choice", "instructions": _GLIDE_INSTRUCTIONS, "criteria": _glide_criteria()},
            "relevant": {"type": "noul", "instructions": _RELEVANT_INSTRUCTIONS, "criteria": _RELEVANT_CRITERIA},
        },
    }
    try:
        resp = requests.post(
            JEV_URL,
            headers={"Authorization": f"Bearer {_api_key()}", "Content-Type": "application/json"},
            json=body,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        answers = resp.json()["answers"]
        glide = answers["glide"]
        relevant = answers["relevant"]
    except JevError:
        raise
    except Exception as exc:  # noqa: BLE001 — network/HTTP/shape: one failure mode to the caller
        raise JevError(f"Jev classify failed: {exc}") from exc

    code = glide.get("choice") or "ot"
    code_confidence = float(glide.get("confidence") or 0.0)
    relevance = float(relevant.get("noul") or 0.0)

    summary_src = (title or description or "").strip()
    summary = summary_src[:200] if summary_src else (code_to_level3_map().get(code) or "unknown event")

    classification = SignalClassification(
        disaster_types=[code],
        relevance=relevance,
        severity=source_severity if source_severity is not None else default_severity,
        summary=summary,
        type_level_1=code_to_level1_map().get(code),
        type_level_2=code_to_level2_map().get(code),
        type_level_3=code_to_level3_map().get(code),
    )
    logger.info(
        "[JEV CLASSIFY] code=%s code_conf=%.2f relevance=%.2f severity=%d (source=%s)",
        code, code_confidence, relevance, classification.severity, source_severity,
    )
    return classification
