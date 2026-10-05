"""Jev (TypeSafe System One) disaster-type classifier, via OpenRouter's
Decisions API. Drop-in replacement for ``classify_locally``: returns the same
``SignalClassification`` so signal grouping + event classification are unchanged
downstream (events reuse this result — see ``providers/event.py``).

Jev reads text and returns typed answers with calibrated probabilities (not
generated prose). One Decisions call runs two parallel questions over the signal:

  * ``glide``    — a Choice over the 52 GLIDE codes → the disaster type (+ a
                   calibrated confidence in that code).
  * ``relevant`` — a Noul "is this an actual, current incident?" → the
                   probability used as the relevance gate.

Relevance (the event-creation gate) is the **Noul**, NOT the Choice confidence:
a signal can be clearly a real incident yet genuinely ambiguous between two codes
(low Choice confidence) — it must still create an event.

Failure posture (every path that isn't a confident, well-formed answer raises
``JevError`` so the caller falls back to the local classifier):
  * HTTP/transport errors, non-retryable 4xx, retries exhausted on 429/5xx;
  * a 200 whose answer is malformed — missing ``choice`` or a missing/non-numeric
    ``noul`` — rather than silently classifying "ot" or gating relevance to 0.0;
  * the circuit is open (a sustained outage fast-fails to MiniLM instead of
    paying the per-call timeout on every signal and stacking past the drain lock).

Config (env):
  OPENROUTER_API_KEY            required — the OpenRouter key.
  JEV_MODEL                     default "typesafe/jev-1.13".
  JEV_URL                       default the Decisions endpoint.
  JEV_TIMEOUT_SECONDS           per-attempt timeout, default 30.
  JEV_RETRIES                   attempts on 429/5xx/timeout, default 3.
  JEV_BACKOFF_SECONDS           base backoff (exponential), default 0.5.
  JEV_CIRCUIT_FAILURE_THRESHOLD consecutive failures before the breaker opens, default 4.
  JEV_CIRCUIT_OPEN_SECONDS      breaker cooldown, default 120.
"""

from __future__ import annotations

import logging
import os
import time
from functools import lru_cache

import httpx

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
_RETRIES = max(1, int(os.environ.get("JEV_RETRIES", "3")))
_BACKOFF_BASE = float(os.environ.get("JEV_BACKOFF_SECONDS", "0.5"))
# 4xx that are worth retrying (rate limit / conflict / too-early); other 4xx are
# caller errors and fail fast.
_RETRYABLE_4XX = {408, 409, 425, 429}

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
    """Raised on any Jev/OpenRouter failure (incl. malformed answers and an open
    circuit) so the caller can fall back to the local classifier."""


class _CircuitBreaker:
    """Minimal breaker in the repo's per-domain style (cf. the geocoder breaker).
    Opens after ``threshold`` consecutive failures and fast-fails for
    ``open_seconds`` so a sustained outage doesn't pay the timeout on every
    signal (and can't stack past the drain's single-flight lock TTL)."""

    def __init__(self, threshold: int, open_seconds: float):
        self._threshold = threshold
        self._open_seconds = open_seconds
        self._failures = 0
        self._open_until = 0.0

    def is_open(self) -> bool:
        return time.monotonic() < self._open_until

    def record_success(self) -> None:
        self._failures = 0
        self._open_until = 0.0

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self._threshold:
            self._open_until = time.monotonic() + self._open_seconds
            logger.error(
                "[JEV CLASSIFY] circuit OPEN after %d consecutive failures — "
                "fast-failing to MiniLM for %.0fs", self._failures, self._open_seconds,
            )


_BREAKER = _CircuitBreaker(
    threshold=max(1, int(os.environ.get("JEV_CIRCUIT_FAILURE_THRESHOLD", "4"))),
    open_seconds=float(os.environ.get("JEV_CIRCUIT_OPEN_SECONDS", "120")),
)


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


def _post_decisions(body: dict) -> dict:
    """POST to the Decisions API; retry with exponential backoff on 429/5xx and
    transport/timeout errors; fail fast on other 4xx. Returns the ``answers`` dict
    or raises ``JevError``."""
    headers = {"Authorization": f"Bearer {_api_key()}", "Content-Type": "application/json"}
    last = "no attempts"
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = httpx.post(JEV_URL, headers=headers, json=body, timeout=_TIMEOUT)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            last = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code < 400:
                payload = resp.json()
                answers = payload.get("answers")
                if not isinstance(answers, dict):
                    raise JevError("Jev response missing `answers`")
                return answers
            snippet = (resp.text or "")[:300]
            if resp.status_code < 500 and resp.status_code not in _RETRYABLE_4XX:
                raise JevError(f"Jev {resp.status_code} (non-retryable): {snippet}")
            last = f"HTTP {resp.status_code}: {snippet}"
        if attempt < _RETRIES:
            time.sleep(_BACKOFF_BASE * (2 ** (attempt - 1)))
    raise JevError(f"Jev request failed after {_RETRIES} attempt(s): {last}")


def classify_with_jev(
    title: str | None,
    description: str | None,
    source_severity: int | None = None,
    default_severity: int = DEFAULT_FALLBACK_SEVERITY,
) -> SignalClassification:
    """Classify one signal with Jev. Same contract as ``classify_locally``.
    Raises ``JevError`` on any failure (incl. a malformed answer or an open
    circuit) so the caller falls back to the local classifier."""
    if _BREAKER.is_open():
        raise JevError("Jev circuit breaker is open — skipping call")

    try:
        text = " ".join(filter(None, [title, description])) or "unknown event"
        body = {
            "model": JEV_MODEL,
            "state": {"text": text},
            "questions": {
                "glide": {"type": "choice", "instructions": _GLIDE_INSTRUCTIONS, "criteria": _glide_criteria()},
                "relevant": {"type": "noul", "instructions": _RELEVANT_INSTRUCTIONS, "criteria": _RELEVANT_CRITERIA},
            },
        }
        answers = _post_decisions(body)

        # Reject malformed-but-200 answers rather than silently classifying "ot"
        # or gating relevance to 0.0 (which would drop every event, invisibly).
        glide = answers.get("glide")
        relevant = answers.get("relevant")
        if not isinstance(glide, dict) or not isinstance(relevant, dict):
            raise JevError("Jev response missing `glide`/`relevant` answer")
        code = glide.get("choice")
        if not code:
            raise JevError("Jev response missing `choice`")
        noul = relevant.get("noul")
        if not isinstance(noul, (int, float)) or isinstance(noul, bool):
            raise JevError(f"Jev response has missing/non-numeric `noul`: {noul!r}")
    except JevError:
        _BREAKER.record_failure()
        raise
    except Exception as exc:  # noqa: BLE001 — any other shape/parse error → fallback
        _BREAKER.record_failure()
        raise JevError(f"Jev classify failed: {exc}") from exc

    _BREAKER.record_success()
    code_confidence = float(glide.get("confidence") or 0.0)
    relevance = float(noul)
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
