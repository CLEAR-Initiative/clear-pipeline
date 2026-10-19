"""Jev (TypeSafe System One) disaster-type classifier, via OpenRouter's
Decisions API. The sole signal/event classifier: returns a
``SignalClassification`` that signal grouping + event classification reuse
downstream (see ``providers/event.py``).

Jev reads text and returns typed answers with calibrated probabilities (not
generated prose). One Decisions call runs a single Choice question over the signal:

  * ``glide`` — a Choice over the 52 GLIDE codes PLUS an explicit "none" no-match
                option. It returns the single best (primary) code, a calibrated
                confidence in it, and the full probability distribution over every
                code.

Multilabel: a signal can describe several co-occurring hazards (a flood that also
triggers landslides and displacement). The Choice picks the one PRIMARY code, and
its probability distribution supplies the rest — every code at or above
``settings.signal_jev_multilabel_threshold`` joins ``disaster_types`` (primary
first). ``disaster_types[0]`` stays the primary code everything downstream keys on.

Relevance (the event-creation gate) is derived from that same distribution as
``1 - P("none")`` — Jev's total probability that the text is SOME real disaster
type rather than the explicit no-match option. Signals below
``settings.relevance_threshold`` are dropped from grouping (see
``defs/signals/stages.py``). This is deliberately distribution-based, not the
Choice confidence: a real incident whose probability is SPLIT across several real
codes (low confidence, high multilabel) still scores ~1.0 and must create an event.
It replaces the earlier ``relevant`` Noul — there is no separate relevance question.

Anti-misclassification: to avoid force-fitting a code onto an incident that fits
none of the specific types, the glide Choice offers an explicit ``none`` option,
and a PRIMARY code picked only weakly (confidence below
``settings.signal_jev_min_code_confidence``) is treated the same way. Either case
is recorded as the taxonomy's existing "other" catch-all (``ot``/``po``) — still a
valid glide code downstream — rather than an unreliable specific type.

Failure posture (every path that isn't a confident, well-formed answer raises
``JevError`` so the caller can requeue the signal — there is no local fallback):
  * HTTP/transport errors, non-retryable 4xx, retries exhausted on 429/5xx;
  * a 200 whose answer is malformed — a missing ``glide`` answer or ``choice`` —
    rather than silently classifying "ot" or gating relevance to 0.0;
  * the circuit is open (a sustained outage fast-fails instead of paying the
    per-call timeout on every signal and stacking past the drain lock).

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
    SignalClassification,
    _load_taxonomy,
    code_to_level1_map,
    code_to_level2_map,
    code_to_level3_map,
)
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

# Sentinel the glide Choice may return when no listed disaster type fits (added to
# the criteria in `_glide_criteria`). Not a real glide code — mapped to the
# taxonomy's existing "other" catch-all for storage (see `classify_with_jev`), so
# every downstream consumer still sees a valid code.
NONE_CHOICE = "none"
# The taxonomy's two "other" buckets. A none/low-confidence result is recorded as
# one of these rather than a force-fit specific code.
OTHER_NATURAL = "ot"   # natural hazard > other
OTHER_CONFLICT = "po"  # conflict > ... > other

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
    "PRIMARY hazard or event described. If several apply, choose the dominant one. "
    f'Pick "{NONE_CHOICE}" ONLY when the text describes an incident that genuinely '
    "fits none of the specific disaster types — prefer this over force-fitting a code."
)
class JevError(RuntimeError):
    """Raised on any Jev/OpenRouter failure (incl. malformed answers and an open
    circuit) so the caller can requeue the signal (there is no local fallback)."""


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
                "fast-failing (signals requeue) for %.0fs", self._failures, self._open_seconds,
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
    # Explicit no-match outcome (TypeSafe Choice idiom): a clear decline path so
    # the model says "nothing fits" instead of force-fitting the nearest code.
    criteria[NONE_CHOICE] = (
        "None of the above — a real incident that does not clearly fit any of the "
        "listed disaster types. Prefer this over guessing a specific code."
    )
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
) -> SignalClassification:
    """Classify one signal with Jev. Raises ``JevError`` on any failure (incl. a
    malformed answer or an open circuit) so the caller can requeue the signal —
    there is no local fallback."""
    if _BREAKER.is_open():
        raise JevError("Jev circuit breaker is open — skipping call")

    try:
        text = " ".join(filter(None, [title, description])) or "unknown event"
        body = {
            "model": JEV_MODEL,
            "state": {"text": text},
            "questions": {
                "glide": {"type": "choice", "instructions": _GLIDE_INSTRUCTIONS, "criteria": _glide_criteria()},
            },
        }
        answers = _post_decisions(body)

        # Reject malformed-but-200 answers rather than silently classifying "ot"
        # or gating relevance to 0.0 (which would drop every event, invisibly).
        glide = answers.get("glide")
        if not isinstance(glide, dict):
            raise JevError("Jev response missing `glide` answer")
        code = glide.get("choice")
        if not code:
            raise JevError("Jev response missing `choice`")
    except JevError:
        _BREAKER.record_failure()
        raise
    except Exception as exc:  # noqa: BLE001 — any other shape/parse error → fallback
        _BREAKER.record_failure()
        raise JevError(f"Jev classify failed: {exc}") from exc

    _BREAKER.record_success()
    code_confidence = float(glide.get("confidence") or 0.0)
    probabilities = glide.get("probabilities")
    if not isinstance(probabilities, dict):
        probabilities = {}

    # Relevance gate, derived from the glide distribution: Jev's total probability
    # that the text is SOME real disaster type = 1 - P("none"). Replaces the old
    # `relevant` Noul. Multilabel-friendly — a real incident whose probability is
    # SPLIT across several real codes still scores ~1.0 (only the explicit no-match
    # option counts against it). Falls back to the code confidence when the model
    # returned no distribution (e.g. an older Jev build).
    if probabilities:
        relevance = max(0.0, 1.0 - float(probabilities.get(NONE_CHOICE) or 0.0))
    else:
        relevance = 0.0 if code == NONE_CHOICE else code_confidence

    # Resolve the PRIMARY code. Prevent force-fit misclassification: record the
    # taxonomy's existing "other" catch-all instead of an unreliable specific type
    # when either the model declined (chose NONE_CHOICE) or it picked a specific
    # code only weakly (confidence below the configured floor — a flat distribution).
    # "other" stays a valid glide code everywhere downstream (grouping, KB, alerts).
    # For the low-confidence case the code's level_1 is still trustworthy (conflict
    # → "po", else "ot"); an explicit "none" carries no level_1, so default to "ot".
    primary = code
    demote_reason: str | None = None
    if code == NONE_CHOICE:
        primary = OTHER_NATURAL
        demote_reason = "model chose none-of-the-above"
    elif code_confidence < settings.signal_jev_min_code_confidence:
        primary = (
            OTHER_CONFLICT if code_to_level1_map().get(code) == "conflict" else OTHER_NATURAL
        )
        demote_reason = (
            f"low code confidence {code_confidence:.2f} < "
            f"{settings.signal_jev_min_code_confidence:.2f}"
        )

    # Multilabel: when the primary stands (a trusted specific code), add every OTHER
    # code at/above the multilabel threshold from the distribution, ordered by
    # probability. When the primary was demoted to "other" the specific codes aren't
    # trustworthy either, so keep a single "other" label.
    if demote_reason:
        disaster_types = [primary]
    else:
        extras = sorted(
            (
                c
                for c, p in probabilities.items()
                if c != NONE_CHOICE and c != primary
                and float(p) >= settings.signal_jev_multilabel_threshold
            ),
            key=lambda c: float(probabilities[c]),
            reverse=True,
        )
        disaster_types = [primary, *extras]

    summary_src = (title or description or "").strip()
    summary = summary_src[:200] if summary_src else (code_to_level3_map().get(primary) or "unknown event")

    classification = SignalClassification(
        disaster_types=disaster_types,
        relevance=relevance,
        severity=source_severity,  # None when the source gave none — not invented
        summary=summary,
        type_level_1=code_to_level1_map().get(primary),
        type_level_2=code_to_level2_map().get(primary),
        type_level_3=code_to_level3_map().get(primary),
    )
    if demote_reason:
        logger.info(
            "[JEV CLASSIFY] unclassifiable (%s): model code=%s → stored %s",
            demote_reason, code, primary,
        )
    logger.info(
        "[JEV CLASSIFY] codes=%s primary_conf=%.2f relevance=%.2f severity=%s (source=%s)",
        disaster_types, code_confidence, relevance, classification.severity, source_severity,
    )
    return classification
