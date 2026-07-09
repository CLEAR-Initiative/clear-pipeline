"""Runtime guardrails for the knowledge-base pipeline.

Env-driven ceilings that keep a runaway ingest from silently burning
a month's LLM budget on a single week. Every asset that touches an
LLM or embedding API consults these before spending.

Design principle: guardrails are advisory to the asset, not enforced
in the providers themselves — an asset can decide whether to skip
contextualization (degraded quality) or hard-abort (no output). The
default policy in the ingest chain is skip-with-warning.

Env vars:
  KB_MAX_CHUNKS_PER_REPORT     (int, default 500)
      Cap on chunks any single report contributes. A malformed PDF
      that tokenises to 10,000 chunks would otherwise pin an entire
      run against Anthropic's rate limits.

  KB_MAX_COST_USD_PER_RUN      (float, default 5.0)
      Soft cap across the whole materialisation. Assets track their
      spend and refuse further LLM calls once the cap is passed. The
      current run finishes what it started (no mid-flight abort).

  KB_SKIP_CONTEXTUALIZATION    (bool, default false)
      Emergency kill-switch — set to "1" / "true" to skip the
      contextualization step entirely and embed the raw chunk text.
      Retrieval quality drops noticeably; use only when the LLM
      provider is down or budget is exhausted.
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Guardrails:
    """Immutable snapshot of the guardrail config for one Dagster run.

    Instances are created once at asset start-up via
    :func:`load_guardrails` and passed down explicitly — no module-level
    globals so multi-run test scenarios stay hermetic.
    """
    max_chunks_per_report: int
    max_cost_usd_per_run: float
    skip_contextualization: bool


def load_guardrails() -> Guardrails:
    return Guardrails(
        max_chunks_per_report=int(os.environ.get("KB_MAX_CHUNKS_PER_REPORT", "500")),
        max_cost_usd_per_run=float(os.environ.get("KB_MAX_COST_USD_PER_RUN", "5.0")),
        skip_contextualization=_parse_bool(
            os.environ.get("KB_SKIP_CONTEXTUALIZATION", "0"),
        ),
    )


class RunBudget:
    """Thread-safe running total of LLM + embedding spend for a single
    Dagster materialisation. An asset increments after each call and
    checks :meth:`allow_more_spend` before the next.

    The class deliberately does NOT abort a mid-flight call — callers
    that need pre-flight enforcement should also check the cap
    themselves before dispatching. This keeps the accounting logic
    simple and honest about "we spent X, cap is Y".
    """

    def __init__(self, cap_usd: float) -> None:
        self._cap = cap_usd
        self._spent = 0.0
        self._lock = threading.Lock()
        # Break down by role so telemetry can show where the run went
        # over — often the contextualization step is the big spender.
        self._by_role: dict[str, float] = {}

    def add_spend(self, role: str, usd: float) -> None:
        with self._lock:
            self._spent += usd
            self._by_role[role] = self._by_role.get(role, 0.0) + usd

    def allow_more_spend(self) -> bool:
        with self._lock:
            return self._spent < self._cap

    @property
    def spent(self) -> float:
        with self._lock:
            return self._spent

    @property
    def by_role(self) -> dict[str, float]:
        with self._lock:
            return dict(self._by_role)

    def log_summary(self, *, run_id: str) -> None:
        with self._lock:
            logger.info(
                "[KB budget] run=%s spent=$%.4f cap=$%.2f by_role=%s",
                run_id, self._spent, self._cap, self._by_role,
            )


def _parse_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "on"}
