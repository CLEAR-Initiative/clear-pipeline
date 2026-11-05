"""Per-run cost budget for the analysis module (doc §7, §11).

Wires ``providers/guardrails.RunBudget`` — which exists but is otherwise unwired
— into the analysis module: a thread-safe soft cap on total LLM spend for one
Dagster run. The runtime (:mod:`runtime`) charges the budget after each call and
gates the next dispatch on :meth:`RunBudget.allow_more_spend`.

The provider layer has no token→USD helper, so this module carries a small price
table (USD per 1M tokens) for the models the analysis role actually uses —
DeepSeek via OpenRouter, plus the Anthropic models the role falls back to — and
converts ``provider.last_usage`` to dollars.

Prices are OpenRouter/Anthropic list rates (see ``defs/evals/candidates.py`` and
the Anthropic price table); they drift, so an unknown model logs a warning and
is counted as $0 rather than blocking a run — the cap stays advisory and honest
about what it could price.
"""

from __future__ import annotations

import logging
import os

from clear_pipeline.providers.guardrails import RunBudget

logger = logging.getLogger(__name__)

# Default per-run cap (USD). Dedicated to the analysis module so it is tuned
# independently of the KB ingest cap (KB_MAX_COST_USD_PER_RUN).
_DEFAULT_CAP_USD = 5.0
_CAP_ENV = "ANALYSIS_MAX_COST_USD_PER_RUN"

# USD per 1,000,000 tokens, as (input, output), keyed by the model string the
# provider reports (``provider.model``). DeepSeek via OpenRouter is the analysis
# default; the Anthropic entries price the extraction-role fallback.
_PRICES_PER_1M: dict[str, tuple[float, float]] = {
    # DeepSeek (OpenRouter slugs) — the analysis defaults.
    "deepseek/deepseek-v3.2": (0.27, 0.40),
    "deepseek/deepseek-v4-flash": (0.14, 0.28),
    # Anthropic fallback (via the extraction role).
    "claude-haiku-4-5": (1.00, 5.00),
    "claude-sonnet-4-6": (3.00, 15.00),
}


def usd_for(model: str, usage: dict | None) -> float:
    """Cost in USD of one call, from its token ``usage`` and the model's price.

    ``usage`` is ``{"input_tokens", "output_tokens"}`` (``provider.last_usage``).
    An unknown model (or missing usage) is counted as $0 with a warning so a
    pricing gap degrades the cap to advisory rather than crashing the run.
    """
    if not usage:
        return 0.0
    price = _PRICES_PER_1M.get(model)
    if price is None:
        logger.warning(
            "[analysis budget] no price for model %r — counting $0 (cap under-counts)",
            model,
        )
        return 0.0
    in_price, out_price = price
    in_tok = int(usage.get("input_tokens", 0) or 0)
    out_tok = int(usage.get("output_tokens", 0) or 0)
    return (in_tok * in_price + out_tok * out_price) / 1_000_000.0


def analysis_cap_usd() -> float:
    """The configured per-run cap (``ANALYSIS_MAX_COST_USD_PER_RUN``)."""
    return float(os.environ.get(_CAP_ENV, str(_DEFAULT_CAP_USD)))


def analysis_run_budget() -> RunBudget:
    """A fresh :class:`RunBudget` for one analysis run, at the configured cap."""
    return RunBudget(analysis_cap_usd())


def charge(budget: RunBudget, provider, *, role: str = "analysis") -> float:
    """Charge ``budget`` for the provider's most recent call and return the USD.

    Reads ``provider.last_usage`` + ``provider.model`` (so a fallback to a
    different model is priced on what actually served). Call once per completed
    LLM call, with one provider instance per concurrent unit so ``last_usage``
    is unambiguous (doc §7 concurrency caveat).
    """
    usd = usd_for(getattr(provider, "model", ""), getattr(provider, "last_usage", None))
    if usd:
        budget.add_spend(role, usd)
    return usd
