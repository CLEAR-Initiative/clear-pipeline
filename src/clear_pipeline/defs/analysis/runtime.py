"""The one bounded-agent runtime (doc §4, §11) — how a single node executes.

Every ``agent``/``aloop`` node runs this same generic path:

    retrieve (optional tool) -> sanitise -> complete_structured(schema)
      -> validate + read the completeness self-rating -> meets_bar?
         -> yes: done · no, and turns/budget remain: loop · else: best-effort

The node is a **single structured call by default** (``caps.max_turns == 1``)
and escalates to a bounded loop only when a ``meets_bar`` predicate is supplied
and keeps failing. It enforces the per-run cost cap (``RunBudget``), records
token usage, reads the model's completeness self-rating (there is no separate
judge — doc §8), and stamps provenance.

Concurrency: pass **one provider instance per concurrent unit** (doc §7) so
``last_usage``/``total_usage`` are unambiguous; the op-level fan-out (a later
PR) calls ``run_node`` once per node on its own provider.

This module is pure orchestration — the tools (RAG/web), the output schema, and
persistence are injected or live elsewhere, so the runtime is unit-testable with
a mocked provider and carries no Dagster/DB/LLM dependency of its own.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel

from .budget import charge, usd_for
from .profiles import Caps
from .registry import ANALYSIS_LLM_ROLE, ProvenanceStamp, provenance_stamp

TModel = TypeVar("TModel", bound=BaseModel)

Status = Literal["ok", "best_effort"]


class BudgetExhaustedError(RuntimeError):
    """Raised when the run budget was already spent before the node could make
    even its first call — nothing was produced. The caller decides whether to
    skip the node or ship a deterministic-only artifact (doc §16 budget policy)."""


@dataclass(frozen=True)
class NodeOutcome(Generic[TModel]):
    """What one node produces — the schema output plus everything the DAG needs
    to persist, filter (doc §8), budget, and audit it."""

    output: TModel
    status: Status
    turns: int
    completeness: float | None
    questions_not_answered: list[str]
    usage: dict
    cost_usd: float
    provenance: ProvenanceStamp
    meta: dict[str, Any] = field(default_factory=dict)


def _read_coverage(output: BaseModel) -> tuple[float | None, list[str]]:
    """Read the completeness self-rating off the output if it carries one
    (the ``QuestionCoverage`` fields); otherwise (None, [])."""
    completeness = getattr(output, "completeness", None)
    qna = list(getattr(output, "questions_not_answered", []) or [])
    return completeness, qna


def run_node(
    *,
    provider,
    system: str,
    build_user: Callable[[Sequence[Any]], str],
    schema: type[TModel],
    caps: Caps,
    budget=None,
    retrieve: Callable[[int], Sequence[Any]] | None = None,
    sanitise: Callable[[Sequence[Any]], Sequence[Any]] | None = None,
    meets_bar: Callable[[TModel], bool] | None = None,
    role: str = ANALYSIS_LLM_ROLE,
) -> NodeOutcome[TModel]:
    """Run one bounded-agent node and return its :class:`NodeOutcome`.

    Parameters
    ----------
    provider:
        An ``LLMProvider`` dedicated to this node (one per concurrent unit).
    system / build_user:
        The system prompt and a builder turning the (sanitised) retrieved
        evidence into the user prompt. ``build_user([])`` is valid when there
        is no retrieval tool.
    schema:
        The versioned Pydantic output type. If it carries ``completeness`` /
        ``questions_not_answered`` (the ``QuestionCoverage`` mixin), the runtime
        records them as the node's self-rating.
    caps:
        Per-node ceilings. ``caps.max_turns == 1`` is the single-call default;
        >1 enables bounded escalation when ``meets_bar`` keeps failing.
    budget:
        Optional :class:`RunBudget`. The next dispatch is gated on
        ``allow_more_spend``; each completed call is charged.
    retrieve / sanitise:
        Optional tool: ``retrieve(turn)`` returns evidence; ``sanitise`` cleans
        it (untrusted web/KB content is data, not instructions — doc §9.2).
    meets_bar:
        Optional predicate on the output; when absent the node never escalates.
    """
    clean = sanitise or (lambda e: e)
    max_turns = max(1, caps.max_turns)

    output: TModel | None = None
    turns = 0
    bar_met = False

    while turns < max_turns:
        if budget is not None and not budget.allow_more_spend():
            break
        turns += 1
        evidence: Sequence[Any] = clean(retrieve(turns)) if retrieve is not None else []
        user = build_user(evidence)
        output = provider.complete_structured(
            system=system, user=user, schema=schema, max_tokens=caps.max_tokens
        )
        if budget is not None:
            charge(budget, provider, role=role)
        bar_met = meets_bar(output) if meets_bar is not None else True
        if bar_met:
            break

    if output is None:
        # Budget disallowed spend before the first call — nothing produced.
        raise BudgetExhaustedError(
            "run budget exhausted before the node made any LLM call"
        )

    completeness, qna = _read_coverage(output)
    usage = dict(getattr(provider, "total_usage", {}) or {})
    return NodeOutcome(
        output=output,
        status="ok" if bar_met else "best_effort",
        turns=turns,
        completeness=completeness,
        questions_not_answered=qna,
        usage=usage,
        cost_usd=usd_for(getattr(provider, "model", ""), usage),
        provenance=provenance_stamp(getattr(provider, "model", "")),
    )
