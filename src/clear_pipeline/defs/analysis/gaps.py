"""Information-gap derivation (doc §8, answer #4).

Deterministic. Turns the descriptive nodes' **completeness self-ratings** +
``questions_not_answered`` into the ranked list of **unmet** gaps — the
required-question-unanswered trigger, built on the existing ``QuestionCoverage``
contract (no new model call). A node's *required question* is its taxonomy
Analytical Question.

A node is an unmet gap when ANY of:
  - it has no descriptive artifact (not generated / failed),
  - its finding is empty (no evidence),
  - its self-rated completeness is below the threshold, or
  - it reported ``questions_not_answered``.

The gap list is what the conditional web gap-fill (a later PR) consumes: only
these gaps are worth reaching outside the DB for — gap-fill is a fallback, not
the norm (doc §8).
"""

from __future__ import annotations

from dataclasses import dataclass

from .taxonomy import TaxonomyNode

# Recommended default (answer #5) — a node self-rating below this is "thin".
DEFAULT_GAP_COMPLETENESS_THRESHOLD = 0.6


@dataclass(frozen=True)
class Gap:
    """One unmet information gap for a taxonomy node."""

    node_key: str
    node_name: str
    task: str
    level: int
    question: str  # the required Analytical Question
    completeness: float | None
    questions_not_answered: list[str]
    reason: str  # not_generated | no_evidence | low_completeness | unanswered_questions | both


def _reason(has_artifact: bool, has_findings: bool, low: bool, unanswered: bool) -> str | None:
    if not has_artifact:
        return "not_generated"
    if not has_findings:
        return "no_evidence"
    if low and unanswered:
        return "both"
    if low:
        return "low_completeness"
    if unanswered:
        return "unanswered_questions"
    return None


def derive_gaps(
    nodes: list[TaxonomyNode],
    artifacts: dict[str, dict],
    *,
    threshold: float = DEFAULT_GAP_COMPLETENESS_THRESHOLD,
) -> list[Gap]:
    """Ranked unmet gaps across ``nodes`` given their descriptive ``artifacts``
    (keyed by ``node.key``; a missing key = not generated). Worst first — nodes
    with the lowest (or absent) completeness lead, so a bounded gap-fill budget
    spends on the thinnest sections.
    """
    gaps: list[Gap] = []
    for node in nodes:
        art = artifacts.get(node.key)
        desc = (art or {}).get("descriptive", {}) if art else {}
        completeness = desc.get("completeness")
        qna = list(desc.get("questions_not_answered") or [])
        has_findings = bool(desc.get("findings"))
        low = completeness is None or completeness < threshold
        unanswered = bool(qna)

        reason = _reason(art is not None, has_findings, low, unanswered)
        if reason is None:
            continue
        gaps.append(
            Gap(
                node_key=node.key,
                node_name=node.name,
                task=node.task,
                level=node.level,
                question=node.question,
                completeness=completeness,
                questions_not_answered=qna,
                reason=reason,
            )
        )

    # Worst-first: absent completeness sorts before any real score.
    gaps.sort(key=lambda g: (g.completeness if g.completeness is not None else -1.0))
    return gaps
