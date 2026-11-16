"""Evaluation as a programmatic filter over self-ratings (doc §8, answer #3).

Deterministic — there is **no separate adversarial/judge LLM**. Each descriptive
node has already self-rated how completely it answered its Analytical Question
given the sources it used; this stage simply **filters/flags** programmatically:
a node whose finding is empty or whose completeness is below a floor is flagged
(dropped from the trusted package), and per-task completeness is summarised.

Thresholds are config, so the filtering policy changes without a model call.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .taxonomy import TaxonomyNode

# Recommended default (answer #5) — findings below this are flagged, not trusted.
DEFAULT_EVAL_COMPLETENESS_FLOOR = 0.4


@dataclass(frozen=True)
class NodeVerdict:
    node_key: str
    task: str
    kept: bool
    completeness: float | None
    # kept | web_rescued | below_floor | no_evidence | not_generated
    reason: str


@dataclass(frozen=True)
class EvaluatedPackage:
    verdicts: list[NodeVerdict]
    kept_node_keys: list[str]
    flagged_node_keys: list[str]
    mean_completeness: float | None
    by_task: dict[str, float] = field(default_factory=dict)  # per-task mean completeness


def filter_package(
    nodes: list[TaxonomyNode],
    artifacts: dict[str, dict],
    *,
    completeness_floor: float = DEFAULT_EVAL_COMPLETENESS_FLOOR,
    rescued_node_keys: set[str] | None = None,
) -> EvaluatedPackage:
    """Filter the descriptive package: keep nodes that cleared the completeness
    floor and carry evidence; flag the rest. Summarise completeness overall and
    per task (section).

    ``rescued_node_keys`` are nodes the web gap-fill answered (resolved/partial
    with enough confidence): they are kept even below the floor — the trusted
    content comes from the tiered web evidence attached at publish time, so a
    thin KB answer no longer drops a question the web could answer.
    """
    rescued = rescued_node_keys or set()
    verdicts: list[NodeVerdict] = []
    scores: list[float] = []
    by_task_scores: dict[str, list[float]] = {}

    for node in nodes:
        art = artifacts.get(node.key)
        desc = (art or {}).get("descriptive", {}) if art else {}
        completeness = desc.get("completeness")
        has_findings = bool(desc.get("findings"))

        if completeness is not None and has_findings and completeness >= completeness_floor:
            kept, reason = True, "kept"
        elif node.key in rescued:
            kept, reason = True, "web_rescued"
        elif art is None:
            kept, reason = False, "not_generated"
        elif not has_findings:
            kept, reason = False, "no_evidence"
        else:
            kept, reason = False, "below_floor"

        verdicts.append(
            NodeVerdict(
                node_key=node.key, task=node.task, kept=kept,
                completeness=completeness, reason=reason,
            )
        )
        if completeness is not None:
            scores.append(completeness)
            by_task_scores.setdefault(node.task, []).append(completeness)

    mean = sum(scores) / len(scores) if scores else None
    by_task = {t: sum(v) / len(v) for t, v in by_task_scores.items()}
    return EvaluatedPackage(
        verdicts=verdicts,
        kept_node_keys=[v.node_key for v in verdicts if v.kept],
        flagged_node_keys=[v.node_key for v in verdicts if not v.kept],
        mean_completeness=mean,
        by_task=by_task,
    )
