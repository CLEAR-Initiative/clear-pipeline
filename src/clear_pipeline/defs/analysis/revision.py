"""Revision (reflexion) + web-rescue selection — pure logic for ``analysis_revision``.

The evaluation stage flags nodes below the completeness floor. Two rescues run
before publish, both driven by that feedback:

- **Reflexion** — a flagged node that *had* KB evidence but used it poorly gets
  one more descriptive pass, shown its prior attempt and told to answer what it
  missed (:func:`select_nodes_to_revise`). A node with no KB evidence can't be
  helped this way; it relies on web rescue instead.
- **Web rescue** — a flagged node whose gap the web gap-fill answered
  (resolved/partial, with enough confidence) is kept even below the floor
  (:func:`web_rescued_keys`); its trusted content is the tiered web evidence,
  attached at publish time.

Pure: no I/O, no model calls — the asset injects the artifacts/gapfills.
"""

from __future__ import annotations

# Web gap-fill statuses that count as answering the gap well enough to rescue.
_RESCUE_STATUSES = {"resolved", "partial"}
DEFAULT_WEB_RESCUE_CONFIDENCE = 0.5

# Flagged-node reasons whose node still has KB findings to re-use, so a reflexion
# pass can help: the self-rating floor miss and the judge's "answered poorly" /
# "off-subject padding" verdicts. ``no_evidence`` / ``not_generated`` are excluded
# (nothing to re-use → left to web rescue).
_REVISABLE_REASONS = {"below_floor", "judge_not_answered", "judge_off_subject"}


def web_rescued_keys(
    gapfills: dict[str, dict],
    *,
    confidence_floor: float = DEFAULT_WEB_RESCUE_CONFIDENCE,
) -> set[str]:
    """node_keys whose web gap-fill resolved/partially-resolved the gap with at
    least ``confidence_floor`` confidence and actually returned findings."""
    out: set[str] = set()
    for key, gf in gapfills.items():
        if not gf:
            continue
        status = gf.get("status")
        confidence = gf.get("confidence") or 0.0
        has_findings = bool(gf.get("value"))  # GapFillResult.value is the findings list
        if status in _RESCUE_STATUSES and confidence >= confidence_floor and has_findings:
            out.add(key)
    return out


def select_nodes_to_revise(
    verdicts: list[dict],
    gapfills: dict[str, dict],
    *,
    max_nodes: int,
) -> list[str]:
    """Which flagged nodes get a reflexion pass, worst-first, capped at
    ``max_nodes``.

    Nodes that still carry KB evidence qualify — ``below_floor`` (self-rating miss)
    and the judge's ``judge_not_answered`` / ``judge_off_subject`` verdicts: they
    had evidence but used it poorly, so a second pass can help. ``no_evidence`` /
    ``not_generated`` nodes have no KB evidence to re-use (reflexion would just
    re-emit empty); they are left to web rescue. Among candidates, the lowest
    completeness goes first, and a node that also has web evidence to lean on
    breaks ties ahead of one that doesn't.
    """
    candidates: list[tuple[str, float, bool]] = []
    for v in verdicts:
        if v.get("kept") or v.get("reason") not in _REVISABLE_REASONS:
            continue
        key = v["node_key"]
        completeness = v.get("completeness")
        candidates.append((key, completeness if completeness is not None else 0.0, bool(gapfills.get(key))))

    candidates.sort(key=lambda t: (t[1], not t[2]))  # worst completeness first; web-having breaks ties
    return [key for key, _, _ in candidates[: max(0, max_nodes)]]
