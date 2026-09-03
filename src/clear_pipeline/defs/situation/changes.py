"""'What changed' generation - a per-section diff note vs an earlier snapshot.

The caller (situation/generate.py) picks WHAT to compare against and passes it
in as `basis`: normally the preceding bucket of the same kind (last month for a
monthly window), falling back to an earlier version of the same bucket when no
preceding one exists. This module makes ONE LLM call comparing that prior
payload against the freshly built one and emits a terse note per section that
MATERIALLY changed (a new figure, a new event, an escalation or de-escalation).
Sections with no material change are omitted; with nothing to compare against,
nothing is produced.

One call, not one per section: the model sees the whole before/after at once,
which is cheaper than a call per section and lets it weigh relative
significance across the report. Failure-isolated like the other components -
any error yields empty changes rather than dropping the analysis.
"""

import json
import logging
from typing import Any

from pydantic import BaseModel, Field

from clear_pipeline.defs.situation.schemas import SituationChanges
from clear_pipeline.providers.llm import LLMProvider

logger = logging.getLogger(__name__)

# Section keys the dashboard renders change-strips under. The LLM is given
# this exact set and may use only these; anything else is dropped.
_CONTEXT_DOMAINS = (
    "demographics", "political", "economy", "socio_culture",
    "security", "legal_policy", "infrastructure", "environment",
)
_SECTORS = ("education", "food_security", "health", "shelter", "wash", "protection")
_VALID_SECTIONS = frozenset(
    {"summary", "hazards", "displacement"}
    | {f"context_risks.{d}" for d in _CONTEXT_DOMAINS}
    | {f"sectors.{s}" for s in _SECTORS}
)


class _ChangeNoteLLM(BaseModel):
    section: str = Field(
        description="One of the provided section keys, copied verbatim.",
    )
    note: str = Field(
        description=(
            "Terse note (max 20 words) on what MATERIALLY changed for this "
            "section since the prior snapshot. Lead with any figure. State the "
            "change, not the current state."
        ),
    )


class _ChangesLLM(BaseModel):
    changes: list[_ChangeNoteLLM] = Field(
        default_factory=list,
        description=(
            "Only sections that materially changed. Omit unchanged sections "
            "entirely. Empty list when nothing material changed."
        ),
    )


def _digest(payload: dict[str, Any]) -> dict[str, str]:
    """Compact per-section text for one payload, keyed by section path.
    Numbers and short bullet text only - enough for the model to spot a
    material change without re-reading the whole blob."""
    out: dict[str, str] = {}

    dp = payload.get("datapoints") or {}
    figures = {
        k: dp.get(k)
        for k in (
            "population_displaced", "population_affected", "population_in_need",
            "returnees", "funding_required_usd", "funding_received_usd",
        )
        if dp.get(k) is not None
    }
    summary = (payload.get("ai_summary") or {}).get("text") or ""
    out["summary"] = (json.dumps(figures) + " " + summary[:600]).strip()

    risks = payload.get("context_risks") or {}
    for d in _CONTEXT_DOMAINS:
        bullets = (risks.get(d) or {}).get("bullets") or []
        if bullets:
            out[f"context_risks.{d}"] = " | ".join(bullets)

    hv = payload.get("hazards_and_vulnerabilities") or {}
    haz = [b.get("description", "") for b in hv.get("hazards") or []]
    vul = [b.get("description", "") for b in hv.get("vulnerabilities") or []]
    if haz or vul:
        out["hazards"] = "HAZARDS: " + " | ".join(haz) + " VULN: " + " | ".join(vul)

    disp = payload.get("displacement") or {}
    push = [b.get("description", "") for b in disp.get("push_factors") or []]
    ret = [b.get("description", "") for b in disp.get("return_intention") or []]
    if push or ret:
        out["displacement"] = "PUSH: " + " | ".join(push) + " RETURN: " + " | ".join(ret)

    sectors = payload.get("sectors") or {}
    for s in _SECTORS:
        blk = sectors.get(s) or {}
        needs = blk.get("top_needs") or []
        sev = blk.get("severity")
        if sev or needs:
            out[f"sectors.{s}"] = f"severity={sev}; needs=" + " | ".join(needs)

    return out


def _format_side_by_side(prior: dict[str, str], new: dict[str, str]) -> str:
    """One block per section: PRIOR vs NEW, for every section present in
    either payload."""
    lines: list[str] = []
    for key in sorted(set(prior) | set(new)):
        lines.append(f"### {key}")
        lines.append(f"PRIOR: {prior.get(key, '(absent)')}")
        lines.append(f"NEW:   {new.get(key, '(absent)')}")
        lines.append("")
    return "\n".join(lines)


def generate_changes(
    llm: LLMProvider,
    *,
    prior_payload: dict[str, Any],
    new_payload: dict[str, Any],
    basis: str,
    prior_generated_at: str,
    compared_to_window_start: str,
    compared_to_label: str,
    cache_key: str,
) -> SituationChanges:
    """One LLM call diffing prior vs new. `basis` is "previous_period" or
    "previous_generation" (see SituationChanges) and only changes how the
    comparison is framed to the model - a period diff is about the world
    moving, a generation diff about our picture of it improving, and
    conflating them produces notes that overstate real-world change.
    Returns empty changes on any failure (the analysis still ships)."""
    prior_d = _digest(prior_payload)
    new_d = _digest(new_payload)

    if basis == "previous_period":
        framing = (
            f"PRIOR is the preceding period ({compared_to_label}); NEW is the "
            "period being reported. Differences reflect the situation moving."
        )
    else:
        framing = (
            f"PRIOR and NEW are two readings of the SAME period "
            f"({compared_to_label}), taken at different times. Differences "
            "reflect newly arrived reporting, not necessarily real-world "
            "change - do not phrase a note as an escalation when it is only "
            "a figure being revised or a gap being filled."
        )

    system = (
        "You are a humanitarian analyst. You are given the PRIOR and NEW "
        "version of a country situation analysis, section by section. "
        + framing
        + " For each section, decide whether anything MATERIALLY changed - a "
        "new or revised figure, a newly reported event, an escalation or "
        "de-escalation. Emit a note ONLY for sections that materially "
        "changed; omit the rest. Notes are terse fragments (max 20 words), "
        "lead with the figure, and state the change, not the current state "
        "(e.g. 'Food basket +13%, new WFP monitor' not 'Food prices high'). "
        "Use only the section keys provided, verbatim. Ignore pure rewording."
    )
    user = (
        "VALID SECTION KEYS:\n"
        + ", ".join(sorted(_VALID_SECTIONS))
        + "\n\nSECTIONS (PRIOR vs NEW):\n\n"
        + _format_side_by_side(prior_d, new_d)
    )

    try:
        result = llm.complete_structured(
            system=system,
            user=user,
            schema=_ChangesLLM,
            max_tokens=1500,
            cache_key=cache_key,
        )
    except Exception as exc:  # noqa: BLE001 - failure isolation
        logger.warning("[situation:changes] LLM call failed: %s", exc)
        return SituationChanges(
            basis=basis,
            compared_to=prior_generated_at,
            compared_to_window_start=compared_to_window_start,
            compared_to_label=compared_to_label,
        )

    notes: dict[str, str] = {}
    for c in result.changes:
        key = (c.section or "").strip()
        note = (c.note or "").strip()
        if key in _VALID_SECTIONS and note:
            notes[key] = note

    return SituationChanges(
        basis=basis,
        compared_to=prior_generated_at,
        compared_to_window_start=compared_to_window_start,
        compared_to_label=compared_to_label,
        notes=notes,
    )
