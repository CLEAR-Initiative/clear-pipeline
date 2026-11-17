"""'What changed vs the prior generation' notes (ADR-0007 §8) — analysis-native.

ONE LLM call diffing the prior published payload against the freshly assembled
one, emitting a terse note per section that MATERIALLY changed (a new/revised
figure, a newly reported event, an escalation or de-escalation). Sections with
no material change are omitted; with no prior to compare against, nothing is
produced.

The digest reads the **v5 field shapes** clear-mvp consumes (and that
``assemble_v5`` produces), so prior (a published v6 row, which carries the v5
fields) and new (freshly assembled) digest apples-to-apples. Native to the
analysis package — no situation dependency. The ``analysis_changes`` asset drives
it per country.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field

from clear_pipeline.defs.analysis.profiles import Caps
from clear_pipeline.defs.analysis.reconciliation import CONTEXT_DOMAIN_V5_KEY, SECTOR_V5_KEY
from clear_pipeline.defs.analysis.runtime import run_node

_DEFAULT_CAPS = Caps(max_turns=1, max_tokens=1500)

_CONTEXT_KEYS = tuple(sorted(set(CONTEXT_DOMAIN_V5_KEY.values())))
_SECTOR_KEYS = tuple(sorted(set(SECTOR_V5_KEY.values())))

# The section keys the dashboard renders change-strips under. The LLM is given
# this exact set and may use only these; anything else is dropped.
VALID_SECTIONS = frozenset(
    {"summary", "hazards", "displacement", "humanitarian_access"}
    | {f"context_risks.{d}" for d in _CONTEXT_KEYS}
    | {f"sectors.{s}" for s in _SECTOR_KEYS}
)


class _ChangeNote(BaseModel):
    section: str = Field(description="One of the provided section keys, copied verbatim.")
    note: str = Field(
        description=(
            "Terse note (max 20 words) on what MATERIALLY changed for this section "
            "since the prior snapshot. Lead with any figure. State the change, not "
            "the current state."
        ),
    )


class _ChangesLLM(BaseModel):
    changes: list[_ChangeNote] = Field(
        default_factory=list,
        description="Only sections that materially changed; omit unchanged ones.",
    )


def digest_v5(payload: dict[str, Any]) -> dict[str, str]:
    """Compact per-section text for one v5-shaped payload, keyed by section path —
    numbers + short bullet text, enough to spot a material change without
    re-reading the whole blob."""
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
    for d in _CONTEXT_KEYS:
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

    access = payload.get("humanitarian_access") or {}
    barriers = [b.get("description", "") for b in access.get("barriers") or []]
    constraints = [b.get("description", "") for b in access.get("constraints") or []]
    if barriers or constraints:
        out["humanitarian_access"] = (
            "BARRIERS: " + " | ".join(barriers) + " CONSTRAINTS: " + " | ".join(constraints)
        )

    sectors = payload.get("sectors") or {}
    for s in _SECTOR_KEYS:
        blk = sectors.get(s) or {}
        needs = blk.get("top_needs") or []
        sev = blk.get("severity")
        if sev or needs:
            out[f"sectors.{s}"] = f"severity={sev}; needs=" + " | ".join(needs)

    return out


def _format_side_by_side(prior: dict[str, str], new: dict[str, str]) -> str:
    lines: list[str] = []
    for key in sorted(set(prior) | set(new)):
        lines.append(f"### {key}")
        lines.append(f"PRIOR: {prior.get(key, '(absent)')}")
        lines.append(f"NEW:   {new.get(key, '(absent)')}")
        lines.append("")
    return "\n".join(lines)


def empty_changes(
    *, basis: str, prior_generated_at: str,
    compared_to_window_start: str, compared_to_label: str,
) -> dict[str, Any]:
    """The v5 ``changes`` block with no notes (no prior, or generation failed)."""
    return {
        "basis": basis,
        "compared_to": prior_generated_at,
        "compared_to_window_start": compared_to_window_start,
        "compared_to_label": compared_to_label,
        "notes": {},
    }


def generate_changes(
    *,
    provider,
    prior_data: dict[str, Any],
    new_data: dict[str, Any],
    basis: str,
    prior_generated_at: str,
    compared_to_window_start: str,
    compared_to_label: str,
    budget=None,
    caps: Caps | None = None,
) -> dict[str, Any]:
    """One LLM call diffing prior vs new. ``basis`` ("previous_period" |
    "previous_generation") only changes how the comparison is framed — a period
    diff is the world moving, a generation diff is our picture improving, and
    conflating them overstates real-world change. Returns the v5 ``changes`` dict;
    an empty one on any failure (the analysis still ships)."""
    caps = caps or _DEFAULT_CAPS
    prior_d = digest_v5(prior_data)
    new_d = digest_v5(new_data)

    if basis == "previous_period":
        framing = (
            f"PRIOR is the preceding period ({compared_to_label}); NEW is the period "
            "being reported. Differences reflect the situation moving."
        )
    else:
        framing = (
            f"PRIOR and NEW are two readings of the SAME period ({compared_to_label}), "
            "taken at different times. Differences reflect newly arrived reporting, not "
            "necessarily real-world change — do not phrase a note as an escalation when "
            "it is only a figure being revised or a gap being filled."
        )

    system = (
        "You are a humanitarian analyst. You are given the PRIOR and NEW version of a "
        "country situation analysis, section by section. " + framing + " For each "
        "section, decide whether anything MATERIALLY changed — a new or revised figure, "
        "a newly reported event, an escalation or de-escalation. Emit a note ONLY for "
        "sections that materially changed; omit the rest. Notes are terse fragments "
        "(max 20 words), lead with the figure, and state the change, not the current "
        "state (e.g. 'Food basket +13%, new WFP monitor' not 'Food prices high'). Use "
        "only the section keys provided, verbatim. Ignore pure rewording."
    )
    user = (
        "VALID SECTION KEYS:\n" + ", ".join(sorted(VALID_SECTIONS))
        + "\n\nSECTIONS (PRIOR vs NEW):\n\n" + _format_side_by_side(prior_d, new_d)
    )

    outcome = run_node(
        provider=provider, system=system, build_user=lambda _e: user,
        schema=_ChangesLLM, caps=caps, budget=budget,
    )
    notes: dict[str, str] = {}
    for c in outcome.output.changes:
        key = (c.section or "").strip()
        note = (c.note or "").strip()
        if key in VALID_SECTIONS and note:
            notes[key] = note

    return {
        "basis": basis,
        "compared_to": prior_generated_at,
        "compared_to_window_start": compared_to_window_start,
        "compared_to_label": compared_to_label,
        "notes": notes,
    }
