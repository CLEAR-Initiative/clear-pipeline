"""'What changed' generation - a per-section diff note vs the prior snapshot.

At generation time the asset already has the previous current snapshot for the
same (country, window) bucket. This module makes ONE LLM call comparing the
prior payload against the freshly built one and emits a terse note per section
that MATERIALLY changed (a new figure, a new event, an escalation or
de-escalation). Sections with no material change are omitted; on the first
generation (no prior) nothing is produced.

One call, not one per section: the model sees the whole before/after at once,
which is cheaper than a call per section and lets it weigh relative
significance across the report. Failure-isolated like the other components -
any error yields empty changes rather than dropping the analysis.
"""

import json
import logging
from typing import Any

from pydantic import BaseModel, Field

from clear_context_pipeline.defs.situation.schemas import SituationChanges
from clear_context_pipeline.providers.llm import LLMProvider

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
    prior_generated_at: str,
    cache_key: str,
) -> SituationChanges:
    """One LLM call diffing prior vs new. Returns empty changes on any
    failure (the analysis still ships without change-notes)."""
    prior_d = _digest(prior_payload)
    new_d = _digest(new_payload)

    system = (
        "You are a humanitarian analyst. You are given the PRIOR and NEW "
        "version of a country situation analysis, section by section. For "
        "each section, decide whether anything MATERIALLY changed - a new or "
        "revised figure, a newly reported event, an escalation or "
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
        return SituationChanges(compared_to=prior_generated_at)

    notes: dict[str, str] = {}
    for c in result.changes:
        key = (c.section or "").strip()
        note = (c.note or "").strip()
        if key in _VALID_SECTIONS and note:
            notes[key] = note

    return SituationChanges(compared_to=prior_generated_at, notes=notes)
