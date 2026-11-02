"""Displacement as an edited story plus a movement map.

The Analysis page shows displacement as at most six findings (label, callout,
one sentence, date) beside a schematic map of where people moved. The older
component asks for two open-ended bullet lists (push factors, return
intention), which reads as a long list and gives the map nothing to draw.

This module holds the structured LLM schema for that story and the
post-processing into the stored ``displacement`` shape the frontend reads:

    summary, findings[], data_check, flows[], map_since

Every finding and flow says whether it is about internally displaced people
(``idp``) or refugees (``refugee``); NRC's focus is IDPs, and the page filters
on it. Every flow must start or end in the analysis's scope (a district,
state, or country), and a flow cites the report or CLEAR event it comes from,
so the map can link an arrow to the event that caused it.

Findings, the data check, and flow evidence keep inline ``[Rn]`` markers
resolved to report ids like every other component. Flow places are names from
the evidence, resolved to location ids by a caller-supplied resolver; a name
that does not resolve stays as a name and the map leaves that end unplaced.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, Field

from clear_pipeline.defs.situation.citations import _resolve_refs, merge_contributing, resolve_bullets
from clear_pipeline.defs.situation.generate_component import GuidedCoverageLLM
from clear_pipeline.defs.situation.rag_helper import RAGContext

MAX_FINDINGS = 6
MAX_FLOWS = 6


class _FindingLLM(BaseModel):
    label: str = Field(
        description=(
            "2-4 words naming what kind of finding this is, e.g. 'What forces people out', "
            "'Across borders', 'Uneven returns', 'Conditions on return', 'Movement barriers', "
            "or a place for a local event: 'Blue Nile · Geisan'."
        ),
    )
    callout: str = Field(
        description=(
            "The ONE thing to remember, shown large: a figure with its unit ('20,000+', "
            "'64% fled insecurity', '59% vs <1%') or a 1-3 word reason ('Conflict', "
            "'Kharif rains'). Never a full sentence."
        ),
    )
    description: str = Field(
        description=(
            "ONE sentence that continues from the callout and gives the supporting fact, "
            "with inline [Rn] markers. Do not repeat the callout."
        ),
    )
    direction: Literal["out", "back", "neutral"] = Field(
        description="'out' = people leaving or being displaced, 'back' = returns, 'neutral' = neither (barriers, conditions).",
    )
    group: Literal["idp", "refugee", "both"] = Field(
        description=(
            "'idp' = about people displaced INSIDE the country (including IDP returns), 'refugee' = about "
            "people who crossed an international border (refugees, asylum seekers, refugee returnees), "
            "'both' = applies to both (drivers, barriers)."
        ),
    )
    as_of: str = Field(
        default="",
        description="The date or period the fact refers to, as the source states it: 'Jul 2026', 'to 31 Aug 2026'. Empty if unknown.",
    )


class _FlowLLM(BaseModel):
    direction: Literal["out", "back", "within"] = Field(
        description=(
            "'out' = people displaced away from a place, 'back' = people returning to a place, "
            "'within' = displaced people moving between two places."
        ),
    )
    group: Literal["idp", "refugee"] = Field(
        description="'idp' = the movement stays inside the country, 'refugee' = it crosses an international border.",
    )
    from_place: str = Field(
        default="",
        description="Where people left from: a state or locality inside the country. Empty if the source does not say.",
    )
    to_place: str = Field(
        default="",
        description=(
            "Where they went: a country name for 'out', a state or locality for 'back' and 'within'. "
            "Empty when the source does not break the destination down."
        ),
    )
    people: int | None = Field(
        default=None,
        description="How many people, as a plain integer. Null when the source gives no size.",
    )
    label: str = Field(
        description="Short map label with the figure: 'South Sudan >1.3M', 'Chad · 5,402', 'Khartoum · 2.6M back'.",
    )
    sublabel: str = Field(
        default="",
        description="A few words of context under the label: 'largest single destination', 'arrivals in June (monthly)'.",
    )
    finding: int | None = Field(
        default=None,
        description="The 1-based number of the finding this flow illustrates, or null.",
    )
    evidence: str = Field(
        default="",
        description=(
            "The ONE [Rn] marker of the evidence this movement comes from. Prefer a CLEAR event "
            "(an evidence item with 'Type:' and 'Displaced:' lines) when one reports the movement."
        ),
    )


class DisplacementStoryLLM(GuidedCoverageLLM):
    summary: str = Field(
        description=(
            "ONE sentence (max ~30 words) with the main takeaway on displacement: what drives it, "
            "where people go, and anything striking about returns. No [Rn] markers."
        ),
    )
    findings: list[_FindingLLM] = Field(
        default_factory=list,
        description=(
            f"At most {MAX_FINDINGS} findings, most important first. Cover drivers, cross-border "
            "movement, returns and conditions on return, and barriers to movement when the evidence "
            "supports them. Merge overlapping facts; prefer findings with a concrete figure. "
            "Fewer findings are better than weak ones."
        ),
    )
    data_check: str = Field(
        default="",
        description=(
            "ONE sentence when two sources in the evidence or the aggregated figures disagree about "
            "the same displacement figure, naming both values, with [Rn] markers. Empty otherwise."
        ),
    )
    flows: list[_FlowLLM] = Field(
        default_factory=list,
        description=(
            f"At most {MAX_FLOWS} movements to draw on a schematic map of the analysed area. Each one "
            "MUST start or end in the analysed area. Prefer movements of internally displaced people; "
            "include refugee movements only when the evidence states them. Only movements the evidence "
            "states, with sizes only when stated."
        ),
    )
    map_since: str = Field(
        default="",
        description="Since when the mapped movements are counted, as the sources put it: 'April 2023'. Empty if mixed or unknown.",
    )


def map_instruction(scope_label: str) -> str:
    """The field-mapping brief, naming the analysed area so flows stay in scope."""
    return (
        f"The analysed area is {scope_label}. Answer as a short displacement story for an analyst's "
        f"dashboard: a one-sentence `summary`, at most {MAX_FINDINGS} `findings` (each: label, callout, "
        "one sentence, direction, group, as_of), an optional `data_check`, and up to "
        f"{MAX_FLOWS} `flows` for the movement map. Every flow must start or end in {scope_label} or a "
        "place inside it; name that end as the specific place when the evidence does, otherwise as the "
        "analysed area itself. Distinguish internally displaced people (`idp`) from refugees "
        "(`refugee`): NRC's focus is internally displaced people. Each finding's description is ONE "
        "sentence with [Rn] markers. Use only facts in the evidence or the aggregated figures; never "
        "invent sizes or places."
    )


Resolver = Callable[[str, Literal["country", "area"]], str | None]
#: True when a location id is the analysed area or inside it.
InScope = Callable[[str], bool]


def _resolve_one(text: str, rag: RAGContext) -> tuple[str, list[str]]:
    clean, per_ids, _ = resolve_bullets([text], rag.hit_report_ids)
    return (clean[0] if clean else text.strip()), (per_ids[0] if per_ids else [])


def assemble_story(
    llm: DisplacementStoryLLM,
    rag: RAGContext,
    resolve_place: Resolver,
    in_scope: InScope | None = None,
) -> dict[str, Any]:
    """Turn the LLM answer into the stored ``displacement`` fields, with
    citations resolved and flow places resolved to location ids.

    With ``in_scope`` (a created analysis), a flow is kept only when one of its
    ends resolves to a place in the analysed area; the model is told the same,
    and this enforces it. A country analysis passes None: every place it can
    resolve is in the country."""
    findings: list[dict[str, Any]] = []
    for f in llm.findings[:MAX_FINDINGS]:
        text, ids = _resolve_one(f.description, rag)
        if not f.label.strip() or not text:
            continue
        findings.append({
            "label": f.label.strip(),
            "callout": f.callout.strip() or None,
            "description": text,
            "direction": f.direction,
            "group": f.group,
            "as_of": f.as_of.strip() or None,
            "source_report_ids": ids,
        })

    data_check = None
    if llm.data_check.strip():
        text, ids = _resolve_one(llm.data_check, rag)
        data_check = {"description": text, "source_report_ids": ids}

    flows: list[dict[str, Any]] = []
    for fl in llm.flows[:MAX_FLOWS]:
        if not fl.label.strip():
            continue
        abroad = fl.group == "refugee"
        from_kind: Literal["country", "area"] = "country" if abroad and fl.direction == "back" else "area"
        to_kind: Literal["country", "area"] = "country" if abroad and fl.direction == "out" else "area"
        from_id = resolve_place(fl.from_place, from_kind) if fl.from_place.strip() else None
        to_id = resolve_place(fl.to_place, to_kind) if fl.to_place.strip() else None
        if in_scope is not None and not any(i and in_scope(i) for i in (from_id, to_id)):
            continue
        # Evidence is a bare marker ("[R3]"), which the bullet cleaner would drop as empty.
        evidence_ids = _resolve_refs(fl.evidence, rag.hit_report_ids)
        flows.append({
            "direction": fl.direction,
            "group": fl.group,
            "from_name": fl.from_place.strip() or None,
            "from_location_id": from_id,
            "to_name": fl.to_place.strip() or None,
            "to_location_id": to_id,
            "label": fl.label.strip(),
            "sublabel": fl.sublabel.strip() or None,
            "people": fl.people if fl.people and fl.people > 0 else None,
            "finding": fl.finding if fl.finding and 1 <= fl.finding <= len(findings) else None,
            "source_report_ids": evidence_ids,
        })

    contributing = merge_contributing(*(
        {rid: [f["description"]] for rid in f["source_report_ids"]} for f in findings
    ))
    return {
        "summary": llm.summary.strip() or None,
        "findings": findings,
        "data_check": data_check,
        "flows": flows,
        "map_since": llm.map_since.strip() or None,
        # The older fields stay filled so the report PDF and other readers keep working.
        "push_factors": [
            {"description": f"{f['label']}: {(f['callout'] + ' ') if f['callout'] else ''}{f['description']}",
             "source_report_ids": f["source_report_ids"]}
            for f in findings if f["direction"] != "back"
        ],
        "return_intention": [
            {"description": f"{f['label']}: {(f['callout'] + ' ') if f['callout'] else ''}{f['description']}",
             "source_report_ids": f["source_report_ids"]}
            for f in findings if f["direction"] == "back"
        ],
        "completeness": llm.completeness,
        "questions_not_answered": list(llm.questions_not_answered),
        "contributing_sources": contributing,
    }
