"""Displacement story post-processing: citations, place lookup, and scope."""

from clear_pipeline.defs.situation.displacement_story import (
    DisplacementStoryLLM,
    assemble_story,
    map_instruction,
)
from clear_pipeline.defs.situation.rag_helper import RAGContext

RAG = RAGContext(
    formatted_for_prompt="",
    contributing_report_ids=["r1", "event:ev-3"],
    hit_report_ids=["r1", "", "event:ev-3"],
)
PLACES = {"gebrat al sheikh": "loc-gas", "sheikan": "loc-sheikan", "east darfur": "loc-ed", "south darfur": "loc-sd"}
IN_SCOPE = {"loc-nk", "loc-gas", "loc-sheikan"}


def resolve(name: str, kind: str) -> str | None:
    return PLACES.get(name.lower()) if kind == "area" else None


def answer(**over) -> DisplacementStoryLLM:
    base = {
        "summary": "Fighting keeps displacing people inside North Kordofan.",
        "findings": [
            {"label": "Gebrat Al Sheikh", "callout": "31,287", "description": "people were displaced [R3].",
             "direction": "out", "group": "idp", "as_of": "Aug 2026"},
            {"label": "Across borders", "callout": "None recorded", "description": "No refugee data [R1].",
             "direction": "out", "group": "refugee"},
        ],
        "flows": [
            {"direction": "out", "group": "idp", "from_place": "Gebrat Al Sheikh", "to_place": "East Darfur",
             "people": 31287, "label": "Gebrat Al Sheikh · 31,287", "finding": 1, "evidence": "[R3]"},
            {"direction": "within", "group": "idp", "from_place": "South Darfur", "to_place": "East Darfur",
             "people": 500, "label": "Elsewhere · 500", "evidence": "[R1]"},
        ],
    }
    base.update(over)
    return DisplacementStoryLLM.model_validate(base)


def test_findings_keep_group_and_resolved_citations():
    out = assemble_story(answer(), RAG, resolve)
    assert [(f["label"], f["group"], f["source_report_ids"]) for f in out["findings"]] == [
        ("Gebrat Al Sheikh", "idp", ["event:ev-3"]),
        ("Across borders", "refugee", ["r1"]),
    ]


def test_flow_cites_the_event_it_comes_from_and_resolves_places():
    flow = assemble_story(answer(), RAG, resolve)["flows"][0]
    assert flow["source_report_ids"] == ["event:ev-3"]
    assert (flow["from_location_id"], flow["to_location_id"]) == ("loc-gas", "loc-ed")
    assert flow["group"] == "idp" and flow["finding"] == 1


def test_scope_drops_flows_that_neither_start_nor_end_in_the_area():
    out = assemble_story(answer(), RAG, resolve, in_scope=lambda i: i in IN_SCOPE)
    assert [f["label"] for f in out["flows"]] == ["Gebrat Al Sheikh · 31,287"]


def test_country_analysis_keeps_every_flow():
    assert len(assemble_story(answer(), RAG, resolve)["flows"]) == 2


def test_older_readers_still_get_push_and_return_bullets():
    out = assemble_story(answer(), RAG, resolve)
    assert out["push_factors"][0]["description"] == "Gebrat Al Sheikh: 31,287 people were displaced."
    assert out["return_intention"] == []


def test_instruction_names_the_scope():
    text = map_instruction("North Kordofan, Sudan")
    assert "North Kordofan, Sudan" in text and "idp" in text
