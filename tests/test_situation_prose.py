"""Tests for the situation-analysis prose projection used by translation.

`extract_situation_prose` must surface every prose leaf and NOTHING else —
numbers, enums (sector severity / evidence_scope), ids, source maps and
coverage ratings must not reach the translator (they'd be corrupted). The
projection also has to mirror the canonical nesting so clear-api's deep-merge
overlay lines up by key/index.
"""

from clear_pipeline.providers.situation_prose import (
    PROSE_COMPONENTS,
    extract_situation_prose,
)


def _full_payload() -> dict:
    return {
        "datapoints": {"population_displaced": {"value": 100}, "number_of_events": 3},
        "ai_summary": {
            "text": "Displacement rose sharply.",
            "source_report_ids": ["r1", "r2"],
            "contributing_sources": {"r1": ["Displacement rose sharply."]},
        },
        "context_risks": {
            "security": {
                "bullets": ["Armed clashes in the north."],
                "source_report_ids": ["r3"],
            },
        },
        "hazards_and_vulnerabilities": {
            "hazards": [{"description": "Seasonal flooding", "source_report_ids": ["r4"]}],
            "vulnerabilities": [{"description": "Weak health system", "source_report_ids": []}],
        },
        "displacement": {
            "push_factors": [{"description": "Conflict", "source_report_ids": ["r5"]}],
            "return_intention": [],
        },
        "sectors": {
            "health": {
                "severity": "critical",
                "impact": ["Clinics overwhelmed"],
                "humanitarian_conditions": ["Disease outbreaks"],
                "vulnerable_sections": ["Children"],
                "top_needs": ["Medicine"],
                "priority_interventions": ["Mobile clinics"],
                "information_coverage": [
                    {"area": "Access", "rating_out_of_10": 4, "report_count": 2},
                ],
                "source_report_ids": ["r6"],
                "evidence_scope": "sector",
            },
        },
        "changes": {
            "basis": "previous_period",
            "compared_to": "2025",
            "notes": {"summary": "Needs escalated in the north."},
        },
        "sources": {"reports": [{"report_id": "r1", "report_title": "UN OCHA update"}]},
    }


def test_projects_only_prose_components():
    prose = extract_situation_prose(_full_payload())
    assert set(prose.keys()) == set(PROSE_COMPONENTS)
    # Numeric/deterministic components are absent entirely.
    assert "datapoints" not in prose
    assert "sources" not in prose


def test_ai_summary_keeps_text_drops_ids():
    prose = extract_situation_prose(_full_payload())
    assert prose["ai_summary"] == {"text": "Displacement rose sharply."}


def test_sector_keeps_prose_drops_enums_ids_ratings():
    health = extract_situation_prose(_full_payload())["sectors"]["health"]
    assert health == {
        "impact": ["Clinics overwhelmed"],
        "humanitarian_conditions": ["Disease outbreaks"],
        "vulnerable_sections": ["Children"],
        "top_needs": ["Medicine"],
        "priority_interventions": ["Mobile clinics"],
        "information_coverage": [{"area": "Access"}],
    }
    # No severity/evidence_scope/source_report_ids leaked to the translator.
    assert "severity" not in health
    assert "evidence_scope" not in health
    assert "source_report_ids" not in health


def test_bullets_reduced_to_descriptions():
    prose = extract_situation_prose(_full_payload())
    assert prose["hazards_and_vulnerabilities"]["hazards"] == [
        {"description": "Seasonal flooding"},
    ]
    assert prose["displacement"]["push_factors"] == [{"description": "Conflict"}]
    assert prose["displacement"]["return_intention"] == []


def test_change_notes_are_prose_values_only():
    prose = extract_situation_prose(_full_payload())
    assert prose["changes"] == {"notes": {"summary": "Needs escalated in the north."}}


def test_empty_payload_yields_stable_empty_shape():
    prose = extract_situation_prose({})
    assert set(prose.keys()) == set(PROSE_COMPONENTS)
    assert prose["ai_summary"] == {"text": ""}
    assert prose["context_risks"] == {}
    assert prose["sectors"] == {}
    assert prose["changes"] == {"notes": {}}
