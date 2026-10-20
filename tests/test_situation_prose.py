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
                "completeness": 0.6,
                "questions_not_answered": ["What is the trend in civilian casualties?"],
            },
        },
        "hazards_and_vulnerabilities": {
            "hazards": [{"description": "Seasonal flooding", "source_report_ids": ["r4"]}],
            "vulnerabilities": [{"description": "Weak health system", "source_report_ids": []}],
            "completeness": 0.5,
            "questions_not_answered": ["Which pre-crisis vulnerabilities dominate?"],
        },
        "displacement": {
            "push_factors": [{"description": "Conflict", "source_report_ids": ["r5"]}],
            "return_intention": [],
            "questions_not_answered": ["What drives return decisions?"],
        },
        "humanitarian_access": {
            "barriers": [
                {"description": "Checkpoints delay aid convoys", "source_report_ids": ["r7"]},
            ],
            "constraints": [
                {"description": "Seasonal flooding blocks secondary roads", "source_report_ids": ["r8"]},
            ],
            "completeness": 0.4,
            "questions_not_answered": ["Where are bureaucratic impediments worst?"],
            "contributing_sources": {"r7": ["Checkpoints delay aid convoys"]},
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
                "completeness": 0.7,
                "questions_not_answered": ["Which facilities remain functional?"],
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
    assert prose["ai_summary"] == {"text": "Displacement rose sharply.", "key_findings": []}


def test_ai_summary_key_findings_reduced_to_descriptions():
    payload = _full_payload()
    payload["ai_summary"]["key_findings"] = [
        {"description": "Drone strikes: markets hit in Darfur.", "source_report_ids": ["r1"]},
    ]
    prose = extract_situation_prose(payload)
    assert prose["ai_summary"]["key_findings"] == [{"description": "Drone strikes: markets hit in Darfur."}]


def test_sector_keeps_prose_drops_enums_ids_ratings():
    health = extract_situation_prose(_full_payload())["sectors"]["health"]
    assert health == {
        "impact": ["Clinics overwhelmed"],
        "humanitarian_conditions": ["Disease outbreaks"],
        "vulnerable_sections": ["Children"],
        "top_needs": ["Medicine"],
        "priority_interventions": ["Mobile clinics"],
        "information_coverage": [{"area": "Access"}],
        "questions_not_answered": ["Which facilities remain functional?"],
    }
    # No severity/evidence_scope/source_report_ids/completeness leaked to the translator.
    assert "severity" not in health
    assert "evidence_scope" not in health
    assert "source_report_ids" not in health
    assert "completeness" not in health


def test_bullets_reduced_to_descriptions():
    prose = extract_situation_prose(_full_payload())
    assert prose["hazards_and_vulnerabilities"]["hazards"] == [
        {"description": "Seasonal flooding"},
    ]
    assert prose["hazards_and_vulnerabilities"]["questions_not_answered"] == [
        "Which pre-crisis vulnerabilities dominate?",
    ]
    assert prose["displacement"]["push_factors"] == [{"description": "Conflict"}]
    assert prose["displacement"]["return_intention"] == []
    assert prose["displacement"]["questions_not_answered"] == [
        "What drives return decisions?",
    ]


def test_humanitarian_access_projected():
    prose = extract_situation_prose(_full_payload())
    access = prose["humanitarian_access"]
    assert access == {
        "barriers": [{"description": "Checkpoints delay aid convoys"}],
        "constraints": [{"description": "Seasonal flooding blocks secondary roads"}],
        "questions_not_answered": ["Where are bureaucratic impediments worst?"],
    }
    assert "completeness" not in access
    assert "contributing_sources" not in access
    assert "source_report_ids" not in access


def test_context_risks_include_questions_not_answered():
    prose = extract_situation_prose(_full_payload())
    security = prose["context_risks"]["security"]
    assert security == {
        "bullets": ["Armed clashes in the north."],
        "questions_not_answered": ["What is the trend in civilian casualties?"],
    }
    assert "completeness" not in security
    assert "source_report_ids" not in security


def test_change_notes_are_prose_values_only():
    prose = extract_situation_prose(_full_payload())
    assert prose["changes"] == {"notes": {"summary": "Needs escalated in the north."}}


def test_empty_payload_yields_stable_empty_shape():
    prose = extract_situation_prose({})
    assert set(prose.keys()) == set(PROSE_COMPONENTS)
    assert prose["ai_summary"] == {"text": "", "key_findings": []}
    assert prose["context_risks"] == {}
    assert prose["humanitarian_access"] == {
        "barriers": [],
        "constraints": [],
        "questions_not_answered": [],
    }
    assert prose["sectors"] == {}
    assert prose["changes"] == {"notes": {}}
