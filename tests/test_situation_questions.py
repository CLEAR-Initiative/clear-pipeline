"""Tests for CSV-backed analysis questions."""

from pathlib import Path

import pytest

from clear_pipeline.defs.situation.questions import (
    CONTEXT_DOMAIN_KEYS,
    SECTOR_KEYS,
    SECTOR_SCHEMA_KEYS,
    format_question_block,
    get_analysis_questions,
    load_analysis_questions,
    question_for_schema_key,
    rag_query_for,
    rag_query_for_schema_key,
    sector_question_for,
    situation_questions,
)


class TestLoadAnalysisQuestions:
    def test_default_csv_loads_without_answer_column(self):
        rows = get_analysis_questions()
        assert len(rows) == 22
        assert all(r.question for r in rows)
        assert all(r.pillar_type for r in rows)
        assert all(r.name for r in rows)

    def test_mapped_context_and_sector_keys(self):
        assert question_for_schema_key("demographics") is not None
        assert question_for_schema_key("demographics").name == "Demographics"
        assert "population" in question_for_schema_key("demographics").question.lower()

        health = sector_question_for("health")
        assert health is not None
        assert health.name == "Health"

        assert question_for_schema_key("displacement") is not None
        assert question_for_schema_key("hazards_and_vulnerabilities") is not None
        assert question_for_schema_key("humanitarian_access") is not None
        assert question_for_schema_key("humanitarian_coordination") is not None

    def test_every_csv_row_maps_to_a_schema_key(self):
        # Full first-level coverage: no CSV pillar is left unmapped.
        unmapped = [r.name for r in get_analysis_questions() if r.schema_key is None]
        assert unmapped == []
        agri = next(r for r in get_analysis_questions() if r.name == "Agriculture")
        assert agri.schema_key == "agriculture"
        assert agri.pillar_type == "Sector"

    def test_format_question_block_uses_csv_text(self):
        q = question_for_schema_key("demographics")
        block = format_question_block(q, fallback_topic="Demographics")
        assert "QUESTION (Demographics)" in block
        assert q.question in block

    def test_format_question_block_fallback_without_question(self):
        block = format_question_block(None, fallback_topic="Custom topic")
        assert "Custom topic" in block
        assert "QUESTION:" in block

    def test_rag_query_uses_analysis_question(self):
        q = question_for_schema_key("displacement")
        query = rag_query_for("Sudan", q)
        assert query.startswith("Sudan ")
        assert q.question in query
        assert rag_query_for_schema_key("Sudan", "health").startswith("Sudan ")
        assert "facilities" in rag_query_for_schema_key("Sudan", "health").lower()

    def test_situation_questions_are_the_three_pillars(self):
        names = {q.name for q in situation_questions()}
        assert names == {"Displacement", "Shock-Event", "Humanitarian Access"}

    def test_taxonomy_exports_match_csv_maps(self):
        assert CONTEXT_DOMAIN_KEYS[0] == "demographics"
        assert "humanitarian_coordination" in CONTEXT_DOMAIN_KEYS
        assert SECTOR_KEYS[0] == ("agriculture", "Agriculture")
        assert SECTOR_SCHEMA_KEYS == tuple(k for k, _ in SECTOR_KEYS)
        assert len(SECTOR_KEYS) == 10

    def test_missing_columns_raises(self, tmp_path: Path):
        bad = tmp_path / "bad.csv"
        bad.write_text("Name,Foo\nA,B\n", encoding="utf-8")
        with pytest.raises(ValueError, match="missing required columns"):
            load_analysis_questions(bad)
