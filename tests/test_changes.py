"""Native 'what changed' generation (ADR-0015). Digest + mocked LLM."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import clear_pipeline.defs.analysis.changes as ch


def _v5(sector_sev="high", bullets=("fuel shortage",)):
    return {
        "datapoints": {"population_in_need": {"value": 1000}},
        "ai_summary": {"text": "Crisis deepening."},
        "context_risks": {"security": {"bullets": list(bullets)}},
        "hazards_and_vulnerabilities": {"hazards": [{"description": "flooding"}], "vulnerabilities": []},
        "displacement": {"push_factors": [{"description": "clashes"}], "return_intention": []},
        "humanitarian_access": {"barriers": [{"description": "checkpoints"}], "constraints": []},
        "sectors": {"health": {"severity": sector_sev, "top_needs": ["trauma care"]}},
    }


def test_digest_reduces_v5_payload_to_section_text():
    d = ch.digest_v5(_v5())
    assert d["context_risks.security"] == "fuel shortage"  # domain digested under its key
    assert "flooding" in d["hazards"]
    assert "clashes" in d["displacement"]
    assert "checkpoints" in d["humanitarian_access"]
    assert "severity=high" in d["sectors.health"]
    assert "population_in_need" in d["summary"]


def test_valid_sections_cover_the_9_context_and_10_sector_keys():
    assert "context_risks.security" in ch.VALID_SECTIONS
    assert "sectors.health" in ch.VALID_SECTIONS
    assert sum(1 for s in ch.VALID_SECTIONS if s.startswith("context_risks.")) == 9
    assert sum(1 for s in ch.VALID_SECTIONS if s.startswith("sectors.")) == 10


def test_generate_changes_keeps_only_valid_sections():
    outcome = MagicMock()
    outcome.output = SimpleNamespace(changes=[
        SimpleNamespace(section="sectors.health", note="severity high→critical"),
        SimpleNamespace(section="not.a.section", note="should be dropped"),
        SimpleNamespace(section="hazards", note=""),  # empty note dropped
    ])
    with patch.object(ch, "run_node", return_value=outcome):
        res = ch.generate_changes(
            provider=MagicMock(), prior_data=_v5(sector_sev="medium"), new_data=_v5(sector_sev="critical"),
            basis="previous_generation", prior_generated_at="2026-11-01T00:00:00Z",
            compared_to_window_start="2026-01-01", compared_to_label="2026",
        )
    assert res["basis"] == "previous_generation"
    assert res["compared_to"] == "2026-11-01T00:00:00Z"
    assert res["notes"] == {"sectors.health": "severity high→critical"}  # invalid + empty dropped


def test_empty_changes_shape():
    e = ch.empty_changes(
        basis="previous_generation", prior_generated_at="t",
        compared_to_window_start="2026-01-01", compared_to_label="2026",
    )
    assert e["notes"] == {} and e["basis"] == "previous_generation"
