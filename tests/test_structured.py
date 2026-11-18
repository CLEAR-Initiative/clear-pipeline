"""Structured per-node generation (ADR-0015, slice 2). Mocked RAG + LLM."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import clear_pipeline.defs.analysis.structured as st
from clear_pipeline.defs.analysis.frame import Frame
from clear_pipeline.defs.analysis.output_schemas import NodeEnvelope
from clear_pipeline.defs.analysis.taxonomy import node_by_key


def _rag(empty=False):
    return SimpleNamespace(
        is_empty=empty, formatted_for_prompt="EVIDENCE",
        contributing_report_ids=["r1", "r2"],
        hit_report_ids=["rep-a", "rep-b", "rep-c"],  # [R1]=rep-a, [R2]=rep-b, [R3]=rep-c
        hits=[
            {"reportId": "rep-a", "reportTitle": "Report A", "sourceUrl": "http://a",
             "publishedAt": "2026-01-01"},
            {"reportId": "rep-b", "reportTitle": "Report B", "sourceUrl": "",
             "publishedAt": "2026-02-01"},
        ],
        hit_count=3,
    )


def _outcome(data):
    out = MagicMock()
    out.output.model_dump.return_value = data
    out.status = "ok"
    out.usage = {"input_tokens": 5}
    out.cost_usd = 0.01
    out.provenance = {"model": "deepseek/deepseek-v3.2"}
    return out


_FRAME = Frame.build(window_start="2026-01-01", window_end="2026-03-31", location_ids=["x"])


def test_structured_sector_emits_severityneeds_envelope():
    node = node_by_key("sectors/health")
    data = {
        "severity": "high", "top_needs": ["trauma care"], "affected": None, "findings": [],
        "summary": "health strained", "completeness": 0.8, "questions_not_answered": ["coverage"],
    }
    with patch.object(st, "fetch_rag_context", return_value=_rag()), \
         patch.object(st, "run_node", return_value=_outcome(data)):
        env, acct = st.analyse_node_structured(
            node=node, frame=_FRAME, scope_label="Sudan", provider=MagicMock(total_usage={}, model="m"),
        )
    assert isinstance(env, NodeEnvelope)
    assert env.archetype == "SeverityNeeds"
    assert env.summary == "health strained"
    assert env.completeness == 0.8
    # the envelope's self-rating fields are split out of the archetype data
    assert "summary" not in env.data and "completeness" not in env.data
    assert env.data["severity"] == "high"
    assert env.data["top_needs"] == ["trauma care"]
    assert env.source_report_ids == ["r1", "r2"]
    env.validate_data()  # parses under SeverityNeeds
    assert acct["status"] == "ok"


def test_structured_resolves_citation_markers_to_report_ids():
    node = node_by_key("sectors/health")
    data = {
        "severity": "high", "top_needs": ["trauma care"], "affected": None,
        "findings": [
            {"description": "clinics overwhelmed [R2]", "locations": [],
             "source_ids": ["R1", "[R3]"]},
        ],
        "summary": "health strained [R1]", "completeness": 0.8, "questions_not_answered": [],
    }
    with patch.object(st, "fetch_rag_context", return_value=_rag()), \
         patch.object(st, "run_node", return_value=_outcome(data)):
        env, _ = st.analyse_node_structured(
            node=node, frame=_FRAME, scope_label="Sudan",
            provider=MagicMock(total_usage={}, model="m"),
        )
    f = env.data["findings"][0]
    # R1 → rep-a, R3 → rep-c (1-based into hit_report_ids); not the raw "R1"/"R3".
    assert f["source_ids"] == ["rep-a", "rep-c"]
    assert f["description"] == "clinics overwhelmed"  # inline [R2] stripped from text
    assert env.summary == "health strained"  # marker stripped from the summary
    env.validate_data()


def test_structured_captures_report_meta_from_rag_hits():
    node = node_by_key("sectors/health")
    data = {
        "severity": "high", "top_needs": [], "affected": None, "findings": [],
        "summary": "s", "completeness": 0.5, "questions_not_answered": [],
    }
    with patch.object(st, "fetch_rag_context", return_value=_rag()), \
         patch.object(st, "run_node", return_value=_outcome(data)):
        _env, acct = st.analyse_node_structured(
            node=node, frame=_FRAME, scope_label="Sudan",
            provider=MagicMock(total_usage={}, model="m"),
        )
    # Titles come from the RAG hits so the overview can embed them (no 'Untitled').
    assert acct["report_meta"]["rep-a"]["report_title"] == "Report A"
    assert acct["report_meta"]["rep-a"]["source_url"] == "http://a"
    assert acct["report_meta"]["rep-b"]["published_at"] == "2026-02-01"


def test_empty_rag_skips_llm_and_emits_empty_envelope():
    node = node_by_key("sectors/health")
    with patch.object(st, "fetch_rag_context", return_value=_rag(empty=True)), \
         patch.object(st, "run_node") as rn:
        env, acct = st.analyse_node_structured(
            node=node, frame=_FRAME, scope_label="Sudan", provider=MagicMock(total_usage={}, model="m"),
        )
    rn.assert_not_called()
    assert env.completeness == 0.0
    assert env.archetype == "SeverityNeeds"
    assert acct["rag_hit_count"] == 0


def test_render_envelope_text_carries_summary_data_and_gaps():
    env = NodeEnvelope(
        node_key="x", archetype="Described", summary="S",
        data={"findings": [{"description": "f"}]}, questions_not_answered=["q"],
    )
    txt = st.render_envelope_text(env)
    assert "S" in txt and "findings" in txt and "NOT ANSWERED" in txt


def test_structured_enabled_reads_env(monkeypatch):
    monkeypatch.delenv("ANALYSIS_STRUCTURED", raising=False)
    assert st.structured_enabled() is False
    monkeypatch.setenv("ANALYSIS_STRUCTURED", "1")
    assert st.structured_enabled() is True
    monkeypatch.setenv("ANALYSIS_STRUCTURED", "off")
    assert st.structured_enabled() is False


def test_envelope_to_descriptive_matches_nodedescriptive_shape():
    env = NodeEnvelope(
        node_key="sectors/health", name="Health", task="Sectors", level=1,
        archetype="SeverityNeeds", summary="strained", completeness=0.7,
        questions_not_answered=["coverage"], source_report_ids=["r1"],
        data={"severity": "high"},
    )
    d = st.envelope_to_descriptive(env)
    # Parses as the legacy published descriptive form (what gaps/judge read).
    from clear_pipeline.defs.analysis.descriptive import NodeDescriptive
    nd = NodeDescriptive.model_validate(d)
    assert nd.node_key == "sectors/health" and nd.node_name == "Health"
    assert nd.completeness == 0.7
    assert nd.questions_not_answered == ["coverage"]
    assert nd.source_report_ids == ["r1"]
    assert nd.findings[0].description == "strained"
    assert nd.findings[0].source_report_ids == ["r1"]


def test_compose_l1_single_call_for_non_section():
    node = node_by_key("sectors/health")
    data = {
        "severity": "high", "top_needs": ["trauma care"], "affected": None, "findings": [],
        "summary": "health strained", "completeness": 0.8, "questions_not_answered": [],
    }
    with patch.object(st, "fetch_rag_context", return_value=_rag()), \
         patch.object(st, "run_node", return_value=_outcome(data)):
        env, accts = st.compose_l1(
            node=node, frame=_FRAME, scope_label="Sudan",
            make_provider=lambda: MagicMock(total_usage={}, model="m"),
        )
    assert env.archetype == "SeverityNeeds"
    assert len(accts) == 1


def test_compose_l1_section_folds_in_l2_children():
    node = node_by_key("pillars_1d/context")  # Context — a Section L1
    assert node is not None and node.level == 1

    child_data = {
        "findings": [{"description": "c", "locations": [], "source_ids": ["r1"]}],
        "summary": "child", "completeness": 0.5, "questions_not_answered": [],
    }
    with patch.object(st, "fetch_rag_context", return_value=_rag()), \
         patch.object(st, "run_node", return_value=_outcome(child_data)):
        env, accts = st.compose_l1(
            node=node, frame=_FRAME, scope_label="Sudan",
            make_provider=lambda: MagicMock(total_usage={}, model="m"),
        )
    assert env.archetype == "Section"
    # One LLM call per L2 child; Context has 9 domains.
    assert len(accts) == len(st.children_of(node)) == 9
    subs = env.data["subsections"]
    assert len(subs) == 9
    # Each subsection is a NodeEnvelope dump carrying the child's name (assemble_v5
    # matches on it) and its archetype data.
    sample = next(iter(subs.values()))
    assert "name" in sample and "data" in sample
    env.validate_data()  # parses under the Section archetype


def test_section_counts_failed_children_as_zero_completeness():
    node = node_by_key("pillars_1d/context")  # 9 domains
    good = {
        "findings": [{"description": "d", "locations": [], "source_ids": []}],
        "summary": "s", "completeness": 0.9, "questions_not_answered": [],
    }
    # First domain answers at 0.9; the other 8 error out.
    with patch.object(st, "fetch_rag_context", return_value=_rag()), \
         patch.object(st, "run_node", side_effect=[_outcome(good)] + [RuntimeError("boom")] * 8):
        env, _ = st.compose_l1(
            node=node, frame=_FRAME, scope_label="Sudan",
            make_provider=lambda: MagicMock(total_usage={}, model="m"),
        )
    assert env.archetype == "Section"
    assert len(env.data["subsections"]) == 9  # all present (failed ones empty)
    # 1 of 9 answered → the Section must NOT look complete (0.9/9 ≈ 0.1), not 0.9
    assert env.completeness < 0.2


def test_build_structured_artifact_is_dual_shape():
    env = NodeEnvelope(
        node_key="sectors/health", name="Health", task="Sectors", level=1,
        archetype="SeverityNeeds", summary="s", completeness=0.6,
        source_report_ids=["r1"], data={"severity": "high"},
    )
    accts = [
        {"status": "ok", "usage": {"input_tokens": 3}, "cost_usd": 0.02,
         "provenance": {"model": "m"}, "rag_hit_count": 2,
         "report_meta": {"rep-a": {"report_title": "Report A"}}},
        {"status": "best_effort", "usage": {"input_tokens": 1}, "cost_usd": 0.01,
         "provenance": {"model": "m"}, "rag_hit_count": 1,
         "report_meta": {"rep-b": {"report_title": "Report B"}}},
    ]
    art = st.build_structured_artifact(
        iso3="sdn", window_kind="yearly", frame=_FRAME,
        node=node_by_key("sectors/health"), env=env, accts=accts,
    )
    assert art["descriptive"]["node_key"] == "sectors/health"  # gaps/judge read this
    assert art["envelope"]["archetype"] == "SeverityNeeds"  # overview reads this
    assert art["status"] == "ok"  # ok if any call ok
    assert art["cost_usd"] == 0.03  # summed
    assert art["usage"]["input_tokens"] == 4  # summed
    assert art["rag_hit_count"] == 3
    # report metadata unioned across calls (for sources.reports titles)
    assert art["report_meta"]["rep-a"]["report_title"] == "Report A"
    assert art["report_meta"]["rep-b"]["report_title"] == "Report B"
