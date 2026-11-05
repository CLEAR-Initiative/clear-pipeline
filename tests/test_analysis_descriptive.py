"""Single-node descriptive analyst (PR-4). Mocked provider + mocked RAG."""

from unittest.mock import patch

from clear_pipeline.defs.analysis import descriptive as desc
from clear_pipeline.defs.analysis.descriptive import DescriptiveLLM, analyse_node
from clear_pipeline.defs.analysis.frame import country_frame
from clear_pipeline.defs.analysis.rag_helper import RAGContext
from clear_pipeline.defs.analysis.taxonomy import l1_nodes


def _node():
    return l1_nodes()[0]  # any real L1 node (has a question)


class FakeProvider:
    def __init__(self, output, model="deepseek/deepseek-v3.2"):
        self.model = model
        self.role = "analysis"
        self.provider_name = "fake"
        self.last_usage = None
        self.total_usage = {"input_tokens": 0, "output_tokens": 0}
        self._output = output
        self.calls = 0

    def complete_structured(self, *, system, user, schema, max_tokens=1024, **kw):
        self.calls += 1
        self.last_usage = {"input_tokens": 500, "output_tokens": 80}
        self.total_usage = {"input_tokens": 500, "output_tokens": 80}
        return self._output


_RAG = RAGContext(
    formatted_for_prompt="[R1] Report A — 2026\nDeaths rose.\n[R2] Report B — 2026\nClashes.",
    contributing_report_ids=["rep-a", "rep-b"],
    hit_report_ids=["rep-a", "rep-b"],
    hit_count=2,
    hits=[{"reportId": "rep-a"}, {"reportId": "rep-b"}],
)
_EMPTY_RAG = RAGContext(formatted_for_prompt="", hit_count=0)


def test_analyse_node_produces_finding_with_resolved_citations():
    out = DescriptiveLLM(
        findings=["Deaths rose sharply [R1]", "Clashes in the capital [R2]"],
        completeness=0.7,
        questions_not_answered=["casualty breakdown by age"],
    )
    p = FakeProvider(out)
    with patch.object(desc, "fetch_rag_context", return_value=_RAG):
        res = analyse_node(
            node=_node(),
            frame=country_frame("loc-a0", window_start="2026-01-01", window_end=None),
            scope_label="Sudan", provider=p,
        )
    assert p.calls == 1
    assert res.status == "ok"
    assert res.rag_hit_count == 2
    d = res.descriptive
    assert [b.description for b in d.findings] == ["Deaths rose sharply", "Clashes in the capital"]
    assert d.findings[0].source_report_ids == ["rep-a"]
    assert d.findings[1].source_report_ids == ["rep-b"]
    assert d.completeness == 0.7
    assert d.questions_not_answered == ["casualty breakdown by age"]
    assert set(d.source_report_ids) == {"rep-a", "rep-b"}
    assert res.cost_usd > 0
    assert res.provenance["model"] == "deepseek/deepseek-v3.2"
    assert res.usage == {"input_tokens": 500, "output_tokens": 80}


def test_empty_retrieval_skips_the_llm():
    p = FakeProvider(DescriptiveLLM(findings=["should not be used"], completeness=1.0))
    with patch.object(desc, "fetch_rag_context", return_value=_EMPTY_RAG):
        res = analyse_node(
            node=_node(),
            frame=country_frame("loc-a0", window_start="2026-01-01", window_end=None),
            scope_label="Sudan", provider=p,
        )
    assert p.calls == 0  # LLM skipped on empty evidence
    assert res.status == "best_effort"
    assert res.rag_hit_count == 0
    assert res.descriptive.findings == []
    assert res.descriptive.completeness == 0.0
    # the whole question is flagged unanswered
    assert res.descriptive.questions_not_answered == [_node().question]
    assert res.cost_usd == 0.0


def test_marker_only_finding_is_dropped():
    out = DescriptiveLLM(findings=["[R1]", "Real finding [R2]"], completeness=0.5)
    p = FakeProvider(out)
    with patch.object(desc, "fetch_rag_context", return_value=_RAG):
        res = analyse_node(
            node=_node(),
            frame=country_frame("loc-a0", window_start="2026-01-01", window_end=None),
            scope_label="Sudan", provider=p,
        )
    descs = [b.description for b in res.descriptive.findings]
    assert descs == ["Real finding"]  # marker-only bullet dropped
