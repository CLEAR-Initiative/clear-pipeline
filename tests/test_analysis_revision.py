"""Revision (reflexion) + web-rescue logic: node selection, rescue detection,
and the evaluation filter's rescue path. Pure — no S3/LLM."""

from clear_pipeline.defs.analysis.evaluation import filter_package
from clear_pipeline.defs.analysis.judge import JudgeVerdict
from clear_pipeline.defs.analysis.revision import (
    select_nodes_to_revise,
    web_rescued_keys,
)
from clear_pipeline.defs.analysis.taxonomy import TaxonomyNode


def _jv(node_key, score, reason, *, kept, out_of_subject=False, share="none"):
    return JudgeVerdict(
        node_key=node_key, score=score, has_content=True, out_of_subject=out_of_subject,
        out_of_subject_share=share, notes="", kept=kept, reason=reason,
    )


def _node(name, task="Sectors"):
    return TaxonomyNode(
        task=task, level=1, name=name, l1=name, l2=None, l3=None,
        definition="d", question=f"Q {name}",
    )


def _desc(completeness, findings=("f",)):
    return {"descriptive": {"completeness": completeness, "findings": [{"description": f} for f in findings]}}


# ── web_rescued_keys ────────────────────────────────────────────────

def test_web_rescue_requires_status_confidence_and_findings():
    gapfills = {
        "a": {"status": "resolved", "confidence": 0.8, "value": [{"text": "x"}]},   # rescued
        "b": {"status": "partial", "confidence": 0.5, "value": [{"text": "y"}]},     # rescued (at floor)
        "c": {"status": "partial", "confidence": 0.3, "value": [{"text": "z"}]},     # too low-confidence
        "d": {"status": "unresolved", "confidence": 0.9, "value": [{"text": "w"}]},  # wrong status
        "e": {"status": "resolved", "confidence": 0.9, "value": []},                 # no findings
    }
    assert web_rescued_keys(gapfills, confidence_floor=0.5) == {"a", "b"}


# ── select_nodes_to_revise ──────────────────────────────────────────

def test_select_only_below_floor_worst_first_capped():
    verdicts = [
        {"node_key": "n1", "kept": False, "reason": "below_floor", "completeness": 0.3},
        {"node_key": "n2", "kept": False, "reason": "below_floor", "completeness": 0.1},
        {"node_key": "n3", "kept": False, "reason": "no_evidence", "completeness": None},   # excluded
        {"node_key": "n4", "kept": False, "reason": "not_generated", "completeness": None}, # excluded
        {"node_key": "n5", "kept": True, "reason": "kept", "completeness": 0.9},            # excluded
    ]
    # worst-first: n2 (0.1) before n1 (0.3); no_evidence/not_generated/kept excluded
    assert select_nodes_to_revise(verdicts, {}, max_nodes=5) == ["n2", "n1"]
    assert select_nodes_to_revise(verdicts, {}, max_nodes=1) == ["n2"]


def test_select_includes_judge_flagged_reasons_excludes_no_evidence():
    verdicts = [
        {"node_key": "n1", "kept": False, "reason": "below_floor", "completeness": 0.3},
        {"node_key": "n2", "kept": False, "reason": "judge_not_answered", "completeness": 0.5},
        {"node_key": "n3", "kept": False, "reason": "judge_off_subject", "completeness": 0.2},
        {"node_key": "n4", "kept": False, "reason": "no_evidence", "completeness": None},     # excluded
        {"node_key": "n5", "kept": False, "reason": "not_generated", "completeness": None},   # excluded
    ]
    # worst-first by completeness: n3(0.2), n1(0.3), n2(0.5); no_evidence/not_generated out
    assert select_nodes_to_revise(verdicts, {}, max_nodes=5) == ["n3", "n1", "n2"]


def test_select_breaks_ties_toward_web_evidence():
    verdicts = [
        {"node_key": "n1", "kept": False, "reason": "below_floor", "completeness": 0.2},
        {"node_key": "n2", "kept": False, "reason": "below_floor", "completeness": 0.2},
    ]
    # equal completeness → the node with web evidence to fold in goes first
    assert select_nodes_to_revise(verdicts, {"n2": {"status": "partial"}}, max_nodes=2) == ["n2", "n1"]


# ── filter_package rescue path ──────────────────────────────────────

def test_filter_keeps_rescued_below_floor_and_no_evidence():
    a, b, c = _node("A"), _node("B"), _node("C")
    artifacts = {
        a.key: _desc(0.8),            # clears floor → kept normally
        b.key: _desc(0.1),            # below floor → would be flagged…
        c.key: {"descriptive": {"completeness": 0.0, "findings": []}},  # no evidence…
    }
    pkg = filter_package([a, b, c], artifacts, completeness_floor=0.4, rescued_node_keys={b.key, c.key})
    assert set(pkg.kept_node_keys) == {a.key, b.key, c.key}
    by_key = {v.node_key: v.reason for v in pkg.verdicts}
    assert by_key[a.key] == "kept"
    assert by_key[b.key] == "web_rescued"
    assert by_key[c.key] == "web_rescued"


def test_filter_without_rescue_flags_weak_nodes():
    a, b = _node("A"), _node("B")
    artifacts = {a.key: _desc(0.8), b.key: _desc(0.1)}
    pkg = filter_package([a, b], artifacts, completeness_floor=0.4)
    assert pkg.kept_node_keys == [a.key]
    assert pkg.flagged_node_keys == [b.key]


def test_judge_is_primary_over_self_rating():
    """Judge rescues a thin-but-correct node and drops a padded high-self-rating
    node — overriding the completeness floor. Web-rescue still overrides the judge."""
    a, b, c = _node("A"), _node("B"), _node("C")
    artifacts = {
        a.key: _desc(0.2),   # below floor, but judge says partial → kept
        b.key: _desc(0.9),   # clears floor, but judge says off-subject padding → flagged
        c.key: _desc(0.1),   # below floor; judged not_answered BUT web-rescued → kept
    }
    judge = {
        a.key: _jv(a.key, "partial", "judge_partial", kept=True),
        b.key: _jv(b.key, "not_answered", "judge_off_subject", kept=False, out_of_subject=True, share="mostly"),
        c.key: _jv(c.key, "not_answered", "judge_not_answered", kept=False),
    }
    pkg = filter_package([a, b, c], artifacts, completeness_floor=0.4,
                         rescued_node_keys={c.key}, judge_verdicts=judge)
    by_key = {v.node_key: v for v in pkg.verdicts}
    assert by_key[a.key].kept and by_key[a.key].reason == "judge_partial"       # rescued by judge
    assert not by_key[b.key].kept and by_key[b.key].reason == "judge_off_subject"  # padding caught
    assert by_key[c.key].kept and by_key[c.key].reason == "web_rescued"         # rescue beats judge
    # judge metadata attached for judged (non-rescued) nodes
    assert by_key[a.key].judge["score"] == "partial"
    assert by_key[c.key].judge is None  # decided by web-rescue, not the judge
