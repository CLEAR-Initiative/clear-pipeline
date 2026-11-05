"""Pure gap derivation + programmatic-filter evaluation (PR-5)."""

import pytest

from clear_pipeline.defs.analysis.evaluation import filter_package
from clear_pipeline.defs.analysis.gaps import derive_gaps
from clear_pipeline.defs.analysis.taxonomy import l1_nodes


def _art(completeness, *, qna=None, findings=True):
    desc = {
        "completeness": completeness,
        "questions_not_answered": qna or [],
        "findings": [{"description": "x", "source_report_ids": ["r1"]}] if findings else [],
    }
    return {"descriptive": desc}


def _nodes(n=4):
    return list(l1_nodes()[:n])


def test_derive_gaps_flags_low_completeness_and_unanswered():
    a, b, c, d = _nodes(4)
    artifacts = {
        a.key: _art(0.9),                       # answered well → NOT a gap
        b.key: _art(0.3),                       # low completeness → gap
        c.key: _art(0.85, qna=["age breakdown"]),  # unanswered question → gap
        # d.key absent → not_generated gap
    }
    gaps = derive_gaps(_nodes(4), artifacts, threshold=0.6)
    by_key = {g.node_key: g for g in gaps}
    assert a.key not in by_key
    assert by_key[b.key].reason == "low_completeness"
    assert by_key[c.key].reason == "unanswered_questions"
    assert by_key[d.key].reason == "not_generated"
    # each gap carries the node's required analytical question
    assert by_key[b.key].question == b.question


def test_derive_gaps_both_and_no_evidence_reasons():
    a, b = _nodes(2)
    artifacts = {
        a.key: _art(0.2, qna=["x"]),       # low AND unanswered → both
        b.key: _art(0.9, findings=False),  # has high completeness but no findings → no_evidence
    }
    gaps = {g.node_key: g.reason for g in derive_gaps(_nodes(2), artifacts, threshold=0.6)}
    assert gaps[a.key] == "both"
    assert gaps[b.key] == "no_evidence"


def test_derive_gaps_ranks_worst_first():
    a, b, c = _nodes(3)
    artifacts = {a.key: _art(0.5), b.key: _art(0.1), c.key: _art(0.55)}
    ranked = [g.node_key for g in derive_gaps(_nodes(3), artifacts, threshold=0.6)]
    assert ranked == [b.key, a.key, c.key]  # 0.1 < 0.5 < 0.55


def test_derive_gaps_absent_completeness_sorts_first():
    a, b = _nodes(2)
    artifacts = {a.key: _art(0.2)}  # b absent (completeness None)
    ranked = [g.node_key for g in derive_gaps(_nodes(2), artifacts, threshold=0.6)]
    assert ranked[0] == b.key  # None sorts before 0.2


def test_filter_package_keeps_above_floor_flags_below():
    a, b, c, d = _nodes(4)
    artifacts = {
        a.key: _art(0.8),                  # kept
        b.key: _art(0.2),                  # below floor → flagged
        c.key: _art(0.9, findings=False),  # no evidence → flagged
        # d absent → not_generated flagged
    }
    pkg = filter_package(_nodes(4), artifacts, completeness_floor=0.4)
    kept = set(pkg.kept_node_keys)
    assert kept == {a.key}
    reasons = {v.node_key: v.reason for v in pkg.verdicts}
    assert reasons[b.key] == "below_floor"
    assert reasons[c.key] == "no_evidence"
    assert reasons[d.key] == "not_generated"
    # mean over the three present completeness values (0.8, 0.2, 0.9)
    assert pkg.mean_completeness == pytest.approx((0.8 + 0.2 + 0.9) / 3)
    assert a.task in pkg.by_task
