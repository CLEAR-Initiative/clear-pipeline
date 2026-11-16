"""Adversarial judge (ported clear_eval): verdict normalisation + guards, node
selection, coverage, recommendations. Pure — no LLM/S3."""

from clear_pipeline.defs.analysis.judge import (
    JudgeLLM,
    JudgeVerdict,
    coverage,
    format_findings,
    normalise_verdict,
    recommendation_for,
    select_nodes_to_judge,
)
from clear_pipeline.defs.analysis.taxonomy import TaxonomyNode


def _node(name, task="Sectors"):
    return TaxonomyNode(
        task=task, level=1, name=name, l1=name, l2=None, l3=None,
        definition="d", question=f"Q {name}",
    )


def _art(completeness, findings=("f",)):
    return {"descriptive": {"completeness": completeness, "findings": [{"description": f} for f in findings]}}


# ── normalise_verdict (guards + kept/flagged mapping) ───────────────

def test_complete_on_topic_is_kept():
    v = normalise_verdict("n", JudgeLLM(score="complete", has_content=True, out_of_subject=False, out_of_subject_share="none"))
    assert v.kept is True and v.reason == "judge_complete"


def test_partial_on_topic_is_kept():
    v = normalise_verdict("n", JudgeLLM(score="partial", out_of_subject_share="none"))
    assert v.kept is True and v.reason == "judge_partial"


def test_not_answered_is_flagged():
    v = normalise_verdict("n", JudgeLLM(score="not_answered"))
    assert v.kept is False and v.reason == "judge_not_answered"


def test_mostly_off_subject_is_flagged_even_if_scored_partial():
    v = normalise_verdict("n", JudgeLLM(score="partial", out_of_subject=True, out_of_subject_share="mostly"))
    assert v.kept is False and v.reason == "judge_off_subject"


def test_entirely_off_subject_downgrades_complete_to_not_answered_and_flags():
    v = normalise_verdict("n", JudgeLLM(score="complete", out_of_subject=True, out_of_subject_share="entirely"))
    assert v.score == "not_answered" and v.kept is False and v.reason == "judge_off_subject"


def test_no_content_is_flagged_no_evidence():
    v = normalise_verdict("n", JudgeLLM(score="complete", has_content=False, out_of_subject_share="none"))
    assert v.kept is False and v.reason == "no_evidence" and v.out_of_subject_share == "no_content"


def test_aliases_are_repaired():
    v = normalise_verdict("n", JudgeLLM(score="N", out_of_subject_share="all", out_of_subject=True))
    # "N" → not_answered, "all" → entirely
    assert v.score == "not_answered" and v.out_of_subject_share == "entirely" and v.kept is False


def test_garbage_score_defaults_to_not_answered():
    v = normalise_verdict("n", JudgeLLM(score="maybe?"))
    assert v.score == "not_answered"


# ── select_nodes_to_judge ───────────────────────────────────────────

def test_select_judges_below_floor_and_near_band_only_worst_first():
    a, b, c, d, e = _node("A"), _node("B"), _node("C"), _node("D"), _node("E")
    arts = {
        a.key: _art(0.9),                 # clearly strong (≥ floor+band) → skip
        b.key: _art(0.55),                # near-floor band (0.4 ≤ .55 < .6) → judge
        c.key: _art(0.2),                 # below floor, has findings → judge
        d.key: {"descriptive": {"completeness": 0.1, "findings": []}},  # no findings → skip
        e.key: _art(None),                # unknown self-rating, has findings → judge
    }
    got = select_nodes_to_judge([a, b, c, d, e], arts, floor=0.4, band=0.2)
    # worst-first: None(-1) then 0.2 then 0.55; a and d excluded
    assert got == [e.key, c.key, b.key]


def test_select_respects_max_nodes():
    a, b = _node("A"), _node("B")
    arts = {a.key: _art(0.1), b.key: _art(0.3)}
    assert select_nodes_to_judge([a, b], arts, floor=0.4, band=0.2, max_nodes=1) == [a.key]


# ── coverage + recommendations + formatting ─────────────────────────

def _v(score, **kw):
    base = dict(node_key="n", score=score, has_content=True, out_of_subject=False,
                out_of_subject_share="none", notes="", kept=score != "not_answered",
                reason=f"judge_{score}")
    base.update(kw)
    return JudgeVerdict(**base)


def test_coverage_weights():
    vs = [_v("complete"), _v("partial"), _v("not_answered")]
    assert coverage(vs) == (1.0 + 0.5 + 0.0) / 3


def test_coverage_none_when_empty():
    assert coverage([]) is None


def test_recommendation_actions():
    assert recommendation_for(_v("complete"))[0] == "Keep"
    assert recommendation_for(_v("partial"))[0] == "Expand"
    assert recommendation_for(_v("not_answered"))[0] == "Add"
    off = _v("not_answered", out_of_subject=True, out_of_subject_share="entirely")
    assert recommendation_for(off)[0] == "Wrong"


def test_format_findings_reads_description_and_caps():
    txt = format_findings([{"description": "one"}, {"description": ""}, {"text": "two"}])
    assert "- one" in txt and "- two" in txt
    assert format_findings([]) == ""
