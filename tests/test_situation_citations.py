"""Tests for per-line source attribution (situation/citations.py).

The narrative LLMs append inline [Rn] markers; these helpers strip them and
build the `report_id -> [generated lines]` map the dashboard cites from.
"""

from clear_context_pipeline.defs.situation.citations import (
    merge_contributing,
    resolve_bullets,
    resolve_prose,
)

HITS = ["rep-A", "rep-B", "rep-C"]  # [R1]=A, [R2]=B, [R3]=C


def test_resolve_bullets_strips_markers_and_inverts():
    clean, per, contrib = resolve_bullets(
        ["Conflict displaced 8.6M people [R1][R3]", "Funding at $43M [R2]"], HITS
    )
    assert clean == ["Conflict displaced 8.6M people", "Funding at $43M"]
    assert per == [["rep-A", "rep-C"], ["rep-B"]]
    assert contrib == {
        "rep-A": ["Conflict displaced 8.6M people"],
        "rep-C": ["Conflict displaced 8.6M people"],
        "rep-B": ["Funding at $43M"],
    }


def test_out_of_range_and_empty_hit_are_dropped():
    hits = ["rep-A", "", "rep-C"]  # [R2] is a hit with no report id
    clean, per, contrib = resolve_bullets(["A claim [R2][R9]", "B claim [R1]"], hits)
    assert clean == ["A claim", "B claim"]
    assert per == [[], ["rep-A"]]  # [R2]->"" and [R9] out of range both dropped
    assert contrib == {"rep-A": ["B claim"]}


def test_duplicate_refs_within_a_bullet_dedupe():
    _clean, per, _contrib = resolve_bullets(["X [R1][R1]"], HITS)
    assert per == [["rep-A"]]


def test_bullet_without_marker_contributes_to_nothing():
    clean, per, contrib = resolve_bullets(["plain bullet, no marker"], HITS)
    assert clean == ["plain bullet, no marker"]
    assert per == [[]]
    assert contrib == {}


def test_marker_punctuation_is_tidied():
    # marker before the full stop must not leave " ." behind
    clean, _per, _contrib = resolve_bullets(["Displacement rose [R1] ."], HITS)
    assert clean == ["Displacement rose ."] or clean == ["Displacement rose."]


def test_resolve_prose_splits_sentences_and_keeps_decimals():
    text = "Conflict displaced 8.6M people. [R1] Funding stands at 43.5M USD. [R2]"
    clean, contrib = resolve_prose(text, HITS)
    # decimals not split; markers stripped from rendered text
    assert "8.6M people" in clean and "43.5M USD" in clean
    assert "[R1]" not in clean and "[R2]" not in clean
    # each sentence attributed to the report whose marker trailed its full stop
    assert contrib == {
        "rep-A": ["Conflict displaced 8.6M people."],
        "rep-B": ["Funding stands at 43.5M USD."],
    }


def test_resolve_prose_marker_before_period_attributes_same_sentence():
    text = "Cholera spread across three states [R3]. Response underfunded [R2]."
    _clean, contrib = resolve_prose(text, HITS)
    assert contrib == {
        "rep-C": ["Cholera spread across three states."],
        "rep-B": ["Response underfunded."],
    }


def test_resolve_prose_empty():
    assert resolve_prose("", HITS) == ("", {})
    assert resolve_prose("   ", HITS) == ("", {})


def test_merge_contributing_unions_and_dedupes_preserving_order():
    a = {"r1": ["l1", "l2"]}
    b = {"r1": ["l2", "l3"], "r2": ["l4"]}
    assert merge_contributing(a, b) == {"r1": ["l1", "l2", "l3"], "r2": ["l4"]}


def test_no_hits_resolves_to_empty_attribution():
    clean, per, contrib = resolve_bullets(["Claim [R1]"], [])
    assert clean == ["Claim"]
    assert per == [[]]
    assert contrib == {}
