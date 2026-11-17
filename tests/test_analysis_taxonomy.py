"""Analysis module foundations (PR-1): taxonomy loader, profiles, frame, registry.

No Dagster / DB / LLM — pure library checks that the committed taxonomy loads
with the declared shape and the config surfaces are sane.
"""

from clear_pipeline.defs.analysis import profiles, registry, taxonomy


def test_taxonomy_counts_match_the_spreadsheet():
    nodes = taxonomy.get_taxonomy()
    assert len(nodes) == 22 + 97 + 576  # 695
    assert len(taxonomy.nodes_at_depth("L1")) == 22
    assert len(taxonomy.nodes_at_depth(2)) == 97
    assert len(taxonomy.nodes_at_depth("L3")) == 576
    assert taxonomy.tasks() == ("Pillars 1D", "Pillars 2D", "Sectors")


def test_every_node_has_an_analytical_question_and_definition():
    for n in taxonomy.get_taxonomy():
        assert n.question.strip(), f"{n.key} has no analytical question"
        assert n.definition.strip(), f"{n.key} has no definition"


def test_l1_is_the_default_fan_out():
    l1 = taxonomy.l1_nodes()
    assert len(l1) == 22
    # L1 is the universal default depth.
    assert profiles.DEFAULT_DEPTH == "L1"
    assert all(n.level == 1 for n in l1)
    # A recognisable L1 node from each task.
    names = {n.name for n in l1}
    assert {"Context", "Displacement", "Health", "Impact"} <= names


def test_node_keys_are_stable_slugs_and_parent_resolves():
    # "Context" L1 nests the 9 context pillars as L2 (the reorg vs the old CSV).
    context = next(n for n in taxonomy.l1_nodes() if n.name == "Context")
    assert context.key == "pillars_1d/context"
    assert context.parent_key is None

    kids = taxonomy.children_of(context)
    assert len(kids) >= 1
    child = kids[0]
    assert child.level == 2
    assert child.parent_key == context.key
    assert taxonomy.parent_of(child) == context
    # key is path-slugged
    assert child.key.startswith("pillars_1d/context/")


def test_depth_rejects_bad_values():
    import pytest

    with pytest.raises(ValueError):
        taxonomy.nodes_at_depth(4)


def test_default_profile_is_single_rag_call_at_l1():
    p = profiles.default_profile()
    assert p.depth == "L1"
    assert p.tools == ("rag",)
    assert p.retrieval == "kb_only"
    assert p.effort == "single_call"
    assert p.caps.max_turns == 1
    # at_depth returns a deeper copy without mutating the original
    deeper = p.at_depth("L3")
    assert deeper.depth == "L3"
    assert p.depth == "L1"


def test_registry_stamp_records_the_serving_model():
    stamp = registry.provenance_stamp("deepseek/deepseek-v3.2")
    assert stamp == {
        "prompt_version": registry.PROMPT_VERSION,
        "model": "deepseek/deepseek-v3.2",
        "schema_version": registry.SCHEMA_VERSION,
    }


# ── resolve_selection — on-demand L2/L3 node deep-dive fan-out ────────────────


def _l1_l2_l3_anchor():
    """A real (L1, L2, [L3...]) chain from the committed taxonomy, so the
    selection tests don't hard-code slugs that a taxonomy regen could rename."""
    for l1 in taxonomy.l1_nodes():
        for l2 in taxonomy.children_of(l1):
            l3s = taxonomy.children_of(l2)
            if len(l3s) >= 2:
                return l1, l2, l3s
    raise AssertionError("no L1→L2→(≥2 L3) chain in the taxonomy")


def test_empty_selection_defaults_to_l1_fan_out():
    # The country-default analysis is unchanged: no selection → all 22 L1.
    assert taxonomy.resolve_selection(None) == taxonomy.l1_nodes()
    assert taxonomy.resolve_selection([]) == taxonomy.l1_nodes()
    assert taxonomy.resolve_selection(["", None]) == taxonomy.l1_nodes()


def test_selecting_two_l3_leaves_generates_only_those_two():
    # User drills Casualties(L1) → Injured(L2) → picks two L3 leaves. We generate
    # the two L3 leaves; the L2 on the path is NOT generated even if also sent.
    _l1, l2, l3s = _l1_l2_l3_anchor()
    leaf_a, leaf_b = l3s[0], l3s[1]
    out = taxonomy.resolve_selection([l2.key, leaf_a.key, leaf_b.key])
    assert {n.key for n in out} == {leaf_a.key, leaf_b.key}
    assert all(n.level == 3 for n in out)


def test_selecting_an_l2_alone_generates_that_l2_leaf():
    # User stops at Injured(L2) with no L3 chosen → Injured IS the leaf.
    _l1, l2, _l3s = _l1_l2_l3_anchor()
    out = taxonomy.resolve_selection([l2.key])
    assert [n.key for n in out] == [l2.key]
    assert out[0].level == 2


def test_frontier_mixes_levels_and_is_deterministic():
    # A selection spanning branches keeps each branch's most-specific node; an L1
    # with no selected descendant stays a leaf alongside a deep L3 from elsewhere.
    _l1, l2, l3s = _l1_l2_l3_anchor()
    lone_l1 = next(n for n in taxonomy.l1_nodes() if n.key != _l1.key)
    out = taxonomy.resolve_selection([lone_l1.key, l2.key, l3s[0].key])
    assert {n.key for n in out} == {lone_l1.key, l3s[0].key}
    # Deterministic order: level then key.
    assert list(out) == sorted(out, key=lambda n: (n.level, n.key))


def test_unknown_key_raises_so_a_bad_request_fails_terminally():
    import pytest

    with pytest.raises(ValueError, match="unknown taxonomy node keys"):
        taxonomy.resolve_selection(["pillars_1d/casualties", "not/a/real/node"])
    assert registry.ANALYSIS_LLM_ROLE == "analysis"
