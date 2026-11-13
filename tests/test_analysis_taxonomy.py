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
    assert registry.ANALYSIS_LLM_ROLE == "analysis"
