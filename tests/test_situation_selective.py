"""Unit tests for leaf-level selective regeneration (#1067).

Pure/fake-driven: the planner (`is_stale`) is pure; `build_leaf_components` is
exercised with `_leaf_rag` / `_leaf_generate` monkeypatched so no search or LLM
runs. Verifies that unchanged leaves are reused from the prior payload and only
changed leaves regenerate, keyed off the per-leaf evidence fingerprint.
"""

from clear_pipeline.defs.situation import selective
from clear_pipeline.defs.situation.rag_helper import RAGContext
from clear_pipeline.defs.situation.schemas import (
    DisplacementNarrative,
    HazardsAndVulnerabilities,
    HumanitarianAccess,
    RiskDomain,
    Scenarios,
    SectorAnalysis,
    SituationAnalysisPayload,
    SourcedBullet,
)
from clear_pipeline.defs.situation.selective import (
    LeafBuildResult,
    build_leaf_components,
    build_leaves,
    is_stale,
)


# ── planner ──────────────────────────────────────────────────────────

def test_is_stale_new_key():
    assert is_stale("sectors.health", ["a"], {}) is True


def test_is_stale_same_set_order_insensitive():
    assert is_stale("sectors.health", ["a", "b"], {"sectors.health": ["b", "a"]}) is False


def test_is_stale_changed_set():
    assert is_stale("sectors.health", ["a", "c"], {"sectors.health": ["a", "b"]}) is True


def test_is_stale_removed_evidence():
    assert is_stale("displacement", [], {"displacement": ["a"]}) is True


# ── registry ─────────────────────────────────────────────────────────

def test_registry_covers_all_leaves():
    leaves = build_leaves()
    keys = {leaf.key for leaf in leaves}
    assert len(leaves) == 23  # 9 context pillars + 3 singletons + 10 sectors + scenarios
    assert "context_risks.security" in keys
    assert "sectors.health" in keys
    assert {"hazards_and_vulnerabilities", "displacement", "humanitarian_access", "scenarios"} <= keys


# ── build_leaf_components (fakes) ─────────────────────────────────────

def _component_with_content(leaf):
    """A type-correct, CONTENT-BEARING component for the leaf (so it's treated as
    a successful generation and its fingerprint is recorded), and the container
    assembly (ContextRisks(**…)/Sectors(**…)) validates."""
    if leaf.container == "context_risks":
        return RiskDomain(bullets=["b"])
    if leaf.container == "sectors":
        return SectorAnalysis(impact=["i"])
    return {
        "hazards_and_vulnerabilities": HazardsAndVulnerabilities(
            hazards=[SourcedBullet(description="h")],
        ),
        "displacement": DisplacementNarrative(
            push_factors=[SourcedBullet(description="p")],
        ),
        "humanitarian_access": HumanitarianAccess(
            barriers=[SourcedBullet(description="b")],
        ),
        "scenarios": Scenarios(most_likely="m"),
    }[leaf.field]


def _empty_component(leaf):
    """The empty default for a leaf — what a failed/empty generation returns."""
    if leaf.container == "context_risks":
        return RiskDomain()
    if leaf.container == "sectors":
        return SectorAnalysis()
    return {
        "hazards_and_vulnerabilities": HazardsAndVulnerabilities(),
        "displacement": DisplacementNarrative(),
        "humanitarian_access": HumanitarianAccess(),
        "scenarios": Scenarios(),
    }[leaf.field]


def _install_fakes(monkeypatch, current_ids: dict[str, list[str]], gen_calls: list[str],
                   empty_for: set[str] | None = None):
    empty_for = empty_for or set()

    def fake_leaf_rag(leaf, *, country_name, period_label, rag_filters):
        ids = current_ids[leaf.key]
        hits = [{"id": i} for i in ids]
        return RAGContext(formatted_for_prompt="x", contributing_report_ids=list(ids),
                          hit_report_ids=list(ids), hits=hits,
                          hit_count=max(1, len(ids))), {"used_fallback": False}

    def fake_leaf_generate(leaf, rag, extras, **kwargs):
        gen_calls.append(leaf.key)
        return _empty_component(leaf) if leaf.key in empty_for else _component_with_content(leaf)

    monkeypatch.setattr(selective, "_leaf_rag", fake_leaf_rag)
    monkeypatch.setattr(selective, "_leaf_generate", fake_leaf_generate)


def test_no_prior_regenerates_every_leaf(monkeypatch):
    leaves = build_leaves()
    current = {leaf.key: [f"{leaf.key}-r1"] for leaf in leaves}
    gen_calls: list[str] = []
    _install_fakes(monkeypatch, current, gen_calls)

    res = build_leaf_components(
        llm=object(), prior_payload=None, country_name="Sudan",
        period_label="2026", aggregated=None, cache_key="k", rag_filters=None,
    )
    assert set(gen_calls) == {leaf.key for leaf in leaves}
    assert len(res.regenerated_keys) == len(leaves)
    assert res.reused_keys == []
    # evidence_index seeded from the probe for every leaf.
    assert res.evidence_index["sectors.health"] == ["sectors.health-r1"]


def test_selective_reuses_unchanged_and_regenerates_only_changed(monkeypatch):
    leaves = build_leaves()
    baseline = {leaf.key: [f"{leaf.key}-r1"] for leaf in leaves}
    prior = SituationAnalysisPayload(evidence_index=baseline).model_dump(mode="json")

    # Current probe identical to baseline EXCEPT one sector gained a report.
    current = dict(baseline)
    current["sectors.health"] = ["sectors.health-r1", "sectors.health-r2"]
    gen_calls: list[str] = []
    _install_fakes(monkeypatch, current, gen_calls)

    res = build_leaf_components(
        llm=object(), prior_payload=prior, country_name="Sudan",
        period_label="2026", aggregated=None, cache_key="k", rag_filters=None,
    )
    assert gen_calls == ["sectors.health"]  # only the changed leaf ran the LLM
    assert res.regenerated_keys == ["sectors.health"]
    assert len(res.reused_keys) == len(leaves) - 1
    # Changed leaf's fingerprint updated; unchanged leaves keep their baseline.
    assert set(res.evidence_index["sectors.health"]) == {"sectors.health-r1", "sectors.health-r2"}
    assert res.evidence_index["displacement"] == ["displacement-r1"]
    assert res.any_regenerated is True


def test_summary_evidence_is_numbered_and_carries_report_ids():
    # The fan-in evidence must be a numbered [Rn] list drawn from the sections'
    # resolved contributing_sources, index-aligned to a report-id list, so the
    # summary can cite [Rn] and we resolve per-line provenance back (#1067 review).
    from clear_pipeline.defs.situation.schemas import ContextRisks, Sectors

    res = LeafBuildResult(
        context_risks=ContextRisks(
            security=RiskDomain(
                bullets=["armed clashes in Kassala"],
                contributing_sources={"r-sec": ["armed clashes in Kassala"]},
            ),
        ),
        hazards_and_vulnerabilities=HazardsAndVulnerabilities(),
        displacement=DisplacementNarrative(),
        humanitarian_access=HumanitarianAccess(),
        sectors=Sectors(
            health=SectorAnalysis(contributing_sources={"r-health": ["cholera cases rising"]}),
        ),
        scenarios=Scenarios(),
        evidence_index={},
    )
    formatted, hit_report_ids = selective._summary_evidence(res)
    assert "[R1] armed clashes in Kassala" in formatted
    assert "cholera cases rising" in formatted
    assert hit_report_ids == ["r-sec", "r-health"]


def test_failed_leaf_is_not_fingerprinted_and_keeps_prior(monkeypatch):
    from clear_pipeline.defs.situation.schemas import Sectors

    leaves = build_leaves()
    baseline = {leaf.key: [f"{leaf.key}-r1"] for leaf in leaves}
    prior_obj = SituationAnalysisPayload(evidence_index=baseline)
    prior_obj.sectors = Sectors(health=SectorAnalysis(impact=["prior health impact"]))
    prior = prior_obj.model_dump(mode="json")

    # sectors.health evidence changed → stale → regenerate; make that regen fail
    # (empty component over non-empty evidence).
    current = dict(baseline)
    current["sectors.health"] = ["sectors.health-r2"]
    gen_calls: list[str] = []
    _install_fakes(monkeypatch, current, gen_calls, empty_for={"sectors.health"})

    res = build_leaf_components(
        llm=object(), prior_payload=prior, country_name="Sudan",
        period_label="2026", aggregated=None, cache_key="k", rag_filters=None,
    )
    assert "sectors.health" in gen_calls              # it was attempted
    assert "sectors.health" in res.regenerated_keys
    assert "sectors.health" not in res.evidence_index  # NOT fingerprinted → retries next run
    assert res.sectors.health.impact == ["prior health impact"]  # prior kept, not blanked


def test_selective_all_unchanged_regenerates_nothing(monkeypatch):
    leaves = build_leaves()
    baseline = {leaf.key: [f"{leaf.key}-r1"] for leaf in leaves}
    prior = SituationAnalysisPayload(evidence_index=baseline).model_dump(mode="json")
    gen_calls: list[str] = []
    _install_fakes(monkeypatch, dict(baseline), gen_calls)

    res = build_leaf_components(
        llm=object(), prior_payload=prior, country_name="Sudan",
        period_label="2026", aggregated=None, cache_key="k", rag_filters=None,
    )
    assert gen_calls == []  # nothing changed → no LLM calls at all
    assert res.regenerated_keys == []
    assert res.any_regenerated is False
    assert len(res.reused_keys) == len(leaves)
