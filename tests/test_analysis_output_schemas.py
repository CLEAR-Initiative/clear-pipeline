"""Archetype models + L1 reconciliation (ADR-0015, slice 1). Pure — no Dagster/LLM."""

import pytest

from clear_pipeline.defs.analysis import reconciliation as rec
from clear_pipeline.defs.analysis.output_schemas import (
    ARCHETYPES,
    NodeEnvelope,
    Section,
)
from clear_pipeline.defs.analysis.taxonomy import node_by_key


def test_seven_archetypes_registered():
    assert set(ARCHETYPES) == {
        "Figures", "Drivers", "SeverityNeeds", "Distribution",
        "StatusGaps", "Described", "Section",
    }


def test_envelope_validates_its_archetype_data():
    env = NodeEnvelope(
        node_key="sectors/health", name="Health", task="Sectors", level=1,
        archetype="SeverityNeeds",
        data={"severity": "high", "top_needs": ["trauma care"], "findings": []},
    )
    parsed = env.validate_data()
    assert parsed.severity == "high"
    assert parsed.top_needs == ["trauma care"]


def test_envelope_rejects_unknown_archetype():
    env = NodeEnvelope(node_key="x", archetype="Nope", data={})
    with pytest.raises(ValueError, match="unknown archetype"):
        env.validate_data()


def test_section_is_recursive():
    leaf = NodeEnvelope(node_key="a/b", archetype="Described", data={"findings": []})
    section = Section(overview="o", subsections={"a/b": leaf})
    assert section.subsections["a/b"].node_key == "a/b"


class TestL1Reconciliation:
    def test_all_22_l1_resolve(self):
        recs = rec.all_l1_reconciliations()
        assert len(recs) == 22
        # every archetype assigned is a real one
        assert all(r.archetype in ARCHETYPES for r in recs)

    def test_sectors_map_to_sector_paths(self):
        health = node_by_key("sectors/health")
        r = rec.reconcile_l1(health)
        assert r.archetype == "SeverityNeeds"
        assert r.v5_path == "sectors.health"
        assert r.is_v5

    def test_overlapping_l1s_keep_their_v5_paths(self):
        paths = {r.node.name: r.v5_path for r in rec.all_l1_reconciliations()}
        assert paths["Displacement"] == "displacement"
        assert paths["Context"] == "context_risks"
        assert paths["Shock-Event"] == "hazards_and_vulnerabilities"
        assert paths["Humanitarian Access"] == "humanitarian_access"

    def test_new_only_l1s_have_no_v5_path(self):
        paths = {r.node.name: r.v5_path for r in rec.all_l1_reconciliations()}
        for name in ("Casualties", "Information And Communication", "Operational Capacity",
                     "Humanitarian Conditions", "Impact", "Priority Needs"):
            assert paths[name] is None, name

    def test_context_9_domains_map_1to1(self):
        domains = rec.context_domain_nodes()
        assert len(domains) == 9
        # every taxonomy Context L2 name has a v5 key
        for n in domains:
            assert n.name in rec.CONTEXT_DOMAIN_V5_KEY, n.name
        # 9 distinct v5 keys
        assert len({rec.CONTEXT_DOMAIN_V5_KEY[n.name] for n in domains}) == 9

    def test_reconcile_rejects_non_l1(self):
        l2 = next(n for n in rec.context_domain_nodes())
        with pytest.raises(ValueError, match="expects an L1"):
            rec.reconcile_l1(l2)
