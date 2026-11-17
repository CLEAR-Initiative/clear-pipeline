"""The per-node schema registry loader (ADR-0015). Pure."""

from clear_pipeline.defs.analysis import schema_registry as sr
from clear_pipeline.defs.analysis.output_schemas import ARCHETYPES, SeverityNeeds
from clear_pipeline.defs.analysis.taxonomy import get_taxonomy


def test_registry_has_one_entry_per_taxonomy_node():
    assert len(sr.registry()) == len(get_taxonomy()) == 695


def test_every_entry_resolves_to_a_real_archetype():
    for e in sr.registry().values():
        assert e.archetype in ARCHETYPES
        assert e.model in ARCHETYPES.values()


def test_v5_contract_nodes_are_pinned_to_their_paths():
    health = sr.entry_for("sectors/health")
    assert (health.archetype, health.v5_path, health.pinned) == ("SeverityNeeds", "sectors.health", True)
    assert sr.model_for("sectors/health") is SeverityNeeds

    sec = sr.entry_for("pillars_1d/context/security")
    assert sec.v5_path == "context_risks.security"
    assert sec.pinned is True


def test_unknown_key_falls_back_to_described_without_crashing():
    e = sr.entry_for("not/a/real/node")
    assert e.archetype == "Described"
    assert e.pinned is False
