"""L1 reconciliation — which taxonomy L1 publishes to which v5 field (ADR-0015).

This is the non-regression contract: the L1 nodes that overlap v5 publish their
assembled object to the SAME ``analyses.data`` path the frontend
(clear-mvp ``mappers/situation-analysis.ts``) reads today; the new-only L1s are
additive under ``data.taxonomy.<task>.<node_key>``. The v5-field *names/shapes*
are the contract — pinned by ``tests/test_v5_contract.py``.

Pure data + lookups; no I/O.
"""

from __future__ import annotations

from clear_pipeline.defs.analysis.taxonomy import TaxonomyNode, l1_nodes, nodes_at_depth

# Taxonomy Context L2 name → v5 ``context_risks`` domain key (1:1; labels differ).
CONTEXT_DOMAIN_V5_KEY: dict[str, str] = {
    "Demographic": "demographics",
    "Economics": "economy",
    "Environment": "environment",
    "Humanitarian coordination": "humanitarian_coordination",
    "Infrastructure & technology": "infrastructure",
    "Legal & policy": "legal_policy",
    "Politics": "political",
    "Security": "security",
    "Socio-cultural": "socio_culture",
}

# Taxonomy Sector L1 name → v5 ``sectors`` key (matches payload._V5_SECTOR_KEY).
SECTOR_V5_KEY: dict[str, str] = {
    "Agriculture": "agriculture",
    "Education": "education",
    "Food Security": "food_security",
    "Health": "health",
    "Livelihoods": "livelihoods",
    "Logistics": "logistics",
    "Nutrition": "nutrition",
    "Protection": "protection",
    "Shelter": "shelter",
    "WASH": "wash",
}


# L1 node NAME → (archetype, v5_path | None). v5_path None = new-only (additive,
# publishes under data.taxonomy.<task>.<node_key>). Sector L1s are handled by
# SECTOR_V5_KEY below rather than enumerated here.
_L1_BY_NAME: dict[str, tuple[str, str | None]] = {
    # Pillars 1D — v5-overlapping
    "Displacement": ("Section", "displacement"),
    "Context": ("Section", "context_risks"),  # 9 L2 → context_risks.<domain>
    "Shock-Event": ("Drivers", "hazards_and_vulnerabilities"),
    "Humanitarian Access": ("StatusGaps", "humanitarian_access"),
    # Pillars 1D — new-only
    "Casualties": ("Figures", None),
    "Information And Communication": ("StatusGaps", None),
    "Operational Capacity": ("StatusGaps", None),
    # Pillars 2D — all new-only
    "Capacities & Response": ("StatusGaps", None),
    "Humanitarian Conditions": ("SeverityNeeds", None),
    "Impact": ("Described", None),
    "Priority Interventions": ("Distribution", None),
    "Priority Needs": ("Distribution", None),
}


class L1Reconciliation:
    """Resolved reconciliation for one L1 node."""

    __slots__ = ("node", "archetype", "v5_path")

    def __init__(self, node: TaxonomyNode, archetype: str, v5_path: str | None):
        self.node = node
        self.archetype = archetype
        self.v5_path = v5_path

    @property
    def is_v5(self) -> bool:
        return self.v5_path is not None


def reconcile_l1(node: TaxonomyNode) -> L1Reconciliation:
    """The archetype + publish path for one L1 node. Sector-task L1s map to
    ``sectors.<key>`` via SECTOR_V5_KEY; everything else via _L1_BY_NAME."""
    if node.level != 1:
        raise ValueError(f"reconcile_l1 expects an L1 node, got level {node.level}: {node.key}")
    if node.name in SECTOR_V5_KEY:
        return L1Reconciliation(node, "SeverityNeeds", f"sectors.{SECTOR_V5_KEY[node.name]}")
    arche, path = _L1_BY_NAME.get(node.name, ("Described", None))
    return L1Reconciliation(node, arche, path)


def all_l1_reconciliations() -> list[L1Reconciliation]:
    """Reconciliation for every L1 node (the 22)."""
    return [reconcile_l1(n) for n in l1_nodes()]


def context_domain_nodes() -> list[TaxonomyNode]:
    """The 9 Context L2 nodes (= the v5 context_risks domains)."""
    return [
        n for n in nodes_at_depth(2)
        if n.task == "Pillars 1D" and n.l1 == "Context"
    ]
