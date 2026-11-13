"""Published analysis payload — v6 (taxonomy-keyed) + an embedded v5 projection.

The new DAG publishes ``analyses.data`` as the full **v6** payload (keyed by the
22 L1 taxonomy nodes) with a best-effort **v5 projection** embedded under
``data["v5"]``, so legacy consumers (clear-mvp, dashboards, clear_eval) keep
reading the v5 ``SituationAnalysisPayload`` view while new ones read v6.

The projection is deliberately lossy (the hybrid decision): the 10 Sector L1s
map ~1:1 to v5 sectors; Displacement / Shock-Event / Humanitarian Access map
across; overview → ai_summary; numbers → datapoints. The single **Context** L1
can't fill v5's 9 context domains (left coarse/empty — it lives in v6), and the
**new-only** L1s (Casualties, Information & Communication, Operational Capacity,
and the whole Pillars-2D task) have no v5 home — they exist only in v6.

Pure: no I/O. The assembly asset (PR-8b) feeds it the node artifacts + numbers +
overview and upserts the result.
"""

from __future__ import annotations

from typing import Any

PAYLOAD_SCHEMA_VERSION = "v6"

# L1 taxonomy node name → v5 situation pillar field.
_V5_PILLAR_FIELD: dict[str, str] = {
    "Displacement": "displacement",
    "Shock-Event": "hazards_and_vulnerabilities",
    "Humanitarian Access": "humanitarian_access",
}

# L1 Sector node name → v5 Sectors key.
_V5_SECTOR_KEY: dict[str, str] = {
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


def build_v6_payload(
    node_artifacts: dict[str, dict],
    numbers: dict | None = None,
    overview: str = "",
) -> dict[str, Any]:
    """Assemble the v6 payload from the per-node descriptive artifacts
    (``analysis_sub`` output, keyed by ``node_key``), the numbers artifact, and
    the cross-section overview text."""
    nodes: dict[str, Any] = {}
    for key, art in node_artifacts.items():
        node = art.get("node", {})
        desc = art.get("descriptive", {})
        nodes[key] = {
            "name": node.get("name"),
            "task": node.get("task"),
            "level": node.get("level"),
            "findings": desc.get("findings", []),
            "completeness": desc.get("completeness"),
            "questions_not_answered": desc.get("questions_not_answered", []),
            "source_report_ids": desc.get("source_report_ids", []),
        }
    return {
        "schema_version": PAYLOAD_SCHEMA_VERSION,
        "taxonomy_depth": "L1",
        "overview": overview,
        "nodes": nodes,
        "numbers": numbers or {},
    }


def _v5_block(node: dict | None) -> dict[str, Any]:
    """Minimal v5-shaped block for one mapped L1 node (best-effort — findings +
    coverage + citations; v5 sub-model-specific sub-fields are left to defaults)."""
    if not node:
        return {}
    return {
        "findings": [f.get("description") for f in node.get("findings", []) if f.get("description")],
        "completeness": node.get("completeness"),
        "questions_not_answered": node.get("questions_not_answered", []),
        "source_report_ids": node.get("source_report_ids", []),
    }


def project_v5(v6: dict) -> dict[str, Any]:
    """Best-effort legacy ``SituationAnalysisPayload``-shaped view of a v6
    payload. Maps the overlapping L1 nodes onto v5 fields; new-only L1s are
    omitted and the Context domains stay coarse (see module docstring)."""
    nodes = v6.get("nodes", {}) or {}
    by_name = {n.get("name"): n for n in nodes.values()}

    sectors = {
        key: _v5_block(by_name[name])
        for name, key in _V5_SECTOR_KEY.items()
        if name in by_name
    }

    out: dict[str, Any] = {
        "schema_version": "v5",
        "ai_summary": {"summary": v6.get("overview", "")},
        "sectors": sectors,
        "context_risks": {},  # coarse — the single Context L1 lives in v6.nodes
        "datapoints": (v6.get("numbers") or {}).get("data", {}),
        "sources": sorted(
            {r for n in nodes.values() for r in (n.get("source_report_ids") or [])}
        ),
    }
    for name, field in _V5_PILLAR_FIELD.items():
        out[field] = _v5_block(by_name.get(name))
    return out


def assemble_published_data(v6: dict) -> dict[str, Any]:
    """The published ``analyses.data``: the v6 payload with the v5 projection
    embedded under ``data["v5"]`` (the hybrid payload)."""
    return {**v6, "v5": project_v5(v6)}
