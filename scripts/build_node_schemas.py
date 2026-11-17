"""Regenerate the per-node output-schema registry (ADR-0015).

Writes ``src/clear_pipeline/defs/analysis/data/node_output_schemas.json`` — one
entry per taxonomy node (``node_key → {archetype, v5_path?, pinned}``) that the
agents resolve to a Pydantic archetype at generation time.

- The v5-contract nodes (the reconciliation table + the 9 Context domains) are
  **pinned**: their archetype + publish path are fixed to the v5 shape and this
  script never reclassifies them.
- Every other node is assigned an archetype from its analytical question (keyword
  heuristic — a deterministic first pass; refine with an LLM-assisted review on
  the diff, per ADR-0015).

Run after any taxonomy change:  uv run python scripts/build_node_schemas.py
Deterministic: same taxonomy → byte-identical output.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from clear_pipeline.defs.analysis import reconciliation as rec
from clear_pipeline.defs.analysis.taxonomy import TaxonomyNode, get_taxonomy

_OUT = (
    Path(__file__).resolve().parent.parent
    / "src/clear_pipeline/defs/analysis/data/node_output_schemas.json"
)

# Keyword → archetype, tried in order (first match wins). Lower-cased substring
# match on the analytical question. The order encodes precedence: a question that
# asks "how many … and what drives" is a Figures question first.
_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("Figures", ("how many", "what numbers", "scale and profile", "what is the scale",
                 "how much", "number of", "figures", "casualt")),
    ("Distribution", ("intention", "preference", "what share", "plan to", "prioriti",
                      "converge", "how do those pr")),
    ("Drivers", ("what drivers", "which drivers", "push", "pull", "factor",
                 "driving", "aggravating", "what shocks", "what hazards")),
    ("SeverityNeeds", ("how severe", "what needs", "unmet", "severity", "most urgent",
                       "how adequate", "how available")),
    ("StatusGaps", ("barrier", "access", "parity", "capacit", "how functional",
                    "how reliable", "how accessible", "constrain", "gaps in",
                    "where do gaps", "how effective")),
]


def _classify(question: str) -> str:
    q = (question or "").lower()
    for archetype, kws in _RULES:
        if any(k in q for k in kws):
            return archetype
    return "Described"  # universal fallback


def _pinned_entry(node: TaxonomyNode) -> dict | None:
    """Fixed v5-contract entry for a node, or None if not pinned."""
    if node.level == 1:
        r = rec.reconcile_l1(node)
        if r.is_v5:
            return {"archetype": r.archetype, "v5_path": r.v5_path, "pinned": True}
        return None
    # The 9 Context L2 domains ARE the v5 context_risks domains → pinned.
    if node.level == 2 and node.task == "Pillars 1D" and node.l1 == "Context":
        v5_key = rec.CONTEXT_DOMAIN_V5_KEY.get(node.name)
        if v5_key:
            return {"archetype": "Described", "v5_path": f"context_risks.{v5_key}", "pinned": True}
    return None


def _load_existing() -> dict[str, dict]:
    """The current on-disk registry (``{node_key: entry}``), or empty on first run."""
    try:
        return json.loads(_OUT.read_text(encoding="utf-8")).get("nodes", {})
    except Exception:  # noqa: BLE001 — missing/unreadable → treat as a first build
        return {}


def build() -> dict[str, dict]:
    nodes = get_taxonomy()
    existing = _load_existing()
    registry: dict[str, dict] = {}
    for n in nodes:
        pinned = _pinned_entry(n)
        if pinned:
            registry[n.key] = pinned  # v5-contract pin — always reasserted
            continue
        # Preserve a maintainer's reviewed pin for a node still in the taxonomy,
        # rather than clobbering it with the keyword classifier each regeneration.
        prev = existing.get(n.key)
        if prev and prev.get("pinned"):
            registry[n.key] = prev
            continue
        registry[n.key] = {"archetype": _classify(n.question), "pinned": False}
    # Validate: one entry per node, no dupes.
    if len(registry) != len(nodes):
        raise SystemExit(f"registry size {len(registry)} != taxonomy {len(nodes)}")
    return registry


def main() -> None:
    registry = build()
    # Stable, sorted, trailing newline → byte-stable diffs.
    _OUT.write_text(
        json.dumps({"nodes": dict(sorted(registry.items()))}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    pinned = sum(1 for e in registry.values() if e.get("pinned"))
    by_arch: dict[str, int] = {}
    for e in registry.values():
        by_arch[e["archetype"]] = by_arch.get(e["archetype"], 0) + 1
    print(f"wrote {len(registry)} nodes ({pinned} pinned) to {_OUT.name}")
    print("by archetype:", dict(sorted(by_arch.items(), key=lambda kv: -kv[1])))
    # Guard: the heuristic drifted if 'Described' swallows everything.
    if by_arch.get("Described", 0) > 0.6 * len(registry):
        print(re.sub(r"\s+", " ", (
            "WARNING: >60% classified Described — the keyword rules likely need "
            "tuning or an LLM-assisted pass (ADR-0015)."
        )))


if __name__ == "__main__":
    main()
