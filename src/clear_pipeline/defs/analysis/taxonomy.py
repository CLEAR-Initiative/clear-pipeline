"""Humanitarian analysis taxonomy — Task → Level 1 → Level 2 → Level 3.

Source of truth for the analysis module's *sections* and their per-node
**analytical questions**. Loaded from the committed ``data/taxonomy.json``
(derived from ``docs/202610-Humanitarian_Taxonomy_Level1-2-3.xlsx``): 3 Tasks,
22 L1, 97 L2, 576 L3 — every node with a Definition and an Analytical Question.

The analysis module fans out over the nodes **at the configured depth**
(``L1`` by default — one analyst call per L1; ``L2``/``L3`` on request). The
node's Analytical Question is the analysis question the analyst must answer and
the seed of its RAG query. This taxonomy is the superset/reorg that supersedes
the legacy 22-row first-level analysis-questions CSV.

Pure (no I/O beyond reading the packaged JSON once) so it is unit-testable
without Dagster / a DB. Regenerate ``data/taxonomy.json`` from the xlsx if the
taxonomy changes; the loader validates the declared counts on load.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from typing import Literal

Depth = Literal["L1", "L2", "L3"]

_DEPTH_TO_LEVEL: dict[Depth, int] = {"L1": 1, "L2": 2, "L3": 3}
_LEVEL_TO_DEPTH: dict[int, Depth] = {1: "L1", 2: "L2", 3: "L3"}

# The data file ships as a resource of the analysis package (``data/`` is a
# plain subdirectory, not an importable package — so traverse from the package).
_PACKAGE = "clear_pipeline.defs.analysis"
_DATA_FILE = "taxonomy.json"


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


@dataclass(frozen=True)
class TaxonomyNode:
    """One node in the taxonomy at a given level.

    ``l1``/``l2``/``l3`` give the full path (``l2``/``l3`` None above that
    level); ``name`` is this node's own label. ``question`` is the Analytical
    Question — the analysis question the analyst answers for this node.
    """

    task: str
    level: int
    name: str
    l1: str
    l2: str | None
    l3: str | None
    definition: str
    question: str

    @property
    def depth(self) -> Depth:
        return _LEVEL_TO_DEPTH[self.level]

    @property
    def path(self) -> tuple[str, ...]:
        """Task → … → this node's label (length = level + 1)."""
        return tuple(x for x in (self.task, self.l1, self.l2, self.l3) if x)

    @property
    def key(self) -> str:
        """Stable slug id, used to content-address this node's artifacts."""
        return "/".join(_slugify(p) for p in self.path)

    @property
    def parent_key(self) -> str | None:
        """Slug of the parent node (the node one level up); None for L1."""
        if self.level == 1:
            return None
        return "/".join(_slugify(p) for p in self.path[:-1])


def _node_from_record(rec: dict) -> TaxonomyNode:
    return TaxonomyNode(
        task=rec["task"],
        level=int(rec["level"]),
        name=rec["name"],
        l1=rec["l1"],
        l2=rec.get("l2"),
        l3=rec.get("l3"),
        definition=rec.get("definition", ""),
        question=rec.get("question", ""),
    )


@lru_cache(maxsize=1)
def _load() -> tuple[TaxonomyNode, ...]:
    raw = (files(_PACKAGE) / "data" / _DATA_FILE).read_text(encoding="utf-8")
    doc = json.loads(raw)
    nodes = tuple(_node_from_record(r) for r in doc["nodes"])

    # Validate against the declared counts so a bad regeneration fails loudly
    # rather than silently dropping sections.
    counts = doc.get("counts", {})
    by_level = {lvl: sum(1 for n in nodes if n.level == lvl) for lvl in (1, 2, 3)}
    expected = {1: counts.get("l1"), 2: counts.get("l2"), 3: counts.get("l3")}
    for lvl, exp in expected.items():
        if exp is not None and by_level[lvl] != exp:
            raise ValueError(
                f"taxonomy.json level-{lvl} count mismatch: declared {exp}, found {by_level[lvl]}"
            )
    n_tasks = len({n.task for n in nodes})
    if counts.get("tasks") is not None and n_tasks != counts["tasks"]:
        raise ValueError(
            f"taxonomy.json task count mismatch: declared {counts['tasks']}, found {n_tasks}"
        )
    missing = [n.key for n in nodes if not n.question]
    if missing:
        raise ValueError(f"taxonomy.json nodes missing an analytical question: {missing[:5]}")
    return nodes


def get_taxonomy() -> tuple[TaxonomyNode, ...]:
    """All taxonomy nodes across every level (L1 + L2 + L3)."""
    return _load()


def tasks() -> tuple[str, ...]:
    """The top-level Task groupings, in first-seen order."""
    seen: dict[str, None] = {}
    for n in _load():
        seen.setdefault(n.task, None)
    return tuple(seen)


def nodes_at_depth(depth: Depth | int) -> tuple[TaxonomyNode, ...]:
    """Nodes at the given analysis depth — the fan-out set.

    ``depth`` accepts ``"L1"``/``"L2"``/``"L3"`` or ``1``/``2``/``3``.
    """
    level = depth if isinstance(depth, int) else _DEPTH_TO_LEVEL[depth]
    if level not in (1, 2, 3):
        raise ValueError(f"unsupported depth {depth!r} — expected L1/L2/L3 or 1/2/3")
    return tuple(n for n in _load() if n.level == level)


def l1_nodes() -> tuple[TaxonomyNode, ...]:
    """The 22 Level-1 nodes — the universal default fan-out (depth = L1)."""
    return nodes_at_depth(1)


def _by_key() -> dict[str, TaxonomyNode]:
    return {n.key: n for n in _load()}


def node_by_key(key: str) -> TaxonomyNode | None:
    return _by_key().get(key)


def parent_of(node: TaxonomyNode) -> TaxonomyNode | None:
    """The node one level up, or None for an L1 node."""
    pk = node.parent_key
    return _by_key().get(pk) if pk else None


def children_of(node: TaxonomyNode) -> tuple[TaxonomyNode, ...]:
    """Direct children (one level deeper) of this node."""
    prefix = node.key + "/"
    return tuple(
        n for n in _load() if n.level == node.level + 1 and n.key.startswith(prefix)
    )
