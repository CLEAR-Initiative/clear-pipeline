"""Load the per-node output-schema registry (ADR-0015).

Reads the committed ``data/node_output_schemas.json`` (produced by
``scripts/build_node_schemas.py``) and resolves a ``node_key`` to its archetype,
its Pydantic model, and — for the v5-contract nodes — its publish path. The agents
call :func:`model_for` to get the structured output schema to fill for a node.

Pure (reads the packaged JSON once, cached); no Dagster / LLM.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files

from pydantic import BaseModel

from clear_pipeline.defs.analysis.output_schemas import ARCHETYPES

_PACKAGE = "clear_pipeline.defs.analysis"
_DATA_FILE = "node_output_schemas.json"


@dataclass(frozen=True)
class SchemaEntry:
    node_key: str
    archetype: str
    v5_path: str | None
    pinned: bool

    @property
    def model(self) -> type[BaseModel]:
        m = ARCHETYPES.get(self.archetype)
        if m is None:
            raise ValueError(f"registry entry {self.node_key} has unknown archetype {self.archetype!r}")
        return m


@lru_cache(maxsize=1)
def _load() -> dict[str, SchemaEntry]:
    raw = (files(_PACKAGE) / "data" / _DATA_FILE).read_text(encoding="utf-8")
    doc = json.loads(raw)
    out: dict[str, SchemaEntry] = {}
    for key, e in doc["nodes"].items():
        arche = e["archetype"]
        if arche not in ARCHETYPES:
            raise ValueError(f"registry node {key}: unknown archetype {arche!r}")
        out[key] = SchemaEntry(
            node_key=key, archetype=arche, v5_path=e.get("v5_path"), pinned=bool(e.get("pinned")),
        )
    return out


def registry() -> dict[str, SchemaEntry]:
    """The full node_key → :class:`SchemaEntry` map."""
    return _load()


def entry_for(node_key: str) -> SchemaEntry:
    """The schema entry for a node. Unknown keys default to the ``Described``
    fallback (a taxonomy/registry drift is caught by the build script's count
    check, not here — generation must never crash on a missing entry)."""
    e = _load().get(node_key)
    if e is not None:
        return e
    return SchemaEntry(node_key=node_key, archetype="Described", v5_path=None, pinned=False)


def archetype_for(node_key: str) -> str:
    return entry_for(node_key).archetype


def model_for(node_key: str) -> type[BaseModel]:
    """The Pydantic archetype model the agent fills for this node."""
    return entry_for(node_key).model
