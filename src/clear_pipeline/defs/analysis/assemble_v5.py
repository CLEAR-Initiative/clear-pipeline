"""Assemble L1 node objects into the v5-superset ``analyses.data`` (ADR-0015).

The overlapping L1 nodes publish to the SAME ``analyses.data`` paths **and field
shapes** clear-mvp's ``mappers/situation-analysis.ts`` reads today (the v5
contract) — so the frontend is unaffected (no regression). New-only L1s are
additive under ``data.taxonomy.<task>.<node_key>``.

Two things make this backward-compatible:

- **Full skeleton, always.** Every overlapping v5 field is emitted with a safe
  empty default, so a node that failed/was skipped leaves an *empty* field rather
  than a *missing* key. (The frontend uses optional chaining, but a present-but-
  empty field also keeps trend/diff views stable.)
- **Exact field names.** The frontend reads ``context_risks.<d>.bullets`` (a
  string list), ``hazards_and_vulnerabilities.hazards[]``, ``humanitarian_access
  .barriers[]``, ``displacement.push_factors[].source_report_ids`` — NOT the
  archetype's own ``findings`` / ``source_ids``. The assemblers translate.

``tests/test_v5_contract.py`` pins the names/shapes. Pure: takes already-generated
:class:`NodeEnvelope`s, returns a dict. No I/O.
"""

from __future__ import annotations

from typing import Any

from clear_pipeline.defs.analysis.output_schemas import NodeEnvelope
from clear_pipeline.defs.analysis.reconciliation import (
    CONTEXT_DOMAIN_V5_KEY,
    SECTOR_V5_KEY,
    reconcile_l1,
)
from clear_pipeline.defs.analysis.taxonomy import l1_nodes


def _subsections(env: NodeEnvelope) -> dict[str, dict]:
    """A Section node's children, keyed by child node_key (each a NodeEnvelope dict)."""
    return (env.data or {}).get("subsections", {}) or {}


def _child_by_name(env: NodeEnvelope, name: str) -> dict | None:
    for child in _subsections(env).values():
        if child.get("name") == name:
            return child
    return None


def _driver_text(f: dict) -> str:
    """One Drivers item → a v5 bullet string (``factor — description``)."""
    factor = (f.get("factor") or "").strip()
    desc = (f.get("description") or "").strip()
    if factor and desc:
        return f"{factor} — {desc}"
    return factor or desc


def _described(items: list[dict] | None, text_keys: tuple[str, ...] = ("description",)) -> list[dict]:
    """A v5 ``RawDescribed`` list: ``[{description, source_report_ids}]``. The
    archetype item's ``source_ids`` are already resolved report ids (the
    structured citation pass), surfaced here under the v5 field name."""
    out: list[dict] = []
    for i in items or []:
        text = ""
        for k in text_keys:
            v = (i.get(k) or "").strip()
            if v:
                text = v
                break
        if not text:
            continue
        out.append({
            "description": text,
            "source_report_ids": list(i.get("source_ids") or i.get("source_report_ids") or []),
        })
    return out


def _driver_described(items: list[dict] | None) -> list[dict]:
    """A Drivers ``factors`` list → v5 ``RawDescribed`` list."""
    out: list[dict] = []
    for f in items or []:
        text = _driver_text(f)
        if text:
            out.append({"description": text, "source_report_ids": list(f.get("source_ids") or [])})
    return out


# ── per-field assemblers — each returns the exact v5 shape the frontend reads ──

def _assemble_sector(env: NodeEnvelope) -> dict[str, Any]:
    """SeverityNeeds → v5 ``sectors.<key>`` (``severity`` + ``top_needs`` — the
    fields ``mapSectors`` reads)."""
    d = env.data or {}
    return {
        "severity": d.get("severity"),
        "top_needs": list(d.get("top_needs") or []),
        "source_report_ids": list(env.source_report_ids),
    }


def _assemble_context(env: NodeEnvelope) -> dict[str, dict]:
    """Context Section → ``context_risks`` keyed by the 9 v5 domain keys, each a
    ``{bullets: [str], source_report_ids}`` (``mapContextRisks`` reads
    ``.bullets``)."""
    out: dict[str, dict] = {}
    for child in _subsections(env).values():
        v5_key = CONTEXT_DOMAIN_V5_KEY.get(child.get("name", ""))
        if not v5_key:
            continue
        findings = (child.get("data") or {}).get("findings") or []
        bullets = [(f.get("description") or "").strip() for f in findings if (f.get("description") or "").strip()]
        out[v5_key] = {"bullets": bullets, "source_report_ids": list(child.get("source_report_ids") or [])}
    return out


def _assemble_displacement(env: NodeEnvelope) -> dict[str, Any]:
    """Displacement Section → the v5 ``displacement`` fields ``mapDisplacement``
    reads (``push_factors`` / ``return_intention`` as ``RawDescribed`` lists)."""
    push = _child_by_name(env, "Push factors")
    intent = _child_by_name(env, "Intentions")
    return {
        "push_factors": _driver_described((push.get("data") or {}).get("factors")) if push else [],
        "return_intention": (
            _described((intent.get("data") or {}).get("options"), ("description", "option"))
            if intent else []
        ),
        "summary": (env.summary or "").strip() or None,
    }


def _assemble_hazards(env: NodeEnvelope) -> dict[str, Any]:
    """Shock-Event Drivers → the v5 ``hazards_and_vulnerabilities`` OBJECT the
    frontend reads (``.hazards`` list, not a bare list)."""
    return {
        "hazards": _driver_described((env.data or {}).get("factors")),
        "vulnerabilities": [],
        "hazards_summary": (env.summary or "").strip() or None,
        "aggravating_factors": [],
        "mitigating_factors": [],
    }


def _assemble_access(env: NodeEnvelope) -> dict[str, Any]:
    """Humanitarian Access StatusGaps → the v5 ``humanitarian_access`` shape
    (``barriers`` / ``constraints`` / ``items`` / ``summary``)."""
    d = env.data or {}
    return {
        "barriers": _described(d.get("gaps")),
        "constraints": [],
        "items": [],
        "summary": (env.summary or "").strip() or None,
    }


# Field-path → assembler for the non-sector, non-context overlapping L1s.
_FIELD_ASSEMBLER = {
    "displacement": _assemble_displacement,
    "hazards_and_vulnerabilities": _assemble_hazards,
    "humanitarian_access": _assemble_access,
}


def _skeleton() -> dict[str, Any]:
    """The full v5 overlapping-field shape, empty — so every field is always
    present even when its node didn't generate (backward compatibility)."""
    return {
        "sectors": {
            k: {"severity": None, "top_needs": [], "source_report_ids": []}
            for k in SECTOR_V5_KEY.values()
        },
        "context_risks": {},
        "displacement": {"push_factors": [], "return_intention": [], "summary": None},
        "hazards_and_vulnerabilities": {
            "hazards": [], "vulnerabilities": [], "hazards_summary": None,
            "aggravating_factors": [], "mitigating_factors": [],
        },
        "humanitarian_access": {"barriers": [], "constraints": [], "items": [], "summary": None},
        # Forward-looking + temporal components the overview overlays (and the
        # taxonomy DAG itself doesn't produce). Present-but-empty keeps the
        # frontend's Forecast / "what changed" paths stable on a failed generation.
        "scenarios": {
            "most_likely": "", "best_case": "", "worst_case": "", "description": "",
            "source_report_ids": [],
        },
        "changes": {
            "basis": None, "compared_to": None, "compared_to_window_start": None,
            "compared_to_label": None, "notes": {},
        },
    }


def assemble_v5(l1_envelopes: dict[str, NodeEnvelope]) -> dict[str, Any]:
    """Assemble the generated L1 node objects into the v5-superset payload.

    ``l1_envelopes`` maps L1 ``node_key`` → its :class:`NodeEnvelope`. The full v5
    skeleton is always returned; present nodes overlay their field, overlapping L1s
    publish to their v5 paths, and new-only L1s land under
    ``data.taxonomy.<task>.<node_key>``."""
    data = _skeleton()
    taxonomy: dict[str, dict] = {}

    for node in l1_nodes():
        env = l1_envelopes.get(node.key)
        if env is None:
            continue
        rec = reconcile_l1(node)
        if rec.v5_path is None:
            task_slug = node.key.split("/", 1)[0]
            taxonomy.setdefault(task_slug, {})[node.key] = env.model_dump()
        elif rec.v5_path.startswith("sectors."):
            data["sectors"][rec.v5_path.split(".", 1)[1]] = _assemble_sector(env)
        elif rec.v5_path == "context_risks":
            data["context_risks"].update(_assemble_context(env))
        else:
            data[rec.v5_path] = _FIELD_ASSEMBLER[rec.v5_path](env)

    if taxonomy:
        data["taxonomy"] = taxonomy
    return data
