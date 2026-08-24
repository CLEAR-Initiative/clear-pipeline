"""Prose projection of a situation-analysis payload for translation.

The situation `data` blob interleaves human-readable prose (summary text,
risk/hazard bullets, sector needs) with things that must NEVER be translated:
numbers, the sector `severity`/`evidence_scope` enums, `source_report_ids`,
`contributing_sources` maps, information-coverage ratings, report titles/urls.

`extract_situation_prose` walks the payload and returns a nested dict carrying
ONLY the prose leaves, in the SAME shape/positions as the canonical blob. The
translator recurses that shape translating string leaves; clear-api then
deep-merges the translated overlay back over the canonical `data`, so the
non-prose fields it omitted stay authoritative.

The set of fields projected here is the contract for
`HASH_FIELDS["situationAnalysis"]` in `translation_hash.py` — the top-level
keys must match. See also the mirror note in
`clear-api/src/utils/translation-hash.ts`.
"""

from typing import Any


# Top-level payload components that carry prose. Mirrors the keys produced
# below and HASH_FIELDS["situationAnalysis"] in translation_hash.py.
PROSE_COMPONENTS = (
    "ai_summary",
    "context_risks",
    "hazards_and_vulnerabilities",
    "displacement",
    "sectors",
    "changes",
)


def _bullet_descriptions(items: Any) -> list[dict[str, str]]:
    """Project a SourcedBullet list to `[{description}]`, dropping the
    per-bullet `source_report_ids` (an id list, never translated)."""
    out: list[dict[str, str]] = []
    for b in items or []:
        if isinstance(b, dict):
            out.append({"description": b.get("description", "")})
    return out


def extract_situation_prose(data: dict[str, Any]) -> dict[str, Any]:
    """Return the prose-only projection of a situation-analysis `data` blob.

    Every top-level key in `PROSE_COMPONENTS` is always present so the shape is
    stable regardless of which components a run populated — an empty component
    projects to its empty container."""
    data = data or {}

    ai = data.get("ai_summary") or {}
    ai_summary = {"text": ai.get("text", "")}

    context_risks = {
        domain: {"bullets": (block or {}).get("bullets", []) or []}
        for domain, block in (data.get("context_risks") or {}).items()
    }

    hv = data.get("hazards_and_vulnerabilities") or {}
    hazards_and_vulnerabilities = {
        "hazards": _bullet_descriptions(hv.get("hazards")),
        "vulnerabilities": _bullet_descriptions(hv.get("vulnerabilities")),
    }

    disp = data.get("displacement") or {}
    displacement = {
        "push_factors": _bullet_descriptions(disp.get("push_factors")),
        "return_intention": _bullet_descriptions(disp.get("return_intention")),
    }

    sectors: dict[str, Any] = {}
    for sector, block in (data.get("sectors") or {}).items():
        block = block or {}
        sectors[sector] = {
            "impact": block.get("impact", []) or [],
            "humanitarian_conditions": block.get("humanitarian_conditions", []) or [],
            "vulnerable_sections": block.get("vulnerable_sections", []) or [],
            "top_needs": block.get("top_needs", []) or [],
            "priority_interventions": block.get("priority_interventions", []) or [],
            # Only the human-readable area label; rating_out_of_10 / report_count
            # are numbers and stay canonical.
            "information_coverage": [
                {"area": (ic or {}).get("area", "")}
                for ic in (block.get("information_coverage") or [])
            ],
        }

    # `changes.notes` values are prose; their keys are section paths (preserved
    # verbatim by the translator). basis / compared_to* are labels/dates → omit.
    changes = data.get("changes") or {}
    changes_out = {"notes": changes.get("notes", {}) or {}}

    return {
        "ai_summary": ai_summary,
        "context_risks": context_risks,
        "hazards_and_vulnerabilities": hazards_and_vulnerabilities,
        "displacement": displacement,
        "sectors": sectors,
        "changes": changes_out,
    }
