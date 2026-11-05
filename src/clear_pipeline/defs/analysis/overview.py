"""Cross-section overview synthesis (doc §3 — ``analysis_overview`` JOIN).

Turns the per-node descriptive artifacts into one cross-cutting situation
overview — the synthesis that becomes the published payload's ``overview`` (and
the v5 ``ai_summary``). One bounded ``run_node`` call over the section findings;
no fresh RAG (it synthesises from what the nodes already found, doc §6).

``build_overview_prompt`` is pure (unit-testable); ``synthesise_overview`` runs
the bounded agent.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from .profiles import Caps
from .runtime import NodeOutcome, run_node

_SYSTEM = (
    "You are a humanitarian analyst writing a concise cross-cutting situation "
    "overview from section findings that are already grounded in evidence. "
    "Synthesise — do not repeat every bullet; surface the throughlines, the most "
    "severe dynamics, and how sections connect. Keep it tight and factual."
)

_DEFAULT_CAPS = Caps(max_turns=1, max_tokens=1200)


class OverviewLLM(BaseModel):
    overview: str = Field(default="", description="Concise cross-cutting situation overview.")
    completeness: float = Field(default=0.0, ge=0.0, le=1.0)


def build_overview_prompt(node_artifacts: dict[str, dict]) -> str:
    """Render the section findings into the synthesis user prompt. Nodes with no
    findings are skipped (nothing to synthesise from)."""
    blocks: list[str] = []
    for art in node_artifacts.values():
        node = art.get("node", {})
        desc = art.get("descriptive", {})
        findings = desc.get("findings", []) or []
        if not findings:
            continue
        bullets = "\n".join(f"- {f.get('description')}" for f in findings if f.get("description"))
        if bullets:
            blocks.append(f"### {node.get('name', '?')}\n{bullets}")
    body = "\n\n".join(blocks) if blocks else "(no section findings available)"
    return (
        "SECTION FINDINGS:\n" + body + "\n\n"
        "Write the cross-cutting situation overview in `overview`."
    )


def synthesise_overview(
    *,
    provider,
    node_artifacts: dict[str, dict],
    budget=None,
    caps: Caps | None = None,
) -> NodeOutcome:
    """Run the bounded overview synthesis over the node artifacts."""
    user = build_overview_prompt(node_artifacts)
    return run_node(
        provider=provider,
        system=_SYSTEM,
        build_user=lambda _evidence: user,
        schema=OverviewLLM,
        caps=caps or _DEFAULT_CAPS,
        budget=budget,
    )
