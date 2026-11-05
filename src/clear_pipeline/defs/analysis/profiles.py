"""``SectionProfile`` — declarative per-section specialisation (doc §4).

The analysis module runs **one** bounded-agent runtime; sections differ only in
configuration, not code. A ``SectionProfile`` carries that configuration —
tools, retrieval mode, prompt id, output schema, effort, **analysis depth**, and
caps — so adding or tuning a section is a config change, never a new agent.

PR-1 defines the profile shape + the universal defaults (**depth = L1**).
``output_schema`` and the concrete tool wiring are filled in when the runtime
and descriptive assets land (PR-2/PR-3); here they are declarative fields.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Literal

from .taxonomy import Depth

# Universal default analysis depth. Only L1 is analysed unless a run (or a
# section's profile) opts deeper — keeps cost bounded for the common case;
# L2/L3 are on-demand (doc §6).
DEFAULT_DEPTH: Depth = "L1"

Retrieval = Literal["kb_only", "web_first", "hybrid"]
Effort = Literal["single_call", "escalate_on_trigger", "always_bounded"]
Tool = Literal["rag", "web", "numbers"]


@dataclass(frozen=True)
class Caps:
    """Hard per-node bounds (doc §11). The per-*run* $ cap is enforced
    separately by the run budget (``providers/guardrails.RunBudget``), wired in
    PR-2; these are the per-node turn/token ceilings."""

    max_turns: int = 1
    max_tokens: int = 1536
    max_usd: float | None = None


@dataclass(frozen=True)
class SectionProfile:
    """How one section specialises the shared runtime (doc §4).

    ``output_schema`` is the versioned Pydantic output type; left ``None`` in
    PR-1 and set when the descriptive schema lands.
    """

    name: str
    tools: tuple[Tool, ...] = ("rag",)
    retrieval: Retrieval = "kb_only"
    prompt_id: str = "descriptive"
    output_schema: type | None = None
    effort: Effort = "single_call"
    depth: Depth = DEFAULT_DEPTH
    caps: Caps = field(default_factory=Caps)

    def at_depth(self, depth: Depth) -> SectionProfile:
        """A copy of this profile at a different analysis depth."""
        return replace(self, depth=depth)


def default_profile(name: str = "descriptive", *, depth: Depth = DEFAULT_DEPTH) -> SectionProfile:
    """The baseline descriptive profile: single RAG call, KB-only, depth L1.

    The common case — a node answered in one structured RAG call that
    self-rates its completeness; escalation / web / deeper depth are opt-in.
    """
    return SectionProfile(
        name=name,
        tools=("rag",),
        retrieval="kb_only",
        prompt_id="descriptive",
        effort="single_call",
        depth=depth,
    )
