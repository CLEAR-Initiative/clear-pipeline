"""Structured per-node analysis output schemas — the 7 archetypes (ADR-0015).

The taxonomy is guidance for *coverage*; a node's answer is a typed **object**,
not prose. Most of the 695 analytical questions fit one of seven output shapes
(the archetypes below). Each node carries a :class:`NodeEnvelope` (provenance +
self-rating) wrapping a typed ``data`` of one archetype. The per-node archetype
assignment lives in the regenerable ``data/node_output_schemas.json`` registry;
this module is the small, fixed set of models it resolves to.

Composition is uniform: a leaf is a ``NodeEnvelope`` with archetype data; a parent
is a ``NodeEnvelope`` whose archetype is :class:`Section` (its children keyed under
``subsections``). So joining leaves into parents and parents into the root is one
merge — the "big analysis object" is the root ``Section``.

Pure (no I/O): unit-testable without Dagster / clear-api / an LLM.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

Severity = Literal["none", "low", "medium", "high", "critical"]


# ── Shared value objects ─────────────────────────────────────────────────────
class RangeFigure(BaseModel):
    """A figure as a range (``low``/``high``) and/or a point estimate, with its
    unit and as-of date. Figures nodes CITE the aggregated ``datapoints`` bucket
    when one covers the figure (one numeric source of truth) — see ADR-0015."""

    low: float | None = None
    high: float | None = None
    point: float | None = None
    unit: str = ""
    as_of: str | None = None


class Described(BaseModel):
    """A qualitative finding with its own locations + report-id provenance."""

    description: str
    locations: list[str] = Field(default_factory=list)
    source_ids: list[str] = Field(default_factory=list)


# ── The 7 archetypes (the ``data`` shapes) ───────────────────────────────────
class FigureItem(BaseModel):
    metric: str
    value: RangeFigure
    group: str | None = None
    location: str | None = None
    source_ids: list[str] = Field(default_factory=list)


class Figures(BaseModel):
    """Counts / scale (Casualties, displacement stock, PiN)."""

    figures: list[FigureItem] = Field(default_factory=list)


class DriverItem(BaseModel):
    factor: str
    description: str
    locations: list[str] = Field(default_factory=list)
    scale_or_severity: str | None = None
    source_ids: list[str] = Field(default_factory=list)


class Drivers(BaseModel):
    """Described drivers/factors (push/pull/aggravating, Shock-Event hazards)."""

    factors: list[DriverItem] = Field(default_factory=list)


class SeverityNeeds(BaseModel):
    """Sector/condition severity + needs — the v5 ``sectors.<key>`` shape."""

    severity: Severity | None = None
    top_needs: list[str] = Field(default_factory=list)
    affected: RangeFigure | None = None
    findings: list[Described] = Field(default_factory=list)


class DistributionOption(BaseModel):
    option: str
    share: RangeFigure | None = None
    by_group: dict[str, str] | None = None
    description: str = ""
    source_ids: list[str] = Field(default_factory=list)


class Distribution(BaseModel):
    """A distribution across options (movement intentions, priority convergence)."""

    options: list[DistributionOption] = Field(default_factory=list)


class StatusGaps(BaseModel):
    """A status assessment with gaps + enablers (access, parity, capacity,
    system functionality)."""

    status: str | None = None
    gaps: list[Described] = Field(default_factory=list)
    enablers: list[Described] = Field(default_factory=list)
    comparison: str | None = None


class DescribedFindings(BaseModel):
    """The universal fallback — structured qualitative findings (Context domains,
    anything without a better fit). Richer than a bare ``list[str]``: each finding
    keeps its locations + sources."""

    findings: list[Described] = Field(default_factory=list)


class Section(BaseModel):
    """A parent node composing its children. ``subsections`` are the children's
    envelopes keyed by node_key; ``roll_up`` holds any archetype fields the parent
    itself publishes (e.g. Displacement rolls its L2 up to push_factors /
    return_intention at assembly time)."""

    overview: str = ""
    subsections: dict[str, NodeEnvelope] = Field(default_factory=dict)
    roll_up: dict[str, object] = Field(default_factory=dict)


# archetype name → model. The ONLY per-node schema models in code; the registry
# (data/node_output_schemas.json) maps every node_key to one of these.
ARCHETYPES: dict[str, type[BaseModel]] = {
    "Figures": Figures,
    "Drivers": Drivers,
    "SeverityNeeds": SeverityNeeds,
    "Distribution": Distribution,
    "StatusGaps": StatusGaps,
    "Described": DescribedFindings,
    "Section": Section,
}


class NodeEnvelope(BaseModel):
    """What every node (leaf or parent) emits: provenance + self-rating + a typed
    ``data`` of exactly one archetype (named in ``archetype``)."""

    node_key: str
    name: str = ""
    task: str = ""
    level: int = 0
    archetype: str
    summary: str = ""
    completeness: float | None = None
    questions_not_answered: list[str] = Field(default_factory=list)
    source_report_ids: list[str] = Field(default_factory=list)
    # The archetype-shaped payload. Kept as a dict so the registry can drive the
    # shape; validate with ``validate_data`` / ``parse_data``.
    data: dict = Field(default_factory=dict)

    def validate_data(self) -> BaseModel:
        """Parse ``data`` under the declared archetype, raising on mismatch."""
        model = ARCHETYPES.get(self.archetype)
        if model is None:
            raise ValueError(f"unknown archetype {self.archetype!r} for {self.node_key}")
        return model.model_validate(self.data)


# Section references NodeEnvelope (recursive) — resolve the forward ref.
Section.model_rebuild()
