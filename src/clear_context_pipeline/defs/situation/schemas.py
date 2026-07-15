"""Pydantic schemas for the situation-analysis payload.

Blob shape mirrors the design proposal — seven top-level components,
each carrying its own ``source_report_ids`` so the dashboard's
sources tab is a JSON walk, not a second query.

Phase B (this cut) has typed models for the deterministic components
(Datapoints + Sources) and the *shape* of every other component so
the JSON writer doesn't emit keys that a future Phase C reader
wouldn't recognise. LLM-generated components ship as empty defaults
for now — dashboard renders an empty state until Phase C wires the
generators.

Schema version is the load-bearing versioning field. Rows with
different versions never mix on trend views; bumping this triggers
full regeneration for all covered countries × years.
"""

from typing import Literal, Optional

from pydantic import BaseModel, Field

SCHEMA_VERSION = "v1"

Severity = Literal["low", "medium", "high", "critical"]

# NRC SAF sectors — same taxonomy the datapoint schema uses.
SafSector = Literal[
    "education", "food_security", "health", "shelter", "wash", "protection",
]


# ────────────────────────────────────────────────────────────────────
# Envelope shared by every non-numeric bullet-style component. Every
# bullet carries the reports that support it so the dashboard can
# render a per-bullet citation on hover.
# ────────────────────────────────────────────────────────────────────


class SourcedBullet(BaseModel):
    """One bulleted claim + the reports it came from."""
    description: str
    source_report_ids: list[str] = Field(default_factory=list)


# ────────────────────────────────────────────────────────────────────
# Component 1 — Datapoints (deterministic)
# ────────────────────────────────────────────────────────────────────


class DatapointsEnvelope(BaseModel):
    """Quality metadata for the whole datapoints block. Hoisted from
    the aggregated_datapoints row so the dashboard can render "based
    on N reports, freshest 2 days ago" without opening the JSON."""
    quality_score: Optional[float] = None
    newest_source_at: Optional[str] = None
    oldest_source_at: Optional[str] = None
    report_count: Optional[int] = None


class Datapoints(BaseModel):
    """Deterministic headline numbers hoisted from the yearly × A0
    aggregated_datapoint bucket. Each field is nullable — a country-
    year with no ingested reports has all-null values but the
    envelope still records the fact of zero contributing sources."""
    population_displaced: Optional[float] = None
    # People in Need — the assessed subset requiring humanitarian
    # assistance. NOT Population Affected (the wider circle of everyone
    # the crisis touched), which nothing extracts today. See CONTEXT.md
    # and docs/adr/0001-affected-extracted-not-sourced-from-events.md.
    population_in_need: Optional[float] = None
    returnees: Optional[float] = None
    number_of_events: int = 0
    funding_required_usd: Optional[float] = None
    funding_received_usd: Optional[float] = None
    envelope: DatapointsEnvelope = Field(default_factory=DatapointsEnvelope)


# ────────────────────────────────────────────────────────────────────
# Component 2 — AI summary (LLM, Phase C — stub)
# ────────────────────────────────────────────────────────────────────


class AISummary(BaseModel):
    """2–4 paragraph narrative synthesis. Empty string means Phase C
    hasn't populated it yet; the dashboard renders an empty state."""
    text: str = ""
    source_report_ids: list[str] = Field(default_factory=list)


# ────────────────────────────────────────────────────────────────────
# Component 3 — Context risks (LLM, Phase C — stub)
# ────────────────────────────────────────────────────────────────────


class RiskDomain(BaseModel):
    bullets: list[str] = Field(default_factory=list)
    source_report_ids: list[str] = Field(default_factory=list)


class ContextRisks(BaseModel):
    """Eight fixed sub-domains. All start empty in Phase B."""
    demographics: RiskDomain = Field(default_factory=RiskDomain)
    political: RiskDomain = Field(default_factory=RiskDomain)
    economy: RiskDomain = Field(default_factory=RiskDomain)
    socio_culture: RiskDomain = Field(default_factory=RiskDomain)
    security: RiskDomain = Field(default_factory=RiskDomain)
    legal_policy: RiskDomain = Field(default_factory=RiskDomain)
    infrastructure: RiskDomain = Field(default_factory=RiskDomain)
    environment: RiskDomain = Field(default_factory=RiskDomain)


# ────────────────────────────────────────────────────────────────────
# Component 4 — Hazards & pre-crisis vulnerabilities (LLM, Phase C — stub)
# ────────────────────────────────────────────────────────────────────


class HazardsAndVulnerabilities(BaseModel):
    hazards: list[SourcedBullet] = Field(default_factory=list)
    vulnerabilities: list[SourcedBullet] = Field(default_factory=list)


# ────────────────────────────────────────────────────────────────────
# Component 5 — Displacement narrative (LLM, Phase C — stub)
# ────────────────────────────────────────────────────────────────────


class DisplacementNarrative(BaseModel):
    push_factors: list[SourcedBullet] = Field(default_factory=list)
    return_intention: list[SourcedBullet] = Field(default_factory=list)


# ────────────────────────────────────────────────────────────────────
# Component 6 — Sector analyses (LLM, Phase D — stub)
# ────────────────────────────────────────────────────────────────────


class InformationCoverageArea(BaseModel):
    """One area of information coverage plus the LLM's confidence /
    completeness rating. `report_count` is the underlying deterministic
    signal (how many reports contributed here) so the dashboard can
    show both the model's judgement and the ground truth."""
    area: str
    rating_out_of_10: int
    report_count: int = 0


class SectorAnalysis(BaseModel):
    """Per-SAF-sector analysis block. Empty defaults let Phase B ship
    the shape without the LLM-heavy generation."""
    severity: Optional[Severity] = None
    impact: list[str] = Field(default_factory=list)
    humanitarian_conditions: list[str] = Field(default_factory=list)
    vulnerable_sections: list[str] = Field(default_factory=list)
    top_needs: list[str] = Field(default_factory=list)
    priority_interventions: list[str] = Field(default_factory=list)
    information_coverage: list[InformationCoverageArea] = Field(default_factory=list)
    source_report_ids: list[str] = Field(default_factory=list)


class Sectors(BaseModel):
    """Fixed set of six SAF sectors — order preserved for the dashboard
    tab layout. Every sector's block is present even when empty so
    the UI doesn't need null-guards."""
    education: SectorAnalysis = Field(default_factory=SectorAnalysis)
    food_security: SectorAnalysis = Field(default_factory=SectorAnalysis)
    health: SectorAnalysis = Field(default_factory=SectorAnalysis)
    shelter: SectorAnalysis = Field(default_factory=SectorAnalysis)
    wash: SectorAnalysis = Field(default_factory=SectorAnalysis)
    protection: SectorAnalysis = Field(default_factory=SectorAnalysis)


# ────────────────────────────────────────────────────────────────────
# Component 7 — Sources (deterministic)
# ────────────────────────────────────────────────────────────────────


class Source(BaseModel):
    """One contributing report — the dashboard's sources tab renders
    a chronological list of these, most recent publication first."""
    report_id: str
    report_title: str
    source_url: str
    published_at: str


class Sources(BaseModel):
    reports: list[Source] = Field(default_factory=list)


# ────────────────────────────────────────────────────────────────────
# Top-level payload
# ────────────────────────────────────────────────────────────────────


class SituationAnalysisPayload(BaseModel):
    """The full ``data`` blob written to
    ``situation_analyses.data``. Every component is always present so
    the dashboard can key off stable JSON paths regardless of which
    Phase populated the row."""
    datapoints: Datapoints = Field(default_factory=Datapoints)
    ai_summary: AISummary = Field(default_factory=AISummary)
    context_risks: ContextRisks = Field(default_factory=ContextRisks)
    hazards_and_vulnerabilities: HazardsAndVulnerabilities = Field(
        default_factory=HazardsAndVulnerabilities,
    )
    displacement: DisplacementNarrative = Field(default_factory=DisplacementNarrative)
    sectors: Sectors = Field(default_factory=Sectors)
    sources: Sources = Field(default_factory=Sources)
