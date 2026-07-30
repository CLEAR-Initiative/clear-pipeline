"""Pydantic schemas for the situation-analysis payload.

Blob shape mirrors the design proposal — seven top-level components,
each carrying its own ``source_report_ids`` so the dashboard's
sources tab is a JSON walk, not a second query.

Every component is typed and every component is generated. Defaults are
empty rather than absent so a component whose generator failed — or that
was skipped via ``SITUATION_SKIP_NARRATIVE`` — still emits its key with
a well-formed empty value, and the dashboard renders an empty state off
a stable JSON path instead of null-guarding every read.

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
    # assistance. NOT Population Affected: the two are extracted and
    # surfaced separately (population_affected below). See CONTEXT.md and
    # docs/adr/0001-affected-extracted-not-sourced-from-events.md.
    population_in_need: Optional[float] = None
    # Population Affected — the wider circle of everyone the crisis
    # touched, a superset of People in Need. Extracted from reports,
    # Max-aggregated, and sparse. Distinct from population_in_need.
    population_affected: Optional[float] = None
    returnees: Optional[float] = None
    number_of_events: int = 0
    funding_required_usd: Optional[float] = None
    funding_received_usd: Optional[float] = None
    envelope: DatapointsEnvelope = Field(default_factory=DatapointsEnvelope)


# ────────────────────────────────────────────────────────────────────
# Component 2 — AI summary (LLM, RAG-grounded)
# ────────────────────────────────────────────────────────────────────


class AISummary(BaseModel):
    """2–4 paragraph narrative synthesis. Empty string means the
    generator failed or was skipped; the dashboard renders an empty
    state rather than a missing key."""
    text: str = ""
    source_report_ids: list[str] = Field(default_factory=list)


# ────────────────────────────────────────────────────────────────────
# Component 3 — Context risks (LLM, RAG-grounded)
# ────────────────────────────────────────────────────────────────────


class RiskDomain(BaseModel):
    bullets: list[str] = Field(default_factory=list)
    source_report_ids: list[str] = Field(default_factory=list)


class ContextRisks(BaseModel):
    """Eight fixed sub-domains, all populated by one generator call and
    therefore sharing one `source_report_ids` set."""
    demographics: RiskDomain = Field(default_factory=RiskDomain)
    political: RiskDomain = Field(default_factory=RiskDomain)
    economy: RiskDomain = Field(default_factory=RiskDomain)
    socio_culture: RiskDomain = Field(default_factory=RiskDomain)
    security: RiskDomain = Field(default_factory=RiskDomain)
    legal_policy: RiskDomain = Field(default_factory=RiskDomain)
    infrastructure: RiskDomain = Field(default_factory=RiskDomain)
    environment: RiskDomain = Field(default_factory=RiskDomain)


# ────────────────────────────────────────────────────────────────────
# Component 4 — Hazards & pre-crisis vulnerabilities (LLM, RAG-grounded)
# ────────────────────────────────────────────────────────────────────


class HazardsAndVulnerabilities(BaseModel):
    hazards: list[SourcedBullet] = Field(default_factory=list)
    vulnerabilities: list[SourcedBullet] = Field(default_factory=list)


# ────────────────────────────────────────────────────────────────────
# Component 5 — Displacement narrative (LLM, RAG-grounded)
# ────────────────────────────────────────────────────────────────────


class DisplacementNarrative(BaseModel):
    push_factors: list[SourcedBullet] = Field(default_factory=list)
    return_intention: list[SourcedBullet] = Field(default_factory=list)


# ────────────────────────────────────────────────────────────────────
# Component 6 — Sector analyses (LLM, one call per SAF sector)
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
    """Per-SAF-sector analysis block. Empty defaults keep the shape
    stable when a sector's generator fails — failure is isolated per
    sector, so one erroring leaves the other five intact."""
    severity: Optional[Severity] = None
    impact: list[str] = Field(default_factory=list)
    humanitarian_conditions: list[str] = Field(default_factory=list)
    vulnerable_sections: list[str] = Field(default_factory=list)
    top_needs: list[str] = Field(default_factory=list)
    priority_interventions: list[str] = Field(default_factory=list)
    information_coverage: list[InformationCoverageArea] = Field(default_factory=list)
    source_report_ids: list[str] = Field(default_factory=list)
    # Provenance of the analysis. 'sector' = built from sector-tagged
    # evidence; 'fallback' = the sector-scoped search was empty and an
    # unfiltered search supplied off-sector evidence, so the grade is an
    # inference rather than sector reporting; None = no analysis produced.
    evidence_scope: Optional[Literal["sector", "fallback"]] = None


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
