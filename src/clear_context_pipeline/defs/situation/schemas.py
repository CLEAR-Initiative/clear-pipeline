"""Pydantic schemas for the situation-analysis payload.

Blob shape mirrors the design proposal - seven top-level components,
each carrying its own ``source_report_ids`` so the dashboard's
sources tab is a JSON walk, not a second query.

Every component is typed and every component is generated. Defaults are
empty rather than absent so a component whose generator failed - or that
was skipped via ``SITUATION_SKIP_NARRATIVE`` - still emits its key with
a well-formed empty value, and the dashboard renders an empty state off
a stable JSON path instead of null-guarding every read.

Schema version is the load-bearing versioning field. Rows with
different versions never mix on trend views; bumping this triggers
full regeneration for all covered countries × years.
"""

from typing import Literal, Optional

from pydantic import BaseModel, Field

# v2: sourced narrative components carry `contributing_sources`
# (report_id -> [generated lines that report contributed to]) for in-line
# citation in the dashboard, alongside the coarse `source_report_ids`.
# v3: numeric headline datapoints carry the full range envelope
# (value + low/high + range_width + bias + confidence) instead of a flattened
# point — stops discarding the ADR-0007 band the aggregate already carries.
SCHEMA_VERSION = "v3"

Severity = Literal["low", "medium", "high", "critical"]

# NRC SAF sectors - same taxonomy the datapoint schema uses.
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
# Component 1 - Datapoints (deterministic)
# ────────────────────────────────────────────────────────────────────


class DatapointsEnvelope(BaseModel):
    """Quality metadata for the whole datapoints block. Hoisted from
    the aggregated_datapoints row so the dashboard can render "based
    on N reports, freshest 2 days ago" without opening the JSON."""
    quality_score: Optional[float] = None
    newest_source_at: Optional[str] = None
    oldest_source_at: Optional[str] = None
    report_count: Optional[int] = None


class StockFlowEstimate(BaseModel):
    """Estimated current total for a stock/flow metric (ADR-0006 §4): the latest
    authoritative stock plus the flows reported after its reference date T₀.
    Flows at/before T₀ are already embedded in `stock` and are not re-added."""
    total: Optional[float] = None
    stock: Optional[float] = None
    flows_since: Optional[float] = None
    t0: Optional[str] = None
    flow_count: Optional[int] = None


class DivergenceSignal(BaseModel):
    """An ADR-0006 §7 early-warning: a report figure diverged more than the
    threshold from the authoritative API figure (which then won). Surfaced so
    the dashboard can flag the disagreement — a possible emerging event or
    extraction error — instead of hiding it behind the reconciled value."""
    field: str
    report_value: float
    api_value: float
    pct_diff: float


class RangeFigure(BaseModel):
    """A headline figure as a RANGE, not a point (clear-context-pipeline ADR-0007).

    `value` is the projected point estimate; `[low, high]` is the honest error bar
    (the aggregate's `value_low`/`value_high`); `range_width` = high − low, a
    first-class uncertainty signal; `bias` tells the consumer which way to project
    the band to a single headline at the display edge (``overreport`` → low,
    ``underreport`` → high, ``neutral`` → midpoint); `confidence` is the field's
    data-quality signal. All nullable — a field with no data is all-None; an exact
    source figure has ``low == high == value``."""
    value: Optional[float] = None
    low: Optional[float] = None
    high: Optional[float] = None
    range_width: Optional[float] = None
    bias: Optional[str] = None  # "overreport" | "underreport" | "neutral"
    confidence: Optional[float] = None


class Datapoints(BaseModel):
    """Deterministic headline numbers hoisted from the yearly × A0
    aggregated_datapoint bucket. Each numeric field is a nullable RangeFigure - a
    country-year with no ingested reports has all-None figures but the envelope
    still records the fact of zero contributing sources."""
    population_displaced: Optional[RangeFigure] = None
    # People in Need - the assessed subset requiring humanitarian
    # assistance. NOT Population Affected: the two are extracted and
    # surfaced separately (population_affected below). See CONTEXT.md and
    # docs/adr/0001-affected-extracted-not-sourced-from-events.md.
    population_in_need: Optional[RangeFigure] = None
    # Population Affected - the wider circle of everyone the crisis
    # touched, a superset of People in Need. Extracted from reports,
    # Max-aggregated, and sparse. Distinct from population_in_need.
    population_affected: Optional[RangeFigure] = None
    # Cumulative returnee STOCK (returned to date) — sourced from the
    # `returnee_stock` aggregate (ADR-0005 §4a split `returnees` into stock +
    # flow); the field name is kept for downstream/narrative stability.
    returnees: Optional[RangeFigure] = None
    # Period-increment FLOWS (additive) that accrue on top of the stocks.
    # `new_returns` was previously unused; surfaced alongside `new_displacements`
    # for the stock+flow current-total narrative (ADR-0006 §4).
    new_displacements: Optional[RangeFigure] = None
    new_returns: Optional[RangeFigure] = None
    # Count of contributing reports — a plain integer, not a range.
    number_of_events: int = 0
    funding_required_usd: Optional[RangeFigure] = None
    funding_received_usd: Optional[RangeFigure] = None
    # Estimated current totals (ADR-0006 §4): latest authoritative stock + the
    # flows since its T₀. Read from the aggregated_datapoint's
    # `estimatedCurrentTotals` field.
    #
    # NOT period-scoped, unlike every other field on this model: clear-api
    # computes it AS OF NOW over a lookback from now, ignoring the bucket window.
    # It is therefore only populated on a snapshot whose window still includes
    # now — a regeneration of a PAST year/month returns null — and the dashboard
    # must label it "as of <generated_at>", never as the period's number. It is
    # also null when no anchoring stock exists in scope.
    estimated_current_displacement: Optional[StockFlowEstimate] = None
    estimated_current_returns: Optional[StockFlowEstimate] = None
    # ADR-0006 §7 divergence early-warnings collected across the datapoints block.
    divergences: list[DivergenceSignal] = Field(default_factory=list)
    envelope: DatapointsEnvelope = Field(default_factory=DatapointsEnvelope)


# ────────────────────────────────────────────────────────────────────
# Component 2 - AI summary (LLM, RAG-grounded)
# ────────────────────────────────────────────────────────────────────


class AISummary(BaseModel):
    """2–4 paragraph narrative synthesis. Empty string means the
    generator failed or was skipped; the dashboard renders an empty
    state rather than a missing key."""
    text: str = ""
    source_report_ids: list[str] = Field(default_factory=list)
    # report_id -> the generated sentences that report contributed to, resolved
    # from the LLM's inline [Rn] citations (v2). Empty when the model emitted no
    # usable markers — the dashboard falls back to source_report_ids then.
    contributing_sources: dict[str, list[str]] = Field(default_factory=dict)


# ────────────────────────────────────────────────────────────────────
# Component 3 - Context risks (LLM, RAG-grounded)
# ────────────────────────────────────────────────────────────────────


class RiskDomain(BaseModel):
    bullets: list[str] = Field(default_factory=list)
    source_report_ids: list[str] = Field(default_factory=list)
    # report_id -> the bullets that report contributed to (v2). See AISummary.
    contributing_sources: dict[str, list[str]] = Field(default_factory=dict)


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
# Component 4 - Hazards & pre-crisis vulnerabilities (LLM, RAG-grounded)
# ────────────────────────────────────────────────────────────────────


class HazardsAndVulnerabilities(BaseModel):
    hazards: list[SourcedBullet] = Field(default_factory=list)
    vulnerabilities: list[SourcedBullet] = Field(default_factory=list)
    # report_id -> the bullets (hazards + vulnerabilities) that report
    # contributed to (v2). Per-bullet ids also live on each SourcedBullet.
    contributing_sources: dict[str, list[str]] = Field(default_factory=dict)


# ────────────────────────────────────────────────────────────────────
# Component 5 - Displacement narrative (LLM, RAG-grounded)
# ────────────────────────────────────────────────────────────────────


class DisplacementNarrative(BaseModel):
    push_factors: list[SourcedBullet] = Field(default_factory=list)
    return_intention: list[SourcedBullet] = Field(default_factory=list)
    # report_id -> the bullets (push_factors + return_intention) that report
    # contributed to (v2). Per-bullet ids also live on each SourcedBullet.
    contributing_sources: dict[str, list[str]] = Field(default_factory=dict)


# ────────────────────────────────────────────────────────────────────
# Component 6 - Sector analyses (LLM, one call per SAF sector)
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
    stable when a sector's generator fails - failure is isolated per
    sector, so one erroring leaves the other five intact."""
    severity: Optional[Severity] = None
    impact: list[str] = Field(default_factory=list)
    humanitarian_conditions: list[str] = Field(default_factory=list)
    vulnerable_sections: list[str] = Field(default_factory=list)
    top_needs: list[str] = Field(default_factory=list)
    priority_interventions: list[str] = Field(default_factory=list)
    information_coverage: list[InformationCoverageArea] = Field(default_factory=list)
    source_report_ids: list[str] = Field(default_factory=list)
    # report_id -> the lines (across impact / conditions / needs / interventions)
    # that report contributed to (v2). See AISummary.
    contributing_sources: dict[str, list[str]] = Field(default_factory=dict)
    # Provenance of the analysis. 'sector' = built from sector-tagged
    # evidence; 'fallback' = the sector-scoped search was empty and an
    # unfiltered search supplied off-sector evidence, so the grade is an
    # inference rather than sector reporting; None = no analysis produced.
    evidence_scope: Optional[Literal["sector", "fallback"]] = None


class Sectors(BaseModel):
    """Fixed set of six SAF sectors - order preserved for the dashboard
    tab layout. Every sector's block is present even when empty so
    the UI doesn't need null-guards."""
    education: SectorAnalysis = Field(default_factory=SectorAnalysis)
    food_security: SectorAnalysis = Field(default_factory=SectorAnalysis)
    health: SectorAnalysis = Field(default_factory=SectorAnalysis)
    shelter: SectorAnalysis = Field(default_factory=SectorAnalysis)
    wash: SectorAnalysis = Field(default_factory=SectorAnalysis)
    protection: SectorAnalysis = Field(default_factory=SectorAnalysis)


# ────────────────────────────────────────────────────────────────────
# Component 7 - Sources (deterministic)
# ────────────────────────────────────────────────────────────────────


class Source(BaseModel):
    """One contributing report - the dashboard's sources tab renders
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


class SituationChanges(BaseModel):
    """Per-section "what changed" notes, generated in one LLM call over the
    before/after payloads (see situation/changes.py). Empty when there is
    nothing to compare against or nothing material changed. Keys are the
    section paths the dashboard renders strips under: "summary",
    "context_risks.<domain>", "hazards", "displacement",
    "sectors.<sector>".

    `basis` says what the notes actually mean, and the dashboard should
    label them accordingly rather than assuming:

    - "previous_period": diffed against the PRECEDING bucket of the same
      kind (last month for a monthly window). The meaningful reading -
      "what changed over the period".
    - "previous_generation": diffed against the prior version of the SAME
      bucket. The fallback when no preceding bucket exists yet; reads as
      "what changed since we last looked", which is generation cadence,
      not real-world change.
    """
    basis: Optional[str] = None
    compared_to: Optional[str] = None
    compared_to_window_start: Optional[str] = None
    compared_to_label: Optional[str] = None
    notes: dict[str, str] = Field(default_factory=dict)


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
    changes: SituationChanges = Field(default_factory=SituationChanges)
