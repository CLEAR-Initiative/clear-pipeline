"""Pydantic sub-schemas for domain-partitioned datapoint extraction.

The design doc (§5) splits the ~50-field exhaustive datapoint schema
into six focused sub-schemas so:
  1. A schema change only re-runs one domain (cheaper backfills).
  2. A parse failure in one domain doesn't drop the others.
  3. Each LLM call fits comfortably inside the model's ideal working
     window — better JSON compliance than a single monolithic call.

Every numeric value is wrapped in :class:`NumericField` so provenance
travels with the number. That's what enables the "click the figure to
see the source paragraph" affordance in the situation-analysis dashboard.

Schema version is the ``SCHEMA_VERSION`` constant below (do not hardcode
it here — it drifts). Bumping it triggers targeted re-extraction (see §7
of the design doc).
"""

from typing import Literal, Optional

import json
from typing import Any

from pydantic import BaseModel, Field, field_validator

# v2 adds Figure Scope to NumericField (scope_location_name +
# scope_location_id). Additive change — v1 rows are still valid, they
# simply carry no scope. Bumping this triggers regeneration of aggregated
# buckets for the affected fields (#274) and keeps v1/v2 rows from mixing
# on trend views.
SCHEMA_VERSION = "v2"


def _tolerate_stringified_json(v: Any) -> Any:
    """`mode="before"` validator body — Claude's tool_use path
    occasionally returns a JSON-encoded string where the schema
    expects a list or dict (typically on richly-nested fields like
    `access_by_location`). We defensively `json.loads()` a leading
    ``[`` / ``{`` before Pydantic's type check so the whole domain
    doesn't fail validation over a single serialisation quirk.

    Non-string inputs pass through unchanged. Malformed JSON strings
    ALSO pass through — Pydantic then rejects with its normal type
    error rather than swallowing garbage silently."""
    if isinstance(v, str):
        stripped = v.strip()
        if stripped.startswith(("[", "{")):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                pass
    return v

# Confidence tiers — soft enum. The prompt names these values but the
# LLM may emit close variants (e.g. "verified-un" or "reported-dtm");
# the aggregator maps unknown values to `unverified` at combine time.
ConfidenceTier = Literal[
    "verified", "reported", "estimated", "media", "unverified",
]

# NRC SAF sectors — enforced as a Literal because the aggregation math
# groups by sector. Off-taxonomy sectors would silently drop out.
SafSector = Literal[
    "Shelter", "WASH", "Protection", "Health", "Food Security", "Education",
]

# Access status — the OCHA convention. Enforced so the dashboard can
# render a fixed colour map.
AccessStatus = Literal["open", "constrained", "partial", "blocked", "unknown"]


class NumericField(BaseModel):
    """One numeric datapoint plus the provenance envelope required to
    trace it back to its source paragraph.

    The `unit` distinguishes counts (`"people"`, `"households"`),
    money (`"USD"`), rates (`"%"`, `"per 100k"`), and duration
    (`"days"`) — the dashboard renders each differently, and the
    aggregator refuses to combine mismatched units.

    `chunk_index` / `page_number` are the two provenance handles: the
    dashboard uses page_number for citations that survive re-chunking,
    while chunk_index lets the vector RAG (Layer 3) surface the exact
    excerpt if a user drills deeper.
    """
    value: float
    unit: str = Field(description='e.g. "people", "USD", "%", "cases"')
    confidence: ConfidenceTier = Field(
        description="How trustworthy is the source of this number?",
    )
    source_quote: str = Field(
        description="The sentence in the report the number came from.",
    )
    chunk_index: Optional[int] = Field(
        default=None,
        description="0-indexed chunk id if known; else null.",
    )
    page_number: Optional[int] = Field(
        default=None,
        description="1-indexed PDF page number; the primary citation handle.",
    )
    # ── Figure Scope (schema v2) ──────────────────────────────────────
    # The ONE place this figure is a total FOR — not every place the
    # report mentions. "1,000 affected in Kordofan" -> "Kordofan", even
    # if the report is framed nationally and names other states. This is
    # what lets the aggregator bucket the figure to the right location
    # instead of fanning it across every mentioned place. See
    # docs/adr/0002-deduplicate-at-figure-scope.md.
    scope_location_name: Optional[str] = Field(
        default=None,
        description=(
            "The single place this figure is a total for — the area the "
            "number covers, NOT every place the report mentions. Null if "
            "the figure can't be pinned to one place (do NOT default to the "
            "country or the first place named). Emit the PLACE NAME only; do "
            "not emit an admin level."
        ),
    )
    # Resolved from scope_location_name post-extraction to a `locations`
    # id. NOT emitted by the LLM — always overwritten by the resolve step.
    # Null means the figure is unscoped (LLM abstained, or the name did
    # not resolve) and must be excluded from cross-report roll-up.
    scope_location_id: Optional[str] = Field(
        default=None,
        description="Resolved post-extraction. Leave null; do not emit.",
    )


class TextField(BaseModel):
    """A non-numeric datapoint — descriptions, notes, categorical
    labels — with the same provenance envelope so citations work
    identically across numeric and narrative fields."""
    value: str
    confidence: ConfidenceTier
    source_quote: str
    chunk_index: Optional[int] = None
    page_number: Optional[int] = None


# ────────────────────────────────────────────────────────────────────
# Domain 1 — TimingAndScope
# ────────────────────────────────────────────────────────────────────


class LocationRef(BaseModel):
    """LLM-emitted location reference. `pcode` wins over `name` at
    resolve time; `admin_level` narrows the name lookup."""
    pcode: Optional[str] = None
    name: Optional[str] = None
    admin_level: Optional[Literal[0, 1, 2, 3]] = None


class TimingAndScope(BaseModel):
    """What window, where, and about what?

    First domain call — its outputs (locations, event types, active
    clusters) feed the incident-key construction in aggregation, so
    a failure here degrades every downstream aggregate. High-quality
    prompting matters most here.
    """
    reporting_period_start: Optional[str] = Field(
        default=None,
        description=(
            "ISO YYYY-MM-DD. The EARLIEST date the report describes "
            "events for — NOT the publication date. Leave null if the "
            "report is reference material without a specific window."
        ),
    )
    reporting_period_end: Optional[str] = Field(
        default=None,
        description=(
            "ISO YYYY-MM-DD. The LATEST date the report describes events "
            "for. Leave null when unclear — the aggregator infers a "
            "fallback and downgrades the reporting-period confidence."
        ),
    )
    reporting_period_confidence: ConfidenceTier = Field(
        default="reported",
        description=(
            "How confident are you that the period above matches what "
            "the report intends? Use `estimated` when you inferred it "
            "from context rather than a stated period."
        ),
    )
    locations: list[LocationRef] = Field(
        default_factory=list,
        description=(
            "Every distinct place the report discusses. Prefer OCHA "
            "pcodes when the report cites them; else the plain place "
            "name plus `admin_level` when clear."
        ),
    )
    event_types: list[str] = Field(
        default_factory=list,
        description=(
            "Free-text tags for the events / crises the report covers "
            "(e.g. 'conflict', 'flood', 'displacement', 'disease-outbreak'). "
            "Multi-hazard reports return multiple tags."
        ),
    )
    active_clusters: list[str] = Field(
        default_factory=list,
        description=(
            "OCHA humanitarian clusters explicitly named as active in "
            "the report (e.g. 'Protection', 'Food Security', 'WASH')."
        ),
    )

    # Nested-model list — Claude's tool_use occasionally returns a
    # JSON-encoded string here. Defensively decode before Pydantic
    # runs the type check so the whole domain doesn't fail.
    _tolerate_locations = field_validator("locations", mode="before")(
        _tolerate_stringified_json,
    )


# ────────────────────────────────────────────────────────────────────
# Domain 2 — Casualties
# ────────────────────────────────────────────────────────────────────


class CasualtyDisaggregation(BaseModel):
    """Sex- and age-disaggregated casualty counts.

    SGBV / PSEA reporting depends on these — a single `killed` scalar
    hides gender-based violence patterns. Leave fields null when the
    report doesn't disaggregate; a null does NOT imply zero.
    """
    men: Optional[NumericField] = None
    women: Optional[NumericField] = None
    children: Optional[NumericField] = None
    unknown: Optional[NumericField] = None
    total: Optional[NumericField] = Field(
        default=None,
        description="Report-stated total. Not required to equal the sum of the disaggregations.",
    )


class Casualties(BaseModel):
    killed: Optional[CasualtyDisaggregation] = None
    injured: Optional[CasualtyDisaggregation] = None
    missing: Optional[CasualtyDisaggregation] = None


# ────────────────────────────────────────────────────────────────────
# Domain 3 — Displacement
# ────────────────────────────────────────────────────────────────────


class DisplacementFlow(BaseModel):
    """One origin→destination pair with a magnitude.

    Populated only when the report explicitly names both endpoints.
    A pure `IDPs displaced from X` figure with no destination lives
    in `stock` / `new_displacements`, not here.
    """
    origin: Optional[LocationRef] = None
    destination: Optional[LocationRef] = None
    value: NumericField


class Displacement(BaseModel):
    """IDPs, refugees, returnees — carefully distinguished.

    `stock` is a state-snapshot: total people currently displaced.
    `new_displacements` is a flow: people newly displaced in the
    reporting window. These aggregate DIFFERENTLY (latest-wins vs
    sum-with-dedup), so the extractor must be strict about the split.
    """
    idp_stock: Optional[NumericField] = Field(
        default=None,
        description="Currently-displaced IDP population at the END of the reporting period.",
    )
    new_displacements: Optional[NumericField] = Field(
        default=None,
        description="People newly displaced DURING the reporting period.",
    )
    returnees: Optional[NumericField] = Field(
        default=None,
        description="People who returned to their origin during the reporting period.",
    )
    refugees: Optional[NumericField] = Field(
        default=None,
        description="People displaced ACROSS an international border. Distinct from IDPs.",
    )
    flows: list[DisplacementFlow] = Field(
        default_factory=list,
        description="Origin→destination pairs when the report names both endpoints.",
    )

    _tolerate_flows = field_validator("flows", mode="before")(_tolerate_stringified_json)


# ────────────────────────────────────────────────────────────────────
# Domain 4 — Needs and Funding
# ────────────────────────────────────────────────────────────────────


class SectorNeeds(BaseModel):
    """One SAF sector's PIN / funding / severity snapshot.

    `people_in_need` is a headline PIN — the number of people the
    sector says need assistance. `people_targeted` is the subset the
    sector's response plan aims to reach; `people_reached` is who
    they actually reached during the reporting period.
    """
    people_in_need: Optional[NumericField] = None
    people_targeted: Optional[NumericField] = None
    people_reached: Optional[NumericField] = None
    operational_presence: Optional[NumericField] = Field(
        default=None,
        description="Number of partner organisations delivering in-sector aid.",
    )
    severity_score: Optional[NumericField] = Field(
        default=None,
        description="Sector-specific severity index (usually 1–5) when the report grades it.",
    )
    funding_required_usd: Optional[NumericField] = None
    funding_received_usd: Optional[NumericField] = None


class NeedsAndFunding(BaseModel):
    """The costliest domain to extract accurately — used the Sonnet-
    tier LLM by default (see §11 of the design doc). Numeric density
    is highest here and off-by-one-order-of-magnitude errors carry
    real operational cost."""
    shelter: Optional[SectorNeeds] = None
    wash: Optional[SectorNeeds] = None
    protection: Optional[SectorNeeds] = None
    health: Optional[SectorNeeds] = None
    food_security: Optional[SectorNeeds] = None
    education: Optional[SectorNeeds] = None

    overall_funding_required_usd: Optional[NumericField] = None
    overall_funding_received_usd: Optional[NumericField] = None
    overall_pin: Optional[NumericField] = Field(
        default=None,
        description="Total PIN across all sectors when the report headlines a country/appeal-wide figure.",
    )


# ────────────────────────────────────────────────────────────────────
# Domain 5 — Access and Incidents
# ────────────────────────────────────────────────────────────────────


class AccessByLocation(BaseModel):
    location: LocationRef
    status: AccessStatus
    confidence: ConfidenceTier
    source_quote: str
    page_number: Optional[int] = None


class InfrastructureDamage(BaseModel):
    """Damaged / destroyed count for a class of civilian infrastructure.

    Drives sector needs downstream — a report of 12 destroyed schools
    is what informs the Education sector's PIN calculation.
    """
    destroyed: Optional[NumericField] = None
    damaged: Optional[NumericField] = None
    non_functional: Optional[NumericField] = None


class AccessAndIncidents(BaseModel):
    access_by_location: list[AccessByLocation] = Field(
        default_factory=list,
        description="Per-admin access classification. One entry per (location, status) pair the report specifies.",
    )
    security_incidents_count: Optional[NumericField] = Field(
        default=None,
        description="Total security incidents in the reporting window.",
    )
    incidents_by_type: dict[str, NumericField] = Field(
        default_factory=dict,
        description=(
            'Keyed by incident type (e.g. "attack-on-health", "kidnapping", '
            '"IED-strike"). Value is the count for that type.'
        ),
    )
    aid_workers_killed: Optional[NumericField] = None
    aid_workers_injured: Optional[NumericField] = None
    aid_workers_abducted: Optional[NumericField] = None

    schools: Optional[InfrastructureDamage] = None
    health_facilities: Optional[InfrastructureDamage] = None
    water_facilities: Optional[InfrastructureDamage] = None
    markets_disrupted: Optional[NumericField] = None

    # This is the domain where we saw the JSON-string-as-list failure
    # in the wild — verified with report 4221396. Both the nested list
    # and the nested dict get the tolerance treatment.
    _tolerate_access_by_location = field_validator("access_by_location", mode="before")(
        _tolerate_stringified_json,
    )
    _tolerate_incidents_by_type = field_validator("incidents_by_type", mode="before")(
        _tolerate_stringified_json,
    )


# ────────────────────────────────────────────────────────────────────
# Domain 6 — Narrative and Confidence
# ────────────────────────────────────────────────────────────────────


class SectorIndicator(BaseModel):
    """Sector-specific single-value indicator (IPC phase, GAM rate,
    disease outbreak count …). Kept in a flat map rather than nested
    per-sector because indicator taxonomies evolve faster than sector
    lists — new indicators can be added without a schema bump."""
    sector: SafSector
    indicator: str = Field(
        description=(
            'e.g. "IPC phase", "GAM rate", "cholera cases", '
            '"immunisation coverage", "out-of-school children", '
            '"water access rate", "latrine coverage".'
        ),
    )
    value: NumericField


class NarrativeAndConfidence(BaseModel):
    """Prose summary + overall confidence + a bag of sector indicators.

    The `brief_summary` is what a chatbot uses when a user wants a
    plain-English take on the report; the aggregator does NOT combine
    summaries across reports (they're marked `non-aggregatable` in §6.2).
    """
    brief_summary: TextField = Field(
        description=(
            "Two-to-four sentence prose summary of what the report says. "
            "Include the country, main hazards, headline numbers when known, "
            "and any access / operational context. Do NOT invent figures — "
            "if a number isn't stated, don't guess."
        ),
    )
    overall_confidence: ConfidenceTier = Field(
        description=(
            "Your assessment of the report as a whole. `verified` = UN mission "
            "verification. `reported` = DTM / cluster / partner. `estimated` "
            "= modeled projection. `media` = news coverage without direct "
            "verification. `unverified` = you couldn't tell."
        ),
    )
    sector_indicators: list[SectorIndicator] = Field(
        default_factory=list,
        description=(
            "One entry per stated sector indicator. Include IPC phase, GAM/SAM "
            "rates, disease outbreak counts, GBV cases, out-of-school children, "
            "water-access percentages, latrine coverage — whichever the report "
            "actually cites."
        ),
    )

    _tolerate_sector_indicators = field_validator("sector_indicators", mode="before")(
        _tolerate_stringified_json,
    )


# ────────────────────────────────────────────────────────────────────
# Registry — used by the extract asset to iterate through domains
# ────────────────────────────────────────────────────────────────────


# Order matters: `TimingAndScope` runs first because downstream domains
# reference its locations / event_types in their prompts.
DOMAINS: list[tuple[str, type[BaseModel]]] = [
    ("timing_and_scope", TimingAndScope),
    ("casualties", Casualties),
    ("displacement", Displacement),
    ("needs_and_funding", NeedsAndFunding),
    ("access_and_incidents", AccessAndIncidents),
    ("narrative_and_confidence", NarrativeAndConfidence),
]
