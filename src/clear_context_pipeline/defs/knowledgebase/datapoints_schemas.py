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
import re
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from clear_context_pipeline.providers.classify import (
    coerce_event_types,
    level2_values,
)

# The disaster_types level_2 vocabulary, rendered once for the event_types prompt.
# Same taxonomy the signal classifier picks from, so a report's extracted event
# types line up with the events' own type_level_2.
_EVENT_TYPE_TAXONOMY = ", ".join(level2_values())

# Schema versions:
#   v1 — pre-launch baseline: Figure Scope on NumericField
#        (scope_location_name + scope_location_id) and
#        needs_and_funding.overall_affected (Population Affected, the widest
#        circle of crisis impact; never sourced from event-driven `events`,
#        see clear-context-pipeline ADR-0001).
#   v2 — source attribution (NumericField.source_name/source_id), document-
#        level information-credibility criteria (DocumentCredibility on
#        narrative_and_confidence), and the returnee stock/flow split
#        (returnee_stock + new_returns replace returnees). See the clear-
#        context-pipeline ADR-0004 / ADR-0005.
#   v3 — interval-and-range model, Phase 1 "Capture" (ADR-0007): every
#        NumericField gains value_low/value_high (magnitude range), qualifier
#        (per-figure bias direction), measure_type (stock/flow/cumulative), and
#        an optional figure-level basis_period_start/end. `value` stays the
#        headline point so downstream (clear-api aggregation) is unaffected; the
#        richer shape is captured now and consumed by the reducer in later phases.
#
# Pre-launch the corpus is a handful of test reports we wipe and re-extract on
# every change, so the version mainly documents the shape. Aggregation still
# combines only same-version rows, so the bump keeps any remaining older rows
# from mixing with the re-extracted v3 rows.
SCHEMA_VERSION = "v3"


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

# Information-credibility rating for the credibility criteria (ADR-0004 §4).
# Three-valued so the aggregator scores met = 1.0 / partial = 0.5 / unmet = 0.0
# and weights each criterion into the 0–10 information_credibility score.
CredibilityRating = Literal["met", "partial", "unmet"]

# ── Interval-and-range model (ADR-0007) ──────────────────────────────
# The per-figure bias direction the source states. Supersedes the field/source
# `qualityBias` prior where present ("at_least" → truth ≥ reported).
Qualifier = Literal["exact", "at_least", "at_most", "approx"]

# What a number measures over time — routes a figure to the right reduction.
# `cumulative_to_date` behaves as a stock (bypasses the flow rate math) and
# sidesteps period-overlap, so it is preferred where a source frames it that way.
MeasureType = Literal["stock_as_of", "period_flow", "cumulative_to_date"]

# Both enums live on EVERY numeric leaf, so a single off-taxonomy value ("flow"
# for "period_flow", "minimum" for "at_least") would raise a ValidationError that
# the extractor catches at DOMAIN granularity — nulling every figure in that
# domain, not just the one leaf. To keep that blast radius from following the new
# enums, coerce leniently at the field boundary: map the obvious synonyms, and
# fall back to a SAFE default (qualifier → "exact" = no bias claim; measure_type →
# None = indeterminate) rather than letting one bad token void the domain.
_QUALIFIER_ALIASES: dict[str, str] = {
    "minimum": "at_least", "min": "at_least", "more_than": "at_least",
    "greater_than": "at_least", "over": "at_least", "atleast": "at_least",
    "maximum": "at_most", "max": "at_most", "less_than": "at_most",
    "fewer_than": "at_most", "up_to": "at_most", "atmost": "at_most",
    "approximate": "approx", "approximately": "approx", "estimated": "approx",
    "estimate": "approx", "around": "approx", "roughly": "approx", "about": "approx",
    "precise": "exact", "point": "exact", "exactly": "exact",
}
# Last-resort band width when a directional qualifier's SOFT bound collapsed onto
# the point (the extractor emitted only the firm bound). The prompt is the primary
# path — it asks for a context-informed finite band — so this fires only on that
# fallback, opening a modest ±15% band in the qualifier's direction rather than
# leaving a degenerate [500,500] that reads as 'exactly 500' for 'at least 500'.
_FALLBACK_OPEN_BAND_FRACTION = 0.15

_MEASURE_TYPE_ALIASES: dict[str, str] = {
    "stock": "stock_as_of", "point_in_time": "stock_as_of", "total": "stock_as_of",
    "as_of": "stock_as_of", "snapshot": "stock_as_of",
    "flow": "period_flow", "new": "period_flow", "incremental": "period_flow",
    "during_period": "period_flow", "period": "period_flow",
    "cumulative": "cumulative_to_date", "running_total": "cumulative_to_date",
    "to_date": "cumulative_to_date", "since": "cumulative_to_date",
}


class FigureCredibility(BaseModel):
    """Per-figure information-credibility overrides (ADR-0004 §4). Each of the six
    intrinsic criteria is optional; a null inherits the report's document-level
    ``DocumentCredibility``. Rate a criterion here ONLY when THIS figure gives a
    distinct signal — a precisely-sourced, well-specified figure inside an
    otherwise vague report, or a suspiciously round media number in a credible
    one. Directness is the figure's ``confidence`` tier and Recency is computed
    at read time, so neither is assessed here."""
    attribution_quality: Optional[CredibilityRating] = None
    internal_consistency: Optional[CredibilityRating] = None
    plausibility_in_context: Optional[CredibilityRating] = None
    geographic_temporal_specificity: Optional[CredibilityRating] = None
    methodology_transparency: Optional[CredibilityRating] = None
    representativeness: Optional[CredibilityRating] = None

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
    # ── Interval-and-range model, Phase 1 capture (ADR-0007) ───────────
    # A figure is a value-RANGE over a time-INTERVAL, tagged by measure type.
    # `value` above stays the headline point (== the figure when exact) so the
    # existing aggregator/dashboard are unaffected; the fields below carry the
    # richer shape the interval reducer consumes in later phases. `value_low` /
    # `value_high` default to `value` (a degenerate point) when the source gives
    # a single number — see `_fill_range_from_point`.
    value_low: Optional[float] = Field(
        default=None,
        description=(
            "Lower bound of the reported magnitude — always FINITE, never open "
            "(a floor of 0 for an 'up to' figure is unusable; infer a plausible "
            "one from context instead). For an exact figure this equals `value` "
            "— leave null then, it is filled in. For 'between 500 and 700', 500; "
            "for 'at least 500', 500 (the firm floor); for 'up to 700', a "
            "plausible lower bound; for 'around 600', the low end of a modest "
            "band."
        ),
    )
    value_high: Optional[float] = Field(
        default=None,
        description=(
            "Upper bound of the reported magnitude — always FINITE, never open "
            "(NOT infinity for an 'at least' figure; infer a plausible ceiling "
            "from the report's own figures + context). Equals `value` for an "
            "exact figure (leave null). For 'at least 500', a plausible upper "
            "bound; for 'up to 700', 700 (the firm ceiling); for 'around 600', "
            "the high end of a modest band."
        ),
    )
    qualifier: Qualifier = Field(
        default="exact",
        description=(
            "The bias direction the source states for THIS figure — this is "
            "per-figure EVIDENCE, distinct from and superseding the field-level "
            "`qualityBias` prior (clear-api FieldRule) where stated. 'exact' (a "
            "precise count → point), 'at_least' (a firm FLOOR — 'more than', "
            "'at least', 'over'), 'at_most' (a firm CEILING — 'up to', 'fewer "
            "than', 'nearly'), or 'approx' (symmetric vagueness — 'around', "
            "'roughly', 'an estimated', '~', or a stated 'between X and Y'). "
            "Whatever the qualifier, value_low/value_high carry a finite band."
        ),
    )
    measure_type: Optional[MeasureType] = Field(
        default=None,
        description=(
            "What the number measures over time, chosen from the wording: "
            "'stock_as_of' = a point-in-time total ('currently', 'as of', "
            "'total displaced'); 'period_flow' = a quantity accrued DURING a "
            "period ('newly displaced', 'during May', 'this week', 'new "
            "cases'); 'cumulative_to_date' = a running total since an origin "
            "('since January', 'cumulative', 'to date'). Null only if the "
            "wording is genuinely indeterminate."
        ),
    )
    # Figure-level period, only when the text states one distinct from the
    # report's overall reporting_period_start/end (which applies otherwise, at
    # aggregation). ISO date strings.
    basis_period_start: Optional[str] = Field(
        default=None,
        description=(
            "Start of the period THIS figure covers, if stated distinctly from "
            "the report's overall period (ISO date, e.g. '2026-04-02'). Else "
            "null — the report's reporting period is used."
        ),
    )
    basis_period_end: Optional[str] = Field(
        default=None,
        description="End of the period this figure covers (ISO date) or null.",
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
    # ── Figure Scope ──────────────────────────────────────────────────
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
    # ── Source attribution ────────────────────────────────────────────
    # The organisation this specific number ORIGINATES from — "according
    # to IOM DTM…" -> "IOM DTM" — not the report's publisher (that's the
    # document-level fallback, applied at aggregation). Mirrors Figure
    # Scope: the LLM emits the name; the resolve step maps it to an id.
    # See docs/adr/0004-source-attribution-and-information-credibility.md.
    source_name: Optional[str] = Field(
        default=None,
        description=(
            "The organisation this specific number is attributed to in the "
            "text (e.g. 'IOM DTM', 'WHO', 'OCHA', 'WFP'). Emit the org NAME "
            "only. Null if the figure names no distinct source — do NOT "
            "default to the report's own publisher."
        ),
    )
    # Resolved from source_name post-extraction to a `data_sources` id via
    # resolveDataSource. NOT emitted by the LLM — always overwritten by the
    # resolve step. Null = no cited source; aggregation then attributes the
    # figure to the report's publisher (report_datapoints.sourceId).
    source_id: Optional[str] = Field(
        default=None,
        description="Resolved post-extraction. Leave null; do not emit.",
    )
    # ── Per-figure information credibility (ADR-0004 §4) ───────────────
    # Optional overrides of the report's document-level credibility, for the
    # criteria where THIS figure differs from the document as a whole. Null
    # criteria inherit the document-level assessment at aggregation time.
    credibility: Optional[FigureCredibility] = Field(
        default=None,
        description=(
            "Per-figure credibility overrides — set a criterion (met/partial/"
            "unmet) ONLY where this specific figure differs from the report "
            "overall (e.g. a well-attributed, precisely-scoped figure in a "
            "vague report). Leave the whole object null when the figure is "
            "typical of the document; it then inherits the document-level "
            "assessment. Do NOT restate directness/recency here."
        ),
    )

    @field_validator("qualifier", mode="before")
    @classmethod
    def _coerce_qualifier(cls, v: object) -> object:
        """Map synonyms → the four canonical qualifiers; unknown → "exact" (no
        bias claim). `qualifier` is a required Literal on every numeric leaf, so
        without this a single off-taxonomy token ("minimum") would raise a
        ValidationError the extractor catches at DOMAIN granularity — nulling every
        figure in the domain, not just this leaf. Coercing here keeps that blast
        radius contained. None/blank also default to "exact"."""
        if v is None:
            return "exact"
        s = re.sub(r"[\s-]+", "_", str(v).strip().lower())
        if s in ("exact", "at_least", "at_most", "approx"):
            return s
        return _QUALIFIER_ALIASES.get(s, "exact")

    @field_validator("measure_type", mode="before")
    @classmethod
    def _coerce_measure_type(cls, v: object) -> object:
        """Map synonyms → the three canonical measure types; unknown → None
        (indeterminate — the same as an omitted value). Never raises, so a stray
        token can't void the domain."""
        if v is None:
            return None
        s = re.sub(r"[\s-]+", "_", str(v).strip().lower())
        if s in ("stock_as_of", "period_flow", "cumulative_to_date"):
            return s
        return _MEASURE_TYPE_ALIASES.get(s)

    @model_validator(mode="after")
    def _fill_range_from_point(self) -> "NumericField":
        """Default the value range to the point when the source gave a single
        number, so `value_low`/`value_high` are always populated (a degenerate
        point == `value`). An inverted range is normalised, then the band is
        widened if needed so `value` always lies inside it.

        `value` is the authoritative headline (clear-api reads it, and its
        interval reducer trusts value ∈ [value_low, value_high] — its band-vs-
        point clamp silently mis-widens otherwise). When the model emits a point
        outside its own bounds ('900' with a '[500, 700]' band, or a swap that
        strands the point), we EXPAND the band to contain the point rather than
        clamp the headline or reject the figure: keep the number the extractor
        chose, make the envelope consistent, never null the record over it."""
        if self.value_low is None:
            self.value_low = self.value
        if self.value_high is None:
            self.value_high = self.value
        if self.value_low > self.value_high:
            self.value_low, self.value_high = self.value_high, self.value_low
        # value is the headline; the band must contain it (fixes an out-of-band
        # point and a qualifier-blind swap that leaves value outside in one step).
        self.value_low = min(self.value_low, self.value)
        self.value_high = max(self.value_high, self.value)
        # Qualifier ↔ band direction. By the extraction convention `value` IS the
        # firm bound of a directional qualifier — the floor of an `at_least`, the
        # ceiling of an `at_most` — so pin that bound to `value` and keep the soft
        # bound on the correct side: an `at_most` band must not sit ABOVE its
        # ceiling (e.g. "up to 700" with a band reaching 900 is contradictory —
        # the ceiling is firm, so value_high is pulled back to it). If the soft
        # bound collapsed onto the point (the extractor gave only the firm bound),
        # open a modest finite band in the qualifier's direction so the figure
        # stays directionally honest instead of degenerating to a pseudo-exact
        # point — a degenerate [500,500] reads as "exactly 500" for "at least 500".
        if self.qualifier == "at_least":
            self.value_low = self.value  # the firm floor
            if self.value_high <= self.value:
                self.value_high = self.value * (1 + _FALLBACK_OPEN_BAND_FRACTION)
        elif self.qualifier == "at_most":
            self.value_high = self.value  # the firm ceiling
            if self.value_low >= self.value:
                self.value_low = self.value * (1 - _FALLBACK_OPEN_BAND_FRACTION)
        return self


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
            "The hazard/event types the report covers, chosen ONLY from the "
            "disaster_types level_2 taxonomy below. Emit the exact label; return "
            "an empty list rather than inventing a tag. These are event TYPES, not "
            "consequences or activities — never 'displacement', 'search-and-rescue', "
            "'humanitarian crisis'. Multi-hazard reports return multiple. Allowed "
            f"values: {_EVENT_TYPE_TAXONOMY}."
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

    # Constrain event_types to the disaster_types level_2 taxonomy and DROP
    # off-taxonomy tags — these feed the incident-key in aggregation, so a free-text
    # 'displacement'/'search-and-rescue' would fragment incidents that the events'
    # type_level_2 keeps together.
    _coerce_event_types = field_validator("event_types", mode="before")(
        coerce_event_types,
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

    Every measure is either a STOCK (a state-snapshot: how many people are
    in this state now) or a FLOW (how many entered it during the reporting
    window). Stocks aggregate latest-wins; flows aggregate by sum across
    non-overlapping periods — so the extractor must be strict about the split:
      * IDPs:     `idp_stock` (stock)      + `new_displacements` (flow)
      * Returns:  `returnee_stock` (stock) + `new_returns` (flow)
    Conflating a running total with a per-period count over-counts (the exact
    returnee bug ADR-0005 §4a fixes), so never put a cumulative total in a flow.
    """
    idp_stock: Optional[NumericField] = Field(
        default=None,
        description="Currently-displaced IDP population at the END of the reporting period.",
    )
    new_displacements: Optional[NumericField] = Field(
        default=None,
        description="People newly displaced DURING the reporting period.",
    )
    returnee_stock: Optional[NumericField] = Field(
        default=None,
        description=(
            "Cumulative total of people who have returned to their area of "
            "origin AS OF the end of the reporting period — a running total "
            "(STOCK), not the period's new returns. Aggregates latest-wins."
        ),
    )
    new_returns: Optional[NumericField] = Field(
        default=None,
        description=(
            "People who returned to their area of origin DURING the reporting "
            "period — a FLOW. Aggregates by sum across non-overlapping periods. "
            "Do NOT put a cumulative return total here (that is returnee_stock)."
        ),
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
        description=(
            "Total People in Need across all sectors, ONLY when the report "
            "headlines a country/appeal-wide in-need figure (an HNO/HRP or "
            "appeal document). Do NOT infer it from, or conflate it with, the "
            "displaced (displacement.*), returnees, refugees, the affected "
            "(overall_affected), or casualties - those are different "
            "populations. A displacement tracker or sitrep that states no "
            "explicit 'in need' figure gets null here, never its IDP total."
        ),
    )
    overall_affected: Optional[NumericField] = Field(
        default=None,
        description=(
            "Population Affected — the widest circle of crisis impact: "
            "everyone touched by the crisis, a superset of People in Need. "
            "Populate ONLY from a figure the report explicitly states as "
            "'affected' / 'impacted' by the crisis. Do NOT infer it from, "
            "or conflate it with, People in Need (overall_pin), the "
            "displaced (displacement.*), or casualties (casualties.*) — "
            "those are narrower, different populations. Leave null when the "
            "report states no explicit affected figure."
        ),
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


class DocumentCredibility(BaseModel):
    """Document-level information-credibility criteria (ADR-0004 §4).

    Six intrinsic criteria the LLM assesses ONCE for the whole report, each
    rated met / partial / unmet. Together with the per-figure `confidence`
    (Directness) and a Recency score computed at read time in clear-api, they
    yield the 0–10 information_credibility that feeds the data-quality score.
    Assessed at document level as the fallback for every figure in the report;
    per-figure overrides are a later refinement (ADR-0004 §4).

    Every criterion is Optional (default None). The prompt still instructs the
    model to rate all six, but Anthropic tool-use is best-effort: a response that
    fills only four of six must NOT raise a ValidationError that nulls the entire
    `narrative_and_confidence` domain (losing brief_summary, overall_confidence
    and every sector_indicator to gain nothing). clear-api treats a missing
    criterion as the neutral 0.5 rating, so a partial assessment degrades
    gracefully. Mirrors FigureCredibility, whose six fields are already Optional.
    (#27)
    """
    attribution_quality: Optional[CredibilityRating] = Field(
        default=None,
        description=(
            "Are the report's claims attributed to identifiable sources "
            "(named agencies, dated assessments) rather than anonymous or "
            "absent attribution? met = clearly attributed throughout."
        ),
    )
    internal_consistency: Optional[CredibilityRating] = Field(
        default=None,
        description=(
            "Do figures and claims within the report agree with each other "
            "(totals match disaggregations, no contradictions)? "
            "met = internally consistent."
        ),
    )
    plausibility_in_context: Optional[CredibilityRating] = Field(
        default=None,
        description=(
            "Are the claims plausible against the COUNTRY BASELINE provided in the "
            "system prompt? met = magnitudes consistent with that baseline; unmet = "
            "figures far outside it (order-of-magnitude off) with no explanation; "
            "partial = somewhat high/low but arguable."
        ),
    )
    geographic_temporal_specificity: Optional[CredibilityRating] = Field(
        default=None,
        description=(
            "Are events located and dated precisely enough to act on "
            "(specific admin areas + dates) rather than vague ('parts of the "
            "country', 'recently')? met = precise."
        ),
    )
    methodology_transparency: Optional[CredibilityRating] = Field(
        default=None,
        description=(
            "Does the report state how figures were collected (assessment "
            "method, sample, coverage) where applicable? "
            "unmet = figures with no stated methodology."
        ),
    )
    representativeness: Optional[CredibilityRating] = Field(
        default=None,
        description=(
            "Does the stated scope match the claims (a 3-village assessment "
            "NOT generalised to a governorate)? met = scope matches claims."
        ),
    )


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
    information_credibility: Optional[DocumentCredibility] = Field(
        default=None,
        description=(
            "Document-level information-credibility — rate ALL six criteria "
            "(met / partial / unmet) for the report as a whole. This is the "
            "credibility fallback for every figure, so always fill it. "
            "Directness is captured separately per figure (`confidence`); "
            "Recency is computed later — do not assess either here."
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
