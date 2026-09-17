---
status: proposed
---

# Constrained-indicator datapoints: the missing figures + closed-enum categorical disaggregation

## Context

`docs/indicators_constrained.csv` is the agreed target list — 27 constrained
humanitarian indicators, each with an operational definition, unit, and a
`disaggregation` column naming the axes it should be broken down by. Tallying it
against the live extraction schema (`defs/knowledgebase/datapoints_schemas.py`,
v4) plus `clear-api`'s aggregation (`FIELD_RULES`):

- **10 covered** — affected population, people in need, current displacement
  (IDP + refugee), new displacement, return, deaths, injuries, missing persons,
  shelter need (sector PIN), assistance reached.
- **2 partial** — inaccessible locations (we carry per-location *status* via
  `AccessByLocation`, but never a *count*); essential-service facilities (we do
  schools/health/water/markets, but not power / communications / generic
  "other").
- **15 missing** — movement (evacuated/relocated), displacement sites count,
  accommodation type, displacement drivers, intentions, family separation
  (UASC), houses destroyed/damaged/uninhabitable, shelter condition,
  access-constrained population, access barriers, roads/routes blocked, response
  gap, service disruption.

Two things shape the fix:

- **Most missing indicators disaggregate along a categorical axis that is not
  sex/age.** The `disaggregation` column repeatedly names *type*, *severity /
  status*, and *cause / category* axes — dwelling type, facility type, movement
  type, accommodation type, damage severity, barrier category, service type, and
  so on. ADR-0008's `Disaggregation` (fixed female/male/age cells) is the wrong
  shape for these.
- **The aggregation machinery from ADR-0008 already generalises.** The reducer
  is a hardcoded `FIELD_RULES` registry that treats each `NumericField` as a
  leaf; a breakdown cell rolls up iff (a) it has its own `FieldRule` and (b) it
  carries the parent's resolved `scope_location_id` (unscoped figures are
  dropped). SADD proved a per-cell-`FieldRule` + post-extraction scope/source
  propagation over exactly this. We reuse that whole path.

## Decision

### 1. Categorical disaggregation is a **closed `Literal` enum**, never open vocab

Extraction's whole job here is to map *unknown source wording → known analytical
categories*. Leaving the vocabulary open (a free-text `dict[str, …]`) does not do
that job — it relabels the mess and defers normalization to a fragile
string-match in the aggregator ("collective centre" vs `collective_centre` vs
"collective center" never rolling up). So each categorical axis is a fixed
`Literal` enum, and the LLM normalizes source language onto it — the task LLMs
are reliable at, done once at the point of understanding.

This matches the existing schema, which already constrains known taxonomies
(`SafSector`, `AccessStatus`, `ConfidenceTier`, and `event_types` against the
disaster-types taxonomy). The open-vocab `incidents_by_type` is the deliberate
exception — security-incident types are genuinely unbounded; these axes are not.

### 2. `"other"` escape hatch — closed, but nothing is dropped

Every axis enum ends in `"other"`. Unlike `event_types` (which *drops*
off-taxonomy tags — acceptable for a tag list, not for a count-bearing figure), a
category we did not anticipate lands in `"other"` with the source wording
preserved in the cell's `source_quote`. When an `"other"` bucket gets heavy for
some axis, we promote that value into the enum in a later schema bump (we are
versioning for one anyway). Result: anticipated categories aggregate
deterministically on fixed keys — no normalization step — and the long tail is
captured, not lost.

### 3. Shape — a categorical breakdown is ALWAYS a `by_<axis>` map on a headline count

There is **one** categorical shape, not two. A count figure keeps its total
`NumericField` and gains an optional `by_<axis>: dict[<AxisEnum>, NumericField]`
map (precedent: `incidents_by_type`, but with a `Literal`-typed key). The map
**hangs off the count leaf** — this is not cosmetic: `_collect_numeric_fields`
stops at a `NumericField` leaf and does not descend, so a breakdown that hangs
off the leaf has its cells shielded from independent scope-resolution and
inherited instead (§4). Each cell is a full `NumericField` (interval envelope,
ADR-0007), reduces through the same machinery, is **not** required to sum to the
total, and a null/absent cell means *not reported*, never zero.

**There are no standalone categorical "distribution" figures.** The CSV's
`category`-unit rows (accommodation #8, drivers #9, intentions #10, access
barriers #22) look like their own indicators but are not — their numbers are
already owned by a *count* indicator, and the category is that count's
disaggregation axis. "5,000 in collective centres, 3,000 with host families" is
not a new figure; the 5,000/3,000 are displaced people (current displacement,
#3), split by accommodation type. So each `category`-unit row is modelled as a
`by_<axis>` map on its parent count, never as a figure of its own — which also
means nothing is double-counted:

| `category`-unit row | Parent count | Modelled as |
|---|---|---|
| Accommodation (#8) | Current displacement (#3) | `idp_stock.by_accommodation_type` |
| Drivers / cause (#9) | New displacement (#4) | `new_displacements.by_cause` |
| Intentions (#10) | Current displacement (#3) | `idp_stock.by_intention` |
| Access barriers (#22) | Access-constrained pop. (#20) | `access_constrained_population.by_barrier` |

**Count-less fallback.** These axes are frequently reported with no number at all
("access impeded by checkpoints and insecurity"; "most intend to return"). A
numeric `dict[Enum, NumericField]` is the wrong shape then — there is nothing to
put in `value`. For the count-less case the axis is captured as a **`list[<AxisEnum>]`
presence set** (like `active_clusters` / `event_types`); the numeric `by_<axis>`
map is used only when the report actually splits the parent count along it.

Sex/age stays on ADR-0008's `Disaggregation` where the figure is a splittable
people-count (family separation, shelter condition, access-constrained
population, response gap, service-disruption people). A figure may therefore
carry *both* a `by_<axis>` categorical map and a sex/age `breakdown`.

### 4. Scope/source/period inheritance — unchanged from ADR-0008

The LLM leaves `scope_location_name` / `source_name` null on every cell; the
post-extraction step in `datapoints_extract.py` propagates the parent's resolved
`scope_location_id`, `source_id`, and basis period into each cell so it inherits
the parent's incident key `(scope location, time bucket, event-type set)`. This
is what makes a cell roll up *as a breakdown of that figure* rather than as a
stray number. Extended to cover the new categorical cells alongside the SADD
cells.

### 5. The per-axis enums (transcribed from the CSV)

Universal axes are **not** new enums — they reuse existing handles: *location /
admin area / origin / destination* → figure scope (`scope_location_*`, and
`DisplacementFlow` for origin→destination pairs); *source / provider* →
`source_name`/`source_id`; *sex / age* → ADR-0008 `Disaggregation`; *population
unit (people vs households)* → `NumericField.unit`.

New closed enums (each ends in `other`; values are lower-snake-cased at the field
boundary like the existing `_coerce_*` validators):

| Enum | Values (from CSV definitions) | Used by |
|---|---|---|
| `DamageSeverity` | severe, moderate, minor, other | houses damaged (16), uninhabitable (17) |
| `DwellingType` | house, apartment, makeshift, traditional, mobile, other | housing (15–17) |
| `MovementType` | evacuation, relocation, other | movement (6) |
| `SiteType` | site, settlement, collective_centre, reception_centre, camp, other | displacement locations (7) |
| `AccommodationType` | host_family, rented, collective_centre, reception_centre, formal_site, informal_site, public_building, open_air, other | current displacement (3), accommodation (8), shelter need/condition (18,19) |
| `DisplacementCause` | conflict, violence, natural_hazard, eviction, housing_destruction, loss_of_services, livelihood_loss, other | new displacement (4), drivers (9) |
| `Intention` | return, remain, move_onward, relocate, undecided, other | intentions (10) |
| `CasualtyStatus` | confirmed, presumed, unverified, other | deaths (11) |
| `MissingCaseStatus` | new, active, resolved, other | missing persons (13) |
| `SeparationCategory` | unaccompanied, separated, other | family separation (14) |
| `ShelterCondition` | inadequate, unsafe, damaged, overcrowded, exposed, other | shelter condition (19) |
| `AccessClassification` | hard_to_reach, inaccessible, besieged, isolated, constrained, other | access-constrained population (20) |
| `AccessBarrier` | insecurity, road_damage, checkpoints, administrative_restrictions, denial, weather, distance, transport, discrimination, lack_of_information, other | access barriers (22), inaccessible-location reason (21) |
| `InfrastructureType` | road, route, bridge, crossing, transport_link, other | physical access (23) |
| `ServiceType` | water, healthcare, education, energy, markets, communications, transport, other | service disruption (27) |

Dropped during implementation: **`FacilityType`/`FacilityStatus`** (facilities #26 was closed additively — see §7 — so no facility-type map was needed) and **`AssistanceType`** (the SAF sector already encodes assistance type, so the response-gap figure needs no per-cell assistance axis).

Secondary axes named in the CSV but **not** modelled as breakdowns in this pass
(captured via the universal handles or deferred; recorded so the omission is
deliberate): *displacement status* (largely encoded by which field — idp vs
refugee vs returnee — a cross-cutting status enum is a future refinement),
*household type*, *direction of access* (22), *disruption type* (27), *injury
severity* (12). These do not block the headline counts and are cheaper to add
later than to over-model now.

### 6. Extraction domains vs the CSV `level1` pillars

Our six extraction domains are **LLM-call batching units**, not the CSV's
analytical `level1` pillars (Impact / Humanitarian Conditions / Capacities &
Response / …). The two axes do not line up 1:1 — a pillar can span domains, and a
domain holds several pillars. The pillar is a *label* derivable from the field
(a lookup), not something we extract by; there is no "Capacities & Response"
domain.

| CSV `level1` pillar | Extraction domain(s) |
|---|---|
| Impact | `AccessAndIncidents` (housing, facilities, service disruption) + `NeedsAndFunding` (affected) |
| Humanitarian Conditions | `NeedsAndFunding` (PIN, severity) + `NarrativeAndConfidence` (IPC/GAM sector indicators) |
| Displacement | `Displacement` |
| Casualties | `Casualties` |
| Protection | `Casualties`/`AccessAndIncidents` (family separation) |
| Shelter | `NeedsAndFunding` (shelter sector) + `AccessAndIncidents` (shelter condition) |
| Humanitarian Access | `AccessAndIncidents` |
| **Capacities & Response** | **`NeedsAndFunding`** (targeted/reached, funding, operational presence, response gap) + `AccessAndIncidents` (aid-worker security) |

### 7. Field additions, by domain (placement minimises new LLM calls)

New fields are added to **existing** domains wherever natural — each domain is a
separate LLM call, so extending a domain is cheaper than adding one:

- **Displacement** — two genuinely new count figures: `movement` (evacuated/
  relocated, `by_movement_type`) and `displacement_sites` (site count,
  `by_site_type`). The `category`-unit rows are NOT new fields — they are
  `by_<axis>` maps on the existing displacement counts (§3): `idp_stock` gains
  `by_accommodation_type` (#8) and `by_intention` (#10); `new_displacements`
  gains `by_cause` (#9). Count-less reports use the `list[<AxisEnum>]` fallback
  instead of the numeric map.
- **Casualties** — `killed`/`missing` gain the `CasualtyStatus` /
  `MissingCaseStatus` axis alongside the existing sex/age cells.
- **AccessAndIncidents** — extend to the physical-impact + access indicators:
  `housing: HousingDamage{destroyed, damaged, uninhabitable}` — each a
  `HousingCount` with `by_dwelling_type` / `by_severity` maps (#15–17,
  **implemented**); close the facilities gap (#26) **additively** — add
  `power_facilities` + `communication_facilities` (`InfrastructureDamage`)
  alongside the existing schools/health/water rather than restructuring them
  into a `dict[FacilityType, …]`, so their clear-api FieldRules stay untouched;
  `access_constrained_population` (count, `AccessClassification`,
  SADD-splittable) carrying a `by_barrier` map (#22) or the `list[AccessBarrier]`
  fallback when count-less; `routes_blocked` (count, `by_infrastructure_type`);
  `service_disruption_people` (count, `by_service_type`, SADD-splittable);
  `inaccessible_locations_count` (upgrades the partial — emit the count, not
  just per-location status).
- **NeedsAndFunding.SectorNeeds** — `people_not_reached` (response gap, #25;
  SADD-splittable), beside the existing targeted/reached. No per-cell
  assistance-type axis: the SAF sector already encodes assistance type.
- **Protection** — family separation (`SeparationCategory`, SADD sex/age). Folded
  into **AccessAndIncidents** as `family_separation` rather than a new
  `Protection` domain, to avoid a 7th LLM call; revisit if protection indicators
  grow.

Migrating the existing `AccessByLocation` / `InfrastructureDamage` shapes is a
backward-compatible generalisation, done under the same version bump.

### 8. Aggregation (clear-api) — deterministic on fixed keys

- One `FieldRule` per new numeric field and per categorical cell, deriving
  `kind` / `qualityBias` / `timeBucket` from the parent (the ADR-0008 pattern —
  drift-proof). Stock-vs-flow per indicator: houses destroyed/damaged =
  `latest_state` (stock); movement, new displacement, response reached, returns =
  `additive_count` (flow); site/location and facility counts = `latest_state`.
- Fixed enum keys roll up with **no normalization step**; the `other` bucket
  aggregates as its own key.
- Wire the new headline labels into the situation datapoints
  (`defs/situation/generate.py::_build_datapoints`) where they map to a situation
  field.

### 9. Schema version bump `v4 → v5`

`SCHEMA_VERSION` (this repo) and `DEFAULT_SCHEMA_VERSION` (clear-api resolver)
bump to `v5` **together**. Aggregation combines only same-version rows, so the
new fields + generalised shapes never mix with v4 rows — a clean re-extraction
boundary. This is a larger, partly shape-changing addition (not the purely
additive optional fields the v4 waves were), so it takes a real version rather
than riding v4.

### 10. No GraphQL schema change

`AggregatedDatapoint.data` / `ReportDatapoint.data` stay opaque `JSON!`. New keys
flow to the dashboard, chatbot, and situation snapshot automatically; rendering
the new breakdowns is a front-end follow-up, out of scope here.

## Consequences

- **Deploy order is safe either way** (as ADR-0008): a `FieldRule` with no data
  yields an empty aggregate; data with no rule is stored but not aggregated. Ship
  both repos together; backfill = v5 re-extraction then aggregation refresh.
- **Large but mechanical `FIELD_RULES` growth** — one rule per new field + per
  categorical cell. The label-uniqueness test guards collisions.
- **Closed enums need maintenance** — a persistently heavy `other` bucket on an
  axis is the signal to promote a value into that enum (a future bump). This is
  the accepted cost of extraction-time normalization; it buys deterministic,
  fragment-free aggregation.
- **`AccessAndIncidents` grows sizeable** — if its single LLM call starts losing
  JSON compliance under the added fields, split it into a physical-impact domain
  (a 7th call). Deferred until measured, not pre-optimised.
- **Cells needn't reconcile to the total** — downstream treats the total as
  authoritative and each breakdown (categorical or sex/age) as partial evidence.

## Related

- [ADR-0002](./0002-deduplicate-at-figure-scope.md) — figure scope + the incident key cells inherit.
- [ADR-0004](./0004-source-attribution-and-information-credibility.md) — per-figure source attribution cells inherit.
- [ADR-0005](./0005-data-quality-scoring-and-bias-aware-aggregation.md) — aggregation kinds + `qualityBias` cells reuse.
- [ADR-0007](./0007-figures-as-ranges-over-intervals.md) — the interval-and-range envelope each cell carries.
- [ADR-0008](./0008-sex-and-age-disaggregated-figures.md) — the per-cell `FieldRule` + scope/source propagation this ADR generalises to categorical axes.
