---
status: proposed
---

# Population figures carry a sex/age breakdown (SADD)

## Context

Humanitarian reporting standards (the IASC Gender with Age Marker; OCHA's
sex- and age-disaggregated data, "SADD") expect population figures to be broken
down by sex and age — a single "120,000 displaced" hides that, say, 52% are
women and 40% are children, which is what actually drives protection, WASH, and
education programming.

Today the extraction schema (`defs/knowledgebase/datapoints_schemas.py`) carries
disaggregation in exactly **one** place — `CasualtyDisaggregation`
(men/women/children/unknown) on killed/injured/missing — and even that is
**extraction-only**: `clear-api`'s aggregator has no `FieldRule` for the
breakdown or the sub-totals, so nothing rolls up (only `killed_total` /
`injured_total` do). The load-bearing population figures — displacement
(`idp_stock`, `new_displacements`, `returnee_stock`, `new_returns`, `refugees`)
and needs (`overall_pin`, `overall_affected`, per-sector `people_in_need`) — are
plain `NumericField` totals with no breakdown at all.

Two properties of the aggregation pipeline constrain any fix:

- **The reducer is a hardcoded registry, not a generic walk.** `FIELD_RULES` in
  `clear-api/src/services/datapoint-aggregation.ts` is an explicit array; the
  reducer reads each rule by an exact dotted `path` and treats a `NumericField`
  as a **leaf** (it does not descend into nested sub-figures). A figure with no
  matching `FieldRule` is invisible to aggregation.
- **Unscoped figures are dropped.** A figure is only rolled up if it has a
  resolved `scope_location_id` (it is otherwise excluded from cross-report
  dedup). So a breakdown cell that doesn't carry the parent's scope would never
  aggregate.

## Decision

### 1. Shape — a `breakdown` object of `NumericField` cells

Each in-scope population field keeps its total `NumericField` and gains an
optional `breakdown: Disaggregation`. The cells are themselves `NumericField`s,
so they carry the full interval-and-range envelope (ADR-0007:
`value_low/high`, `qualifier`, `measure_type`, `basis_period_*`, provenance) and
reduce through the **same** aggregation machinery as any other figure.

`Disaggregation` captures the **marginals** reports actually state — not the full
sex×age cross (which sitreps rarely give in full and which would roughly double
the schema):

- By sex: `female`, `male`, `sex_unknown`
- By age: `children_0_17`, `adults_18_59`, `elderly_60plus`

The `total` remains the parent `NumericField`. Cells are independent
observations, **not** required to sum to the total (a report may disaggregate
only partially). A null cell means *not reported*, never zero. The full sex×age
cross is a later phase; the model leaves room for it.

### 2. Scope/source inheritance — cells share the parent's incident key

The LLM leaves `scope_location_name` / `source_name` **null** on cells. A new
post-extraction step (`defs/knowledgebase/datapoints_extract.py`) propagates the
parent figure's resolved `scope_location_id`, `source_id`, and basis period into
every cell. This is not optional: it is what gives each cell the parent's
incident key `(scope location, time bucket, event-type set)` so it (a) rolls up
at all, and (b) dedupes as a breakdown *of that figure* rather than as a stray
number. Cells are excluded from the independent figure-scope resolver pass.

### 3. Aggregation — one `FieldRule` per cell, inheriting the parent's kind

Each aggregated cell gets its own `FieldRule` (a distinct `path` + unique
`label`) that inherits the parent field's `kind` / `qualityBias` / `timeBucket`:

| Parent | kind | Example cell rule |
|---|---|---|
| `displacement.idp_stock` | latest_state (month) | `displacement.idp_stock.breakdown.female` → `idp_stock_female` |
| `displacement.new_displacements` | additive_count (week) | `…breakdown.children_0_17` → `new_displacements_children_0_17` |
| `needs_and_funding.overall_affected` | max (month) | `…breakdown.male` → `overall_affected_male` |

`aggregateReports`, `finaliseReadTimeQuality`, and both read paths iterate
`FIELD_RULES`, so they pick the cells up automatically once the rules exist. No
generic-walk change; no resolver change.

### 4. Phase 1 field scope

Phase 1 covers `displacement.{idp_stock, new_displacements, refugees}` and
`needs_and_funding.{overall_pin, overall_affected}` — the five country-wide
population totals with the highest traffic and, crucially, the ones that
**already aggregate today**. The goal of Phase 1 is to prove the whole mechanism
(schema → scope propagation → per-cell `FieldRule` → roll-up) on the cheapest
surface, so it deliberately touches no figure that would first need new *parent*
aggregation.

What is phased to Phase 2, and **why** — this is sequencing, not exclusion:

- **Response tracking — `people_targeted` / `people_reached` (sector-level).**
  These are arguably the *highest-value* SADD targets: the IASC Gender-with-Age
  Marker is largely about whether a response reaches women and children in
  proportion to need, which is a reached-SADD question, not a PIN-SADD one. They
  are deferred only because they **do not aggregate at all today** — they have no
  `FieldRule`, so they are extracted into `SectorNeeds` but never roll up into
  `aggregatedDatapoint`. Disaggregating them is therefore blocked on first adding
  `FieldRule`s for the *totals* (and confirming their absence from `FIELD_RULES`
  is not deliberate). That parent-aggregation gap + the SADD cells are sequenced
  as the **next** ticket after Phase 1, not dropped.
- **Sector-level `people_in_need`** and **`returnee_stock` / `new_returns`.**
  These aggregate today, so they are purely a volume choice — sector-level SADD
  is 6 sectors × cells, ~2–3× the Phase-1 rule count. Folded into the same
  Phase-2 sector-level ticket.
- **Realigning `CasualtyDisaggregation`** onto this shape + giving it FieldRules
  (it disaggregates in extraction today but aggregates nowhere) — a separate,
  self-contained follow-up.

### 5. Schema version bump `v3 → v4`

`SCHEMA_VERSION` bumps to `v4`, triggering targeted re-extraction of the
displacement + needs domains. Aggregation only combines same-version rows, so v4
SADD rows never mix with v3 totals — the cutover is clean.

### 6. No GraphQL schema change

`AggregatedDatapoint.data` / `ReportDatapoint.data` are opaque `JSON!` scalars by
design. SADD keys flow to the dashboard, chatbot, and the situation-analysis
snapshot automatically. Rendering the breakdown is a front-end follow-up, out of
scope for the pipeline + clear-api change.

## Consequences

- **Deploy order is safe either way.** A `FieldRule` whose path has no data
  yields an empty aggregate; SADD data with no rule is stored but not aggregated.
  Ship the two repos together; backfill by running the v4 re-extraction then the
  aggregation refresh.
- **~25 new `FieldRule` entries** in Phase 1. Mechanical, O(1) each; the
  label-uniqueness test guards collisions.
- **Cells inherit the parent's `qualityBias`** (e.g. displacement → underreport),
  so a low-quality breakdown is corrected in the same direction as its total.
- **HAPI already carries gender/age rows the adapters discard**
  (`isHapiTotalRow`). A later `API_ADAPTERS` entry can feed real, non-LLM SADD
  into these same labels — high value, deferred because it flips existing
  adapter test assertions.
- **Cells needn't reconcile to the total.** Downstream must treat the total as
  authoritative and the breakdown as partial evidence, not assume Σcells = total.

## Related

- [ADR-0002](./0002-deduplicate-at-figure-scope.md) — figure scope + the incident key cells inherit.
- [ADR-0004](./0004-source-attribution-and-information-credibility.md) — per-figure source attribution cells inherit.
- [ADR-0005](./0005-data-quality-scoring-and-bias-aware-aggregation.md) — aggregation kinds + `qualityBias` cells reuse.
- [ADR-0006](./0006-location-metadata-reconciliation-in-aggregation.md) — the HAPI/`API_ADAPTERS` path a SADD adapter would extend.
- [ADR-0007](./0007-figures-as-ranges-over-intervals.md) — the interval-and-range envelope each cell carries.
