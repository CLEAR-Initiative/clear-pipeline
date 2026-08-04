---
status: proposed
---

# Reconciling authoritative `location_metadata` into datapoint aggregation

## Context

Datapoint aggregation (`clear-api/src/services/datapoint-aggregation.ts`) today combines
**only `report_datapoints`** — figures the pipeline extracts by LLM from ReliefWeb reports.
Meanwhile `location_metadata` already holds the **structured, authoritative, bitemporal**
versions of many of those same metrics, pulled directly from source APIs (IOM DTM, OCHA
HPC/FTS, UNHCR, IPC/CH, WFP, …) and refreshed on a daily/monthly cadence. We re-derive by
LLM numbers we already hold in first-class form, and we never cross-check one against the
other. `Data_quality_specs.md` names **cross-document corroboration** as a later phase;
`location_metadata` is exactly that corroboration source.

Two concrete problems motivate acting now:

1. **`displacement.returnees` over-counts.** The field conflates a **stock** (cumulative
   returnees to date, e.g. UNHCR) with a **flow** (returns in a period) and *sums* both,
   producing absurd totals.
2. **Stale reports go uncorrected.** When a ReliefWeb report lags, aggregation has no way
   to reach the fresher authoritative figure that `location_metadata` already carries.

This ADR decides **how `location_metadata` participates in aggregation**: as a direct
read-time input (not materialised rows), for reconciliation, gap-filling, and freshness.
It builds on ADR-0004 (source registry + credibility) and ADR-0005 (data-quality formula
+ bias-aware selection).

## Decision

### 1. Direct read at aggregation — no synthetic `report_datapoints`

`location_metadata` is refreshed daily; materialising it as `report_datapoint` rows would
immediately stale. Instead, the aggregator **reads the current `location_metadata` at
runtime** for the bucket's location(s) and merges the relevant figures in as high-quality
contributors. Both tables live in clear-api's Postgres, so this is a local read (no
cross-service call).

### 2. Three roles

Per *(metric, location, period)*, `location_metadata` is used to:

- **Anchor / gap-fill** — where a metric has an authoritative figure, it enters as a
  contributor; where reports have *no* figure, it populates the bucket outright.
- **Reconcile** — where reports *and* the API both have a figure, they compete under the
  ADR-0005 machinery, plus the divergence guard in §7.
- **Freshen** — the daily-refreshed API figure is typically the freshest, so it wins the
  "report is old" case for free.

### 3. Which metrics reconcile (the rest are context overlays)

Only these `location_metadata` types feed a numeric aggregate field; the mapping:

| `location_metadata` type | Source org | Reconciles with | Aggregate kind |
|---|---|---|---|
| `iom_dtm_displacement` | IOM DTM | `displacement.idp_stock` | latest_wins **(stock anchor)** |
| `hapi_refugees` | UNHCR | `displacement.refugees` | latest_wins |
| `hapi_returnees` | UNHCR | `displacement.returnee_stock` (new, §4) | latest_wins **(stock anchor)** |
| `hapi_humanitarian_needs` | OCHA HPC | `needs_and_funding.*.people_in_need`, `overall_pin` | latest_wins |
| `hapi_funding` | OCHA FTS | `overall_funding_{required,received}_usd` | latest_wins / additive (§6) |
| `hapi_food_security` | IPC / CH | `needs_and_funding.food_security.people_in_need` | latest_wins |

The remaining types — `hapi_food_prices`, `hapi_poverty_rate`, `ocha_3w`,
`acaps_seasonal_calendar`, `acaps_inform`, `nrc_*`, `logie_*`, `msna_*` — are **context
overlays** (operational presence, prices, severity, access, seasonality). They inform the
situation analysis and the recency windows but do **not** feed a numeric aggregate, so
they carry no double-count risk.

**Double-count-via-sum risk is narrow.** Of the *additive* aggregate fields, only two have
a `location_metadata` counterpart: **returnees** (fixed by the stock/flow split, §4) and
**funding_received** (safe by single-appeal grouping, §6). The other additive fields —
`casualties.killed/injured`, `security_incidents_count`, `aid_workers_killed`,
`new_displacements` — have **no** API source (DTM gives IDP *stock*, not the *flow*
`new_displacements` sums), so no overlap to double-count. Every other reconciling field is
`latest_wins` → pick-one → safe by construction.

### 4. Stock-and-flow model for displacement

Replace the stock/flow conflation with an explicit accounting model, applied per metric
with **independent anchoring** (noted simplification — see Consequences):

```
estimated_total(now) = latest_authoritative_stock(T₀) + Σ flows with as-of date > T₀
```

- **T₀ = the reference/as-of date of the anchoring stock** (DTM round date, UNHCR update
  date) — *not* ingest time. A stock already embeds every flow before T₀, so **only flows
  after T₀ are added**; earlier flows are dropped as already-counted. This is the invariant
  that kills the over-count.
- **`displacement.returnees` is retired** and split into:
  - `returnee_stock` — latest_wins, anchored on UNHCR cumulative returnees.
  - `new_returns` — additive **forward of T₀**, from report flows.
- Symmetrically, IDP displacement is `idp_stock` (DTM anchor) + `new_displacements`
  (forward flow). No cross-metric netting in V1 (returns do not decrement IDP stock).
- The **estimated current total is a read-time derived field** at the country / all-time
  level (like read-time recency in ADR-0005): the resolver walks to the latest stock
  bucket, then sums flow buckets after its T₀. Per-bucket stock and flow stay separate and
  clean.
- **Overlapping flows** (two reports covering the same period for the same area) are **not
  summed** — take the **max with data-quality override** within the overlap, then sum
  across non-overlapping spans. (Max, not latest, because displacement flow bias is
  *underreport* — the higher figure is the better estimate; ties break on data quality.)

### 5. Dedup

- **Key** = *(canonical source, metric, location, period-bucket)* — value-independent.
- Within a dedup group (the API figure plus any report *echoes* of that same
  source/metric/location/period), **collapse to the latest underlying data** (by reference
  period / round, then publication). Cited-source identity alone is insufficient because
  the *same* source can publish different values at different times → we lean to the
  latest.
- For the additive fields this grouping is load-bearing: it forces the API figure and its
  report echoes into **one** group so within-group selection fires *before* any cross-group
  sum. Detecting a report echo requires the report's **cited source** (ADR-0004) — which is
  why source-attribution sequences first.

### 6. `funding_received`

One appeal per country → the API figure and any report echo land in **one** group →
within-group **latest-wins with data-quality override**. No cross-group sum, so despite
being an "additive" field it does not double-count.

### 7. Trust ordering: recency bias, with an authoritative divergence guard

- **Default: latest wins, API or report** — no "API always wins by authority" rule.
  Authority enters only as higher `data_quality` (ADR-0005 §4), which lets the API override
  a *slightly*-fresher weak report within `validity_window / x`; a genuinely fresher report
  (a developing situation) still wins.
- **Divergence guard:** when a report and the authoritative API figure disagree by
  **> 25 %** (initial value), the **API figure wins** — a wildly divergent report is
  treated as an extraction error or outlier. Symmetry (whether the API wins when the report
  is higher *or* lower) is an open sign-off parameter.
- **Do not swallow the divergence.** Emit it as a **reconciliation / early-warning signal**
  ("report +40 % vs DTM baseline") even when the aggregate uses the API value, so the
  robustness of the guard doesn't cost us the signal of a genuine emerging event.

### 8. Data quality for API-sourced contributors

API figures are not LLM-extracted, so they never run the 8-criterion credibility rubric
(ADR-0004 §4). They instead receive a **deterministic credibility profile**: Directness =
*reported* (0.8 — official aggregation of field data, not firsthand, not media);
Attribution, Internal consistency, Geo/temporal specificity, Methodology, Representativeness
and Plausibility = *met*; **Recency computed read-time**. That yields credibility ≈ 9/10,
which with a reliability-3 source (×2.5 = 7.5) gives `data_quality ≈ 6.75` — high enough to
anchor reliably. The profile is config, tunable per source.

### 9. Recency for API figures: `now − valid_from` of the open version

An API contributor's recency keys on **`now − valid_from` where `valid_to IS NULL`** (the
current bitemporal version). This is sound **because of the blob-unchanged guard**: an
unchanged daily re-pull does not bump `valid_from`, so this measures "how long this exact
value has been the current truth," not "how long since we last polled." Only the open
version is ever scored; superseded versions are history. Note this is a *different date*
from T₀ (§4): T₀ (the described reference period) governs the flow cutoff; `valid_from`
(when the value became current) governs recency.

## Consequences

- **Aggregation gains a second input.** Winner selection, dedup, and the additive sum now
  span `report_datapoints` + a read-time `location_metadata` read. Regression-test against
  the existing suite plus new stock/flow, overlap-max, divergence, and gap-fill cases.
- **Field-model change:** `displacement.returnees` → `returnee_stock` + `new_returns`;
  the estimated current total becomes a read-time derived field. Consumers of `returnees`
  update.
- **Independent anchoring is a known simplification:** per-metric stock+forward-flow does
  not enforce the full displacement identity (returns reducing IDP stock, exits). Accepted
  for V1; revisit if cross-metric drift shows up.
- **Divergence threshold (25 %) and symmetry** are domain config, editable post-launch.
- **Depends on ADR-0004** for cited-source echo detection (dedup §5) — reinforces
  sequencing source-attribution first.
- **Context overlays** (`ocha_3w`, `nrc_*`, `acaps_*`, `logie_*`, prices, poverty) are
  wired into the situation analysis and recency windows, not the numeric aggregates.

## Related

- ADR-0004 — source registry + information-credibility (echo detection, reliability grades).
- ADR-0005 — data-quality formula + bias-aware, read-time-recency aggregation this extends.
- `docs/data-source-specs/Data_quality_specs.md` — names cross-document corroboration (this).
- `docs/location-metadata-reconciliation-design.md` — the conceptual sign-off companion.
- `docs/humanitarian-datapoint-extraction.md` §6 — aggregation model reworked by ADR-0005/06.
