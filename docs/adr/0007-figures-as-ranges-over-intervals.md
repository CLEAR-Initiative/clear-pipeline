---
status: proposed
---

# Datapoint figures are value-ranges over time-intervals, not points

## Context

Aggregation (`clear-api/src/services/datapoint-aggregation.ts`) models every extracted
figure as a **single value** pinned to a **single time bucket** by `reportingPeriodEnd`.
Five failure modes the team keeps hitting all trace back to that one modelling choice:

1. **Stock vs flow.** A running total ("50,000 displaced") and a period increment
   ("50,000 newly displaced") aggregate completely differently — latest-wins vs sum — but
   the extractor has to infer which from prose, and the cost of getting it wrong is large
   and asymmetric (flow-as-stock undercounts; stock-as-flow sums snapshots into nonsense).
2. **Overlapping periods.** Report A gives casualties for 2–10 Apr, report B for 5–15 Apr.
   Bucketed by end-date they land in different weeks and **sum**, double-counting the 5–10
   Apr overlap. Every `additive_count` field is exposed.
3. **Bucket-boundary spanning.** A figure for 25 Apr–5 May is forced into one weekly/monthly
   bucket by its end-date, discarding the portion that belongs to the other bucket.
4. **Subset / superset.** "1M killed (all causes)" and "100k killed (drone strikes)" carry
   different `event_types`, so they land in different incident groups and **sum** to 1.1M —
   even though the second is contained in the first.
5. **Quality bias.** Some low-quality figures over-report, some under-report, and the right
   correction differs per field. ADR-0005's `qualityBias` picks one figure by direction —
   but that pick is **lossy and happens mid-pipeline**, and a single field-level direction
   can't model bimodal or source-dependent bias.

Partial mechanisms already exist — ADR-0005's aggregation kinds + `qualityBias`, ADR-0006's
returnee stock/flow split + `estimateStockFlowTotal`, the `eventKey` incident grouping
(ADR-0002), the `max` kind for `population_affected`, and the divergence guard. But they all
operate on **points at a single date**, so the roots survive.

Those roots are two:

- **Root A — figures are intervals, but we treat them as points.** Bucketing by one date is
  the direct cause of #3, the cause of #2, and is entangled with #1 (a stock is a
  point-in-time reading; a flow is a quantity over an interval — the same object, a different
  reduction).
- **Root B — we sum unless proven otherwise, but the safe default is "assume overlap."**
  #4 (category overlap), #2 (temporal overlap), and #5 (bias) are one question: *do these two
  figures count the same people?* Summing is safe only when they provably do not; everywhere
  else the silent-double-count risk dominates.

This ADR decides the **data model** that removes both roots. It builds on ADR-0004
(source + credibility), ADR-0005 (data-quality + bias-aware selection), and ADR-0006
(location_metadata reconciliation), and it re-uses their machinery rather than replacing it.

## Decision

Model every numeric figure as a **value-range over a time-interval, tagged with a measure
type** — `{ value_low, value_high, qualifier, basis_period:[start,end], measure_type }` — and
make the time bucket a **query window** that figures are reduced *into*, not a storage key
they are assigned *to*. A point at a single date is the degenerate case.

### 1. `measure_type` is a first-class extracted attribute

Add `measure_type ∈ { stock_as_of, period_flow, cumulative_to_date }` to every extracted
`NumericField`, chosen by the extractor from linguistic cues ("total / currently / as of" →
stock; "new / additional / during" → flow; "since <date>" → cumulative). It is no longer
implicit in *which field* the LLM selects. A `cumulative_to_date` (and a `stock_as_of` on an
additive field) is a running total to its as-of date; at aggregation it is **first-differenced**
into the period increments it implies and reconciled with the reported flows (§4), so it is
neither summed as a flow nor added on top of the flows it already contains. *(This supersedes
this ADR's original "cumulative behaves as a stock and sidesteps overlap" framing — decided in
the #124 review, which chose first-differencing over cumulative-as-stock for cross-bucket
precision.)*

### 2. Figures carry `basis_period`; a bucket is a query window

Every figure keeps its true `[start, end]` (we already store `reportingPeriodStart/End`; we
start *using* them). A figure is **not** assigned to one bucket — it **contributes to every
standard window its interval intersects**, reduced per its `measure_type`:

- **Stock (`stock_as_of`)** → a *point* at its as-of date; lands in the single window containing
  that instant; latest-wins within a window (today's `latest_state`, keyed on the as-of date).
- **Cumulative (`cumulative_to_date`)** → a running total; **first-differenced** into per-interval
  flows (§4) so it contributes correctly to every window its span crosses, not only the one
  holding its as-of.
- **Flow** → an *interval quantity*; the window value is the interval reduction (§4).

The pre-computed `aggregated_datapoints` rows (one per `window × kind × location`) are
**unchanged in shape** — only the function that fills them changes. Consumers (situation
analysis) read the same rows.

### 3. Figures carry a value-range; aggregate in range-space, project late

Extract `value_low`, `value_high`, and a `qualifier` ("at least" / "up to" / "around" /
exact). The qualifier encodes a firm **per-figure bound** ("at least 500k" → floor, "up to" →
ceiling). It **composes** with the field-level `qualityBias` as a hard *constraint* rather than
replacing it: the floor and ceiling bound the projection in **both** directions and the bias
only breaks the tie inside `[floor, ceiling]`; opposing qualifiers (`at_least X` with `at_most
Y`, X > Y) are an impossible contradiction → fall back deterministically, never breach a bound.
*(Refined from "supersedes" in the #124 review — a qualifier and the field bias are different
axes that can disagree, so they compose.)*

Aggregation is performed **in range-space (lossless)**; the collapse to a single number is
**deferred to the consumer edge** and driven by bias-as-projection: an `overreport` field
projects to the **low** end, `underreport` to the **high** end, `neutral` to the
quality-weighted midpoint. The full range is always retained on the envelope. This turns
ADR-0005's bias from a destructive mid-pipeline pick into a late display choice, and makes
the aggregate's **range width** a first-class uncertainty signal alongside `data_quality`.

### 4. The reduction contract, per kind

The `eventKey` incident grouping (ADR-0002) still decides *who competes*; the change is the
reduction *within* and *across* groups:

- **Additive flow** — interval arithmetic: partition the timeline at all figure edges **and**
  all bucket boundaries; on each atomic sub-interval reconcile the covering figures'
  **rate-ranges** via the existing `pickWinner` (data-quality override, else `qualityBias`
  direction), multiply by the sub-interval length, and add to the window that contains it.
  One sweep handles overlap (#2) and boundary-spanning (#3), and re-uses ADR-0005's bias.
- **Cumulative / stock-as-of on an additive field** — a running total, not an increment. Sort
  the snapshots by as-of and **first-difference** them (`Cᵢ − Cᵢ₋₁`; a *drop* is a counter reset →
  `Cᵢ` is a fresh total; a *no-origin* earliest base spans the day ending at its as-of so it isn't
  reconciled away), then feed the derived increments through the same additive-flow sweep.
  Reported flows entirely inside a cumulative's coverage are subsumed (dropped); a partial-overlap
  flow keeps its outside portion, scaled. The increments telescope to the **latest** running total
  — the same "latest stock + forward flows since" model as `estimateStockFlowTotal` (clear-api,
  ADR-0006 §4), additionally distributed across buckets so sub-window queries are correct. Competing
  snapshots at the same as-of are reconciled to one before differencing.
- **Latest-state / stock** — **intersect** the ranges of comparable-quality bounds
  (independent bounds tighten the estimate); a clearly-higher-quality figure overrides;
  **non-overlapping** ranges are a contradiction → divergence signal.
- **Max / containment** — the superset figure's range wins; a subset whose range does not fit
  inside it is a contradiction → signal.

### 5. Containment (#4) is decided from `event_types` we already store

An **unqualified/empty** `event_types` set is treated as the **superset** → take the max
against a qualified sub-cause, do not add. Two **distinct qualified** sets are assumed
**disjoint** → sum. Unknown relationship defaults to **max, not sum** — undercounting a
genuinely-disjoint pair is recoverable (the breakdown is still stored); double-counting an
inclusive pair silently inflates the headline. A richer parent/child taxonomy is a later
refinement, not required for the headline case.

### 6. The divergence guard tests range-overlap, not a fixed percentage

ADR-0006 §7 fires on a fixed 25% delta between two points. In range-space the honest test is
**do the ranges overlap?** — overlapping ranges are not in conflict; **disjoint** ranges are.
Same for report-vs-anchor: an anchor inside the range tightens it; an anchor the range
excludes is the divergence. The percentage threshold is retired.

### 7. Staged rollout (see the design doc for the phase breakdown)

1. **Extract + store** `measure_type`, `basis_period`, `[low, high]`, `qualifier` — one
   schema-version bump, even while aggregation still projects to a point. Captures the
   information irreversibly; no downstream change.
2. **Derived confidence interval** from the spread of contributing figures — honest error
   bars into situation analysis without the full interval arithmetic (~70% of the value).
3. **Full interval-and-range reducer** — §2–§6 — gated on the range-combination math
   behaving on the real report mix.

## Consequences

**Positive**

- Removes both roots: bucketing distortion (#2, #3) and default-sum double-counting (#2, #4).
- `measure_type` makes the highest-cost extraction decision (#1) explicit and auditable.
- Bias (#5) becomes lossless-until-the-edge and evidence-based (the qualifier), and gains a
  robust alternative to directional picking (range combination, quality-weighted projection).
- Uncertainty **propagates** instead of being discarded and hidden behind a point + a detached
  score; the aggregate carries honest error bars — which humanitarian decisions want.
- Re-uses existing machinery: `pickWinner`/`qualityBias`, `eventKey` grouping, the divergence
  guard, the `aggregated_datapoints` cache shape, and the situation-analysis read path all
  stay; only the reducer and the refresh-invalidation set change.

**Negative / risks**

- **Range explosion.** Worst-case interval arithmetic widens with every additive sum;
  independence-assuming statistical combination is tighter but needs a distribution
  assumption. This is the hardest modelling call (design doc §7, sign-off item).
- **Extraction burden.** Two numbers + a qualifier + a measure type is more for the LLM to get
  wrong; a KB re-extraction cost applies (one schema bump).
- **Consumer surface.** Situation analysis, the dashboard, and the LLM narrative prompts must
  speak "range" (or point + interval).
- **Refresh cost.** A figure now invalidates every window its interval touches, not just the
  end-date's window — a bounded but real increase in recompute scope.

## Alternatives considered

- **Keep points; add a derived confidence interval only** (phase 2 alone). Cheaper and honest
  about spread, but cannot fix #2/#3 (still bucketed by one date) and keeps bias lossy. Adopted
  as an interim step, not the endpoint.
- **Probabilistic (distribution) figures instead of ranges.** Strictly more expressive
  (variances combine tighter than worst-case ranges) but demands a distributional assumption
  per figure the sources do not provide, and is far heavier to extract, store, and explain.
  Ranges are the pragmatic middle; a probabilistic upgrade can layer on later.
- **Full event-type containment taxonomy up front** (#4). Correct but blocks the headline fix
  on taxonomy work + sign-off; the unqualified-as-superset heuristic captures the common case
  now, with the taxonomy as a later refinement.
