# Interval-and-Range Datapoint Model — Design for Sign-off

**Status:** Proposed · **Decision record:** [ADR-0007](adr/0007-figures-as-ranges-over-intervals.md) ·
**Builds on:** ADR-0004 (source + credibility), ADR-0005 (data-quality + bias-aware
selection), ADR-0006 (location_metadata reconciliation)

---

## 1. Purpose

Five recurring aggregation problems (stock-vs-flow, overlapping periods, bucket-boundary
spanning, subset/superset figures, and per-type quality bias) share two root causes: we
model each figure as a **single value at a single date**, and we **sum unless proven
otherwise**. This document specifies the model that removes both — figures become
**value-ranges over time-intervals, tagged by measure type** — with the schema deltas, the
reducer algorithm, worked examples, the consumer/refresh impact, the open modelling calls,
and a staged rollout. It is written for management review + sign-off before build.

## 2. Executive summary

- A figure becomes `{ value_low, value_high, qualifier, basis_period:[start,end], measure_type }`.
  A point at a date is the degenerate case, so nothing regresses for well-behaved figures.
- A **time bucket becomes a query window**, not a storage key. A figure contributes to every
  standard window its interval intersects; stocks as points, flows by interval integration.
  The `aggregated_datapoints` cache shape and the situation-analysis read path **do not change**.
- Aggregation runs **in range-space (lossless)**; the collapse to one number is **deferred to
  the consumer**, driven by bias-as-projection. ADR-0005's bias stops being a mid-pipeline
  destructive pick.
- The overlap/boundary/bias handling **re-uses** the existing `pickWinner` + `qualityBias` +
  `eventKey` grouping + divergence guard; the two genuinely new pieces are the breakpoint
  reducer and an extended refresh-invalidation set.
- Rollout is staged so the first, cheapest phase (extract + store the richer figure) is
  irreversible-in-a-good-way and unblocks everything, while the heavy interval-arithmetic
  reducer is gated on the range-combination math behaving on real data.

## 3. The problem, in one picture

| # | Symptom | Root |
|---|---|---|
| 1 | stock misread as flow (or vice-versa) → under/over-count | measure type implicit at extraction |
| 2 | overlapping report periods double-count on additive fields | figure = point at end-date |
| 3 | a 25 Apr–5 May figure mis-bucketed into one week/month | figure = point at end-date |
| 4 | "1M killed" + "100k drone deaths" summed to 1.1M | default-sum across event groups |
| 5 | bias correction is lossy + one-size-per-field | bias applied as a mid-pipeline pick |

Root A (figures are intervals treated as points) drives #1–#3; Root B (default-sum where the
safe default is assume-overlap) drives #2, #4, #5. See ADR-0007 for the full framing.

## 4. The model

A numeric figure is a **value-range over a time-interval, tagged by measure type**:

```
NumericField {
  value_low:   number          # lower bound of the reported magnitude
  value_high:  number          # upper bound (== value_low for an exact figure)
  qualifier:   "exact" | "at_least" | "at_most" | "approx"
  basis_period: [start, end]   # the period the figure describes (a point when start==end)
  measure_type: "stock_as_of" | "period_flow" | "cumulative_to_date"
  # …existing: unit, confidence, scope_location_id, source_id, credibility (ADR-0004)…
}
```

Two orthogonal axes — **value-range** `[low,high]` (magnitude uncertainty) and **time-interval**
`[start,end]` (period) — so a figure is a 2-D object; a flow's per-day **rate** is itself a
range `[low/days, high/days]`. `measure_type` routes a figure to the right reduction; the
`qualifier` encodes per-figure bias direction ("at_least" → firm lower bound).

## 5. Schema deltas

- **Extraction** (`datapoints_schemas.py::NumericField`): add `value_low`, `value_high`,
  `qualifier`, `measure_type`. `basis_period` reuses the report's `reporting_period_start/end`
  where a figure does not state its own. One `SCHEMA_VERSION` bump (full re-extraction).
- **`report_datapoints`** (clear-api): the JSON blob carries the new fields; no column change.
- **Aggregated envelope** (`QualityEnvelope`): the per-field value becomes
  `{ value_low, value_high, value_point, … }` where `value_point` is the projected headline
  (§8) kept for compatibility; add `range_width` as an uncertainty signal beside `data_quality`.
- **`aggregated_datapoints` rows**: unchanged in shape.

## 6. The reducer — a bucket is a query window

A figure is not "allotted to a bucket." Each **standard window** (ISO week, calendar month,
year, all-time — the existing bucket grid) is computed by **reducing the figures whose
interval intersects it**. Pre-computation into `aggregated_datapoints` is unchanged; only the
fill function changes. `eventKey` (ADR-0002) still decides *who competes*.

### 6.1 Stocks

A `stock_as_of` / `cumulative_to_date` figure is a **point** at its as-of date. It lands in
the single window containing that instant; **latest-wins** within a window (today's
`latest_state`, keyed on the as-of date rather than "which bucket the end-date fell in"). A
point cannot span a boundary, so #3 does not arise for stocks. Competing bounds are
**intersected** (§7.2).

### 6.2 Flows — the breakpoint-partition sweep (handles #2 and #3 together)

For a flow field, one location, one event-group, over the refresh range:

1. **Breakpoints** = every figure edge **∪** every bucket boundary in range. Making bucket
   boundaries breakpoints guarantees each atomic sub-interval lies inside exactly one bucket
   and is covered by a fixed set of figures.
2. For each atomic sub-interval `[bᵢ, bᵢ₊₁)`:
   a. covering figures = those whose `[start,end] ⊇` it; each contributes a **rate-range**
      `[low/days, high/days]`.
   b. reconcile the covering rate-ranges via the existing **`pickWinner`** (data-quality
      override, else `qualityBias` direction) → one reconciled rate-range.
   c. `contribution = reconciled_rate × days(sub-interval)`.
   d. add it to the bucket that contains the sub-interval.
3. Each bucket's flow = Σ its sub-intervals' contributions (interval arithmetic).

### 6.3 Worked example (killed; same location + event type; weekly buckets)

- A = `[2 Apr, 10 Apr]`, 800 → 100/day. B = `[5 Apr, 15 Apr]`, 660 → 66/day. `killed` is
  **overreport**.
- `[2,5)` — only A → 100/day × 3 = **300**
- `[5,10)` — A & B overlap → overreport bias takes the **lower** rate 66/day × 5 = **330**
- `[10,15)` — only B → 66/day × 5 = **330**
- **Total = 960.** (Naïve sum = 1460, double-counting the overlap; naïve max = 800, losing the
  disjoint tails.) Each sub-interval is added to the week that contains it, so a period that
  straddles two weeks splits correctly — **#3 falls out of the same loop.**

With ranges, step 2b keeps the disagreement *in* the band rather than collapsing it: the
overlap sub-interval's contribution is a **rate-range** reconciled by bias, and the bucket
total is a range (§7).

## 7. Range operations, per kind

| Kind | Range operation | Notes |
|---|---|---|
| **Additive flow** (disjoint) | `[a,b] + [c,d] = [a+c, b+d]` | ranges add |
| **Overlap** (same span) | biased combine of the covering **rate-ranges** | disagreement stays in the band |
| **Latest-state / stock** | **intersection** of comparable-quality bounds | independent bounds *tighten*; non-overlap → contradiction |
| **Max / containment** (#4) | superset range wins; subset must fit inside | `subset_low > superset_high` → contradiction |

### 7.1 Additive
`[500k,700k] + [200k,300k] = [700k,1.0M]`.

### 7.2 Stock intersection — the sleeper feature
Two reports bounding the same stock: `[500,700] ∩ [600,800] = [600,700]` — **tighter than
either input**. Independent partial bounds sharpen the estimate instead of one overwriting the
other. When they **don't** overlap (`[500,600]` vs `[700,800]`) it is a genuine contradiction →
divergence signal (§9). A clearly-higher-quality figure overrides rather than intersects.

### 7.3 Containment / max (#4)
An **unqualified/empty** `event_types` set is the **superset** → take the max against a
qualified sub-cause, do not add. Two **distinct qualified** sets → assume disjoint → sum.
Unknown → **max, not sum** (undercount is recoverable; double-count is silent and permanent).
A parent/child `disaster_types` taxonomy is a later refinement, not required for the headline.

## 8. Bias as a late projection (not a mid-pipeline pick)

Aggregate in range-space; **project to one number only at the consumer edge**:

- `overreport` field → project to the **low** end (conservative against inflation)
- `underreport` → the **high** end
- `neutral` → the quality-weighted midpoint

The full `[low,high]` stays on the envelope; `range_width` is a first-class uncertainty signal
alongside `data_quality`. The `qualifier` gives the direction **per figure** where the source
stated it ("at_least" → truth ≥ reported); the field/source `qualityBias` prior is the fallback.
Quality modulates **width** — a low-quality point is *widened* into an implied range
(`effective_range ≈ extracted_range widened by (1 − quality)`); a high-quality one stays tight.

## 9. Divergence guard on range-overlap

ADR-0006 §7's fixed 25% delta is replaced by an **overlap test**: overlapping ranges are not in
conflict; **disjoint** ranges are, and emit the early-warning signal. Report-vs-anchor: an
anchor (DTM/IPC/FTS) inside the range **tightens** it; an anchor the range **excludes** is the
divergence. This removes the arbitrary threshold and is strictly more principled.

## 10. What changes vs. what stays

| | Today | Interval-and-range model |
|---|---|---|
| Raw figure | value + `reportingPeriodEnd` | `[low,high]` + `qualifier` + `[start,end]` + `measure_type` |
| Aggregation | assign to 1 bucket, sum groups | intersect interval with each window; reduce (flows) / point-place (stocks); range ops |
| Bias | lossy pick mid-pipeline | lossless range-space; project at the edge |
| `aggregated_datapoints` rows | one per window×kind×location | **unchanged** |
| Situation-analysis read | reads bucket rows | **unchanged** (gains an optional range) |
| Incident grouping (`eventKey`) | governs combine | **unchanged** — only within/across-group reduction changes |
| Divergence guard | 25% on points | range-overlap test |
| Refresh trigger | invalidate the end-date's window | invalidate **every window the interval touches** |

Blast radius: the reducer + the refresh-invalidation set + the consumer's range display.
Storage shape, cache model, and read path stay put.

## 11. The hard modelling calls (need a decision)

1. **Range combination method.** Worst-case interval arithmetic (`[a,b]+[c,d]`) is safe but
   **widens** with every additive sum — 50 figures → a uselessly wide band.
   Independence-assuming statistical combination (variances add, ~√n narrower) is tighter but
   assumes a distribution. **Proposed:** treat extracted ranges as ~90% intervals and combine
   assuming partial independence, capped by worst-case. *This is the single biggest sign-off
   item.*
2. **Open/one-sided ranges — DECIDED: no open ends; capture a finite band at extraction.**
   An open range (`[500k, ∞)`) is deliberately-unusable data — it blows up every range
   operation and forces an arbitrary downstream cap anyway. So the extractor captures a
   **finite, evidence-based band at capture time** instead: `approx` → a modest symmetric band
   sized to the wording ("an estimated 600" ≈ [570,630], "roughly 600" ≈ [500,700]); `at_least`
   → the stated **floor** + a plausible upper bound inferred from the report's own figures and
   context (never ∞); `at_most` → the stated **ceiling** + a plausible lower bound (never 0). The
   width is the model's judgement from the source, not a fixed constant, so no plausibility-
   ceiling knob is needed downstream. (This supersedes the earlier "cap and render 500k+" proposal.)
3. **Contradiction policy** (non-overlapping bounds for one stock). **Proposed:** higher quality
   wins the value, always emit the divergence signal, never silently widen to the union.
4. **Rate-uniformity.** The flow reducer assumes a figure's magnitude is spread evenly over its
   period. **Mitigation:** `cumulative_to_date` figures bypass the rate math entirely (treated as
   stocks); route as many fields through that path as sources allow.

## 12. Staged rollout

| Phase | Scope | Value | Cost |
|---|---|---|---|
| **1 — Capture** | Extract + store `measure_type`, `basis_period`, `[low,high]`, `qualifier`; aggregation still projects to a point | Information captured irreversibly; unblocks 2 & 3; fixes #1 | One schema bump + re-extraction; **no** downstream change |
| **2 — Error bars** | Derived confidence interval from the *spread* of contributing figures; surface a range in situation analysis | ~70% of the honesty win; no interval arithmetic | Aggregator + consumer display only |
| **3 — Full reducer** | Breakpoint interval reducer (§6), range ops (§7), late bias-projection (§8), overlap-test divergence (§9), containment (§7.3), extended refresh-invalidation | Fixes #2–#5 fully | The heavy lift; gated on §11.1 behaving on the real report mix |

Phase 1 is recommended **unconditionally** and belongs in the *same* schema bump as any other
pending extraction change (avoid a second full re-extraction). Phases 2 and 3 are independently
shippable.

## 13. What needs sign-off

1. **Adopt the model** (ADR-0007): figures as value-ranges over time-intervals, tagged by
   measure type; buckets as query windows; aggregate lossless, project late.
2. **Range-combination method** (§11.1) — the ~90%-interval, partial-independence proposal.
3. **Contradiction policy** (§11.3). *(Open ranges are decided — §11.2: finite band captured at
   extraction, no open ends, no downstream ceiling knob.)*
4. **Containment default = max, not sum** for unknown event-type relationships (§7.3).
5. **Phase 1 in the next schema bump**, unconditionally.
6. **Consumer commitment**: situation analysis + dashboard + LLM narrative prompts will speak
   "range" (or point + interval) from phase 2 on.

## 14. Assumptions & scope (to confirm during build)

- The extractor can reliably lift `[low, high]` + `qualifier` + `measure_type` from prose. Phase
  1 measures this before phases 2–3 depend on it.
- `basis_period` is available (figure-stated, else the report's reporting period). Figures with
  no derivable period fall back to point-at-publication with a flag.
- Rate-uniformity is an accepted estimate for genuine `period_flow` figures; `cumulative_to_date`
  is preferred wherever a source frames it that way.
- No consumer reads the raw per-figure point in a way that a projected `value_point` cannot
  satisfy (verify against clear-mvp + the chatbot before phase 3).

## Related materials (held by the engineering team)

- ADR-0007 — the decision record for this model.
- `clear-api/src/services/datapoint-aggregation.ts` — the reducer this design modifies.
- ADR-0005 / `data-quality-scoring-design.md` — the bias + kinds machinery re-used here.
- ADR-0006 / `location-metadata-reconciliation-design.md` — the anchoring + divergence guard
  this design extends to ranges.
