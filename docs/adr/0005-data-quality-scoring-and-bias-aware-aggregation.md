---
status: proposed
---

# Data-quality scoring: `source_reliability × information_credibility`, with bias-aware aggregation

## Context

Aggregation (`clear-api/src/services/datapoint-aggregation.ts`) currently expresses
"quality" as **confidence only**: each field's `quality_score` is the mean of
`CONFIDENCE_WEIGHTS[winner.confidence]`, and the bucket `dataQualityScore` is the mean of
those. That ignores *who* the source is and *how credible* the document is, and it treats
every low-quality figure as if it erred in the same direction.

ADR-0004 makes two new signals available per datapoint: a **source** (→ a `reliability`
grade on `dataSources`) and an **information credibility** score. This ADR decides how
they combine into a single **data quality** score, how that score replaces the
confidence-only one, and — the load-bearing part — how quality-weighting must respect the
**direction** in which each field's low-quality figures err.

## Decision

### 1. The formula (replaces confidence-only quality)

```
data_quality = ((source_reliability × 2.5) × information_credibility) / 10        # 0–10
```

- `source_reliability ∈ 1..4` (ADR-0004 registry) → ×2.5 → 2.5–10; `null → 1`.
- `information_credibility ∈ 0..10` (ADR-0004 rubric).
- `data_quality` is computed **per contributing datapoint** and replaces the
  confidence-only `quality_score` / `dataQualityScore`. `confidence_mix` and a
  confidence-only score are retained as a *directness* view for the UI, but `data_quality`
  is the headline.

### 2. Recency is applied at **read time**, not baked into the cache

`information_credibility` includes Recency (ADR-0004), which depends on *now*. If final
`data_quality` were frozen into the `aggregated_datapoints` cache at recompute time, it
would **decay silently** — a bucket untouched for weeks would carry a stale Recency purely
because time passed. Therefore:

- The cached bucket stores the **time-invariant part**: the 7 intrinsic credibility
  criteria (→ a partial credibility score), the source id/reliability, and
  `newest_report_at`.
- The **resolver computes Recency live** (`now` vs the field's validity window) and
  finalises `data_quality` on read — on both the cached and the on-demand rollup paths.

Recency scoring: **met** if `now − reportingPeriodEnd ≤ window`, **partial** if `≤ 2×
window`, **unmet** beyond (window per field, § table below). This rewards freshly
*published* figures — including a fresh, reconciled report describing an *old* period —
without penalising old *content* (content is placed by period into its bucket).

This also makes the previously-documented `is_stale` flag (§6.5) redundant: "no fresh
reports on the current bucket" now shows up as **low Recency → low `data_quality`**, which
is continuous and flows into the number rather than sitting beside it. **`is_stale` is
dropped**; only `newest_report_at` / `oldest_report_at` remain (a UI can still derive an
"as of N days ago" label).

### 3. Per-field quality **bias** — low-quality figures don't err symmetrically

Add a per-field `quality_bias ∈ {overreport, underreport, neutral}`. For some fields
low-quality figures skew **high** (media inflation, widest-reach claims); for others
**low** (incomplete capture, access-limited undercount). Quality-weighting must know the
direction.

| Field(s) | Bias | Rationale |
|---|---|---|
| `needs_and_funding.overall_affected` (`max`) | overreport | widest-reach / round-up claims |
| `casualties.killed` / `injured`, `access_and_incidents.aid_workers_killed` | overreport | media inflation of tolls |
| `displacement.{idp_stock, new_displacements, returnees, refugees}` | underreport | incomplete movement capture |
| `needs_and_funding.*.people_in_need`, `overall_pin` | underreport | access-limited undercount |
| `access_and_incidents.security_incidents_count` | underreport | under-recorded incidents |
| `needs_and_funding.overall_funding_{required,received}_usd` | neutral | reported precisely |

### 4. Bias-aware winner selection (generalises the confidence override)

`data_quality` replaces `confidence` in winner selection, and the existing
`latest_wins_with_confidence_override` generalises:

- The **freshest** (latest `publishedAt`) row wins **unless** another row has meaningfully
  higher `data_quality` **and** is not too old to be relevant:
  `publishedAt(freshest) − publishedAt(other) ≤ validity_window(field) / x`.
  The fixed **3-day window is dropped** — the gate is now the per-field
  `validity_window / x`.
- On **comparable quality** (`|Δ data_quality| < D`, `D = 1.0`), the tiebreak goes toward
  truth per bias: **lower** value for `overreport`, **higher** for `underreport`
  (`neutral` → keep freshest).
- For **`max` fields** (`overall_affected`): do not take the raw max — **drop the bottom
  quartile** of contributing figures by `data_quality`, then take the max of the rest, so
  a single low-quality outlier can't set the ceiling.
- `latest_wins` (pure) is kept for point-in-time **state** fields where freshness is
  definitional (`idp_stock`, `refugees`, sector/overall PIN, funding required) — quality/
  bias overrides apply to the additive + `max` fields.

### 5. Retrospective updates: trigger the refresh on the batch's period span

The cascade already propagates a bucket recompute up the window tiers
(`higherTierWindows`) and the location tree (`ancestorChain`), and into situation-analysis
invalidation. The gap is the **trigger**: `datapoints_aggregate.py` refreshes a rolling
`[now − lookback, now]` window keyed on `reportingPeriodEnd`, so a report published *now*
about an *old* period falls outside it and its old bucket never recomputes.

**Fix:** refresh the **union** of (a) the rolling recent window and (b)
`[min … max reportingPeriodEnd]` of the reports that actually landed this run. A
retrospective report then refreshes its correct old bucket, and the existing cascade does
the rest.

### 6. Situation analysis: refresh values, not narrative

A retrospective bucket change must **update the structured figure components** of the
affected situation analysis (recomputed from the revised aggregates) but **not regenerate
the LLM narrative** for small updates — the prose is a point-in-time snapshot (optionally
flagged "figures revised"). This requires the situation analysis to expose figures as
structured data the dashboard renders directly; the seven-component structure already
separates computed components from the LLM narrative (2–6), so the computed components are
refreshed and the narrative components are left. Full narrative regeneration stays on the
normal periodic cadence.

### Field → validity window (used by Recency §2 and the override gate §4)

| Field(s) | Category | Window | `x` |
|---|---|--:|--:|
| `casualties.{killed,injured}`, `security_incidents_count`, `aid_workers_killed` | Conflict events | 7 d | 2 |
| `displacement.{idp_stock, new_displacements, returnees, refugees}` | Displacement | 30 d | 3 |
| `overall_affected` | Operational updates | 30 d | 3 |
| `overall_funding_{required,received}_usd` | Operational updates¹ | 30 d | 3 |
| `*.people_in_need`, `overall_pin` | Needs assessments | 90 d | 3 |
| `food_security.people_in_need` | IPC / Cadre Harmonisé | ~120 d | 2 |
| `event_types`, `active_clusters` (labels) | — | n/a | — |

¹ Funding follows appeal cycles (quarterly-ish); 30 d is conservative.

## Consequences

- **Aggregation math changes** for every numeric field: winner selection now keys on
  `data_quality` + bias, and `max` is quartile-gated. The change is defensibility-positive
  but must be regression-tested (the existing 45 tests + new bias/quartile/override-window
  cases).
- **Read-time `data_quality`** means the resolver does a small extra computation and the
  cache stores partial (intrinsic) credibility — no forced recompute cadence, no decay.
- **Consumers** of `quality_score` / `dataQualityScore` (dashboard, situation analysis,
  GraphQL surface) change meaning → update when this lands.
- The model-replacement **eval** datapoint reference schema changes (new fields) → eval
  regenerate.
- **Non-goal / limitation:** for `underreport` fields, quality-weighting corrects *low
  values* but cannot recover *missing incidents* (a weak source omitting a district). That
  is a coverage problem, out of scope here.
- The per-field `quality_bias` map is domain judgment; it is data/config, editable as the
  corpus teaches us more.

## Related

- ADR-0004 — the source registry + information-credibility rubric this consumes.
- ADR-0006 — reconciles authoritative `location_metadata` into this aggregation (stock/flow
  split, divergence guard, API-contributor credibility profile).
- ADR-0001 / ADR-0002 — affected-from-reports and figure-scope dedup, the aggregation this
  extends.
- `docs/humanitarian-datapoint-extraction.md` §6.1–6.5 — reworked by this ADR (confidence →
  data quality, drop `is_stale`, retrospective trigger).
