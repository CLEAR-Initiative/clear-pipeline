# Implementation Plan — Data Quality & Location-Metadata Reconciliation

**Status:** Proposed · **Owner:** CLEAR data pipeline team

Implements ADR-0004 (source attribution + credibility), ADR-0005 (data-quality scoring +
bias-aware aggregation), and ADR-0006 (location-metadata reconciliation) across **clear-api**
(schema, aggregation, resolvers) and **clear-pipeline** (extraction, enrich,
aggregate refresh).

> **Note on ADR-0006:** it is **forthcoming** — it lands with the Location Metadata
> workstream PR, not the data-quality PRs. The returnee stock/flow *field split* it
> originally motivated ships with data quality and is documented in **ADR-0005 §4a**;
> the remaining ADR-0006 references below (§4, §7, §8) describe the not-yet-shipped
> reconciliation work and resolve once that PR merges.

---

## Two workstreams and how they depend

| | Workstream A — **Data Quality** | Workstream B — **Location Metadata** |
|---|---|---|
| ADRs | 0004 + 0005 | 0006 |
| Core idea | Attribute a source + credibility to every figure; combine into `data_quality`; make aggregation bias-aware | Read authoritative `location_metadata` at aggregation time to anchor / gap-fill / reconcile |
| Depends on | — | **A** (echo-dedup needs cited source from A; reconciliation reuses A's bias-aware selection) |

**B cannot ship before A's source-attribution + aggregation land.** So the order is A then B,
with one important optimisation across both:

> **Bundle every extraction-schema change into a single `schema_version` bump.**
> Source id, credibility criteria (A), *and* the returnee stock/flow split (B) all change the
> datapoint/enrich schema. A `schema_version` bump re-extracts the entire back-catalogue —
> expensive. Do it **once** (Phase 2), not three times.

---

## Phase 0 — Domain sign-off gate (no code; non-blocking for build)

These are **config/data**, so engineering can build against the *proposed* defaults now and
finalise values on sign-off. Track, don't wait:

| Item | Source | Doc |
|---|---|---|
| Source reliability seed grades | ADR-0004 §5 | Data-Quality sign-off doc §5.2 |
| Credibility criteria weights | ADR-0004 §4 | §6.1 |
| Per-field quality-bias map | ADR-0005 §3 | §7.1 |
| Validity windows + override divisors | ADR-0005 §table | §7.2 |
| Divergence threshold (25 %) + symmetry | ADR-0006 §7 | Reconciliation sign-off §9 |
| Stock-and-flow model confirmation | ADR-0006 §4 | Reconciliation sign-off §9 |

---

## Phase 1 — clear-api schema & source registry (Workstream A foundation)

| Ticket | Repo | Scope | Depends | Acceptance |
|---|---|---|---|---|
| **A1a** | clear-api | Prisma migration: `dataSources` += `synonyms String[]`, `reliability Int?`; add `type` value `"organisation"`; `report_datapoints` += `sourceId` FK. Enable `pg_trgm` extension. | — | Migration applies; FK + columns present; `pg_trgm` available |
| **A1b** | clear-api | `resolveDataSource(name, homepage?) → id` resolver (mirror `resolveKnowledgebaseLocation`): exact/synonym → `infoUrl` → `pg_trgm` fuzzy (≥0.6) → create ungraded. | A1a | DTM/OCHA name variants resolve to one id; unknown name creates `reliability=null` row |
| **A1c** | clear-api | Reliability **seed script** (ADR-0004 §5 grades). Ships proposed defaults; editable post-sign-off. | A1a | Seed populates grades; re-runnable |
| **B1-schema** | clear-api | Add `returnee_stock` + `new_returns` to the aggregation field list (retire `returnees`); add `data_quality` + intrinsic-credibility fields to the aggregated-datapoint cache shape. | A1a | New fields present; old `returnees` path removed |

**Manual (user):** run the migration and the seed script (per standing rule — I write, you run).

---

## Phase 2 — Extraction schema — ONE `schema_version` bump (A + B)

> **Decided:** bump the version and **fully re-extract** the back-catalogue (not forward-only
> V1). Rationale: the returnee stock/flow fix is a correctness bug un-backfillable from stored
> data, and unscored history would be silently downranked. Big-bang re-extraction keeps the
> strict same-version aggregation guard — no version-tolerance code needed.

| Ticket | Repo | Scope | Depends | Acceptance |
|---|---|---|---|---|
| **A2** | clear-pipeline | Add `source` to `enrich.py::ExtractedParameters`, `datapoints_schemas.py::NumericField` (cited source id, default = publisher), and chunk tags. Wire ReliefWeb `report.source` → `report_datapoints.sourceId` (primary else first). Call `resolveDataSource` during enrich/extraction. | A1b | Every datapoint carries a source id; publisher recorded; chunk tags carry source |
| **A3** | clear-pipeline | Add 7 intrinsic credibility criteria to per-datapoint extraction (directness = existing `confidence`; + attribution, consistency, plausibility w/ crisis brief, specificity, methodology, representativeness). One document-level fallback assessment per report. Store on `NumericField` + `report_datapoints`. | A2 | Each datapoint has criteria; doc-level fallback stored; KB **cost guardrail** verified |
| **B1-extract** | clear-pipeline | Split returns into **stock** vs **flow** in the extraction schema (feeds `returnee_stock` / `new_returns`); clarify `idp_stock` vs `new_displacements`. | A2 | Returns no longer conflated at extraction |

Bump `schema_version` **once** covering A2 + A3 + B1-extract.

**Manual (user):** trigger re-extraction of the back-catalogue (schema-version bump drives it).

---

## Phase 3 — Data-quality aggregation core (Workstream A)

| Ticket | Repo | Scope | Depends | Acceptance |
|---|---|---|---|---|
| **A4** | clear-api | `data_quality = ((reliability×2.5) × info_credibility)/10` per contributor. Cache the **time-invariant** part (7 intrinsic criteria + source id/reliability + `newest_report_at`); resolver computes **Recency live** and finalises `data_quality` on read (both cached + on-demand paths). Retain confidence-only as a *directness* view. | Ph2 | `data_quality` computed; no silent recency decay; directness view retained |
| **A5** | clear-api | Per-field `quality_bias` map. Generalise `latest_wins_with_confidence_override` → `data_quality` + bias: override gate = `window/x`; comparable-quality (`|Δ|<1.0`) tiebreak by bias; `max` fields drop-bottom-quartile then max. Keep pure `latest_wins` for state fields. Field→window table. | A4 | Winner selection keys on `data_quality`+bias; `max` quartile-gated |
| **A5-retro** | clear-pipeline + clear-api | Retrospective trigger: refresh **union** of rolling window + `[min…max reportingPeriodEnd]` of the batch (`datapoints_aggregate.py`). Situation analysis: refresh **computed figure components**, not narrative. | A5 | Retrospective report refreshes its old bucket; situation figures refresh without narrative regen |
| **A-tests** | clear-api | Extend the aggregation test suite (existing + bias / quartile / override-window / retrospective cases). | A5-retro | Green suite incl. new cases |

---

## Phase 4 — Location-metadata reconciliation (Workstream B; needs Ph2 + Ph3)

| Ticket | Repo | Scope | Depends | Acceptance |
|---|---|---|---|---|
| **B2** | clear-api | Direct read of current `location_metadata` per bucket location at aggregation time. **Per-source adapters** to extract the canonical figure from each blob (DTM→`idp_stock`; UNHCR→`refugees`/`returnee_stock`; OCHA HPC→PIN; OCHA FTS→funding; IPC→food-security). API-contributor **deterministic credibility profile** (ADR-0006 §8). Recency = `now − valid_from` of the open version. | A5 | API figures enter aggregation; gaps filled |
| **B2-dedup** | clear-api | Dedup grouping `(canonical source, metric, area, period)`; collapse report **echoes** into the API group (latest within group). | B2 | Report echo of an API figure does not double-count |
| **B3** | clear-api | Read-time **estimated current total** at country/all-time: latest stock + Σ flows after T₀ (reference date, not `valid_from`). Overlapping flows → **max with data-quality override**. | B2 | Current total = stock + forward flows; overlaps maxed not summed |
| **B4** | clear-api | Divergence guard: report vs API > **25 %** → API wins; emit **early-warning signal** (don't swallow). Symmetry per sign-off. | B2-dedup | Divergent report overridden; signal surfaced |
| **B-tests** | clear-api | Reconciliation tests: gap-fill, echo-dedup, stock+flow forward-sum, overlap-max, divergence guard. | B4 | Green suite |

---

## Phase 5 — Cross-cutting consumers & eval

| Ticket | Repo | Scope | Depends |
|---|---|---|---|
| **X1** | clear-pipeline | Regenerate the model-replacement **eval reference schema** (new source + credibility + stock/flow fields). | Ph2 |
| **X2** | clear-mvp / clear-api | Update consumers of `quality_score` / `dataQualityScore` (dashboard, situation analysis, GraphQL surface) for the new `data_quality` meaning + the returnee-field split + the divergence signal. | Ph3, Ph4 |

---

## Dependency summary

```
Phase 0 (sign-off, non-blocking)
Phase 1 (clear-api schema + registry)
        └─> Phase 2 (ONE schema_version bump: source + credibility + stock/flow) ──> re-extract
                    └─> Phase 3 (data-quality formula, bias-aware selection, retrospective)
                                └─> Phase 4 (location-metadata reconciliation)
                    └─> Phase 5-X1 (eval regen)   Phase 3+4 ──> Phase 5-X2 (consumers)
```

## Manual / gated steps (I write, you run — per standing rules)

- Phase 1: run the schema migration + reliability seed.
- Phase 2: trigger the back-catalogue re-extraction (schema-version bump).
- Phase 0: finalise the six sign-off items; edit the seed/config once values land.
- All git via `/start-ticket` + `/ship-ticket`; nothing committed/pushed without explicit approval.

## Testing & rollout notes

- **Aggregation is the highest-risk change** — every numeric field's winner selection moves.
  Land A5 behind the existing suite + new cases before B builds on it.
- **Re-extraction cost** is the main spend — the single bump in Phase 2 is the guardrail.
- **`pg_trgm`** must exist before the fuzzy source matcher ships (Phase 1).
- **API-contributor credibility profile** and the **25 % threshold** are config — safe to tune
  after launch without redeploying logic.

## Suggested ticket order

`A1a → A1b → A1c → B1-schema` · `A2 → A3 → B1-extract` (one bump) · `A4 → A5 → A5-retro → A-tests` ·
`B2 → B2-dedup → B3 → B4 → B-tests` · `X1 (after Ph2)` · `X2 (after Ph3+Ph4)`
