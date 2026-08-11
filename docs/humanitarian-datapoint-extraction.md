# Humanitarian Datapoint Extraction — Design Doc

**Status:** Proposed
**Owner:** Platform team
**Audience:** Product + Engineering leadership, Data Science
**Related:** [Knowledge-base vector RAG design](./knowledgebase-vector-rag.md) *(existing, in production)*

---

## 1. Executive Summary

We propose a structured datapoint extraction pipeline that runs in parallel with the existing vector RAG knowledge base. From every ReliefWeb (and future manually-uploaded) humanitarian report, an LLM extracts a fixed, versioned schema of quantitative datapoints — casualties, displacement, needs, funding, access status, and ~50 more — together with per-field provenance and confidence. These per-report datapoints are then aggregated into pre-computed roll-ups (weekly per admin-2, monthly per admin-1, yearly and all-time per country) that power the situation-analysis dashboard and low-cost LLM queries.

The vector RAG surface is preserved for open-ended, synthesis-style queries ("what are the main humanitarian concerns in Kordofan?"). Structured datapoints become the deterministic layer for factual queries ("how many people were displaced in Kordofan last week?") and for narrative generation that requires grounded numbers rather than freeform reasoning.

**Expected outcome:** the situation-analysis dashboard queries a Postgres row in <200ms instead of running a multi-second vector retrieval + LLM synthesis for numbers that are, in principle, deterministic.

---

## 2. Problem Statement

The current knowledge-base pipeline embeds report chunks into a pgvector column and serves hybrid dense + BM25 retrieval. This works well for narrative queries where the caller wants relevant excerpts. It works poorly for:

- **Factual number queries** — the LLM synthesises "45k displaced" from retrieved chunks; a second query gets "46k" from a slightly different set. Non-deterministic answers on numbers that a spreadsheet would return identically.
- **Aggregation across many reports** — asking "how many people were displaced in Sudan in Q2 2026" requires retrieving hundreds of chunks, deduplicating overlapping incidents, and reconciling conflicting figures. This is not what vector RAG is built for.
- **Dashboard latency** — a situation-analysis dashboard cannot afford a 3–5 second synthesis for every number rendered on the page.
- **Cost per query** — each dashboard load runs multiple LLM synthesis calls. Structured cache is orders of magnitude cheaper.

We need a second pipeline layer whose job is to reduce reports into a structured, quantifiable form.

---

## 3. Solution Overview

Three **read layers** on the query side, ordered fastest → slowest and cheapest → costliest. Layers 1 and 2 are what this doc proposes. Layer 3 is the vector RAG that already exists in production.

### 3.1 Ingest architecture

A single ingest pipeline fans out to feed the storage backing Layers 1–3. The PDF text-extraction step is shared with the existing vector RAG chain, so no PDF is fetched or extracted twice.

```
┌──────────────────── report (PDF in S3) ─────────────────────┐
│                                                             │
│           pdf_text  (shared text extraction)                │
│                    │                                        │
│         ┌──────────┴───────────┐                            │
│         ▼                      ▼                            │
│   chunks → enriched      domain-partitioned                 │
│   → embeddings           LLM extraction                     │
│         │                (6 sub-schema calls)               │
│         │                      │                            │
└─────────┼──────────────────────┼────────────────────────────┘
          ▼                      ▼
   knowledgebase          report_datapoints          [Layer 2 storage]
   (vectors)              (structured per-doc)
   [Layer 3 storage]              │
                                  ▼  (weekly Dagster asset,
                                  │   §6.6 pre-computes 4 tiers)
                                  ▼
                          aggregated_datapoints       [Layer 1 storage]
                          weekly × A2 (atomic)
                          monthly × A1
                          yearly × country
                          all-time × country
```

### 3.2 Read layers — query routing

The dashboard / chatbot backend picks the appropriate layer per query:

| Layer | Backed by | Latency | LLM at read time? | Use for |
|---|---|---|---|---|
| **Layer 1** — Aggregated cache **(NEW)** | `aggregated_datapoints` | < 200 ms cached, < 300 ms on-demand rollup | No | Dashboard numbers, threshold alerts, factual chatbot queries with country / date / sector filters |
| **Layer 2** — Per-report cache **(NEW)** | `report_datapoints` | < 100 ms | No | "What did report X specifically say?" Provenance drill-down for citations. Fallback when Layer 1 is stale |
| **Layer 3** — Vector RAG **(EXISTING)** | `knowledgebase` (pgvector) | 1–5 s | Yes (embedding + synthesis) | Narrative synthesis, open-ended questions, "deep thinking" queries where the answer is prose and requires reasoning across many reports |

**Routing rules:**

- Factual number query → **Layer 1** (falls through to Layer 2 or Layer 3 if the number isn't cached).
- Narrative query → **Layer 3**.
- Numeric + narrative ("summarise the situation in Kordofan with figures") → hybrid: Layer 1 / 2 supplies grounded numbers; Layer 3 retrieves the narrative excerpts; the LLM composes a single answer.

The chatbot backend classifies incoming queries into these three buckets (heuristic first, LLM classifier as a follow-up — see §13).

---

## 4. Data Model

Two new tables in `clear-api`'s Postgres.

### `report_datapoints` — one row per report

```prisma
model reportDatapoint {
  id                     String   @id @default(cuid())
  reportId               String   @unique @map("report_id")   // FK to knowledgebase.report_id
  reportTitle            String   @map("report_title")
  sourceUrl              String   @map("source_url")
  publishedAt            DateTime @map("published_at")

  // Distinguish reporting period (what the CONTENT describes) from
  // publication date. Sitreps typically describe a 2–6 week window
  // preceding publication; conflating these breaks aggregation.
  reportingPeriodStart   DateTime? @map("reporting_period_start")
  reportingPeriodEnd     DateTime? @map("reporting_period_end")

  // Scope for pre-filter joins
  locationIds            String[]  @map("location_ids")        // resolved to locations.id
  locationPcodes         String[]  @map("location_pcodes")     // raw pcodes when unresolved
  eventTypes             String[]  @map("event_types")

  // Denormalised "hot" numbers for cheap filter / sort without opening the JSON blob
  totalAffected          Int?      @map("total_affected")
  totalDisplaced         Int?      @map("total_displaced")
  totalKilled            Int?      @map("total_killed")

  // Exhaustive structured payload — sector breakdowns, indicators, sub-object shapes.
  // Per-field shape: { value, unit, confidence, source_quote, chunk_index, page_number }.
  data                   Json

  schemaVersion          String    @map("schema_version")      // "v1"
  extractedByModel       String    @map("extracted_by_model")  // "claude-sonnet-4-6"
  extractedAt            DateTime  @default(now()) @map("extracted_at")

  @@index([reportingPeriodStart, reportingPeriodEnd])
  @@index([locationIds], type: Gin)
  @@index([eventTypes], type: Gin)
  @@index([schemaVersion])
  @@map("report_datapoints")
}
```

### `aggregated_datapoints` — one row per (window, level, location)

```prisma
model aggregatedDatapoint {
  id                     String   @id @default(cuid())

  windowStart            DateTime @map("window_start")
  windowEnd              DateTime @map("window_end")
  windowKind             String   @map("window_kind")     // "weekly" | "monthly" | "yearly" | "all"
  locationId             String?  @map("location_id")     // null = country-wide

  // Aggregated payload. Per-field shape:
  //   { value, quality_score, confidence_mix, newest_report_at,
  //     oldest_report_at, contributing_report_ids }
  data                   Json

  contributingReportIds  String[] @map("contributing_report_ids")

  // Denormalised bucket-level quality metadata for cheap filter / sort
  newestSourceAt         DateTime @map("newest_source_at")
  oldestSourceAt         DateTime @map("oldest_source_at")
  dataQualityScore       Float    @map("data_quality_score")   // 0..1
  reportCount            Int      @map("report_count")

  // Bitemporal validity — every recompute inserts a new row with
  // validFrom = now() and stamps validTo on the previous "current"
  // row in the same transaction. History rows are preserved by
  // design so "what did the dashboard show a week ago?" is a clean
  // query. Different admin levels tick at different rates naturally.
  validFrom              DateTime @default(now()) @map("valid_from")
  validTo                DateTime? @map("valid_to")

  schemaVersion          String   @map("schema_version")
  computedAt             DateTime @default(now()) @map("computed_at")

  // Partial unique enforced in the migration:
  //   UNIQUE (window_start, window_end, window_kind, location_id,
  //           schema_version) WHERE valid_to IS NULL NULLS NOT DISTINCT.
  // History rows (validTo NOT NULL) don't participate.
  @@index([locationId, windowStart])
  @@index([validTo])
  @@index([windowKind, windowStart, locationId, schemaVersion])
  @@map("aggregated_datapoints")
}
```

**Key design decisions:**

- **JSONB for the exhaustive payload**, typed columns for the ~5 hot fields the dashboard filters and sorts on. Balances flexibility (schema evolves) with query performance.
- **Per-field provenance in the JSON blob** — `{ value, confidence, source_quote, chunk_index, page }` per numeric value. Enables the "click the number to see the source paragraph" affordance humanitarian users expect.
- **Schema version on every row.** Aggregation only combines same-version rows; a schema change targets specific reports for re-extraction rather than invalidating everything.
- **Denormalised bucket quality on aggregated rows.** Dashboard renders "based on 5 reports, freshest 2 days ago, 82% quality" without opening the JSON.

---

## 5. Extraction Pipeline

### 5.1 Domain-partitioned extraction

A single LLM call emitting all ~50 datapoints in one shot is fragile: any schema tweak reruns everything, and a bad field can taint the whole batch. We partition into **six focused sub-schemas**, each with its own LLM call, sharing a prompt-cached document-level prefix so incremental cost is small.

| # | Domain | Fields (representative) |
|---|---|---|
| 1 | `TimingAndScope` | reporting_period_from/to, locations, event_types, active_clusters |
| 2 | `Casualties` | killed / injured / missing, each disaggregated by men/women/children/unknown |
| 3 | `Displacement` | IDP stock, new displacements, returnees, refugees, origin→destination flows |
| 4 | `NeedsAndFunding` | PIN / operational presence / demographics / severity / funding per SAF sector (Shelter, WASH, Protection, Health, Food Security, Education); overall PIN; **overall population affected** (widest crisis reach — see §5.5); overall funding required/received |
| 5 | `AccessAndIncidents` | access status per admin, security incidents, aid workers affected, infrastructure damage (schools, health facilities, water points, markets) |
| 6 | `NarrativeAndConfidence` | brief summary, overall confidence, sector indicators (IPC phase, GAM/SAM, disease outbreaks, GBV cases, out-of-school children, water access, latrine coverage) |

Each sub-schema is a Pydantic model. The extraction asset iterates over these six and merges the outputs into a single `data` JSON blob keyed by `{ timing_and_scope: {...}, casualties: {...}, ... }`.

### 5.2 Per-field provenance

Every numeric field in every sub-schema is not a scalar — it's a small object:

```python
class NumericField(BaseModel):
    value: float
    unit: str                     # "people", "USD", "%", "cases"
    confidence: str               # soft enum (see §6.1)
    source_quote: str             # the sentence the number came from
    chunk_index: int              # which chunk of the report
    page_number: int              # for the "cite the source" UX
    # ── Figure Scope ────────────────────────────────────────────
    # The ONE place this number is a total FOR — not every place the
    # report mentions. "1,000 affected in Kordofan" → "Kordofan", even
    # if the report is framed nationally and names other states.
    scope_location_name: str      # LLM-emitted place name; null if unpinnable
    scope_location_id: str        # resolved to locations.id post-extraction
```

**Figure Scope (`scope_location_name` / `scope_location_id`).** ReliefWeb reports are analytical — a figure is *already a total* over some area (e.g. "1,000 affected in Kordofan"), and the report typically names many other places as context. The extractor emits `scope_location_name` = the single place the number is a total for (null if it can't be pinned — the LLM must **not** default to the country or the first place named). A resolver then fills `scope_location_id` from `locations`. This is what lets aggregation bucket a figure to the right location instead of fanning it across every mentioned place; a figure with no resolved scope is excluded from cross-report roll-up. See [ADR-0002](./adr/0002-deduplicate-at-figure-scope.md).

This ~4× the LLM output volume per call. Combined with domain partitioning, the total structured-output volume per report is:

- **Base rate (raw datapoints):** ~50 fields × ~40 tokens each = ~2000 output tokens
- **With provenance envelope:** ~50 × ~160 tokens = ~8000 output tokens
- **Prompt cache reuse across 6 calls:** doc-level context ~4k tokens, paid once; 5 subsequent calls read cached (5% cost).

Order of magnitude: ~$0.05–0.15 per report on Claude Sonnet 4.6, ~$0.02–0.05 on Claude Haiku 4.5 if the caller accepts a small quality trade-off. See §11 for detailed cost math.

### 5.3 Location + time resolution

The LLM emits location references as `{ name, pcode?, admin_level? }`. A resolver step (already built for the vector RAG path) maps these to `locations.id` in clear-api. Unresolved pcodes remain in `locationPcodes` for a future backfill pass.

Time strings ("last week", "since April") are resolved against `publishedAt`. If the report doesn't state a reporting period explicitly, the extractor infers `reporting_period_start` as `publishedAt - 30 days` and flags it with a low-confidence `estimated` on the timing field. Downstream aggregation can filter out estimated periods when precision matters.

### 5.4 Failure isolation

Each sub-schema call runs with its own retry loop. A parse failure in `Casualties` doesn't drop `Displacement`. If any single sub-schema exhausts retries, its slot in the `data` blob is written as `null` with a `failure_reason` marker so the aggregator can skip the field cleanly and the operator can re-run just that domain.

### 5.5 Population Affected

**Population Affected** — the widest circle of crisis impact (everyone "affected" / "impacted" by the crisis) — is extracted here as `needs_and_funding.overall_affected` (a `NumericField`). It is **extracted from reports, not sourced from `events.populationAffected`**, and the extractor is instructed to take only an explicit affected/impacted figure — never to infer it from displacement, PIN, or casualty numbers, and to leave it `null` when the report states none.

Two deliberate properties:

- **Always evidenced, null when unknown.** Like every datapoint it carries the full Quality Envelope (`value`, `unit`, `confidence`, `source_quote`, `chunk_index`, `page_number`) plus Figure Scope. A country-window with no reported affected figure renders as "no data", never a default. This is the opposite contract to `events.populationAffected`, which may be imputed from a distribution or a 5-year mean for alert-ranking — the two describe different populations and are intentionally **not** reconciled.
- **Aggregated with `Max`** (see §6.2), not sum or latest: the largest evidenced affected figure across the window is the best estimate of total reach, and a later, narrower report shouldn't shrink it.

Rationale and the `events` comparison in full: [ADR-0001](./adr/0001-affected-extracted-not-sourced-from-events.md). (`overall_affected` is distinct from `overall_pin` = People in Need — a narrower, appeal-driven figure that is `latest_state`-aggregated and typically sparse.)

---

## 6. Aggregation Algorithm

### 6.1 Confidence taxonomy — soft enum with weights

The extractor is prompted with this taxonomy but is allowed to emit close variants. Anything outside the set is bucketed to `unverified` at aggregation time.

| Tier | Weight | Rough definition |
|---|---|---|
| `verified` | 1.0 | UN / govt mission verification, direct measurement |
| `reported` | 0.8 | DTM, cluster leads, official partner reports |
| `estimated` | 0.5 | Modeled / projected numbers (WorldPop-derived, IPC estimates) |
| `media` | 0.3 | Press / news coverage without corroboration |
| `unverified` | 0.1 | Unclear provenance / disclaimer-heavy |

Weights live in a config table so future tuning doesn't require redeployment.

> **Being superseded — data quality (source reliability × information credibility).**
> This confidence tier captures only one dimension (directness of observation). It is
> becoming the *Directness* criterion inside a full **data-quality** score that also
> grades the **source** (a reliability registry on `data_sources`) and the document's
> **information credibility** (8-criterion LLM assessment), combined as
> `((reliability × 2.5) × credibility) / 10` and used for bias-aware winner selection.
> The complete model — reliability seed grades, credibility weights, per-field quality
> bias, and validity windows — lives in **[data-quality-scoring-design.md](./data-quality-scoring-design.md)** (decision records: ADR-0004, ADR-0005). The nitty-gritty is intentionally kept there, not duplicated here.

### 6.2 Aggregation math per field-kind

The aggregator is a switch table over field kind. Every field in the exhaustive schema is tagged with its kind.

| Field kind | Aggregation | Example fields |
|---|---|---|
| **Additive count** | dedup by (event, location, date_bucket), then the interval-and-range reduce — flow sweep + cumulative differencing + event-type containment (**§6.8**), not a naïve sum | killed, new_displacements, incidents, funding_received |
| **Latest state** | latest `publishedAt` wins | IDP stock, PIN, IPC phase, risk level |
| **Set union** | union of contributing values | locations_affected, event_types, active_clusters |
| **Max** | pick the largest quality-adjusted value | population_affected (upper-bound reporting) |
| **Non-aggregatable** | narrative synthesis at read time | brief_summary, needs_description |

#### 6.2.1 How each datapoint is combined

§6.2 lists four ways figures from many reports are merged into one. This is the **per-datapoint reference**: which rule each datapoint uses, and how close together two reports must be to count as the *same* figure (so nothing is double-counted). §6.4 (Deduplication) explains the mechanics behind this table.

The four rules, and which report "wins" when two describe the same thing:

- **Summed** — figures are added across reports. Two reports of the *same* event are first de-duplicated — the most recent report wins, though a UN/government-**verified** figure can override a slightly newer unverified one — then the distinct figures are added.
- **Latest wins** — the most recent report's figure is used; earlier ones are never added on top.
- **Highest** — the largest figure wins (the largest within each report first, then the most recent across reports).
- **Combined list** — every report contributes; all values merge into one de-duplicated list.

| Datapoint | How it's combined | Same figure if reported within |
|---|---|---|
| People killed · people injured | Summed | the same week |
| Security incidents · aid workers killed | Summed | the same week |
| New displacements · returnees | Summed | the same week |
| Funding received | Summed | the same week |
| People displaced (current total) · refugees | Latest wins | the same month |
| People in Need — each sector (Shelter, WASH, Protection, Health, Food Security, Education) and the overall total | Latest wins | the same month |
| Funding required | Latest wins | the same month |
| Population Affected (§5.5) | Highest | the same month |
| Event types · active clusters | Combined list | — |

**Why the combine rule differs:** counts of *things that happened* (deaths, new displacements, incidents, money received) are **summed** — each report adds new events. Point-in-time *states* (how many people are currently displaced or in need, how much funding is still required) are **latest-wins** — a newer report replaces the old figure rather than adding to it. Population Affected takes the **highest** figure because it describes the widest reach of the crisis, which a later, narrower report shouldn't shrink.

**Why the window is a week:** reports arrive weekly and each figure is already a total over the report's period ("600 affected between two dates"), so a *summed* figure counts as the same measurement when two reports cover the same **week** — different weeks are genuinely different and add up. Slow-moving *states* (people in need, currently displaced) use a **month**. The full grouping rule is in §6.4.2 and the tie-break rules (which report wins) in §6.4.3. The 2–6 week **overlapping** windows this single-date bucketing can't express are now handled by the breakpoint flow sweep — see **§6.8** (ADR-0007), which reconciles overlapping period *ranges* instead of bucketing one date.

Any datapoint not in this list (e.g. the narrative summary) is kept as text and not merged into a number.

### 6.3 Quality-weighted aggregation

> **Note:** the confidence-weighted math below is being replaced by a composite
> **data-quality** score. Per contributing figure:
>
> &nbsp;&nbsp;&nbsp;&nbsp;`data_quality = ((source_reliability × 2.5) × information_credibility) / 10`  → 0–10
>
> where `source_reliability` is a 1–4 registry grade (`null` → 1; the ×2.5 rescales 1–4
> onto a 0–10 axis) and `information_credibility` is a 0–10 weighted 8-criterion score
> (directness = the confidence tier above, plus recency, attribution, internal
> consistency, plausibility, specificity, methodology, representativeness). This drives
> **bias-aware** winner selection with **read-time recency**; `quality_score` becomes a
> directness-only view while `data_quality` is the headline. Full model — weights,
> reliability seed, per-field bias, and validity windows: [data-quality-scoring-design.md](./data-quality-scoring-design.md) (ADR-0004, ADR-0005).

For an additive count field with confidence-weighted sum:

```
For a bucket (windowStart..windowEnd, location L):
    contributing_reports = report_datapoints where
        reportingPeriodEnd ∈ window
        AND locationIds contains a descendant of L

    incident_key = (event_type, canonical_location, date_bucket)
    per_incident = group contributing_reports by incident_key
                     within each group, latest publishedAt wins

    weighted_values = for each per_incident row:
                        weight_for(row.confidence) * row.value
    aggregate.value = SUM(weighted_values) / normalization

    aggregate.quality_score = weighted_average(confidence_weights)
    aggregate.confidence_mix = distribution of confidence tiers
    aggregate.newest_report_at = MAX(publishedAt)
    aggregate.oldest_report_at = MIN(publishedAt)
    aggregate.contributing_report_ids = distinct report ids used
```

Two important properties:

- **Deduplication is incident-level, not report-level.** Two reports covering the same displacement incident do not double-count; the higher-confidence one wins. Full rules in §6.4.
- **The quality envelope always ships alongside the value.** The dashboard can grey out or annotate low-quality figures without a second query.

### 6.4 Deduplication semantics

Deduplication is the load-bearing part of aggregation: it's what turns "sum of every reported number" (double-counted, misleading) into "sum of distinct incidents" (defensible). This section pins down the rules.

#### 6.4.1 The incident key

An incident key is a tuple `(figure_scope_location, time_bucket, event_type_set)` that identifies "the same real-world thing" across reports. Two extracted datapoints with the same key are treated as competing observations of one figure; the aggregator picks one and discards the rest. All three dimensions are now shipped — the incident key is `location | time_bucket | event_type_set`, so co-located distinct events (a conflict toll and a flood toll in the same place/week) group and sum separately instead of collapsing, and the additive combine caps an unqualified superset against its sub-causes (§7.3, see §6.8). The remaining gap is **canonicalisation**: the event-type dimension is currently the report's raw `event_types` (lowercased + sorted), not yet mapped through the glide-code taxonomy in the table below — so `"armed clash"` and `"battle"` are still distinct keys until that mapping lands. See [ADR-0002](./adr/0002-deduplicate-at-figure-scope.md).

| Dimension | Canonicalisation rule |
|---|---|
| **Figure scope (location)** | The location a *figure* is scoped to — the place the number covers — **not** every place the report mentions (ADR-0002). "1,000 affected in Kordofan" is scoped to Kordofan even if Sudan and El Obeid also appear. Prefer the resolved `locations.id`; fall back to a normalised pcode (uppercase, no punctuation) when the resolver returned null. When both are missing, the row is excluded from cross-report dedup and counted only under its own report — never rolled up. |
| **Event-type set** | The report's `event_types` mapped through the `disaster_types` taxonomy in clear-api ("armed clash", "battle", "armed confrontation" → one glide code), then treated **atomically**: a figure totalling across `{conflict, flood}` is one set and is never split between them (ADR-0002). Unmapped strings retain their raw value but are logged for taxonomy expansion. |
| **Time bucket** | Granularity depends on the field kind — see the table below. |

#### 6.4.2 Grouping window — how close two reports must be to count as the same figure

Our source reports are **analytical and weekly**, and a figure is already a total over the report's **reporting period** ("600 affected between X and Y") — not an event on a specific day (see [ADR-0002](./adr/0002-deduplicate-at-figure-scope.md)). So the grouping window is the **reporting week**, applied to the report's period-end date:

| Kind of figure | Window | Rationale |
|---|---|---|
| **Summed** figures (killed, injured, new displacements, returnees, security incidents, aid workers killed, funding received) | **Week** | Two reports covering the same week + figure scope + event-type set are the same weekly total → deduped. Different weeks (or a different event-type set) are genuinely different → summed. A *day* window would never group two weekly reports (dedup effectively off → same-week restatements double-count); a *month* window would merge four distinct weeks (→ undercount). |
| **Max** figures (population affected) | **Month** | The widest-reach figure over a period; a month groups a period's restatements and keeps the largest (§6.4.3 `max_within_report_then_latest`), so a later, narrower report can't shrink it. |
| **State snapshots** (people displaced / in need, refugees, funding required, IPC phase) | **Month** | These change slowly and are latest-wins, so a month groups a period's reports and takes the most recent. |
| **Set-union** labels (event types, clusters) | — | No window; every report's values are merged into one list. |

**Overlapping periods — handled by the flow sweep (§6.8).** Sitreps often cover **2–6 week windows** that overlap, which a calendar-week bucket can't express: two reports whose periods overlap but *end* in different weeks would land in different weeks and both count. The interval-and-range reducer (ADR-0007, §6.8) fixes this — it compares the reports' period **ranges** (`basis_period_start`..`end`), cuts the timeline at every figure edge and bucket boundary, and reconciles the covering rate-ranges on each atomic sub-interval instead of bucketing a single date. The weekly window described here is the point-figure fallback (figures with no multi-day basis period), which the sweep reduces to unchanged.

#### 6.4.3 Within-group winner selection

Within an incident group, one row wins and is emitted; others are dropped. The default policy for state-like fields is **latest `publishedAt` wins** (per §4 decision). Additive counts and MAX fields need policy nuance:

| Policy | Applied to | Rule |
|---|---|---|
| `latest_wins` | State snapshots | Highest `publishedAt`. Confidence weight breaks ties. |
| `latest_wins_with_confidence_override` | Additive counts | Among the rows within the freshest's override reach (`validityWindowDays / overrideDivisor`), take the top **data-quality** tier (reliability × credibility, [ADR-0005](./data-quality-scoring-design.md)); the field's directional **bias** breaks the tie (`overreport` → lower, `underreport` → higher), and each figure's **qualifier** constrains that projection (an `at_least` floor / `at_most` ceiling can't be crossed — ADR-0007, §6.8). This replaces the original "a `verified` row within 3 days overrides" rule. |
| `max_within_report_then_latest` | MAX fields (`population_affected`) | First pick the MAX value **within each report** (a report may mention the same figure twice), then apply `latest_wins` across reports. Prevents double-counting a re-quoted number. |
| `set_union_all` | Set-union fields | All rows contribute — no winner, no dedup. |

#### 6.4.4 Same-report multi-mention

A single report often quotes the same number multiple times (executive summary + body + sector breakdown). Without in-report dedup, a MAX or SUM field would count it once per mention.

Rule: **collapse same-report duplicates before cross-report dedup.** Within one `report_id`, pick the single mention with the highest confidence (ties broken by the earliest `chunk_index` — the summary usually leads). This is applied to every field before it enters the incident-group logic in §6.4.3.

#### 6.4.5 Worked examples

**A) Two DTM reports, same week, same pcode (Kordofan, `new_displacements`)**
- Both extract `{ value: 42000, confidence: "reported", event: "conflict-displacement", location: SD0701, bucket: 2026-W27 }`.
- Same incident key → one group.
- `latest_wins_with_confidence_override`: same confidence tier, latest `publishedAt` wins.
- Aggregate: `42000` (not `84000`). `contributing_report_ids` records both.

**B) DTM (high data-quality, 40k) vs media (unverified, 55k) for the same incident**
- Same incident key.
- Media report is 2 days newer; would win under naive `latest_wins`.
- `latest_wins_with_confidence_override`: within the override reach, the DTM row's data-quality (reliability × credibility) puts it alone in the top tier, so it wins with `40k` — and on `killed` (overreport) the directional bias also leans low, agreeing. (Had they been comparable quality, the overreport bias alone would pick the lower figure.)
- `quality_score` for the bucket reflects the DTM row's weight; the media `55k` is recorded in `confidence_mix` for transparency but doesn't contribute value.

**C) Two weekly reports of the same week's toll (El Fasher)**
- Report A (period ending 2026-07-02): `{ event_type_set: {armed-clash}, figure_scope: SD0201 (A2), killed: 3 }` — a weekly **total** for the scope, not a single-incident record (per [ADR-0002](./adr/0002-deduplicate-at-figure-scope.md) the source reports totals, not incident logs).
- Report B (period ending 2026-07-04, same ISO week): `{ event_type_set: {armed-clash}, figure_scope: SD0201 (A2), killed: 5 }`
- Same week + same figure scope + same event-type set → the same weekly total → **deduped, not summed**. The later report wins → `killed = 5` (a higher data-quality figure within the override reach would win instead, biased low for `killed` — §6.4.3). Reports from a *different* week — or a *different* event-type set (e.g. a co-located flood, `{flood}`) — are different figures and sum (ADR-0002).

**D) Same report re-quotes displacement figure in 4 places**
- Same-report multi-mention collapse (§6.4.4): pick one mention (highest confidence, earliest chunk).
- Then incident-group dedup runs against the single collapsed mention.

**E) Report emits raw text that resolver couldn't map to a `locations.id` or pcode**
- Row is excluded from cross-report dedup.
- Its numeric value is preserved on the per-report row (queryable via `reportDatapoint`) but does NOT contribute to any `aggregated_datapoint`.
- Nightly resolver-backfill pass may recover it when `locations` expands; the affected aggregation buckets are then marked stale.

#### 6.4.6 Edge cases

| Case | Behaviour |
|---|---|
| Missing `event_type` on an additive-count row | Assigned to `"unspecified"` event bucket. Dedup uses `(location, date_bucket)` only. Flagged in `data_quality_score` calculation with a small penalty. |
| Cross-boundary events (border area between two A2s) | Owned by the A2 that contains the incident coordinates. If both A2s are reported, the finest-resolution match wins; the coarser mention is dropped from cross-report dedup but survives as report-scoped data. |
| Multi-week events reported repeatedly as ongoing (e.g. a siege) | Treated as `state snapshots` — latest per month bucket wins. Never summed. Extraction schema tags these as `latest_state`. |
| Different attribution for the same incident (Report A: "attack by X", Report B: "attack by Y") | Attribution is NOT part of the incident key. Same incident wins. Both attributions are preserved in the winner's `source_quote`. |

#### 6.4.7 Aggregator pseudocode

```
def aggregate_field(field_kind, contributing_reports, location_scope, window):
    # 1. Same-report multi-mention collapse
    per_report_picks = {}
    for r in contributing_reports:
        for mention in r.mentions_of(field_kind):
            existing = per_report_picks.get(r.report_id)
            if existing is None or beats(mention, existing, policy="highest_conf_earliest_chunk"):
                per_report_picks[r.report_id] = mention

    # 2. Incident grouping
    groups = defaultdict(list)
    for m in per_report_picks.values():
        key = incident_key(m, field_kind)      # (event, location, time_bucket)
        if key is not None:                    # location-missing rows are dropped here
            groups[key].append(m)

    # 3. Within-group winner selection
    winners = []
    for key, group in groups.items():
        winner = resolve_within_group(group, policy=field_kind.within_group_policy)
        winners.append(winner)

    # 4. Cross-group combination per field-kind rule (from §6.2)
    return combine(winners, rule=field_kind.combine_rule)
```

`incident_key`, `resolve_within_group`, and `combine` are the three extension points where new field kinds and new policies plug in. This is the shared function §6.6 imports for both the Dagster pre-compute asset and the clear-api runtime resolver. Two steps have grown well past this sketch and are detailed in **§6.8**: `resolve_within_group` for additive fields is the bias-and-qualifier projection (not a plain latest/verified pick), and `combine` for additive fields is the interval-and-range reducer (breakpoint flow sweep → cumulative differencing → event-type containment), not the `weight_for(confidence) × value` sum shown in §6.3.

### 6.5 Staleness handling

We do NOT filter out old reports at aggregation time. Instead we surface staleness in the response so the caller decides:

- `newest_source_at` — freshness signal; dashboard renders "as of N days ago" indicators.
- `oldest_source_at` — spread of the sources; large spreads warn of mixing stale + fresh data.
- `is_stale` — bucket-level flag set when contributing reports haven't been touched in >N days AND no fresh contributions have arrived. A UI can grey the whole card.

### 6.6 Runtime aggregation for arbitrary windows

The pre-computed cache stores four tiers (weekly × A2, monthly × A1, yearly × country, all-time × country). Any query outside these tiers aggregates up from weekly × A2 at resolver time:

```
aggregatedDatapoint(location_id, window_from, window_to, level, asOf=now):
    # 1. Cache hit path — current version unless caller passed an asOf
    cached = find snapshot where
        (windowStart, windowEnd, windowKind, locationId) exactly matches
        AND schemaVersion == current
        AND validFrom <= asOf
        AND (validTo IS NULL OR validTo > asOf)
    if cached:
        return cached

    # 2. On-demand rollup from atomic weekly × A2
    a2_descendants = locations.ancestorIds contains location_id AND level=2
    atoms = weekly × A2 snapshots where
        windowStart >= window_from
        AND windowEnd <= window_to
        AND locationId in a2_descendants
        AND validTo IS NULL              # current versions only
    return aggregate(atoms)  # same math as pre-compute path
```

**Same aggregation function serves both the Dagster pre-compute asset and the resolver.** One implementation, guaranteed consistency between cached and on-demand results.

### 6.7 Cache invalidation (bitemporal validity)

The `aggregated_datapoints` table carries `validFrom` / `validTo`
columns instead of a boolean staleness flag — every recompute
appends a new "current" row and stamps the previous one as
superseded, so history is preserved by design.

When a new `report_datapoint` row lands:

1. Identify the affected buckets — every current bucket
   (`validTo IS NULL`) whose window covers `reportingPeriodEnd` and
   whose location scope contains any of the report's locations.
   This spans all four tiers at once: weekly × A2 for each of the
   report's admin-2 locations, monthly × A1 for the parent admin-1,
   yearly and all-time × country.
2. Stamp `validTo = now()` on those rows. They no longer participate
   in the current-version query but survive as history.
3. The next scheduled Dagster aggregation run inserts fresh rows with
   `validFrom = now()`.
4. Between the two, queries for those buckets fall through to
   on-demand aggregation over `report_datapoints` directly — users
   always see fresh numbers.

Between invalidation and recompute, the resolver falls back to the on-demand path for stale tiers. Users always see fresh numbers; the cache is a latency optimisation only.

### 6.8 Interval-and-range model (ADR-0007) — the shipped reducer

The math in §6.2–§6.4 collapses each figure to a single point early and buckets it by a single date. The **interval-and-range model** ([ADR-0007](./adr/0007-figures-as-ranges-over-intervals.md), [design](./interval-range-datapoint-model-design.md)) generalises that: **a figure is a value-RANGE over a time-INTERVAL, tagged by measure type**, aggregated **losslessly** with bias **projected last**. This is what the clear-api reducer (`datapoint-aggregation.ts`) runs today; the sections above describe the point-only special case it still reduces to for exact figures.

**What every figure now carries** (captured at extraction, schema v3):

| Field | Meaning |
|---|---|
| `value` | The headline point — unchanged; still what a version-less read returns. |
| `value_low` / `value_high` | The magnitude band, **always finite** (never an open `[500, ∞)`). Equals `value` for an exact figure (zero-width). |
| `qualifier` | Per-figure evidence of direction: `exact` / `at_least` (firm floor) / `at_most` (firm ceiling) / `approx` (symmetric). |
| `measure_type` | `stock_as_of` (point-in-time) / `period_flow` (accrued during a period) / `cumulative_to_date` (running total). |
| `basis_period_start/end` | The figure's own period when stated, else the report's reporting period. |

**Guiding principle — aggregate lossless, project bias late.** Ranges are combined without collapsing to a point; the field-level quality bias (ADR-0005 §3) is applied *last*, as a projection of the aggregate band onto a single headline (`overreport` → the low end, `underreport` → the high, `neutral` → freshest).

**Breakpoint flow sweep (§6.2 of the ADR) — the overlapping-period fix.** For additive/flow fields where any figure carries a real multi-day `basis_period`, the reducer no longer buckets by a single date. It cuts the timeline at **every figure edge AND bucket boundary**, and on each atomic sub-interval reconciles the covering figures' **daily rate-ranges** into one:

- the reconciled **band** is the *union* of the covering rate-ranges (`min low … max high`) — two sitreps that disagree about the same days surface that disagreement as **width**, not a silent pick;
- the reconciled **point** is the **bias projection** onto that band, with **no recency gate** — both figures genuinely measure the same elapsed days, so quality/bias decides, not publish order (a later sitrep re-counting the same window is a second observation, not fresher truth);
- the sub-interval rate is integrated over its length and added to the bucket that contains it, so an overlap **reconciles** instead of double-counting and a period straddling two buckets **splits by rate**.

Worked example: `A[2–10 Apr] 800` (100/day) + `B[5–15 Apr] 660` (66/day), `killed` = overreport → **960**, not 1460 (naïve sum) or 800 (max); the A-vs-B disagreement on the overlap shows up as upward band width.

**Event-type containment = max (§7.3).** Within a bucket, an *unqualified* (empty event-type) figure is a **superset** of its qualified sub-causes, so the bucket total is `max(Σ qualified, the widest unqualified)` — never the whole added on top of its own parts (`1M killed` + `100k drone deaths` → **1M**, not 1.1M). Max, not sum, on the *unknown* relationship: an undercount is recoverable, a silent double-count is not. Distinct qualified event-type sets are disjoint and still **sum**.

**Published confidence band.** The aggregate ships `value_low` / `value_high` / `range_width` alongside `value`, plus the field's `bias` direction — an honest uncertainty envelope built the *same way the point was* (flows integrated over their intervals, supersets capped, distinct causes summed), so `value` always lies inside `[value_low, value_high]`. A consumer can render error bars or project its own headline.

**Range-overlap divergence guard (§9).** For a `latest_state` field with an authoritative API anchor (ADR-0006 §7): if the **report figures carry a real band** and the API value falls **inside** it, that's agreement (the anchor tightens the estimate — no signal); an anchor the band **excludes** is the divergence (API wins, the gap is surfaced). "Real band" is a **per-figure** property (a stated range), not the aggregate spread — so two *exact* figures that merely disagree don't fake a band; they fall back to the ADR-0006 §7 fixed **25%** tolerance, and pure exact-vs-exact disagreement still trips the guard.

**Backward compatibility.** A pre-v3 (point) figure has `value_low = value_high = value`, so it reduces exactly as before. One nuance: the basis *period* falls back to the report's reporting period, so a v2 figure whose report states a multi-day period **does** enter the sweep — a lone such figure integrates back to its own value (no change), and only *overlapping* v2 figures move (from the old double-count to a reconciled total).

**Qualifier as a directional constraint.** The per-figure `qualifier` composes with the field-level `qualityBias` rather than replacing it — they are different axes (what the *source asserted* about this figure's bound vs the field's *systematic skew*). The qualifier is a **hard constraint**, the bias breaks the tie within it: an `at_least` figure's floor may not be projected below, an `at_most` figure's ceiling not above, whatever the field bias says; `approx`/`exact` add no constraint (so an all-exact corpus is unchanged). Wired in the bias-projection step (`biasWinner`, and the flow sweep's rate reconciliation).

**Measure-type reconciliation — running totals are differenced, not summed.** A `stock_as_of` / `cumulative_to_date` figure is a running total to its as-of date, not a period increment. On an additive field these are **first-differenced** into the increments they imply (consecutive snapshots → `Cᵢ − Cᵢ₋₁` over the interval between them; the earliest → the total over `[origin, as-of]`), and reported flows that fall **inside** a cumulative's coverage are dropped as already-counted (flows *outside* it are kept and extend the series). The result flows through the same breakpoint sweep, so a running total is integrated over exactly its own span and never added on top of the flows it already contains — `C(Mar31)=3000` then `C(Apr30)=5000` yields **5000**, not 8000. `measure_type` does **not** change the field-level combine strategy: `FieldRule.kind` still owns sum vs latest vs max vs union; it only refines stock-vs-flow within an additive field. *Edge:* a reported flow that only **partially** overlaps a cumulative's coverage boundary is dropped (conservative — favours a small undercount over a double-count).

**Per-country refresh scope.** `refreshAggregatedDatapoints` and `hasAggregatedDatapoints` take an optional `countryLocationId` (an admin-0 id) so the country-partitioned pipeline recomputes / first-run-checks one country's subtree at a time instead of a global pass.

---

## 7. Schema Versioning

Every extracted row (per-report and aggregated) carries `schema_version`. Aggregation only combines same-version rows. When the taxonomy evolves:

1. Bump the schema version (`v1` → `v2`).
2. New reports extract with `v2`.
3. Aggregations serve `v1` from cache; new rows compute `v2` in parallel.
4. Backfill: a targeted script re-runs extraction for reports that need `v2` (e.g., only reports in the last 6 months of aggregation windows).
5. Once coverage is sufficient, aggregations flip to `v2`.
6. `v1` rows are kept as history but stop being queried.

This lets the taxonomy evolve without invalidating the whole corpus.

---

## 8. Integration Points

### 8.1 dagster-quickstart / clear-context-pipeline

Two new assets, downstream of the existing `reliefweb_weekly_pdf_text`:

- `reliefweb_weekly_datapoints` — reads pdf_text output, runs the 6 domain-partitioned LLM calls per report, resolves locations, writes to clear-api's `report_datapoints` via a new `upsertReportDatapoints` mutation.
- `reliefweb_weekly_aggregations` — reads all `report_datapoints`, computes the 4 aggregation tiers, writes to `aggregated_datapoints` via a new `upsertAggregatedDatapoints` mutation.

The pdf_text upstream is shared with the vector RAG chain — no duplicate PDF fetch or extraction.

### 8.2 clear-api

New GraphQL surface:

- Mutation `upsertReportDatapoints(reportId, data, provenance, ...)` — pipeline-role only.
- Mutation `upsertAggregatedDatapoints(windowKind, windowStart, windowEnd, locationId, data, ...)` — pipeline-role only.
- Query `reportDatapoint(reportId)` — returns the per-report row.
- Query `aggregatedDatapoint(locationId, from, to, level)` — cache-first, falls back to on-demand rollup.

The runtime aggregation resolver reuses the aggregation function the Dagster pre-compute asset uses, imported from a shared module.

### 8.3 Situation-analysis dashboard

Reads exclusively via `aggregatedDatapoint`. Renders quality metadata (`quality_score`, `newest_source_at`, `report_count`) as visual affordances (e.g., grey-out for stale, orange badge for low-quality, tooltip listing contributing reports).

### 8.4 Chatbot query routing

The chatbot backend decides whether an incoming user query is factual (route to `aggregatedDatapoint`) or narrative (route to `searchKnowledgebase`). Simple heuristics work for a first pass; a small LLM classifier is the follow-up.

---

## 9. Cost Analysis

**Per-report extraction cost** (Claude Sonnet 4.6, prompt-cached doc prefix):

| Component | Tokens | Cost |
|---|---|---|
| Doc-level prefix (cached after call 1) | 4,000 | $0.012 first call, ~$0.001 subsequent |
| 6 domain-partitioned calls, output ~1,500 tokens each | ~9,000 out | ~$0.135 |
| **Per-report total** | | **~$0.15** |

Using Claude Haiku 4.5 for domains 1, 2, 3, 5, 6 (routine extraction) and Sonnet for 4 (needs / funding — the numerical density is highest and errors are costlier here):

| **Mixed-model per-report** | | **~$0.06** |

**Weekly volume (Sudan POC):** ~5–10 reports → ~$0.60–1.50/week.
**Monthly at 3 countries × 10 reports/week:** ~$18–45/month.
**Aggregation compute:** near-zero (deterministic SQL, no LLM in the aggregator).

**Embedding costs are unchanged** — the vector RAG path already exists.

**Guardrails** (already implemented for the vector path, reused here):
- `KB_MAX_CHUNKS_PER_REPORT` — cap on chunk fan-out.
- `KB_MAX_COST_USD_PER_RUN` — soft ceiling per Dagster materialisation.

---

## 10. Rollout Plan

**Phase 1 — Sudan-only foundation (2–3 weeks)**
- Schema migration for `report_datapoints` + `aggregated_datapoints`.
- 6 Pydantic sub-schemas defined and prompt-engineered.
- Dagster asset `reliefweb_weekly_datapoints`.
- clear-api mutations + `reportDatapoint(reportId)` query.
- Manual dashboard test: query per-report data via GraphQL.

**Phase 2 — Aggregation tiers (1–2 weeks)**
- Dagster asset `reliefweb_weekly_aggregations` computing all 4 tiers.
- Aggregation function shared with clear-api resolver.
- `aggregatedDatapoint(...)` query with cache-first + runtime-rollup fallback.
- Cache invalidation on report writes.

**Phase 3 — Dashboard integration (owned by dashboard team)**
- Situation-analysis dashboard consumes `aggregatedDatapoint` for numbers.
- Provenance drill-down UI (click number → source paragraph).
- Freshness / quality visual affordances.

**Phase 4 — Multi-country + chatbot routing (2 weeks)**
- Enable extraction for Afghanistan + Venezuela (schema is country-agnostic).
- Chatbot backend query router (factual → structured; narrative → vector).

---

## 11. Success Metrics

**Latency**
- p50 latency of `aggregatedDatapoint` for cached tier: < 100ms
- p50 latency for on-demand rollup: < 300ms
- Situation-analysis dashboard first-paint: < 1s (from ~5s with vector RAG synthesis).

**Coverage**
- ≥ 90% of ingested reports produce at least 20 populated datapoints.
- ≥ 80% of numeric datapoints carry a resolved location.

**Quality**
- Reviewer-audit agreement on a sampled 5% of extracted reports: ≥ 85%.
- Confidence-tier calibration: `verified`-tagged datapoints match auditor-verified values within 10% ≥ 90% of the time.

**Cost**
- Per-report extraction cost ≤ $0.20 sustained (guardrail auto-cutoff if breached).
- Monthly LLM spend ≤ $100 through Phase 4.

---

## 12. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| LLM extraction drift as prompts / models evolve. | Schema versioning; reviewer audit sample every N reports; regression corpus of 20 hand-labelled reports for prompt changes. |
| Cost overruns from a bad batch. | Existing `KB_MAX_COST_USD_PER_RUN` guardrail; skip contextualisation kill-switch already in place. |
| Provenance link rot (source S3 objects deleted). | S3 lifecycle policy retains PDFs for 2 years; `source_quote` in the JSON blob is a durable fallback. |
| Aggregation double-count from overlapping reports. | Incident-level dedup keys `(event, location, date_bucket)`; higher-confidence source wins in each incident group. |
| Location resolution misses (LLM emits an unmatchable pcode). | Raw pcodes preserved in `locationPcodes`; nightly backfill pass re-attempts resolution as `locations` grows. |
| Schema explosion — additions balloon the JSON blob. | Field-by-field justification required for schema bumps; deprecation policy for unused fields. |

---

## 13. Open Questions

- **Reviewer audit workflow** — who owns the sample audit and how often? Manual sampling by a data steward is the assumed default; a lightweight review UI is a Phase 3 follow-up.
- **IPC phase source of truth** — IPC publishes its own dataset with lag. Do we extract IPC from ReliefWeb narrative or import directly from the IPC data API? Recommend importing directly (better latency + accuracy), but out of scope for this doc.
- **Chatbot query classifier** — heuristic vs. LLM classifier for factual/narrative routing. Recommend heuristic first, LLM upgrade after we have query telemetry.

---

## 14. Approvals

| Reviewer | Role | Sign-off |
|---|---|---|
| | Product | |
| | Engineering | |
| | Data Science | |
| | Humanitarian domain lead | |

*Once approved, implementation kicks off with Phase 1. The extraction pipeline reuses the existing Dagster orchestration, provider abstractions, and clear-api patterns — no new infrastructure is required.*
