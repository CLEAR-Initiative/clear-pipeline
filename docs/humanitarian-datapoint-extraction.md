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
| 4 | `NeedsAndFunding` | PIN / operational presence / demographics / severity / funding per SAF sector (Shelter, WASH, Protection, Health, Food Security, Education); overall funding required/received |
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
```

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

### 6.2 Aggregation math per field-kind

The aggregator is a switch table over field kind. Every field in the exhaustive schema is tagged with its kind.

| Field kind | Aggregation | Example fields |
|---|---|---|
| **Additive count** | quality-weighted sum, dedup by (event, location, date_bucket) | killed, new_displacements, incidents, funding_received |
| **Latest state** | latest `publishedAt` wins | IDP stock, PIN, IPC phase, risk level |
| **Set union** | union of contributing values | locations_affected, event_types, active_clusters |
| **Max** | pick the largest quality-adjusted value | population_affected (upper-bound reporting) |
| **Non-aggregatable** | narrative synthesis at read time | brief_summary, needs_description |

### 6.3 Quality-weighted aggregation

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

An incident key is a tuple `(event, location, time_bucket)` that identifies "the same real-world thing" across reports. Two extracted datapoints with the same key are treated as competing observations of one incident; the aggregator picks one and discards the rest.

| Dimension | Canonicalisation rule |
|---|---|
| **Event** | Map the extractor's raw `event_type` string through the `disaster_types` taxonomy already in clear-api. "Armed clash", "battle", "armed confrontation" all fold to a single glide code. Unmapped strings retain their raw value but are logged for taxonomy expansion. |
| **Location** | Prefer the resolved `locations.id`. Fall back to a normalised pcode (uppercase, no punctuation) when the ID resolver returned null. When both are missing, the row is excluded from cross-report dedup and counted only under its own report — never rolled up. |
| **Time bucket** | Granularity depends on the field kind — see the table below. |

#### 6.4.2 Time-bucket granularity per field kind

Different classes of humanitarian data have different natural cadences. A single `date_bucket = 1 day` rule under-groups displacement (which is reported weekly by DTM) and over-groups discrete attacks (which happen on specific days). Each field carries a `bucket_size` in its schema:

| Field kind | Bucket size | Rationale |
|---|---|---|
| Discrete-event counts (attacks, security incidents, GBV cases) | **Day** | Different days = different incidents by definition. |
| Displacement flows (new displacements, returnees) | **Week** | Matches DTM reporting cadence. Two DTM rows for the same week and pcode are the same measurement. |
| State snapshots (IDP stock, IPC phase, PIN) | **Month** | State fields change slowly; two March reports of the same stock figure are the same snapshot. |
| Funding totals (received, required) | **Reporting period** | Deduped against the appeal / plan's own period, not calendar bucket. |

#### 6.4.3 Within-group winner selection

Within an incident group, one row wins and is emitted; others are dropped. The default policy for state-like fields is **latest `publishedAt` wins** (per §4 decision). Additive counts and MAX fields need policy nuance:

| Policy | Applied to | Rule |
|---|---|---|
| `latest_wins` | State snapshots | Highest `publishedAt`. Confidence weight breaks ties. |
| `latest_wins_with_confidence_override` | Additive counts | Highest `publishedAt` wins, UNLESS a `verified`-tier row exists within 3 days of the winner — that verified row overrides. Configurable window. |
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

**B) DTM (verified, 40k) vs media (unverified, 55k) for the same incident**
- Same incident key.
- Media report is 2 days newer; would win under naive `latest_wins`.
- `latest_wins_with_confidence_override`: verified row is within the 3-day window → verified wins with value `40k`.
- `quality_score` for the bucket reflects the DTM row's confidence weight; media row is recorded in `confidence_mix` for transparency but doesn't contribute value.

**C) Attack on hospital, El Fasher, same week, different days**
- Report A: `{ event: "attack-on-health", location: SD0201, date: 2026-07-02, killed: 3 }`
- Report B: `{ event: "attack-on-health", location: SD0201, date: 2026-07-04, killed: 5 }`
- Discrete-event kind → **day** bucket → different keys → both counted.
- Aggregate `killed = 8`.

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

`incident_key`, `resolve_within_group`, and `combine` are the three extension points where new field kinds and new policies plug in. This is the shared function §6.6 imports for both the Dagster pre-compute asset and the clear-api runtime resolver.

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
