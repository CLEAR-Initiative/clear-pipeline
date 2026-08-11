# Multi-country ReliefWeb KB ingest — implementation plan

**Ticket:** Multi-country ReliefWeb KB ingest + per-country initial backfill (`cmsolx2p60001l2046u0nvqc1`)
**Architecture (decided):** Dynamic partitions by ISO3.

## Goal

Adding a country to clear-api's `pipelineCountries` makes the ReliefWeb KB path
ingest → extract → aggregate that country automatically, with the country's
**first** run using the initial (90-day) lookback independently of other
countries, and per-country isolation for retries/backfills.

## Current state (why it isn't automatic today)

- The KB chain is **9 assets** across two groups, driven by one Monday-06:00
  schedule (`reliefweb_weekly_schedule` → `reliefweb_weekly_kb_job`, selected by
  `AssetSelection.groups("reliefweb", "reliefweb_kb")`):
  - `reliefweb`: `reliefweb_weekly_reports_in_s3` → `…_pdf_manifest` → `…_pdfs_in_s3`
  - `reliefweb_kb`: `…_pdf_text` → `…_chunks` → `…_enriched_chunks` → `…_knowledgebase_upsert`; and `…_pdf_text` → `reliefweb_weekly_datapoints` → `reliefweb_weekly_datapoint_aggregations`
- **`COUNTRY_ISO3 = "sdn"` is duplicated as a module constant in 6 files**
  (`reliefweb_to_s3.py`, `knowledgebase/{pdf_text,chunks,enrich,datapoints_extract}.py`,
  plus references), each building an S3 prefix and the ReliefWeb query filter.
- **First-run lookback is asymmetric:** ingest's `_is_first_ingest` keys off the
  country's own S3 prefix (already per-country ✓); aggregation's
  `has_aggregated_datapoints(SCHEMA_VERSION)` is **global per schema version**, so
  a new country added after Sudan gets the 7-day window, saved only by
  retrospective widening (batch's earliest `reportingPeriodEnd`, clamped to
  `KB_AGGREGATION_MAX_RETRO_DAYS`=400d).
- Assets pass **slim summary lists** via the default IO manager (heavy artifacts
  live in S3); every dependency edge is **1:1** (a country's asset consumes the
  same country's upstream). → identity partition mapping applies with no custom
  mapper.
- `definitions.py` uses `load_from_defs_folder`, so new sensors/schedules in the
  defs tree are auto-discovered. No manual registration.
- No `DynamicPartitionsDefinition` / sensor precedent in the repo yet (only
  `StaticPartitionsDefinition` in `defs/evals`). This infra is net-new.

## Design

Partition every KB asset by ISO3 with a single **`DynamicPartitionsDefinition`**,
keep the partition set in sync with `pipelineCountries` via a **sensor**, and fan
the weekly cron out to **one run per partition**.

### Phase 1 — Pipeline: partition the KB path (core)

1. **New `defs/reliefweb_partitions.py`** (single source of truth):
   - `country_partitions = dg.DynamicPartitionsDefinition(name="reliefweb_country")`
   - `def list_pipeline_iso3s() -> list[str]:` — `clear_api.get_pipeline_countries()`
     → lowercased `iso3`s. (Runtime call; the sensor uses it.)
   - Keep `FORMAT_NAME` / `FORMAT_SLUG` here too (format is still fixed).
2. **Delete the 6 duplicate `COUNTRY_ISO3` constants.** Each asset takes
   `iso3 = context.partition_key`; parametrize the S3-key/prefix helpers
   (`_reports_prefix`, `_pdf_key`, `S3_TEXT_PREFIX`, `S3_CHUNKS_PREFIX`,
   `S3_ENRICHED_PREFIX`, `S3_DATAPOINTS_PREFIX`, the ReliefWeb query filter at
   `reliefweb_to_s3.py:173`) to take `iso3` instead of reading the module global.
3. **Add `partitions_def=country_partitions` to all 9 assets** (both groups).
   Identity mapping is the default, so the existing parameter deps and the
   ordering-only `deps=[…]` edges keep working per-partition.
4. **Partitioned schedule** (replaces the current `ScheduleDefinition`): a
   `@dg.schedule` on `reliefweb_weekly_kb_job` that reads the live partition set
   (`context.instance.get_dynamic_partitions("reliefweb_country")`) and yields one
   `RunRequest(partition_key=iso3)` per country. Cron unchanged (`0 6 * * MON`, UTC).
5. **Partition-sync sensor** (`reliefweb_country_partition_sensor`): each tick,
   diff `list_pipeline_iso3s()` against the current dynamic partitions and
   `context.instance.add_dynamic_partitions(...)` the new ones. **Add-only** —
   removing a partition drops its materialization history, so log de-listed
   countries for a human instead of auto-removing. **The sensor only registers
   the partition — it does NOT auto-emit a backfill run** (decided). A newly-added
   country is picked up by the normal Monday schedule; its first run is the
   90-day initial ingest because `_is_first_ingest` is true. An operator can also
   **manually backfill** a country immediately from the Dagster UI (materialize
   the `reliefweb_weekly_kb_job` for that partition key) — which must work.
6. **Seed `sdn`.** Add `"sdn"` to the partition set on first sensor tick. Existing
   Sudan data already lives under `reliefweb/reports/sdn/…`, so **no data
   migration** — the `sdn` partition adopts it, `_is_first_ingest` returns false,
   and Sudan continues on the weekly window unchanged.

After Phase 1, aggregation still uses the global `has_aggregated_datapoints` +
retrospective widening. Because each country's partition run processes only that
country's `reliefweb_weekly_datapoints`, its batch's earliest period widens the
(still-global) refresh to cover that country's ~90-day first ingest. So a new
country **is** backfilled — via retro-widening, not the INITIAL var.

### Phase 2 — clear-api: per-country aggregation scope (in this ticket)

Removes the reliance on retro-widening + the 400-day clamp, and removes the
redundant global refresh each partition run triggers.

1. Add an optional `countryLocationId` (or `iso3`) arg to **`hasAggregatedDatapoints`**
   and **`refreshAggregatedDatapoints`** in clear-api (schema + resolver + service).
2. In `reliefweb_weekly_datapoint_aggregations`, resolve the partition's country
   location id (via `clear_api` — `pipelineCountries` already returns
   name/iso3/bbox; reuse the country-location resolver) and pass it to both calls.
   → genuine per-country INITIAL window and a **scoped** refresh (only that
   country's buckets recompute).

Deploy order if Phase 2 ships: **clear-api first**, then the pipeline (standing
clear-api-first rule).

## Files touched

- **New:** `defs/reliefweb_partitions.py` (partitions def + iso3 list + format
  constants); the sync sensor + partitioned schedule (either in `countries.py` or
  alongside the job in `reliefweb_to_s3.py`).
- **Edit (Phase 1):** `reliefweb_to_s3.py` (3 assets + helpers + job/schedule),
  `knowledgebase/{pdf_text,chunks,enrich,upsert,datapoints_extract,datapoints_aggregate}.py`
  (6 assets + prefix constants → iso3-parametrized).
- **Edit (Phase 2):** clear-api `hasAggregatedDatapoints` + `refreshAggregatedDatapoints`
  (typeDefs + resolver + aggregation service), and the `clear_api.py` provider
  wrappers here.

## Testing

- **Unit:** S3 keys/prefixes built from an arbitrary `iso3`; `list_pipeline_iso3s`
  lowercasing/mapping; sensor diff is add-only (new → added, existing → no-op,
  de-listed → logged not removed); schedule emits exactly one `RunRequest` per
  live partition.
- **Regression:** existing Sudan tests that hardcode `"sdn"` move to a partition
  fixture / `partition_key`. Interval-range + situation tests untouched.
- **Manual (dev):** materialize a **new** partition end-to-end (e.g. `eth`) →
  assert 90-day initial ingest, extraction, and an aggregation window covering
  ~90 days; assert `sdn` still runs the weekly window; re-run is idempotent.

## Decisions locked

- **Phase 2 is in this ticket** — the per-country `hasAggregatedDatapoints` +
  `refreshAggregatedDatapoints` scope ships here, so aggregation is genuinely
  per-country and the refresh is scoped (no redundant global refresh).
- **No auto backfill on onboarding** — the sensor only registers the partition.
  A new country runs on the normal Monday schedule (first run = 90-day initial);
  operators can manually backfill a partition from the Dagster UI on demand.

## Remaining risks

- **De-listing a country.** Add-only keeps history safe; removal is manual.
- **Cost/concurrency.** N countries × weekly LLM spend; partitions parallelize
  across runs but Dagster run-concurrency and ReliefWeb/LLM rate limits still
  apply. May want a concurrency cap on the partitioned job.

## Rollout

1. (If Phase 2) deploy clear-api, then the pipeline.
2. Sensor turns on → seeds `sdn` + any already-listed countries.
3. Verify the Monday schedule fans out one run per partition.
4. Onboard a new country by adding it to `pipelineCountries`; confirm the sensor
   adds the partition and it backfills at the initial lookback.
