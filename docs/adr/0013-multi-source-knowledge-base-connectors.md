---
status: proposed
---

# Multi-source knowledge base via connectors + an S3-staged asset fan-in

## Context

The knowledge-base build is ReliefWeb-only. `defs/reliefweb_to_s3.py` fetches
ReliefWeb reports (3 assets: `reliefweb_weekly_reports_in_s3` → `_pdf_manifest`
→ `_pdfs_in_s3`), and a country-partitioned downstream chain
(`reliefweb_weekly_pdf_text → _chunks → _enriched_chunks → knowledgebase upsert`,
plus `figures`, `datapoints`, `datapoint_aggregations`) turns PDFs-in-S3 into the
knowledgebase. The downstream is **source-agnostic in logic** — it consumes
"PDFs in S3 + a per-report metadata record" — but is named and wired to
ReliefWeb.

We want more PDF sources (and to fold **manual upload** in, replacing the
bespoke `manual_ingest` path) feeding the *same* downstream, the way the signals
pipeline fans many providers (acled/gdacs/…) into one drain. The downstream
should process a document as soon as any source produces one.

Landed already (additive, inert):

- **#155** — `providers/kb_sources/`: `KbRawDocument` (the staged-PDF contract —
  the exact per-report shape `reliefweb_weekly_pdf_text` already emits),
  `KbConnector` protocol + registry, and source-parameterized S3 path builders.
  `test_reliefweb_paths_byte_identical_to_legacy` pins that `source="reliefweb"`
  reproduces `reliefweb_partitions.py`'s keys **byte-for-byte**.
- **#156** — `ReliefWebConnector.fetch`, reusing the existing
  `_fetch_window`/`_upload_pdf`/`_s3_object_exists` helpers and staging to the
  **same** S3 keys; additive, no change to the live asset graph.

## Decision

Introduce an **S3-staged Dagster asset fan-in** (chosen over a per-source asset
factory and over a clear-api DB-queue drain — see Alternatives):

1. A new country-partitioned fan-in asset — **`kb_staged_docs`** — iterates every
   registered `KbConnector`, calls `fetch(country_iso3, window_start, window_end)`
   (each connector stages its PDFs to its own `<source>/…` S3 namespace and
   returns `KbRawDocument`s), and returns the **union** across connectors for the
   partition. The ingest window is computed exactly as
   `reliefweb_weekly_reports_in_s3` does today (first-ingest initial-backfill vs
   weekly delta, via `_is_first_ingest` + the lookback env vars).
2. Re-point **`reliefweb_weekly_pdf_text`** to consume `kb_staged_docs` (a list of
   `KbRawDocument`, keyed by `pdf_s3_key`) instead of
   `reliefweb_weekly_pdf_manifest`/`_pdfs_in_s3`. The rest of the downstream is
   unchanged — it already keys off the per-report record.
3. The three ReliefWeb fetch assets collapse into `ReliefWebConnector.fetch`
   (called by `kb_staged_docs`), so `reliefweb_weekly_{reports_in_s3,
   pdf_manifest, pdfs_in_s3}` are removed.
4. A new source is then "write a `providers/kb_sources/<source>.py` connector +
   register it" — `kb_staged_docs` picks it up with no graph change.

### Invariants that MUST hold (no re-materialization, no re-ingest)

These are the reason this is safe; a reviewer should check each:

- **Keep the downstream asset *keys* unchanged.** Generalize bodies, not keys:
  `reliefweb_weekly_pdf_text`, `_chunks`, `_enriched_chunks`, the upsert,
  `figures`, `datapoints`, `datapoint_aggregations` keep their current
  `AssetKey`s (rename the functions only with `@dg.asset(name="…")` pinning the
  old key if cosmetic cleanup is wanted). Unchanged keys → full materialization
  history → the scheduler never sees them as new.
- **No `AutoMaterializePolicy` exists on these assets** (verified: they are driven
  by `reliefweb_weekly_kb_job = define_asset_job(selection=AssetSelection.groups(
  "reliefweb", "reliefweb_kb"))` + a `@dg.schedule` + a `@dg.sensor`). So even a
  key change can't auto-backfill history; materialization stays schedule/sensor
  driven over the current partition.
- **ReliefWeb's S3 paths stay byte-identical** (`source="reliefweb"` +
  `FORMAT_SLUG`, pinned by the #155 test). The skip-if-exists idempotency in
  `pdf_text` (`_existing_page_count`), `enrich` (existing enriched JSONL), and
  `datapoints` (existing `reportDatapoint`) keys off these paths — so even a
  re-run of a partition re-extracts/re-embeds nothing.
- **`report_id`s stay stable and source-namespaced** (ReliefWeb's native ids;
  `manual:<sha>` for uploads), so `(sourceId, externalId)` dedup downstream is
  unchanged.
- The removed fetch assets (`reliefweb_weekly_reports_in_s3` etc.) lose their own
  materialization history (cosmetic); nothing downstream re-runs because of it,
  per the no-auto-materialize point above.

### Trigger ("downstream acts as soon as a connector has data")

Keep the existing group-selected `reliefweb_weekly_kb_job` + schedule/sensor.
`kb_staged_docs` joins a selected group so the weekly cron runs
`kb_staged_docs → pdf_text → …` end to end; a newly-registered connector is
included automatically (the asset enumerates the registry at run time). No
`AutoMaterializePolicy` is introduced — the group job already runs the chain, and
adding one would reintroduce the backfill risk the invariants above avoid.

### Manual upload (phase 3)

Manual upload becomes a **push** `KbConnector` (no `fetch`): the
`uploadKnowledgebaseDocument` path stages one `KbRawDocument` to
`manual/…` and triggers the downstream for that document (a run keyed to the
upload), reusing the same `pdf_text → …` code and retiring the bespoke
`manual_ingest` chain. Its publisher is marked trusted (reliability 4) as today.

## Consequences

- One downstream, many sources; a new source is a small connector module. Manual
  upload stops being a parallel code path.
- The three ReliefWeb fetch assets are replaced by a connector; their Dagster
  history is lost (cosmetic).
- Fan-in is per-country-partition; a slow/failing connector is isolated
  per-connector within `kb_staged_docs` (one source's failure must not drop the
  others — mirror the per-report resilience already in the download asset).
- This is the only step that changes *which assets run*; it must be validated in
  **dev** before prod (see checklist).

## Alternatives considered

- **Per-source asset factory** (one downstream lineage per source via a factory):
  N copies of the downstream; doesn't literally fan in; manual upload stays
  separate. Rejected — more surface, less unification.
- **clear-api DB-queue drain** (a `kb_documents` table with NEW/PROCESSED, a
  sensor-driven drain — the exact signals model): most robust dedup/status, but a
  new Prisma table + resolvers + offline migration, and it migrates the KB off
  the asset-lineage model. Rejected for now as heavier than needed; the S3-staged
  fan-in stays asset-native and schema-free. Revisit if per-document status/retry
  tracking becomes necessary.

## Dev-first validation checklist (before prod)

1. `kb_staged_docs` for `sdn` returns the same set of documents (report_ids +
   `pdf_s3_key`s) the current `reliefweb_weekly_pdf_manifest` produces for the
   same window.
2. A full dev run of `reliefweb_weekly_kb_job` materializes
   `kb_staged_docs → pdf_text → … → upsert` with **zero** new text/enriched/
   datapoint artifacts written for already-processed reports (skip-if-exists all
   hit) — confirm via asset metadata (`skipped_already_present`, reused counts).
3. The schedule/sensor still enqueues one run per live country partition; run
   keys unchanged.
4. No partition shows a full re-materialization in the Dagster UI after deploy.
5. Register a trivial second connector in dev and confirm its docs flow through
   without any downstream change.
