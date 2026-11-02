---
status: proposed
---

# Multi-source knowledge base: per-source fetch connectors + a sensor-triggered source-neutral downstream

## Context

The knowledge-base build is ReliefWeb-only. `defs/reliefweb_to_s3.py` fetches
ReliefWeb reports (3 assets: `reliefweb_weekly_reports_in_s3` → `_pdf_manifest`
→ `_pdfs_in_s3`), and a country-partitioned downstream chain
(`reliefweb_weekly_pdf_text → _chunks → _enriched_chunks → knowledgebase upsert`,
plus `figures`, `datapoints`, `datapoint_aggregations`) turns PDFs-in-S3 into the
knowledgebase. The downstream is **source-agnostic in logic** — it consumes
"PDFs in S3 + a per-report metadata record" — but is named/wired to ReliefWeb,
and a single schedule + `reliefweb_weekly_kb_job` runs fetch → downstream as one
group.

We want more PDF sources (and to fold **manual upload** in, replacing the bespoke
`manual_ingest` path), with:

- a **separate fetch schedule per source** (each source polls on its own cadence,
  independently — one source's outage never blocks another), and
- **one source-neutral downstream** that **auto-starts as soon as *any* source
  stages new data**,

mirroring how the signals pipeline fans many providers into shared processing.

Landed already (additive, inert):

- **#155** (merged) — `providers/kb_sources/`: `KbRawDocument` (the staged-PDF
  contract — the exact per-report shape `pdf_text` already emits), `KbConnector`
  protocol + registry, and source-parameterized S3 path builders.
  `test_reliefweb_paths_byte_identical_to_legacy` pins that `source="reliefweb"`
  reproduces `reliefweb_partitions.py`'s keys **byte-for-byte**.
- **#156** (in review) — `ReliefWebConnector.fetch`, reusing the existing fetch /
  download helpers and staging to the **same** S3 keys.

## Decision

Each source is a `KbConnector` with its **own fetch asset + schedule**; a single
**source-neutral downstream** processes staged documents from any source, kicked
off by a **sensor** the moment any source stages new data. (Chosen over a single
fan-in-fetch job and over a clear-api DB-queue drain — see Alternatives.)

1. **Per-source fetch, per-source schedule.** Each connector gets a
   country-partitioned fetch asset (e.g. `reliefweb_fetch`, `<source>_fetch`)
   that calls `KbConnector.fetch(...)` — staging PDFs to its own `<source>/…` S3
   namespace and recording the staged `KbRawDocument`s — plus its own
   `@dg.schedule` on that source's cadence. ReliefWeb's three fetch assets
   collapse into `reliefweb_fetch` (calling `ReliefWebConnector.fetch`); the
   window is computed as today (first-ingest backfill vs weekly delta).
2. **Source-neutral downstream.** Rename `reliefweb_weekly_* → kb_*`
   (`kb_pdf_text → kb_chunks → kb_enriched_chunks → kb_upsert`, plus `kb_figures`,
   `kb_datapoints`, `kb_datapoint_aggregations`). Each processes a document
   regardless of source and writes its artifacts at the document's own
   `<source>/kb/…` path — so ReliefWeb's land at `reliefweb/kb/…` unchanged.
3. **Sensor-triggered.** A sensor watches the per-source fetch assets'
   materializations (cursor-tracked) and launches the downstream for the country
   partition that just got new data. So the downstream **auto-starts as soon as
   any source has data** and processes only the new documents. A newly-registered
   source + schedule is picked up without touching the downstream or the sensor.
4. A new source = `providers/kb_sources/<source>.py` (connector) + a fetch asset
   + a schedule. Downstream + sensor untouched.

### Invariants — why renaming the downstream is safe

The earlier concern was re-**ingest** cost, which is governed by the **S3 paths**,
not the asset name:

- **Per-source S3 paths are unchanged.** `kb_pdf_text` writes a document's text to
  `<doc.source>/kb/text/<iso3>/<slug>` — for ReliefWeb exactly `reliefweb/kb/text/…`
  (byte-identical, pinned by the #155 test). So the skip-if-exists guards in
  `pdf_text` (`_existing_page_count`), `enrich` (existing enriched JSONL), and
  `datapoints` (existing `reportDatapoint`) still hit → no re-extract, no
  re-embed, even on a re-run of an already-processed partition.
- **The trigger is a cursor-based sensor**, so it processes only newly-staged
  documents — it does **not** backfill history. Do NOT put an eager
  `AutoMaterializePolicy` on the renamed partitioned assets: with fresh keys it
  could queue every partition. (There is no auto-materialize policy today; keep
  it that way — the sensor is the controlled trigger.)
- **What the rename costs:** only the *Dagster materialization history* of the old
  `reliefweb_weekly_*` keys (cosmetic — the UI timeline resets). No data moves,
  nothing re-ingests.
- **`report_id`s stay stable + source-namespaced** (ReliefWeb native ids,
  `manual:<sha>` for uploads) so `(sourceId, externalId)` dedup is unchanged.

## Manual upload (phase 3)

Manual upload becomes a **push** `KbConnector` (no `fetch`): the
`uploadKnowledgebaseDocument` path stages one `KbRawDocument` under `manual/…`
and its staging event trips the same sensor, so the upload flows through the
identical `kb_pdf_text → …` downstream — retiring the bespoke `manual_ingest`
chain. Its publisher is still marked trusted (reliability 4).

## Consequences

- Each source fetches on its own schedule and in isolation; the downstream runs
  per new batch of staged docs from any source. New source = a connector + a
  fetch asset + a schedule.
- The `reliefweb_weekly_*` asset keys are renamed to `kb_*` (one-time cosmetic
  Dagster-history reset; no re-ingest per the invariants).
- A slow/failing source is isolated to its own fetch asset/schedule; within the
  downstream, per-document failures stay isolated (mirror the per-report
  resilience already in the pipeline).
- The sensor + the rename are the parts that change which assets run / how they
  are triggered — validate in **dev** before prod (checklist below).

## Alternatives considered

- **Single fan-in-fetch job** (one job calls every connector and runs the
  downstream in the same group): couples all sources to one schedule and one run
  — rejected; the ask is independent per-source schedules + auto-trigger.
- **Per-source downstream factory** (a full downstream lineage per source):
  N copies; no shared processing; manual upload stays separate. Rejected.
- **clear-api DB-queue drain** (a `kb_documents` NEW/PROCESSED table + drain —
  the exact signals model): most robust dedup/status/retry, but a new Prisma
  table + resolvers + offline migration, and it moves the KB off asset lineage.
  Rejected for now as heavier than needed; revisit if per-document status/retry
  tracking becomes necessary.

## Dev-first validation checklist (before prod)

1. `reliefweb_fetch` for `sdn` stages the same report_ids + `pdf_s3_key`s the
   current `reliefweb_weekly_pdf_manifest`/`_pdfs_in_s3` produce for the window.
2. After a `reliefweb_fetch` run, the **sensor** launches the downstream for
   `sdn` and it writes **zero** new text/enriched/datapoint artifacts for
   already-processed reports (skip-if-exists all hit) — confirm via asset
   metadata (reused / `skipped_already_present` counts).
3. Each source's schedule fires independently on its own cadence; run keys
   per-source-per-window.
4. No partition shows a full re-materialization of *work* (only the one-time
   history reset from the key rename).
5. Register a trivial second connector + schedule in dev; confirm its staged docs
   trip the sensor and flow through the unchanged downstream.
