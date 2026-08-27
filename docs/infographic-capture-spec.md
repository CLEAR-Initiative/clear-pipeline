# Infographic Capture — Design & Implementation Spec

**Ticket:** [#499] Capture infographics (charts/maps/tables) from ReliefWeb reports — vision transcription + image asset store
**Feature:** CLEAR Data Ingestion Architecture (v1)
**Status:** Draft — decisions locked, ready to spec into slices
**Last updated:** 2026-08-25

---

## Executive summary

**What.** Humanitarian PDF reports (ReliefWeb sitreps, IOM DTM snapshots, market monitors)
carry many of their most important numbers inside **charts, maps, and tables**. Our ingest
today reads only the plain text, so those figures are either **missed** (when they sit in an
image) or **scrambled** (when a table is flattened into unordered text). This feature adds
the ability to capture them.

**How, in one line.** Cheaply detect which pages contain a graphic → crop the figure → have
a low-cost vision AI (Claude Haiku) "read" it into structured data plus a short description
→ feed that data into the same figure-extraction and search pipeline we already run on text,
and store the cropped image so it's retrievable by the same tags (location, disaster type,
needs, time) and can be attached to answers or reused to generate infographics on demand.

**Why it matters.** It closes a real data-coverage gap: figures like "1.76M people displaced
in South Darfur" often appear only in an infographic. Capturing them improves the accuracy
and completeness of the numbers the platform reports, and makes the source visuals
searchable and attachable.

**What we validated.** We ran a detection probe over 6 real reports (118 pages): ~half of
pages carry a graphic, and **~95% of figures can be located with free, built-in PDF tooling**
— so the first version needs **no heavy AI model** for finding figures. Tables are the most
common figure type, which means the cheapest slice delivers most of the value.

**Cost.** Negligible. Processing the current ~300-report corpus is a **one-time ≈ $20**, and
**a few dollars a month** ongoing. Cost is not a constraint on the design.

**Effort & sequencing.** Delivered in phases. The first slice — extracting structured tables
— needs **no AI and no image handling** yet returns the largest share of figures. Vision,
the image store, and a retrieval API follow.

**Status.** The key design decisions are locked (see §5). Two scope questions remain open
(§11): whether to also ingest the underlying datasets behind dense incident maps, and the
per-report processing cap.

## Glossary (for non-engineering readers)

- **Infographic** — a visual that carries data: a chart, a map, a table, or a composite
  "dashboard" panel combining several of these.
- **Figure** — any single data-bearing visual on a page (one chart, one table, one map).
- **Bounding box (bbox)** — the rectangle on the page that encloses a figure; what we crop to.
- **Vision AI / Claude Haiku** — an AI model that can look at an image and describe/transcribe
  it; Haiku is Anthropic's fastest, cheapest tier.
- **Transcription** — the structured text (numbers + labels + a description) the vision AI
  produces from a figure. Not an image; ordinary text our existing pipeline can consume.
- **Embedding / semantic search** — turning text into a numeric fingerprint so we can find
  the most relevant passages for a question. Figure transcriptions are embedded the same way
  as report text, so they're searchable alongside it.
- **Enrichment / tags** — automatically labelling each piece of content with its location,
  disaster type, needs sector, and time period — the filters used to retrieve it.
- **DTM** — IOM's Displacement Tracking Matrix; its country "snapshots" are the archetypal
  composite infographics referenced throughout.
- **pdfplumber** — the open-source library we already use to read PDFs; it also exposes each
  page's images, tables, and drawn shapes, which is what makes cheap figure-detection possible.

---

## 1. Problem

The knowledge-base ingest is **text-only**. `defs/knowledgebase/_pdf_extract.py` runs
`pdfplumber.extract_text()` (fallback `pypdf.extract_text()`) and keeps only
`{page_num, text}`. Consequences:

- **Graphs & maps** (raster or vector) are not captured; only stray text objects
  leak in, unordered. Image-only / scanned pages yield whitespace and are **dropped**.
- **Tables** are flattened to structure-less text (no `extract_tables()`), so
  multi-column rows interleave and row↔column meaning is lost.

ReliefWeb sitreps carry many of the **key figures** — PIN-by-state, displacement /
funding tables, big-number infographics — in exactly these tables and graphics. Those
numbers are currently missed (if image) or garbled (if table). This is a direct
data-coverage and data-quality gap for datapoint extraction and RAG.

## 2. Goals / non-goals

**Goals**

1. **Capture the DATA** in charts/maps/tables so figures become extractable (with page
   provenance) and retrievable — flowing into the existing enrichment + datapoint paths.
2. **Store the IMAGES** as retrievable assets, indexed by the **same parameters**
   (location / disaster-type / needs / time), filtered to genuine infographics (not
   photos/logos), so they can be **attached** — e.g. for on-demand infographic generation.

**Non-goals (v1)**

- Pixel-level marker geolocation / counting from a rendered map — unreliable; use the
  source dataset instead (§8).
- Multimodal image embeddings / a new vector space. Transcription is text and reuses the
  existing text embedder, so dimension stays uniform by construction.
- OCR of scanned documents as a primary concern — the corpus is ~all text-layer PDFs (§4).

## 3. Current state (code)

- `defs/knowledgebase/_pdf_extract.py` → `extract_pages()` = text only.
- `chunks.py` (800-token windows, page-range preserved) → `enrich.py` tags
  `locationIds` / `eventTypes` / `needSectors` / `timeRange` → `providers/embedding.py`
  (provider, **dim 1024**) → `upsert.py` writes chunk + embedding + those metadata fields.
- Whitespace/empty pages are dropped — those are precisely the infographic/scanned pages.
- clear-api `services/datapoint-aggregation.ts` already surfaces per-figure provenance
  (`contributing_figures`: page/chunk/`source_quote`), so figures extracted from
  transcriptions inherit the same traceback for free.

---

## 4. Findings — Phase-0 structural probe (real data)

Ran a pdfplumber structural probe over the 6 sample reports in `evals/reports/`
(`scratchpad/probe_bboxes.py`). Per page we measured the cheap gating signals and how
many figures have a **structural bbox** (embedded-image or ruled-table → croppable today)
vs would need a layout model.

| Report | Pages | Graphic | img | table | vector-dense |
|---|--:|--:|--:|--:|--:|
| DTM Displacement Snapshot | 35 | 34/35 | 2 | 29 | 27 |
| Protection Cluster alert | 2 | 1/2 | 0 | 1 | 0 |
| Sudan Country Brief | 4 | 2/4 | 2 | 0 | 0 |
| Sudan Crisis (60+, truncated) | 60* | 1/60 | 0 | 1 | 0 |
| UNFPA El Obeid Flash Update | 4 | 4/4 | 2 | 4 | 0 |
| Market Monitor | 13 | 13/13 | 8 | 13 | 3 |

**Aggregate (118 pages):**

- Graphic fraction **g ≈ 47%** of pages; avg **~20 pages/report** (sample skews large).
- Embedded-image 12% · **ruled table 41%** · vector-dense 25% · **low-text/scanned 2%**.
- Structurally-croppable figures = **163** (images 31 + tables 132).
- Graphic pages needing a layout model (no image **and** no table bbox) = **3 → 5%**.

**Implications that shape the design:**

1. **Ship v1 on pdfplumber alone — no layout model.** `page.images` + `find_tables()`
   give a croppable bbox for **~95% of figures**; the ~5% vector-only pages fall back to
   full-page. Biggest dependency/risk removed.
2. **Tables dominate** (41% of pages, 132/163 figures) → the tables slice (§7 Phase 1)
   covers the *plurality of figures with zero vision cost*. Do it first.
3. **Vision (Haiku) is a smaller job than sized** — mainly embedded-image charts/maps
   (~12%) + the ~5% vector-only pages; tables go through free structural extraction.
4. **~Nothing is scanned** (2% low-text) → these are text-layer PDFs; vision reconstructs
   chart/table *structure & values*, not scanned text. OCR is not a v1 concern.

*Caveats to validate in the spike:* `find_tables()` precision (132 tables on 118 pages —
some may be layout grids/borders; render a sample of bboxes to confirm), and crop padding
(bbox + margin must capture the title/legend/axis labels, not just the plot area).

---

## 5. Locked decisions (2026-08-25)

1. **Image-kind classification → folded into the vision call.** One Claude call returns
   `kind` (chart / map / table / photo / logo) as its first field, then transcribes data
   for chart/map/table and captions photos; logos dropped. No separate classifier pass.
2. **Storage → crop-first.** Crop the figure region and store that; store the whole page
   only as a fallback when cropping is unreliable. Cropping isolates the figure — cleaner
   S3 asset, cleaner LLM attachment (no header/footer/adjacent-figure noise), and better
   transcription input. It also separates multiple figures on one page into distinct assets.
3. **Attribution → via `source_id`.** Report images inherit the report's publishing
   source, already tracked in `report_datapoints.source_id`. Carry the same `source_id`
   on the `report_figure` asset. No separate licensing pipeline.
4. **Vision model → Claude Haiku** for v1 (cost-friendly; escalate a low-confidence page
   to a stronger model only if quality needs it).
5. **Sources → reuse the registry.** `data_sources` is already populated; reuse the
   existing `resolveDataSource` resolution to attach a figure's `source_id`, auto-creating
   an ungraded `organisation` row only on no-match — same behaviour as report figures today.

---

## 6. Architecture

```
PDF (S3) ──► [A] detect regions (pdfplumber, free) ──► per-page figure bboxes
                     │
        ┌────────────┴───────────────────────────────┐
        ▼ table bbox                                  ▼ image/vector bbox
   [B1] extract_tables() → markdown            [C] crop region → S3 (report_figure)
        │  (no vision, Phase 1)                       │
        │                                             ▼
        │                                    [D] Haiku vision on the crop:
        │                                        {kind, title, as_of, unit,
        │                                         rows[], callouts[], description}
        │                                             │  (logos dropped; photos captioned)
        └───────────────┬─────────────────────────────┘
                        ▼
   [E] transcription text  ──► chunks.py ──► SAME embedder (dim 1024)
                              ──► enrich.py (locationIds/eventTypes/needSectors/timeRange)
                              ──► RAG index + datapoint extraction (figures w/ page + source_quote)
                        ▼
   [F] report_figure asset row: s3_key + kind + same metadata + source_id  (indexed by params)
```

### [A] Region detection (cheap, no render, no LLM)
From pdfplumber's already-parsed page objects, first decide **whole-page vs per-region**,
then produce **figure bounding boxes** (a per-region page may yield several):

- **Composite full-page infographics → whole page as one unit (takes precedence).** When a
  page is *itself* one infographic (high graphic coverage / little prose — e.g. a DTM
  snapshot panel; the probe saw 34/35 DTM pages graphic + vector-dense), do **not** slice
  it. Region-splitting would fragment a single panel — the age/sex grid registers as a
  `find_tables()` table, the population pyramid as vectors, the headline numbers as loose
  text — and lose the whole. Detect via a page-level rule (`graphic_coverage ≳ 70%` and/or
  low `text_chars` relative to graphic area) → crop the **whole page (or the panel's
  bounding region)** as one asset and transcribe it as a **composite** (§D `groups`). The
  South Darfur DTM panel is the canonical example: one crop → headline totals + timing
  split + sex split + age×sex matrix, all captured together.
- Otherwise, a page with a **discrete figure amid prose** → per-region bboxes:
  - **Embedded raster images** → `page.images`, filtered to area ≥ ~4% of the page (drops
    logos/icons). bbox given → crop directly.
  - **Ruled tables** → `page.find_tables()` → each table's bbox.
  - **Vector charts/maps + borderless tables** (~5% of graphic pages) → **deferred**:
    cluster vector primitives (`rects`/`lines`/`curves`) + nearby text into connected
    regions, or add a layout-detection model later. v1 = **full-page fallback** for these.
- Signals also used for gating: text-coverage / low-text, vector density; pages where
  pdfplumber OOMs → pypdf fallback are graphics-dense by definition.

> Note: because cost is negligible at this corpus size (§9), gating is a **latency /
> index-cleanliness** optimisation, not a budget control. v1 may even run vision on every
> detected region and skip aggressive thresholding.

### [B1] Tables → structured extraction (Phase 1, no vision)
`page.extract_tables()` on the detected table bboxes → serialise to **markdown**, appended
to the page text and/or carried as a table artifact into datapoint extraction. Covers the
plurality of figures with zero LLM cost.

### [C] Crop → S3
Crop each figure bbox **with padding** (so titles/legends/axes survive), else store the
full page (fallback). Dedup by image hash (collapses repeated banners/logos). Persist to
S3 with `source_id` + credit. **Do not** ask the LLM for pixel coords — location is
structural; the LLM only reads the crop.

### [D] Vision transcription (Haiku, data-bearing kinds)
Render the crop → Claude Haiku → fixed JSON schema:

```json
{ "kind": "chart|map|table|photo|logo|other",
  "title": "...", "as_of": "2026-06", "unit": "people",
  "rows": [{"label": "North Darfur", "value": 1200000, "estimated": false}],
  "callouts": ["El Fasher: 800,000 cut off"],
  "description": "one-line summary" }
```

Charts → data rows (values exact if printed, `estimated` when read off bar heights);
tables → grid; labeled/choropleth maps → region→category + callouts. `kind=logo` → drop;
`kind=photo` → caption only, tagged non-data.

**Composite panels** (a single infographic bundling several sub-figures — DTM snapshots are
the archetype) don't fit a flat `rows[]`. The same call returns nested **`groups`**, each a
labelled sub-block with its own rows, so the whole panel is captured in one transcription:

```json
{ "kind": "other",                       // composite / dashboard panel (data-bearing)
  "title": "South Darfur — Displacement & Return Snapshot",
  "location": "South Darfur", "as_of": "2023", "unit": "people / %",
  "groups": [
    { "name": "Headline totals", "rows": [
      {"label": "IDPs", "value": 1763432, "mom_change": "<1% up"},
      {"label": "Returnees", "value": 102503, "mom_change": "1% up"} ] },
    { "name": "Displacement timing", "rows": [
      {"label": "Displaced post-April 2023", "value": 921201, "pct": 52},
      {"label": "Displaced prior to April 2023", "value": 842231, "pct": 48} ] },
    { "name": "Sex distribution", "rows": [
      {"group": "IDPs", "female_pct": 49, "male_pct": 51},
      {"group": "Returnees", "female_pct": 48, "male_pct": 52} ] },
    { "name": "Age x sex (Figure 27, % within group)", "rows": [
      {"group":"IDP","age":"18-59","f":20,"m":19}, "... one row per age band x group ..." ] }
  ],
  "callouts": ["Data based on household surveys, representative at locality level"],
  "description": "one-line summary" }
```

`kind` is for routing/filtering, not for deciding capture: a composite is `other` but
**data-bearing → transcribed and stored** (only `logo`/`photo` skip transcription). A
composite page maps to **one `report_figure`** (the whole panel) whose transcription
carries all groups; datapoint extraction reads the headline figures (here South Darfur IDPs
1,763,432 and returnees 102,503) from the groups, page-attributed. *(Open: whether to add an
explicit `infographic`/`composite` kind so composites are filterable as their own type —
§11.)*

### [E] Reuse the existing text pipeline
The transcription is **text**, so it flows through the SAME chunk → **same embedding model
(same 1024-dim)** → **same enrichment** (`locationIds`/`eventTypes`/`needSectors`/
`timeRange`) → RAG + datapoint extraction. Figures land with `source_quote` (the
transcribed line) + `page_number`, matching the existing `contributing_figures`
provenance. No new vector space, no schema drift.

### [F] Image asset store — `report_figure` (indexed by params, NO embedding)
Metadata is **inherited from the figure's transcription enrichment**, so the image carries
the same tags as its text. Retrieval = structured-param filter (uniform with chunk
filters); optionally rank by the transcription-text embedding.

---

## 7. Data model

New `report_figure` asset (clear-api):

| Field | Notes |
|---|---|
| `id` | |
| `report_id` | FK to the report |
| `page_number` | 1-indexed source page (provenance) |
| `s3_key` | the cropped image (full page on fallback) |
| `bbox` | source-page bounding box of the crop |
| `kind` | `chart` \| `map` \| `table` \| `photo` (logos not stored) |
| `title`, `description` | from the vision pass |
| `source_id` | publishing source (as `report_datapoints`) — attribution |
| `locationIds`, `eventTypes`, `needSectors`, `timeRangeStart/End` | from enrichment on the transcription — the retrieval filters |
| `crop_confidence` / `is_full_page` | whether this is a clean crop or the page fallback |

Index by `(locationIds, eventTypes, needSectors, timeRange, kind)` (GIN/btree).
The **transcription text** is a normal knowledge-base chunk (existing schema) — no change
there; it just originates from a figure and is page-attributed.

---

## 8. Hard case — dense unlabeled-marker maps

E.g. an attacks-on-healthcare map: many unlabeled point markers. Vision **cannot** reliably
count or geolocate them (it snaps to guessed regions and hallucinates counts). So:

- **Capture only the text-bearing parts:** title, legend, time period, any **printed
  aggregate/total**, source attribution, qualitative distribution.
- **Get the marker-level truth from the source dataset** the map renders — WHO SSA
  (Surveillance System for Attacks on Health Care), ACLED, or OCHA — via API/CSV, where
  each incident has exact `(lat, lon, date, type)` and is admin-resolvable. Some already
  flow through the ACLED ingest.
- **Use the map to detect *what dataset + period to pull*, not as the data.** Store the
  image flagged "visual — figures from `<source>`".

Scope of source-dataset ingestion (WHO SSA / expanded ACLED) is a **Phase 5 / separate
ticket** decision (§10).

---

## 9. Cost

Cost is dominated by Haiku vision; detection is free CPU, classification is folded into the
vision call, tables go through free `extract_tables()`, embeddings/S3 are rounding error.

- **Per flagged page** ≈ **$0.005–0.008** (Haiku 4.5 $1/1M in, $5/1M out; ~2k image tokens
  + ~500 prompt + ~600 output; cropping roughly halves image tokens).
- **Corpus:** ~300 reports (≈3 months). Using the probe's upper-bound g≈47%, P≈20:
  `300 × 20 × 0.47 × $0.006 ≈ **$17** backfill` — and that's conservative-high (sample
  skews to big graphics-heavy reports; tables route through free extraction, not vision).
- **Ongoing:** ~$2–7 / month. **Storage:** cropped figures ~1–2 GB × ~$0.02/GB → pennies.

**Conclusion: cost is a non-factor** (< ~$20 one-time, pennies/month) — comfortably inside
`KB_MAX_COST_USD_PER_RUN` (default $5/run). Optimise for accuracy/simplicity, not tokens;
reprocessing the whole corpus to tune the prompt is ~$7 a pass.

---

## 10. Execution plan (phased)

- **Phase 0 — Spike (partly done).** Structural probe over `evals/reports/` complete (§4).
  Remaining: validate `find_tables()` precision on rendered bboxes and crop-padding on a
  sample; confirm the ~5% vector-only rate. *Deliverable: go/no-go + calibrated thresholds.*
- **Phase 1 — Tables quick win.** `extract_tables()` on table bboxes → markdown into page
  text (+ table artifact). Immediate figure-coverage lift, no vision, no images. Highest ROI.
- **Phase 2 — Detection + crop → S3 + `report_figure` store.** Region detection (image +
  table bboxes; full-page fallback), crop-with-padding, dedup, store all kinds, create
  `report_figure` rows with enrichment metadata + `source_id`. Establishes the asset store
  and the photo/infographic filter *before* transcription.
- **Phase 3 — Vision transcription (Haiku).** Gated vision pass on data-bearing crops →
  structured rows (classify folded in) → merged into chunks/figures with provenance;
  per-report page cap (mirror `KB_MAX_CHUNKS_PER_REPORT`) + cache by report/crop hash.
- **Phase 4 — Image retrieval API.** clear-api query to fetch `report_figure` assets by
  params (+ optional semantic rank); wire into on-demand infographic generation / RAG
  attachment.
- **Phase 5 — Hard-case maps + source datasets.** Classify dense-marker maps; capture
  headline + source; (scope TBD) add WHO SSA / expand ACLED for marker-level data.

Deferred: vector-region layout model (only if the ~5% proves too lossy); image (multimodal)
embedding for visual similarity.

---

## 11. Open questions — need sign-off

- **Phase 5 scope:** source-dataset ingestion (WHO SSA / expanded ACLED) here or a
  separate ticket?
- **Per-report page cap / cost budget** for the Haiku vision pass (nominal — cost is tiny).
- **`find_tables()` precision** — confirm detected tables are real (spike).
- **Crop padding** — confirm titles/legends/axes are captured (spike).
- **Composite kind** — do composite/full-page panels get an explicit `infographic`/
  `composite` `kind` (filterable as their own type), or stay `other` + `groups`
  transcription (v1 default)?
- **Whole-page vs per-region threshold** — the `graphic_coverage` cut-off that routes a
  page to whole-panel capture vs per-region cropping (calibrate in the spike; DTM snapshot
  pages are the positive class).
- **PII / photos of identifiable people** — attribution via `source_id` is settled; decide
  whether photos of people need any additional display gating beyond source credit.

---

## 12. Testing / evals

- **Sample corpus:** `evals/reports/` — 6 real ReliefWeb PDFs spanning a graphics-dense DTM
  snapshot, a market monitor (charts/tables), a flash update, a country brief, a protection
  alert, and a long mostly-text crisis report. Representative variety for detection + crop
  + transcription evals.
- **Canonical composite test case:** the DTM snapshot's per-state panels (e.g. *South
  Darfur*) — one full-page infographic bundling headline totals + timing split + sex split +
  age×sex pyramid. Exercises whole-page-as-one-unit detection (§6 A) and the `groups`
  transcription schema (§6 D); the eval key asserts the headline figures (IDPs 1,763,432;
  returnees 102,503) extract with `location=South Darfur` + page provenance.
- **Probe:** `scratchpad/probe_bboxes.py` (structural signals per page). Extend into a
  standing eval: detection recall (figures found vs eyeballed), crop quality, `find_tables`
  precision, and transcription accuracy (transcribed figures vs a hand-labelled key).

---

## 13. References

- Extraction: `src/clear_context_pipeline/defs/knowledgebase/_pdf_extract.py`, `pdf_text.py`
- Chunk / enrich / embed / upsert: `chunks.py`, `enrich.py`, `providers/embedding.py`,
  `upsert.py` (metadata: `locationIds` / `eventTypes` / `needSectors` / `timeRange`)
- Figure provenance: clear-api `services/datapoint-aggregation.ts` → `contributing_figures`
- Source attribution: `report_datapoints.source_id`, `resolveDataSource` (dataSource resolver)
- Related design: `docs/data-quality-scoring-design.md`,
  `docs/data-quality-and-reconciliation-implementation-plan.md`
