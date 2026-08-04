---
status: proposed
---

# Source attribution via the `dataSources` registry + LLM information-credibility scoring

## Context

Two gaps limit how far a `report_datapoint` can be trusted today:

1. **No source is tracked.** ReliefWeb reports are fetched with `profile: "full"`,
   so each report's publisher (`report.source` — OCHA, UNICEF, …) is in the metadata
   JSONL, but it is dropped: it never reaches `report_datapoints` or the chunk tags.
   Worse, the **publisher is not the actual source of a figure** — an OCHA sitrep
   routinely cites IOM DTM displacement, WHO health, WFP prices. The org whose data a
   given number *is* is untracked.
2. **Quality is confidence-only.** Every `NumericField` carries a single
   `confidence` tier (`verified/reported/estimated/media/unverified`), and aggregation
   turns it into `quality_score`. That captures *one* dimension — how direct the
   observation is — and nothing about the reliability of the source or the credibility
   of the document.

`docs/data-source-specs/Data_quality_specs.md` defines the target: a **source
reliability** grade per source (0–4, Admiralty-style, manually maintained) and an
**information credibility** score per document (0–10, LLM-assessed across usability +
credibility criteria). This ADR decides the **source model and the credibility
scoring**; the quality formula and its use in aggregation are ADR-0005.

## Decision

### 1. Source lives on `dataSources`, referenced by id — not a free-text string

Reuse the existing `dataSources` table (which already seeds `dtm`, `acled`, `gdacs`,
`dataminr`, `field_officer`) as the source registry. Add:

- `synonyms String[]` — alias set for name normalisation.
- `reliability Int?` — 1–4 (Admiralty scale); `null` = "cannot be judged" (default for
  a new, ungraded source). Manually maintained by a domain expert.
- a `type` value `"organisation"` for report publishers / cited orgs, distinct from the
  automated feeds (`api` / `manual`) already in the table.

Everything that records a source stores a **`dataSources.id`**, not a string:

- `report_datapoints.sourceId` (real FK) = the report's **publisher** source.
- `NumericField.source` (id string in the JSON blob) = the **cited** source of *that*
  figure when the LLM extracts one, **else the publisher id** as the default. Because
  it defaults to the publisher, `reliability` is *always* resolvable per datapoint — no
  special fallback branch (the fallback is baked into the stored id).
- The chunk tags (`enrich.py::ExtractedParameters`) gain a `source` field (the org(s)
  the chunk's info comes from) alongside `locations / time_range / event_types /
  need_sectors`.

### 2. Name → id resolution: synonym → URL → fuzzy → create

A clear-api resolver `resolveDataSource(name, homepage?) → id` (mirroring
`resolveKnowledgebaseLocation`), called by the pipeline during enrich/datapoint
extraction:

1. normalised exact match on `name` **or** any `synonyms` → id
2. else, if a `homepage` is available, match on `infoUrl` → same org under a new name →
   **append the synonym**, return id
3. else **fuzzy match** (`pg_trgm` similarity ≥ a threshold, caller-tunable via
   `minSimilarity`, default 0.6, clamped to [0,1]) on `name` + `synonyms` → append
   synonym, return id
4. else **create** a new row (`reliability = null`, `type = "organisation"`) → id

The **URL fallback (2) only helps the publisher** — ReliefWeb `report.source` objects
carry a `homepage`; the LLM-cited source (pulled from body text) has no URL, so it
relies on (1) + (3) + (4). Fuzzy match before create is what keeps cited-source variants
from proliferating into duplicate rows. New/ungraded rows surface for a human to grade.

### 3. Publisher selection

`report.source` is an array (a report can list multiple publishers — rare in practice).
`report_datapoints.sourceId` is a single FK → take the **primary** source ReliefWeb
flags, else the **first**.

### 4. Information credibility: an 8-criterion weighted rubric, per datapoint with
document-level fallback

Assessed per **datapoint** (so it sits consistently with `confidence`, which is already
per-figure), inheriting a single **document-level** assessment as fallback where a
figure gives no signal for a criterion. Each criterion is rated **met (1) / partial
(0.5) / unmet (0)**; the weighted sum is the 0–10 `information_credibility`:

| Criterion | Weight | Where computed |
|---|--:|---|
| Directness of observation | 2.0 | extraction (**= the existing `confidence` tier**, kept on its 5-level scale: verified 1.0 / reported 0.8 / estimated 0.5 / media 0.3 / unverified 0.1, × weight) |
| Recency | 1.5 | **read-time** — see ADR-0005 |
| Attribution quality | 1.5 | extraction (LLM) |
| Internal consistency | 1.5 | extraction (LLM) |
| Plausibility in context | 1.5 | extraction (LLM, with a compact crisis brief in the prompt) |
| Geographic/temporal specificity | 1.0 | extraction (LLM; partly derivable from the locations + reporting period we already extract) |
| Methodology transparency | 0.5 | extraction (LLM) |
| Representativeness | 0.5 | extraction (LLM) |

- **Directness is the exception** to met/partial/unmet: it is already a 5-level tier and
  is fed on that scale × its weight, not collapsed to three levels.
- **Recency is the exception** to "computed at extraction": it depends on *now*
  (freshness relative to the field's validity window), so it is finalised at the
  read/resolve layer — see ADR-0005. The other seven criteria are intrinsic to the
  document, so they are baked at extraction and stored on the `NumericField`; the
  document-level assessment is stored on `report_datapoints` as the fallback.
- **Source reliability is NOT a credibility criterion.** The spec lists a "source
  reliability grade" prior under credibility, but the ADR-0005 formula also multiplies
  by reliability — using it in both places double-penalises weak sources. It is kept
  only as the outer multiplier (ADR-0005).

### 5. Reliability scale + seed

`reliability ∈ {1,2,3,4}`, `null` = ungraded (treated as `1` by the ADR-0005 formula).
Seed (a domain expert signs off + edits; the table is the source of truth):

| Reliability | Sources |
|--:|---|
| 4 | *reserve — Lancet/Nature-tier; likely none in-corpus yet* |
| 3 | OCHA, UNHCR, UNICEF, WHO, WFP, FAO, IOM/**DTM**, IPC/Cadre Harmonisé, FEWS NET, ACAPS, MSF, ICRC, NRC, Save the Children, **ACLED**, **GDACS**, `field_officer` (NRC's own field staff) |
| 2 | reputable national newspapers, smaller established NGOs, **Dataminr** |
| 1 | media with known bias / inconsistent accuracy |
| null → 1 | new/unresolved sources pending grading |

## Consequences

- **Schema migration** (clear-api): `dataSources` gains `synonyms` + `reliability`;
  `report_datapoints` gains `sourceId`. Datapoint/chunk schema gains `source` +
  credibility criteria → **`schema_version` bump → re-extraction** of existing
  `report_datapoints` (the pipeline already re-extracts on a version bump; aggregation
  only combines same-version rows, so no mixed-version math).
- **New source-normalisation component** and a **review queue** for ungraded / possibly
  duplicate sources. Cited-source disambiguation without a URL is the main accuracy
  risk; the fuzzy-match-before-create + synonym set is the mitigation, not a guarantee.
- **LLM cost**: the seven intrinsic criteria ride *inside* the existing per-domain
  datapoint call (no new call per figure), plus one document-level assessment call per
  report. Must be verified against the KB cost guardrail.
- `null → 1` means a brand-new, uncanonicalised source scores *low* (not neutral) until
  graded — a deliberate conservative default.
- Enables ADR-0005's data-quality formula, which consumes `NumericField.source`
  (→ reliability) + `information_credibility` per datapoint.

## Related

- ADR-0005 — the data-quality formula + quality-weighted, bias-aware aggregation.
- `docs/data-source-specs/Data_quality_specs.md` — the source-reliability + credibility
  specification this implements.
- `docs/humanitarian-datapoint-extraction.md` §6.1 (confidence) — reworked into the
  credibility model by this ADR.
