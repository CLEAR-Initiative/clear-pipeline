# Medallion pipeline implementation: architecture and first source (Dataminr)

Documents what got built in `defs/gx_pipeline/` — a generic bronze → silver →
gold factory, GX-gated at every promotion, with Dataminr as the first
source wired in. ("Medallion" names the layering pattern; the package and
class names below describe what the code is instead of which pattern built
it — see §3.) Companion to the per-source projected-pipeline docs
(`data-quality-dataminr-pipeline-map.md`, `-acled-`, `-darfur24-`), which
describe the *target*; this doc describes what's actually running.

## 1. Scope

Implements the bronze/silver/gold layers and the incremental push to
clear-api for one source (Dataminr), built as a **generic factory** so
ACLED, Darfur24, and IDMC are additive, not copy-paste. Nothing here writes
to clear-api until the final push step — bronze, silver, and gold are all
S3 artifacts.

Not covered: per-source quality-rule thresholds, aggregations for
ontology-new business objects, and integration tests against real
S3/clear-api — those stay open per the per-source mapping docs.

## 2. Package layout

```
src/clear_pipeline/defs/gx_pipeline/
├── sources.py    # GXSource protocol + per-source adapters (ONLY per-source code)
├── factory.py    # build_gx_source_assets(source) -> 8 assets + 6 GX checks + 1 job
├── gx_utils.py   # Great Expectations Core helper, fully source-agnostic
├── iceberg_events.py  # gold events, SCD2 (§6) — technology-specific, not pattern-named
└── assets.py     # loops GX_SOURCES -> module globals, for Dagster auto-discovery
```

The package/class names are deliberately source-generic and pattern-neutral
— they say what each piece *is* (a GX-gated source, a factory that builds
its assets), not that it happens to implement a bronze/silver/gold
medallion layering internally. That layering is real and worth naming when
*describing* the architecture (this doc does, freely) — it just isn't the
right vocabulary for the code's own identity, the way naming a class
`CQRSHandler` would leak an implementation pattern into an interface that
should just say what it handles.

Mirrors `defs/signals/`'s existing `connectors.py` + `factory.py` +
`assets.py` split deliberately, so the pattern is already familiar to
anyone who's touched the production ingest path.

**Isolated from `defs/signals/`**: this stands up the Dagster-native target
architecture alongside today's production path (`createSignal` at poll
time), not a replacement for it yet.

### The asset chain, per source

```mermaid
flowchart LR
    Bronze["&lt;source&gt;_bronze<br/>raw record -> S3"] -->|"bronze_shape<br/>BLOCKING"| Silver["&lt;source&gt;_silver<br/>normalize, no clear-api write"]
    Silver -->|"silver_completeness<br/>BLOCKING"| Classify["&lt;source&gt;_classify<br/>relevance + type"]
    Classify -->|"classify_populated<br/>observational"| Geo["&lt;source&gt;_geo<br/>heuristic district key"]
    Geo -->|"geo_resolution_rate<br/>observational"| Temporal["&lt;source&gt;_temporal<br/>new-vs-merged vs active window"]
    Temporal -->|"temporal_match_ratio<br/>observational"| Match["&lt;source&gt;_match<br/>create-or-merge, in-memory"]
    Match --> Gold["&lt;source&gt;_gold<br/>write signals + events tables"]
    Gold -->|"gold_integrity<br/>BLOCKING"| Push["&lt;source&gt;_push<br/>pushedAt IS NULL only -> clear-api"]
```

Solid arrows are the data path (each asset's output feeds the next via
`ins=`/`AssetIn`, §7); the check labels ride alongside as `@dg.asset_check`s
on the asset each one gates, not separate nodes in the actual graph.

Each layer's GX check:

| Asset | Check | Blocking? |
|---|---|---|
| `bronze` | shape: id + timestamp present, non-empty batch | Yes |
| `silver` | completeness (title/description), severity range, coord range | Yes |
| `classify` | relevance populated | No — observational |
| `geo` | district-resolution rate | No — observational |
| `temporal` | match-outcome in `{new_event, merged}` | No — observational |
| `gold` | severity range, referential integrity (`eventId` present) | Yes |

Blocking checks halt the next promotion on a suite-level failure past a
configurable threshold (`gx_utils.BLOCK_THRESHOLD`, default 0.5); below
threshold, they warn and the batch proceeds. Per-record failures are
isolated inside each asset (a `try/except` per record), matching the
production ingest's existing failure policy.

## 3. Adding a new source

`GXSource` adapters call each source's `providers/<source>.py` module
directly — **no import from `defs/signals` at all**, not even the
production connector classes. This was a deliberate correction, not the
original shape: the first version had each adapter wrap its matching
`defs/signals/connectors.py` connector by composition. Checking what those
connector classes actually contained showed every method the adapter used
was a one-line passthrough to `providers/<source>.py` — no logic worth
reusing lived in `defs/signals/connectors.py` itself, it exists there only
to satisfy the *production drain's* protocol (`project`/
`to_content_update_input`, which this package never calls). Repointing to
`providers/` directly is byte-identical behavior with one upside: this
package now has zero import dependency on `defs/signals/`, so deleting
`defs/signals/` later (once this package fully replaces the production
poll -> drain path) requires no change here at all.

```mermaid
flowchart TB
    subgraph Providers["providers/&lt;source&gt;.py — shared with production, unchanged"]
        PD["providers/dataminr.py<br/>fetch_signals, get/set_last_synced"]
        PA["providers/acled.py<br/>(same shape)"]
    end

    subgraph GX["defs/gx_pipeline/sources.py"]
        Proto["GXSource protocol<br/>poll, external_id, published_at, raw_bytes,<br/>parse, api_source_id, last_synced,<br/>set_watermark, + to_silver_input()"]
        DMS["DataminrGXSource<br/>calls providers/dataminr.py directly<br/>to_silver_input = build_signal_input(promote=False)"]
        AMS["ACLEDGXSource<br/>not yet written — same recipe"]
        Reg["GX_SOURCES registry"]
    end

    PD -->|called directly| DMS
    PA -.->|would be called directly| AMS
    DMS -->|implements| Proto
    AMS -.->|would implement| Proto
    DMS --> Reg
    AMS -.-> Reg
    Reg --> Factory["factory.py::build_gx_source_assets(source)<br/>— reads only, never changes"]
```

Dotted lines mark what doesn't exist yet (ACLED shown as the worked
example — Darfur24/IDMC follow the same shape). Adding a source touches
`sources.py` only:

1. In `sources.py`, write a small adapter class that calls the source's
   `providers/<source>.py` module directly for everything bronze already
   needs (`poll`, `external_id`, `published_at`, `raw_bytes`, `parse`,
   `api_source_id`, `last_synced`, `set_watermark`), plus **one new
   method**, `to_silver_input(record, source_id) -> dict`, a pure
   transform with no clear-api write.
2. Register the adapter in `GX_SOURCES`.
3. Nothing else changes — `factory.py` and `assets.py` are source-agnostic.

For ACLED and Darfur24, step 1 needs a `promote: bool = False`-style
passthrough added to `build_acled_signal_input` / `build_darfur24_signal_input`
first, mirroring the fix already made to `build_signal_input` (§4). Darfur24
never calls the geoparser at all, so it may not need one.

**On `defs/signals/`'s remaining dependency**: `factory.py` still imports
`defs/signals/lake.py` for generic S3 read/write/list helpers
(`s3_client`/`raw_key`/`write_raw`/`write_json`/`read_json`/`list_keys`).
That module has no source-specific or drain-specific logic — it's a shared
utility that happens to live inside `defs/signals/` as an accident of
where it was first written, not because this package needs anything
`defs/signals/`-specific from it. When `defs/signals/` is actually retired,
the fix is moving that one file to a `providers/`-level location (mirroring
where `providers/s3.py` already sits) and updating both this package's
import and production's own (`defs/signals/factory.py`, `stages.py`). Not
done now — it would mean touching production's import paths today for zero
behavior change, purely to pre-empt a retirement that hasn't happened yet.

## 4. Shared-code changes

Small, additive edits to code the production pipeline also uses — not
duplicated logic:

- **`providers/signal.py::build_signal_input`** gained a keyword-only
  `promote: bool = True` parameter, threaded to `enrich_with_geoparser`.
  Default preserves today's production behavior exactly; the medallion
  silver stage passes `promote=False`. See §5.1 for why this was necessary.
- **`defs/signals/lake.py`**:
  - `raw_key(..., layer: str = "raw")` — silver reuses the same
    day-partitioned key shape (`layer="silver"`), bronze unchanged.
  - New generic `write_json` / `read_json` / `list_keys` helpers for
    silver/gold's arbitrary-key JSON reads and writes (gold isn't
    date-partitioned like bronze/silver — it's keyed by signal/event id
    and rewritten in place as state changes).

## 5. Design decisions made during implementation

Two real gaps surfaced while building this that the projected-pipeline
docs hadn't fully resolved — both required a decision, not just naming.

### 5.1 Geoparser promotion was a hidden clear-api write

`build_signal_input` unconditionally called `enrich_with_geoparser` with
its default `promote=True`, which can create a real L4 landmark location
row in clear-api (`findOrCreateLandmarkL4`, a mutation) as a side effect of
what the projected-pipeline doc called a "no clear-api write" silver stage.
Reusing the function as-is would have silently broken that guarantee.

**Fix**: added the `promote` passthrough (§4) rather than duplicating
`build_signal_input`'s severity/casualties/description logic in the
medallion module. Silver still captures `geoparsedData` either way — only
the opportunistic location-row creation is skipped.

### 5.2 Admin-2 resolution needs a location that doesn't exist yet

Production's `resolve_signal_admin2` needs a location dict with
`id`/`level`/`ancestorIds` — which only exists **after** `createSignal` has
run server-side PostGIS resolution. Since silver/geo/temporal/match all
happen before any clear-api write, there is no authoritative admin-2 to
cluster on at that point.

**Decision** (first-pass, flagged in `factory.py`'s docstring with its
upgrade path):

- **Pre-push clustering** (`<source>_geo`) uses a heuristic: the second
  comma-segment of the geoparser's `display_name` (e.g. "El Fasher, North
  Darfur, Sudan" → "North Darfur"). Approximate, not authoritative.
- **The real `Event.locationId`** is resolved in `<source>_push`, right
  after each signal's `createSignal` call returns a real, PostGIS-resolved
  location — using the exact same `resolve_signal_admin2` production
  already relies on.

Upgrade path: once a read-only admin-2-from-coordinates lookup exists
outside clear-api, swap it into the geo stage and drop the heuristic.

```mermaid
sequenceDiagram
    participant Geo as &lt;source&gt;_geo
    participant Match as &lt;source&gt;_match
    participant Gold as &lt;source&gt;_gold (S3)
    participant Push as &lt;source&gt;_push
    participant API as clear-api

    Note over Geo: districtKey = display_name.split(",")[1]<br/>(heuristic — no clear-api call)
    Geo->>Match: cluster candidates by (districtKey, eventType)
    Match->>Gold: write gold event, districtKey still heuristic
    Note over Gold: gold rows sit with pushedAt = null<br/>until a push run picks them up
    Push->>API: createSignal(signalInput) per unpushed row
    API-->>Push: real, PostGIS-resolved location
    Note over Push: resolve_signal_admin2(createSignal result)<br/>— the SAME function production uses
    Push->>API: createEvent / updateEvent with the AUTHORITATIVE locationId
```

The heuristic only ever decides *which signals cluster together* before a
push exists to check against; the value that actually lands on the `Event`
row is always the real one, resolved the same way production resolves it
today.

### 5.3 Other documented simplifications

- **No LLM rewrite of merged event title/description.** Bootstrap only:
  first signal's title/description on create, latest signal's on merge.
  Upgrade path: call an equivalent rewrite once the gold event shape is
  stable and worth the LLM cost.
- **Single-writer S3 read-modify-write**, no cross-run locking (production
  wraps its equivalent step in `redis_lock`). Fine for one Dagster run at a
  time; add a lock if this ever runs concurrently.

## 6. Gold events persistence: SCD2, decided but not yet built

Today's committed code writes `gold/<source>/events/<eventId>.json` as a
single mutable file, overwritten in place every time a new signal merges
in — no history retained. That's fine for `<source>_push`'s own needs (it
only ever reads the current state), but it means there's no way to answer
"what did this Event look like before signal X merged in," which matters
for audit and debugging severity/title changes after the fact.

**Not everything in gold needs this** — only the events table is a genuine
slowly-changing dimension:

| Gold table | Nature | SCD2? |
|---|---|---|
| `signals` | written once, `pushedAt` flips null → timestamp | No — one status flip, Type-1 overwrite stays fine |
| `events` | title/description/severity/population/signalIds all change as signals merge in over the active window | **Yes** |

### 6.1 Format decision: Apache Iceberg, not Delta Lake

Both are "Parquet data files + a metadata/transaction log" — the data
itself is never locked in either way, and either format's native `MERGE`
gives the standard SCD2 upsert-or-close-and-insert pattern for free
instead of hand-rolling versioned files + a compaction story.

The deciding factor is **governance, not engineering**, and it matters
more here than usual because this project already leans toward
self-hosted, sovereignty-conscious infrastructure (S3-compatible storage,
Scaleway as the intended second cloud) rather than a single managed
vendor's stack:

- **Iceberg** was donated by Netflix to the **Apache Software
  Foundation**, which structurally requires a diverse committer/PMC base —
  no single company can dominate the roadmap by contribution volume alone.
  It also has broad *native*, multi-vendor support (Snowflake, AWS — S3
  Tables is built on it — BigQuery/BigLake, Trino, Flink) that isn't
  anchored to one company's compute product.
- **Delta Lake** was created by Databricks and donated to the **Linux
  Foundation**, which is genuinely open-license but doesn't carry the same
  anti-capture structural requirement — Databricks remains by far the
  dominant contributor and roadmap driver in practice. Its strongest
  support is naturally where Databricks sits in the stack.

**Caveat, deliberately not resolved here**: PyIceberg's native Python
write/merge path has historically lagged delta-rs's — this needs a direct
check (`pip show pyiceberg`, its current changelog, a small write/merge
smoke test) before committing, not an assumption carried over from either
side of this conversation. If PyIceberg's merge support turns out too
immature for a clean SCD2 implementation, that's a real reason to revisit
Delta despite the governance trade-off above — worth surfacing as a
blocker if hit, not silently working around.

### 6.2 SCD2 markers

```python
{
    "eventId": "...",              # business key, stable across versions
    "version": 3,                   # monotonic per eventId
    "effectiveFrom": "2026-09-10T12:00:00Z",
    "effectiveTo": None,             # None = current row
    "isCurrent": True,               # redundant with effectiveTo is None, kept for query ergonomics
    "contentHash": "sha256(...)",    # hash of the mutable fields — skip writing
                                       # a new version when nothing changed
    # ...the rest of today's event fields (title, description, severity,
    # signalIds, population, ...)
}
```

`effectiveTo: None` rather than a far-future sentinel date — this project
has no raw-SQL warehouse joins to keep NULL-free, and Iceberg/DuckDB both
handle NULL ranges cleanly. `contentHash` mirrors the content-hash dedup
pattern already used elsewhere in this codebase (IDMC's `_content_hash`,
the translation staleness check) — without it, every `push` run that
touches an event would write a "new version" even when nothing about it
actually changed, turning the history into noise instead of a signal.

```mermaid
flowchart TD
    NewSig["new signal merges into event"] --> Hash["compute contentHash<br/>over title/description/severity/signalIds/population"]
    Hash --> Cmp{"contentHash == current version's?"}
    Cmp -->|yes| Skip["no-op — current row stays current<br/>(avoids version-spam on idempotent re-runs)"]
    Cmp -->|no| Close["MERGE: close current row<br/>effectiveTo = now, isCurrent = false"]
    Close --> Insert["insert new row<br/>version += 1, effectiveFrom = now, isCurrent = true"]
```

### 6.3 Status

Built (`defs/gx_pipeline/iceberg_events.py`), wired into `factory.py`, verified
on Dataminr.

**§6.1's caveat resolved, not assumed.** Before writing any pipeline code,
PyIceberg 0.12.0 was installed and its actual write API checked directly:
`Table.upsert` exists but is Type-1 (updates matching rows in place —
wrong tool for SCD2, would destroy history), while `Table.overwrite(df,
overwrite_filter=...)` does exactly what's needed — atomically replace the
rows matching a filter with new content, so "close the current row +
insert the next version" happens as one commit. Confirmed empirically with
a standalone script against a real local Iceberg table before it was ever
wired into `factory.py`: append for a brand-new key, `overwrite` for a
changed one, full history retained, `isCurrent` scans return only the
current row. The pattern is now the exact content of `iceberg_events.py`'s
`merge_event`.

**Catalog**: `pyiceberg`'s SQL catalog (`ICEBERG_CATALOG_URI` /
`ICEBERG_WAREHOUSE`, `signals/config.py`, documented in `.env.example`).
SQLite by default — fine for dev/CI, not durable across pods/redeploys.

Checked before recommending Postgres for production, not assumed: does
"reuse the existing Postgres" mean literally `DAGSTER_POSTGRES_URL`?
`deploy/dagster.yaml` says no — that database is explicitly "DEDICATED to
Dagster... Dagster owns and migrates its own schema." Putting Iceberg's
tables there would violate a boundary this repo already drew on purpose.
The production value is a **separate database on the same Postgres
server** — same instance, so still genuinely "no new infrastructure," just
`CREATE DATABASE iceberg_catalog;` next to the one Dagster owns.

Verified against a real Postgres 16 instance (a throwaway container, not
assumed from docs): `postgresql+psycopg2://...` — the `+psycopg2` driver
suffix is required, plain `postgresql://` isn't enough for PyIceberg's
SQLAlchemy-based SQL catalog. `psycopg2` is already present transitively
via `dagster-postgres`, so no new dependency either. The catalog creates
exactly two tables (`iceberg_tables`, `iceberg_namespace_properties`) — no
collision risk with Dagster's own tables even before considering the
separate-database decision above.

Also checked, since it's the obvious follow-up question: can a write skip
the catalog entirely? PyIceberg's catalog-less `StaticTable` exists, but
it's backed by a `NoopCatalog` whose `commit_table` unconditionally raises
`NotImplementedError` — confirmed by reading the installed source, not
assumed. It's built for reading one known, pinned snapshot, not for the
ongoing merge pattern this table needs. Some catalog is structurally
required for writing — it's the thing that durably tracks "which
`metadata.json` is current" between separate Dagster run processes.

**`clearApiEventId` lives outside the Iceberg table on purpose** — a small
side JSON file (`gold/<source>/events_push_state/<eventId>.json`), written
only by `<source>_push`. It's operational push-cursor state, not business
content: baking it into the table would spawn a spurious new SCD2 version
on every push regardless of whether the event's content actually changed.
`<source>_push` is a pure *reader* of the Iceberg table — the only writer
is `<source>_gold`.

**Verified**: `tests/test_medallion_pipeline.py` now includes
`test_iceberg_events_scd2_versioning` (append, idempotent no-op, a real
content change producing v2 with v1 closed, full history queryable) and
the end-to-end Dagster test asserts against the actual Iceberg table
(`current_events_df`/`current_event`) instead of S3 JSON.

Gold **signals** keeps its original shape (S3 JSON, `pushedAt` overwrite)
— it was never a slowly-changing dimension, see §6's table.

## 7. A real wiring bug, found by testing rather than assumed away

The first draft wired Dagster's per-asset data dependencies with
`deps=["upstream_asset"]` plus a `**kwargs` catch-all parameter on the
downstream function, expecting Dagster to inject the upstream value by
name. **It doesn't** — `deps=` alone declares ordering only; Dagster
delivers an *empty* `kwargs` with no error, so every downstream asset would
have silently processed nothing.

Confirmed with a minimal reproduction before touching the real factory,
then fixed throughout with explicit input mapping:

```python
@dg.asset(name=f"{src}_silver", ins={"bronze_df": dg.AssetIn(key=f"{src}_bronze")})
def _silver(context, bronze_df: pd.DataFrame) -> pd.DataFrame:
    ...
```

This is exactly the class of bug a structural "does it import cleanly"
check would miss — worth calling out because it's why §8's end-to-end test
runs the real Dagster execution engine rather than calling functions
directly.

## 8. Verification

- **`tests/test_medallion_pipeline.py`** — a real `dagster.materialize()`
  run (not mocked at the Dagster level) against a fake in-memory S3 and
  fake `create_signal`/`create_event`/`escalate_to_alert`. Two synthetic
  records in the same district merge into one gold event; both push and
  get `pushedAt` stamped; a second run against the same state re-pushes
  nothing, confirming the incremental cursor. This is the test that would
  have caught §7's bug.
- **`ruff check`** — clean on every new/changed file.
- **`ty check`** — clean on `gx_utils.py` and `sources.py` (fixed one real
  finding: `DataminrGXSource.source` was a read-only `@property`
  against a `GXSource` protocol declaring a mutable `str` field —
  narrowed the protocol member to a read-only property). `factory.py`
  reports false positives against Great Expectations' dynamically-built
  Pydantic expectation classes, which no static checker resolves without
  GX-specific stubs — confirmed false by the real GX validation runs
  succeeding in the test above.
- **Existing suite**: `tests/test_signals_ingest_drain.py` still passes
  (17/18 — the one failure is a pre-existing sandbox permission error
  downloading a HuggingFace model, unrelated to this change and present on
  `dev` beforehand).

## 9. Not covered here

- Per-source quality-rule thresholds (blocking vs. non-blocking cutoffs) →
  task 2.
- Aggregations for ontology business objects beyond Signal/Event/Alert →
  task 3 (blocked on business-side ontology clarifications).
- ACLED / Darfur24 / IDMC adapters → §3's recipe, not yet written.
- Iceberg-backed gold events (SCD2) → §6, decided, not yet built.
- Integration tests against real S3/clear-api → task 6.
