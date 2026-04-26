# CLEAR Pipeline — Architecture

This document is the authoritative reference for how the pipeline is
structured today. It supersedes the earlier single-source Dataminr-only
sketch. For historical context on the original spec, see [PRD.md](./PRD.md).

## Responsibilities

The clear-pipeline is a standalone Python service that:

1. **Polls external data sources** (Dataminr, GDACS, ACLED) on timers.
2. **Normalises signals** — resolves locations, stores raw payloads.
3. **Classifies signals** with Claude (disaster type, severity, relevance).
4. **Clusters signals into events** using Claude to decide "add to existing"
   vs "create new".
5. **Escalates high-severity events to alerts** — Claude-assessed for
   Dataminr, auto-escalated for trusted manual sources.
6. **Enriches situations** — computes `populationInArea` from WorldPop
   raster and generates `title`/`summary` via Claude, dispatched by
   clear-api.
7. **Manages admin geometry** — fetches OCHA HDX admin boundaries and
   populates location polygons / population caches.
8. **Archives stale records** — nightly job that moves old alerts to
   `archived` status.

Everything is wired through Celery + Redis. State (signals, events,
alerts, situations, locations) lives in the CLEAR Postgres/PostGIS DB,
accessed via the clear-api GraphQL endpoint.

---

## Process / Deployment Shape

```
┌─────────────────────────────────────────────────────────────┐
│  Celery Worker + Beat (single container)                    │
│  celery -A src.celery_app worker --beat --concurrency=4     │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Beat scheduler (cron)                               │   │
│  │  poll_dataminr    every 15s                         │   │
│  │  poll_gdacs       every 30m                         │   │
│  │  poll_acled       every 60m                         │   │
│  │  daily/weekly/monthly alert digests                 │   │
│  │  archive_stale_alerts  03:00 UTC                    │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Worker prefork pool (4 child processes)             │   │
│  │  executes all tasks from src/tasks/                 │   │
│  └─────────────────────────────────────────────────────┘   │
└──────────────────────┬──────────────────────────────────────┘
                       │
              ┌────────┴─────────┐
              │   Redis (remote) │
              │ broker + results │
              │ + state cache    │
              └────────┬─────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  clear-api (GraphQL over HTTPS)                              │
│   - Apollo Server + Prisma + Postgres/PostGIS                │
│   - Subscription fan-out, situation dispatch                 │
└─────────────────────────────────────────────────────────────┘
```

The clear-api also dispatches Celery tasks **back** to the pipeline by
publishing directly to the same Redis broker (see
`clear-api/src/services/celery.ts`). Used for situation enrichment.

---

## Repository Layout

```
clear-pipeline/
├── src/
│   ├── celery_app.py           # Celery app + beat schedule + log signals
│   ├── config.py               # Pydantic settings (env-driven)
│   ├── logging_setup.py        # stdout + Logtail + Sentry wiring
│   ├── clients/
│   │   ├── claude.py           # Anthropic SDK wrapper + rate-limit handling
│   │   ├── dataminr.py         # Dataminr First Alert auth + fetch
│   │   ├── acled.py            # ACLED auth + fetch
│   │   ├── gdacs.py            # GDACS public API client
│   │   └── graphql.py          # Generic GraphQL client + all CLEAR mutations/queries
│   ├── models/
│   │   ├── clear.py            # Claude structured-output models
│   │   └── dataminr.py         # Dataminr response models
│   ├── prompts/
│   │   ├── classify.py         # Signal classification (level-1 constrained)
│   │   ├── group.py            # Event grouping
│   │   ├── assess.py           # Alert assessment
│   │   └── situation.py        # Situation narrative
│   ├── services/
│   │   ├── alert.py            # Alert escalation via Claude
│   │   ├── event.py            # Event clustering (group_signal)
│   │   ├── signal.py           # Signal creation + location mapping
│   │   ├── location.py         # Claude-driven location extraction
│   │   └── population.py       # WorldPop raster masking
│   └── tasks/
│       ├── poll.py             # poll_dataminr
│       ├── poll_gdacs.py       # poll_gdacs
│       ├── poll_acled.py       # poll_acled
│       ├── process.py          # process_signal / _manual_ / _gdacs_ / _acled_
│       ├── situation.py        # enrich_situation (population + narrative)
│       ├── population.py       # backfill_location_population
│       ├── geometries.py       # backfill_admin_geometries (OCHA HDX)
│       ├── archive.py          # archive_stale_alerts (daily)
│       └── notify.py           # daily/weekly/monthly digest dispatch
└── scripts/                    # Synchronous one-shot runners (no Celery)
    ├── backfill_admin_geometries_sync.py
    ├── backfill_location_pcodes.py       # spatial + fuzzy-name → pCode
    ├── backfill_location_population.py
    ├── backfill_missing_admin_locations.py
    ├── find_duplicate_pcodes.py
    └── upload_population_tiff.py
```

---

## Data Sources

| Source | Cadence | Client | Auth | Structured? |
|---|---|---|---|---|
| **Dataminr First Alert** | 15 s | `clients/dataminr.py` | OAuth2 client credentials (token cached in Redis, ~3.5 h) | Semi — Claude classifies |
| **GDACS** | 30 min | `clients/gdacs.py` | Public (no auth) | Yes — skip Claude classify, use GDACS metadata |
| **ACLED** | 60 min | `clients/acled.py` | Session cookie (cached 23 h in Redis) | Yes — skip Claude classify, derive severity from fatalities |

Each poll task dispatches one `process_*_signal` task per new signal.

Manual signals from trusted sources (`field_officer`, `partner`,
`government`) are dispatched by clear-api's `createManualSignal` mutation
via the Redis broker → `process_manual_signal` task.

---

## End-to-End Signal Flow

### Dataminr path

```
poll_dataminr (beat, every 15s)
  │
  ├── refresh token if needed
  ├── fetch since last_synced
  ├── dedup against Redis seen-set
  │
  └─→ process_signal  [per signal]
        │
        ├── ingest_signal     (resolve location, createSignal mutation)
        ├── Claude: classify  (disaster types, severity, relevance)
        ├── update_signal_severity if changed
        │
        └── if relevance ≥ threshold:
              group_signal  (Claude: add-to-existing vs create-new event)
                │
                └── if event.severity ≥ 4:
                      assess_and_escalate  (Claude: alert-worthy?)
                        │
                        └── if yes → createAlert(status="published")
                              → clear-api fans out notifications
                              (filtered by subscriber minSeverity + location ancestors)
```

### GDACS / ACLED paths

Structured feeds — we **skip Claude classification** and build the
`SignalClassification` directly from source metadata (`glide_type`,
`alert_level`, `fatalities`). Everything downstream (`group_signal`,
`assess_and_escalate`) is the same.

### Manual signal path (trusted sources)

```
process_manual_signal
  ├── Claude: classify (title + description)
  ├── group_signal (event clustering)
  └── if source_type in TRUSTED_SOURCE_NAMES:
        escalate_event  (direct, no Claude gate)
```

### Situation enrichment path

Triggered by **clear-api** when an analyst runs `createSituationFromEvents`
or `addEventToSituation`:

```
clear-api → sendCeleryTask → Redis → enrich_situation (src/tasks/situation.py)
  │
  ├── _compute_population_in_area(district_ids)
  │     ├── for each district: read cached location.population
  │     └── for districts missing cache: union polygons → raster mask
  │
  └── _generate_narrative(events)
        └── Claude: title + summary across linked events
  │
  └── updateSituationPopulation mutation (single write-back)
        { populationInArea, title, summary }
```

---

## Admin Geometry & Population (Sudan)

### Sources

- **Admin boundaries**: OCHA HDX
  (`sdn_admin_boundaries.geojson.zip` — admin0/1/2 GeoJSON).
- **Population raster**: WorldPop 2026 constrained 100 m
  (`sdn_pop_2026_CN_100m_R2025A_v1.tif`) — stored in S3 and downloaded /
  cached locally on first use.

### CLEAR locations model

```
level 0: country (Sudan)
level 1: state
level 2: district  ← boundaries here
level 4: signal point (from signal lat/lng)
```

`locations.pCode` is the canonical OCHA pCode. `locations.geometry` is
PostGIS (Point or MultiPolygon). `locations.population` caches the
raster-derived population of each admin's polygon.

### Backfill order (one-time setup per country)

1. `scripts/backfill_admin_geometries_sync.py --levels 0` — country polygon.
2. `scripts/backfill_location_pcodes.py --name-fallback` — spatial + fuzzy
   name match to assign pCodes to existing CLEAR rows.
3. `scripts/backfill_missing_admin_locations.py` — create any admin units
   CLEAR doesn't yet have (links via OCHA parent pCodes).
4. `scripts/backfill_admin_geometries_sync.py` — now matches by pCode
   cleanly, populates real MultiPolygons.
5. `scripts/backfill_location_population.py` — caches
   `location.population` per admin level (raster mask).
6. `scripts/find_duplicate_pcodes.py` — verify, manually resolve dupes.

### Runtime usage

- **Signal creation**: resolves lat/lng → nearest level-4 point
  (creates one if none within ~500 m).
- **Situation population**: `enrich_situation` reads cached
  `location.population` for each event's districts and falls back to
  raster only when missing.

---

## Disaster Type Hierarchy

Three-level taxonomy populated by
`clear-api/scripts/seed-disaster-types.ts`:

```
level_1 (top category)    e.g. "conflict", "natural hazard", "famine"
  level_2 (group)         e.g. "protests", "battles", "flood"
    level_3 (sub-type)    e.g. "peaceful protest", "armed clash", "flood"
```

- `disasterTypes.glideNumber` — GLIDE code (natural hazards) or CLEAR id
  (conflict / crisis), unique per `level_3`.
- `events.types[]` stores glide codes (array; all codes must share the
  same `level_1` — enforced by the classification prompt).
- `userAlertSubscriptions.alertType` stores a glide code per row. The
  clear-mvp UI uses a tree picker; selecting a level_1 or level_2
  expands to the underlying glide codes client-side and creates rows in
  one `subscribeToAlertsBatch` call.

---

## Redis Usage

| Key Pattern | Purpose | TTL |
|---|---|---|
| `dataminr:token` | Dataminr access token | 3.5 h |
| `dataminr:last_synced` | ISO timestamp of latest ingested Dataminr signal | persistent |
| `dataminr:seen:{alertId}` | Dedup set | 48 h |
| `acled:cookie` | ACLED session cookie | 23 h |
| `classification:{signalId}` | Claude classification result | 24 h |
| `events:active` | Active events for grouping prompts | 5 min |
| `locations:cache` | Location lookups for geo-resolution | 6 h |
| `celery-*` / `_kombu.binding.*` | Celery broker state | managed by Celery |

---

## Claude Integration

### Where it's called

| Stage | File | Prompt | Output |
|---|---|---|---|
| Signal classification | `services/signal.py` + `process_signal` | `prompts/classify.py` | `{ disaster_types, severity, relevance, summary }` |
| Event grouping | `services/event.py` | `prompts/group.py` | `{ action, event_id?, title?, description?, types?, population_affected? }` |
| Alert assessment | `services/alert.py` | `prompts/assess.py` | `{ should_alert, status, reasoning }` |
| Situation narrative | `tasks/situation.py` | `prompts/situation.py` | `{ title, summary }` |
| Location extraction | `services/location.py` | inline | `{ origin, destination, general }` |

### Response parsing

`clients/claude.py :: _extract_json` handles three cases in order:
1. Response is already pure JSON.
2. JSON inside a `` ``` `` markdown fence.
3. First balanced `{ ... }` object inside prose.

This makes the pipeline tolerant of models that occasionally wrap JSON in
explanation.

### Rate-limit handling

Three defensive layers:

1. **SDK auto-retry** — `Anthropic(max_retries=5)` with exponential
   backoff on 408/409/429/5xx.
2. **Typed exception** — `ClaudeRateLimited` raised by `call_claude` when
   the SDK exhausts retries; carries `retry_after` extracted from the
   response header (`retry-after`), with ±25 % jitter to prevent
   thundering-herd.
3. **Celery-aware reschedule** — each Claude-calling task catches
   `ClaudeRateLimited` *before* its generic `except Exception` and
   retries with `countdown=exc.retry_after` (vs the default 10 s which
   is too short for 429 backoff).

Non-retryable errors (400/401/403/404 `APIStatusError`) bubble up with
their status code logged.

---

## Scheduled Tasks

| Task | Schedule (UTC) | Purpose |
|---|---|---|
| `poll_dataminr` | every `POLL_INTERVAL_SECONDS` (15 s) | Dataminr ingest |
| `poll_gdacs` | every `GDACS_POLL_INTERVAL_MINUTES` (30 m) | GDACS ingest |
| `poll_acled` | every `ACLED_POLL_INTERVAL_MINUTES` (60 m) | ACLED ingest |
| `send_daily_digest` | 07:00 daily | Daily alert email digest |
| `send_weekly_digest` | 07:00 Monday | Weekly email digest |
| `send_monthly_digest` | 07:00 1st of month | Monthly email digest |
| `archive_stale_alerts` | 03:00 daily | `status → archived` for alerts whose event.lastSignalCreatedAt > 14 d |

Additional tasks (triggered, not scheduled):
- `process_signal` / `process_manual_signal` / `process_gdacs_signal` /
  `process_acled_signal`
- `enrich_situation` (dispatched by clear-api)
- `backfill_admin_geometries`, `backfill_location_population` (manual)

---

## Observability

- **stdout** — Python logging with `[SOURCE]` prefix per feed
  (`[DATAMINR]`, `[GDACS]`, `[ACLED]`, `[POPULATION]`, `[SITUATION]`,
  `[GEOMETRIES]`, `[ARCHIVE]`, `[CLAUDE]`).
- **Better Stack (Logtail)** — attached when `LOGTAIL_SOURCE_TOKEN` is
  set, via `logging_setup.py`. Uses `after_setup_logger` /
  `after_setup_task_logger` Celery signals so child-process task logs
  are visible.
- **Sentry** — error tracking + Celery integration when `SENTRY_DSN` is
  set. Scrubs `authorization`/`cookie`/`x-api-key` before send.

---

## Environment Variables

Minimum set the worker needs to run:

```env
# Data sources
DATAMINR_CLIENT_ID=
DATAMINR_CLIENT_SECRET=
ACLED_EMAIL=
ACLED_API_KEY=

# Claude
ANTHROPIC_API_KEY=
CLAUDE_MODEL=claude-sonnet-4-6

# CLEAR API
CLEAR_API_URL=https://api.clear.example.com/graphql
CLEAR_API_KEY=sk_live_...

# Redis (broker + cache + state)
REDIS_URL=redis://...
CELERY_BROKER_URL=redis://...

# S3 (population raster + media)
S3_ENDPOINT=https://t3.storageapi.dev
S3_BUCKET=clear-media
S3_REGION=auto
S3_ACCESS_KEY_ID=
S3_SECRET_ACCESS_KEY=

# Observability (optional)
LOGTAIL_SOURCE_TOKEN=
SENTRY_DSN=
SENTRY_ENV=production
LOG_LEVEL=INFO

# Tuning
POLL_INTERVAL_SECONDS=15
GDACS_POLL_INTERVAL_MINUTES=30
ACLED_POLL_INTERVAL_MINUTES=60
INITIAL_LOOKBACK_DAYS=7
RELEVANCE_THRESHOLD=0.5
```

---

## GraphQL Mutations the Pipeline Calls

| Mutation | Task(s) | Purpose |
|---|---|---|
| `createSignal` | `process_signal`, `process_gdacs_signal`, `process_acled_signal` | Ingest every new signal |
| `updateSignalSeverity` | process tasks | Align with Claude classification |
| `createEvent` / `updateEvent` | `group_signal` | Cluster signals |
| `createAlert` | `assess_and_escalate` | High-severity escalation (published) |
| `escalateEvent` | `process_manual_signal` | Trusted source auto-escalate |
| `notifyAlertSubscribers` / `notifyAlertDigest` | `notify.py` digests | Email fan-out |
| `updateLocationGeometry` / `updateLocationPopulation` / `updateLocation` / `createLocation` | geometry & population backfill scripts | Admin data management |
| `updateSituationPopulation` | `enrich_situation` | Write back title/summary/populationInArea |
| `archiveStaleAlerts` | `archive_stale_alerts` | Bulk archival |

All mutations go through `clients/graphql.py :: _execute` which adds
`Authorization: Bearer ${CLEAR_API_KEY}` and retries 3× with exponential
backoff on HTTP errors.

---

## Historical Pipeline Diagram

The mermaid sequence diagram in the previous version of this doc was
**Dataminr-only** and is superseded by the flow diagrams in "End-to-End
Signal Flow" above. For the original product spec, see
[PRD.md](./PRD.md).
