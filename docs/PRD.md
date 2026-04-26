# CLEAR Pipeline — Product Requirements Document

> **Note (historical):** This PRD captures the original single-source
> (Dataminr) pipeline spec that kicked off the project. The pipeline has
> since expanded to include GDACS, ACLED, manual signals, situation
> enrichment, admin-geometry backfill, population computation, and more.
> For the current system reference, see [ARCHITECTURE.md](./ARCHITECTURE.md).

## Overview

**clear-pipeline** is a standalone Python data ingestion service that retrieves real-time signals from the Dataminr First Alert API (Dataminr calls them "alerts", we call them **signals**), processes them through Claude-powered ML stages for classification, event clustering, and severity assessment, then writes structured results into the CLEAR system via the Apollo GraphQL API (`clear-api`).

## Tech Stack

- **Python 3.12+**
- **Celery** — task queue for periodic polling and async processing
- **Redis** — message broker for Celery + intermediate cache
- **Claude API (Anthropic SDK)** — ML operations (classification, event grouping, severity assessment)
- **httpx + gql** — GraphQL mutations against clear-api
- **Pydantic** — data validation and schema definitions

## Terminology

| Dataminr term | CLEAR term | Description |
|---|---|---|
| Alert | **Signal** | Raw data item from a data source |
| _(N/A)_ | **Event** | Cluster of related signals forming a coherent situation |
| _(N/A)_ | **Alert** | An event escalated for user notification |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Celery Beat                            │
│              (every POLL_INTERVAL_SECONDS)                  │
└───────────────────────┬─────────────────────────────────────┘
                        │ triggers
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Task: poll_dataminr                             │
│  1. Get/refresh Dataminr auth token (cached in Redis)       │
│  2. Determine time window:                                  │
│     - First run: last 7 days → now                          │
│     - Subsequent: last synced timestamp → now               │
│  3. GET /firstalert/v1/alerts (paginate via nextPage)       │
│  4. Deduplicate against Redis seen-set                      │
│  5. For each new signal → dispatch process_signal task      │
│  6. Update last-synced timestamp in Redis                   │
└───────────────────────┬─────────────────────────────────────┘
                        │ fan-out
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Task: process_signal                            │
│  1. Parse Dataminr payload → normalize fields               │
│  2. Resolve lat/lon → nearest CLEAR location (PostGIS)      │
│  3. Call createSignal mutation on clear-api                  │
│  4. Claude: classify signal (disaster type, relevance)      │
│  5. If relevant → event grouping stage                      │
│     - Claude: match to existing event or create new         │
│     - Call createEvent or link via signalEvents              │
│  6. If high severity → alert escalation                     │
│     - Claude: assess alert-worthiness                       │
│     - Call createAlert mutation                             │
└─────────────────────────────────────────────────────────────┘
```

## Time-Window Sync Strategy

Instead of persisting a Dataminr pagination cursor, the pipeline uses a **time-window approach**:

1. **First run**: Fetch all signals from 7 days ago until now
2. **Subsequent runs**: Fetch from the timestamp of the most recently ingested signal in the DB until now
3. The "last synced" timestamp is stored in Redis (`dataminr:last_synced`) and also derivable by querying the latest signal's `publishedAt` from clear-api
4. Dataminr responses are paginated via `nextPage` — follow all pages within the time window, then stop

This means no cursor persistence is needed across restarts. On restart, the pipeline queries clear-api for the latest signal's `publishedAt` and resumes from there.

## Redis Usage

| Key Pattern | Purpose | TTL |
|---|---|---|
| `dataminr:token` | Cached Dataminr access token | 3.5 hours |
| `dataminr:last_synced` | ISO timestamp of last ingested signal | persistent |
| `dataminr:seen:{alertId}` | Deduplication set | 48 hours |
| `classification:{signalId}` | Claude classification result cache | 24 hours |
| `events:active` | Cached active event summaries for grouping | 1 hour |
| `locations:cache` | CLEAR locations for geo-resolution | 6 hours |

## GraphQL Mutations Used

| Stage | Mutation | When |
|---|---|---|
| Signal ingestion | `createSignal` | Every Dataminr signal |
| Event creation | `createEvent` | Claude determines signal is relevant + novel |
| Alert escalation | `createAlert` | Claude determines event is high severity |

## Data Flow: Dataminr → CLEAR Signal

| Dataminr Field | CLEAR Signal Field | Notes |
|---|---|---|
| _(entire payload)_ | `rawData` | Full JSON preserved |
| `alertTimestamp` | `publishedAt` | ISO-8601 → DateTime |
| _(poll time)_ | `collectedAt` | Auto-set by DB default |
| `headline` | `title` | |
| `subHeadline.title` + `subHeadline.subHeadlines` | `description` | Concatenated |
| `publicPost.href` | `url` | |
| `estimatedEventLocation.coordinates` | `originId` | Resolve [lat,lon] → nearest CLEAR location via PostGIS |
| _(dataminr data source cuid)_ | `sourceId` | FK to `data_sources` table |

## Location Resolution via PostGIS

The pipeline resolves Dataminr coordinates to CLEAR locations using a **PostGIS spatial query** via a dedicated GraphQL query or raw SQL through the API:

1. Dataminr provides `estimatedEventLocation.coordinates` as `[lat, lon]`
2. Pipeline calls a location resolution endpoint/query on clear-api that runs:
   ```sql
   SELECT id, name, level,
          ST_Distance(geometry, ST_SetSRID(ST_MakePoint(lon, lat), 4326)) AS distance
   FROM locations
   ORDER BY geometry <-> ST_SetSRID(ST_MakePoint(lon, lat), 4326)
   LIMIT 1;
   ```
3. Returns the nearest location ID at the most granular level available
4. Results cached in Redis keyed by rounded coordinates (0.1° grid)

**Note**: Real PostGIS geometries for Sudan (states, localities) must be loaded into the `locations` table. The pipeline will NOT use seed placeholder data — it needs actual boundaries/points from sources like GADM, Natural Earth, or HDX.

## Claude ML Operations

### Stage 1: Signal Classification (per signal)

**Input**: Signal title, description, rawData, location name
**Output**: `{ disasterTypes: string[], relevance: float, severity: int, summary: string }`

Claude classifies the signal:
- Map to disaster types from the `disaster_types` table (glide numbers)
- Assess relevance to Sudan humanitarian context (0.0–1.0)
- Rate severity (1–5)
- Generate a one-line summary

### Stage 2: Event Grouping (if relevance > threshold)

**Input**: New signal classification + list of active events (cached from clear-api)
**Output**: `{ action: "create_new" | "add_to_existing", eventId?: string, title?: string, description?: string, types?: string[] }`

Claude decides:
- Does this signal belong to an existing active event?
- Or does it represent a new situation requiring a new event?
- If new: generate event title, description, types, estimate valid_from/valid_to

### Stage 3: Alert Assessment (after event creation/update)

**Input**: Event with all linked signals, types, location context
**Output**: `{ shouldAlert: boolean, status: "draft" | "published" }`

Claude determines:
- Is this event severe enough to warrant an alert?
- Should it auto-publish or go to draft for human review?

## Configuration

All credentials are sourced from existing project `.env` files:

```env
# Dataminr (from clear_poc_remote/.env)
DATAMINR_CLIENT_ID=0oaszlxfd5A7a9WwY5d7
DATAMINR_CLIENT_SECRET=gsGhMHwP-PWBHbL8LqLa-I9vYmv0qij_QaGtWwiJayliLAY8ECjeLVHPVqT9NOxe

# Redis (from clear_poc_remote/.env)
REDIS_URL=redis://default:HFpxCPUUZerQtlSxtVCMUGPaVunSrEES@switchyard.proxy.rlwy.net:22081

# CLEAR API (from clear-api/.env)
CLEAR_API_URL=http://localhost:4000/graphql
CLEAR_API_KEY=  # needs a service account API key (sk_live_...)

# Claude (Anthropic)
ANTHROPIC_API_KEY=  # needs provisioning

# Celery
CELERY_BROKER_URL=redis://default:HFpxCPUUZerQtlSxtVCMUGPaVunSrEES@switchyard.proxy.rlwy.net:22081

# Pipeline settings
POLL_INTERVAL_SECONDS=15
INITIAL_LOOKBACK_DAYS=7
RELEVANCE_THRESHOLD=0.5
```

---

## Resolved Prerequisites

- **CLEAR API service account** — Will create a pipeline service account with admin role and add `sk_live_...` key to `.env`
- **Anthropic API key** — Will provision and add to `.env`
- **Dataminr alert lists** — Sudan topic lists are configured in the Dataminr web app
- **Sudan PostGIS data** — Will load real boundaries from GADM/HDX (task T7.1)

## Open Questions (resolve during development)

1. **Claude model choice** — Recommend `claude-sonnet-4-6` for all stages (good balance of cost/speed/accuracy). Can upgrade individual stages later.

2. **Relevance threshold** — What `relevance` score from Claude should trigger event creation? Starting with 0.5, configurable via env.

3. **Alert auto-publish** — All pipeline-generated alerts start as `draft` for human review in v1.

4. **Location resolution query** — Need to add a `nearestLocation(lat, lon)` query to clear-api. Will implement as part of T3.4.

---

## Task List

### Phase 1: Project Setup

- [ ] **T1.1** Initialize Python project (pyproject.toml with uv, .gitignore, .env.example)
- [ ] **T1.2** Set up project structure:
  ```
  clear_pipeline/
  ├── __init__.py
  ├── config.py           # Pydantic settings from env
  ├── celery_app.py        # Celery app + beat schedule
  ├── tasks/
  │   ├── __init__.py
  │   ├── poll.py          # poll_dataminr task
  │   └── process.py       # process_signal task
  ├── clients/
  │   ├── __init__.py
  │   ├── dataminr.py      # Dataminr API client (auth + fetch)
  │   ├── graphql.py       # CLEAR API GraphQL client
  │   └── claude.py        # Anthropic SDK wrapper
  ├── models/
  │   ├── __init__.py
  │   ├── dataminr.py      # Pydantic: Dataminr response schemas
  │   └── clear.py         # Pydantic: GraphQL mutation inputs
  ├── services/
  │   ├── __init__.py
  │   ├── signal.py        # Signal creation + field mapping
  │   ├── event.py         # Event grouping logic
  │   ├── alert.py         # Alert escalation logic
  │   └── geo.py           # PostGIS location resolution
  └── prompts/
      ├── __init__.py
      ├── classify.py      # Signal classification prompt
      ├── group.py         # Event grouping prompt
      └── assess.py        # Alert assessment prompt
  ```
- [ ] **T1.3** Create `config.py` (Pydantic BaseSettings loading from .env)
- [ ] **T1.4** Create Celery app with beat schedule (poll every POLL_INTERVAL_SECONDS)
- [ ] **T1.5** Create Dockerfile + docker-compose.yml (pipeline worker + local Redis)

### Phase 2: Dataminr Client

- [ ] **T2.1** Implement auth client — POST /auth/v1/token, cache token in Redis (3.5h TTL)
- [ ] **T2.2** Implement signal fetcher — GET /firstalert/v1/alerts with time-window filtering and nextPage pagination
- [ ] **T2.3** Create Pydantic models for Dataminr response (new API: alertId, alertTimestamp, estimatedEventLocation as object, headline, publicPost, listsMatched, etc.)
- [ ] **T2.4** Implement deduplication via Redis seen-set (alertId, 48h TTL)
- [ ] **T2.5** Implement time-window sync logic (first run = 7 days back, subsequent = from last synced)

### Phase 3: GraphQL Client + Location Resolution

- [ ] **T3.1** Implement GraphQL client (httpx, Bearer sk_live_ auth)
- [ ] **T3.2** Define mutation strings: createSignal, createEvent, createAlert
- [ ] **T3.3** Define query strings: latest signal timestamp, active events, locations
- [ ] **T3.4** Add `nearestLocation` query to clear-api (PostGIS ST_Distance)
- [ ] **T3.5** Implement geo resolution service (lat/lon → location ID, Redis cache by grid cell)
- [ ] **T3.6** Add retry logic with exponential backoff for all GraphQL calls

### Phase 4: Signal Ingestion Pipeline

- [ ] **T4.1** Implement Dataminr → CLEAR signal field mapping (see data flow table above)
- [ ] **T4.2** Implement `poll_dataminr` Celery task (auth, fetch, dedup, fan-out)
- [ ] **T4.3** Implement `process_signal` Celery task — signal creation stage only
- [ ] **T4.4** Integration test: mock Dataminr response → signal created in clear-api

### Phase 5: Claude ML Processing

- [ ] **T5.1** Implement Claude client wrapper (Anthropic SDK, structured output parsing)
- [ ] **T5.2** Write signal classification prompt + response parser
- [ ] **T5.3** Write event grouping prompt + response parser
- [ ] **T5.4** Write alert assessment prompt + response parser
- [ ] **T5.5** Add Redis caching for Claude responses (classification keyed by signalId)
- [ ] **T5.6** Unit tests with mocked Claude responses

### Phase 6: Event & Alert Pipeline

- [ ] **T6.1** Implement event grouping service (create new or add to existing via signalEvents)
- [ ] **T6.2** Implement active events cache (fetch from clear-api, cache in Redis 1h)
- [ ] **T6.3** Implement alert escalation service (all alerts as draft in v1)
- [ ] **T6.4** Wire classification → grouping → escalation into process_signal task
- [ ] **T6.5** End-to-end test: Dataminr signal → classified → event created → alert (if severe)

### Phase 7: Sudan Location Data + Operations

- [ ] **T7.1** Create Sudan PostGIS data load script (GADM/HDX → locations table)
- [ ] **T7.2** Dockerfile for pipeline Celery worker
- [ ] **T7.3** docker-compose.yml (pipeline + redis, connects to clear-api)
- [ ] **T7.4** Structured logging (JSON, with signal/event/alert IDs)
- [ ] **T7.5** README with setup, configuration, and run instructions
