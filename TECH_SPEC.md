# CLEAR Pipeline — Technical Specification

## 1. Overview

**clear-pipeline** is a standalone Python data ingestion service that retrieves real-time alerts from the Dataminr FirstAlert API, runs ML and rule-based detection, and writes structured results into the CLEAR system via the Apollo GraphQL API (`clear-apollo`).

It replaces the Django-embedded pipeline in `clear_poc` with a decoupled, independently deployable service. The core ingestion and detection logic remains the same; the key difference is that **all database writes go through the Apollo GraphQL API** rather than Django ORM.

### Why a separate service?

| Concern | `clear_poc` (current) | `clear-pipeline` (new) |
|---|---|---|
| Data storage | Direct Django ORM writes | GraphQL mutations via Apollo API |
| Deployment | Monolithic Django app | Standalone Python service |
| Coupling | Tightly coupled to Django models, signals, location app | Loosely coupled — communicates only via API |
| Scalability | Shares Django worker pool | Independent Celery workers, scales separately |

---

## 2. Existing Pipeline Recap (clear_poc)

The current pipeline in `clear_poc` follows this flow:

1. **Celery Beat** triggers `full_pipeline(source_id=6, variable_id=19)` on a schedule.
2. **Retrieval** (`dataminr.py → get()`) — Authenticates with the Dataminr FirstAlert API using `DmAuth` tokens, then retrieves alerts via cursor-based pagination from `/alerts/1/alerts?alertversion=v19`. Raw JSON is saved to disk.
3. **Processing** (`dataminr.py → process()`) — Parses each alert, extracts fields (headline, coordinates, topics, categories, linked alerts, public post), matches locations via a gazetteer, and creates `VariableData` records in the Django database.
4. **Signal emission** — `data_processing_completed` Django signal fires after successful processing.
5. **Detection triggering** (`signal_handlers.py`) — The signal handler finds active detectors configured for `dataminr_alerts` and triggers them with a 6-hour lookback window. Dataminr detectors are **chained**: BERT runs first, then Scoring, to avoid duplicate alerts.
6. **BERT Detection** (`dataminr_bert_detector.py`) — Fine-tuned BERT model classifies headlines as alert/non-alert (threshold: 0.5). Determines shock type via keyword overrides + centroid classification. Calculates severity from `alertType` criticality + headline impact extraction.
7. **Scoring Detection** (`scoring_detector.py`) — Rule-based field scoring (exact match, contains, regex, numeric), keyword scoring, location multipliers, temporal clustering. Skips records already detected by BERT.
8. **Alert creation** — Detections are persisted as `Detection` records, which can be promoted to Signals → Events → Alerts.

---

## 3. New Architecture

### 3.1 Technology Stack

| Component | Technology |
|---|---|
| Language | Python 3.12+ |
| Task queue | Celery + Redis |
| Scheduler | Celery Beat |
| HTTP client | `httpx` (async support) or `requests` |
| GraphQL client | `gql` with `httpx` transport |
| ML inference | `transformers` + `torch` (BERT), `sentence-transformers` (shock type) |
| Configuration | Environment variables + `.env` files |
| Logging | `structlog` (structured JSON logging) |
| Containerisation | Docker + Docker Compose |

### 3.2 Project Structure

```
clear-pipeline/
├── TECH_SPEC.md
├── ARCHITECTURE.md
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── .env.example
├── src/
│   ├── __init__.py
│   ├── config.py                  # Settings from env vars
│   ├── celery_app.py              # Celery application & beat schedule
│   ├── apollo_client.py           # GraphQL client for clear-apollo
│   ├── sources/
│   │   ├── __init__.py
│   │   ├── base.py                # Abstract base source
│   │   └── dataminr.py            # Dataminr FirstAlert API source
│   ├── detectors/
│   │   ├── __init__.py
│   │   ├── base.py                # Abstract base detector
│   │   ├── bert_detector.py       # BERT headline classifier
│   │   └── scoring_detector.py    # Rule-based scoring detector
│   ├── tasks/
│   │   ├── __init__.py
│   │   ├── ingestion.py           # retrieve, process, full_pipeline tasks
│   │   └── detection.py           # run_detector, chained detection tasks
│   └── models/
│       ├── __init__.py
│       └── schemas.py             # Pydantic models for data validation
└── tests/
    ├── __init__.py
    ├── test_dataminr.py
    ├── test_detectors.py
    └── test_apollo_client.py
```

---

## 4. Dataminr Ingestion

This section mirrors the existing implementation in `clear_poc/data_pipeline/sources/dataminr.py`. The logic is identical; only the storage layer changes.

### 4.1 Authentication

| Field | Value |
|---|---|
| Endpoint | `POST https://firstalert-api.dataminr.com/auth/1/userAuthorization` |
| Content-Type | `application/x-www-form-urlencoded` |
| Parameters | `grant_type=api_key`, `scope=first_alert_api`, `api_user_id`, `api_password` |
| Response | `{ "authorizationToken": "...", "expirationTime": <ms_since_epoch> }` |
| Token type | `DmAuth` |
| Refresh | Not supported — re-authenticate to get a new token |

Token is cached in-memory (per worker process) with expiration tracking. On 401 response, token is cleared and re-acquired.

### 4.2 Alert Retrieval

| Field | Value |
|---|---|
| Endpoint | `GET https://firstalert-api.dataminr.com/alerts/1/alerts` |
| Authorization | `DmAuth {token}` |
| Required params | `alertversion=19` |
| Pagination | Cursor-based — response includes `to` field, passed as URL-encoded `from` param |
| Rate limit | 180 requests per 10 minutes |
| Max requests per run | 5 (configurable) |

Response shape:
```json
{
  "alerts": [...],
  "to": "<next_cursor>"
}
```

### 4.3 Alert Processing (Field Extraction)

Each raw alert is parsed into a structured record. Fields extracted:

| Field | Source path | Notes |
|---|---|---|
| `alertId` | `alert.alertId` | Unique identifier |
| `eventTime` | `alert.eventTime` | Unix timestamp (ms) |
| `headline` | `alert.headline` | Primary text |
| `subHeadline` | `alert.subHeadline.title`, `.subHeadlines` | Secondary context |
| `alertType` | `alert.alertType.name` | Criticality: Flash, Urgent, Alert |
| `coordinates` | `alert.estimatedEventLocation[1,2]` | `[name, lat, lng, radius, grid]` |
| `categories` | `alert.alertLists[].name` | Alert list categories |
| `topics` | `alert.alertTopics[].name`, `.id` | Topic names and IDs |
| `linkedAlerts` | `alert.linkedAlerts[0].parentId`, `.count` | Parent alert chain |
| `publicPost` | `alert.publicPost.link`, `.text`, `.translatedText`, `.media` | Source post |
| `firstAlertURL` | `alert.firstAlertURL` | Deep link |
| `termsOfUse` | `alert.termsOfUse` | Compliance requirement |

Processed text is composed as:
```
"Headline: {headline} | Title: {subHeadline.title} | Context: {subHeadline.text} | Source: {publicPost.text} | Translated: {publicPost.translatedText}"
```

### 4.4 Location Resolution

In the current system, locations are matched against a gazetteer via `handle_unmatched_location()`. In the new pipeline:

1. Query Apollo API for existing locations: `locations(level: 0)` to build a local lookup cache.
2. Attempt to match `estimatedEventLocation[0]` (location name) against cached locations.
3. If no match, call `createLocation` mutation to create a new location record, or flag as unmatched in the detection's `rawData.metadata`.
4. The matched/created location ID is used in subsequent `createDetection` calls via `locationIds`.

---

## 5. Apollo API Integration

### 5.1 Authentication

The pipeline authenticates with Apollo using an **API key** (Bearer token):

```
Authorization: Bearer sk_live_<64-char-base64url>
```

The API key must belong to a user with `admin` or `editor` role (required for `createDetection`).

### 5.2 GraphQL Client

A dedicated `ApolloClient` class wraps all API interactions:

```python
class ApolloClient:
    def __init__(self, endpoint: str, api_key: str):
        ...

    def create_detection(self, title, confidence, raw_data, source_id, location_ids, detected_at) -> str:
        """Create a Detection record. Returns detection ID."""
        ...

    def create_data_source(self, name, type, base_url, info_url) -> str:
        """Register a data source. Returns source ID."""
        ...

    def create_location(self, geo_id, name, level, latitude, longitude) -> str:
        """Create a location. Returns location ID."""
        ...

    def get_locations(self, level: int = None) -> list[dict]:
        """Fetch locations for cache."""
        ...

    def get_or_create_data_source(self, name, type, base_url) -> str:
        """Idempotent source registration."""
        ...
```

### 5.3 Data Mapping: VariableData → Detection

The current `VariableData` model maps to Apollo's `Detection` type:

| VariableData field | Apollo Detection field | Notes |
|---|---|---|
| `text` (composed headline) | `title` | Truncated to 200 chars |
| N/A | `confidence` | Set to `1.0` at ingestion; detectors update later |
| N/A | `status` | `"raw"` at ingestion |
| `start_date` | `detectedAt` | ISO 8601 datetime |
| `raw_data` (full JSON + `_extracted_fields`) | `rawData` | Stored as JSON scalar |
| `gid` (location FK) | `locationIds` | Array of Apollo location IDs |
| `variable.source` → Dataminr | `sourceId` | Apollo DataSource ID |

### 5.4 Data Mapping: Detector Output → Detection

When detectors produce results, they are also written as Detections (or updates to existing ones):

| Detector output field | Apollo Detection field | Notes |
|---|---|---|
| `title` | `title` | Headline text (200 char max) |
| `confidence_score` | `confidence` | BERT probability or normalized score |
| `detection_data` | `rawData` | Full detection metadata JSON |
| `detection_timestamp` | `detectedAt` | When the event was detected |
| `locations` | `locationIds` | Resolved location IDs |
| N/A | `status` | `"processed"` |
| N/A | `sourceId` | Dataminr DataSource ID |

### 5.5 GraphQL Mutations Used

#### createDetection
```graphql
mutation CreateDetection($input: CreateDetectionInput!) {
  createDetection(input: $input) {
    id
    title
    confidence
    status
    detectedAt
    rawData
    source { id name }
    locations { id name }
  }
}
```

Input:
```json
{
  "title": "Headline: Airstrike reported in...",
  "confidence": 0.87,
  "status": "processed",
  "detectedAt": "2026-03-18T14:30:00Z",
  "rawData": { "alertId": "...", "_extracted_fields": {...}, "bert_confidence": 0.87, ... },
  "sourceId": "clxyz...",
  "locationIds": ["clxyz..."]
}
```

#### createDataSource (one-time setup)
```graphql
mutation CreateDataSource($input: CreateDataSourceInput!) {
  createDataSource(input: $input) {
    id
    name
  }
}
```

Input:
```json
{
  "name": "Dataminr",
  "type": "api",
  "isActive": true,
  "baseUrl": "https://firstalert-api.dataminr.com",
  "infoUrl": "https://www.dataminr.com/first-alert"
}
```

#### createLocation (as needed)
```graphql
mutation CreateLocation($input: CreateLocationInput!) {
  createLocation(input: $input) {
    id
    geoId
    name
    level
  }
}
```

---

## 6. Detection Pipeline

### 6.1 BERT Detector

Identical to `clear_poc/alert_framework/detectors/dataminr_bert_detector.py`:

- **Model**: Fine-tuned BERT for binary classification (alert vs non-alert)
- **Input**: Headlines from recently ingested detections (6-hour lookback)
- **Threshold**: 0.5 confidence (configurable)
- **Batch size**: 8 headlines per batch
- **Shock type classification**:
  1. Keyword overrides (priority order): Conflict, Health emergencies, Food security, Natural disasters
  2. Centroid classification: Encode headline with `all-MiniLM-L6-v2`, compute cosine similarity against pre-computed centroids
- **Severity calculation**:
  - Base: `alertType` criticality (Flash=4, Urgent=3, Alert=2)
  - Boost (0-2): Numbers + impact words in headline (death/displacement/injury thresholds)
  - Capped at 5
- **Output**: For each alert-classified headline, call `createDetection` with `status: "processed"`, confidence score, shock type in `rawData`
- **Model caching**: Global per-worker cache (~500MB model loaded once)

### 6.2 Scoring Detector

Identical to `clear_poc/alert_framework/detectors/scoring_detector.py`:

- **Field scoring**: Configurable rules on `rawData` fields — `exact_match`, `contains`, `regex`, `numeric`
- **Keyword scoring**: Weighted keyword matching on headline text, `max` or `sum` mode
- **Location multipliers**: Score boost for priority locations
- **Thresholds**: critical (25), high (15), medium (8), low (4)
- **Temporal clustering**: Group alerts within configurable time window (default 6 hours)
- **Shock type mapping**: From `alertTopics` or content pattern matching
- **Deduplication**: Skips records already detected by BERT (checks existing detections for same `rawData.alertId`)

### 6.3 Detection Chaining

Mirrors the logic in `signal_handlers.py`:

```
BERT Detector → (completes) → Scoring Detector
```

Implemented as a Celery chain:
```python
from celery import chain

chain(
    run_detector.si("bert", start_date, end_date),
    run_detector.si("scoring", start_date, end_date),
).apply_async()
```

This ensures BERT results are persisted before Scoring runs, enabling cross-detector deduplication.

### 6.4 Data Flow for Detectors

Instead of querying `VariableData` from a local database, detectors:

1. Query Apollo API for recent detections: `detections(status: "raw")` with date filter
2. Run classification/scoring on the returned records
3. Update detections via `updateDetection` mutation (set `status: "processed"`, add confidence, add detection metadata to `rawData`)
4. Or create new detection records for cluster-level detections

---

## 7. Task Orchestration

### 7.1 Celery Beat Schedule

```python
beat_schedule = {
    "dataminr-full-pipeline": {
        "task": "tasks.ingestion.full_pipeline",
        "schedule": crontab(minute="*/10"),  # Every 10 minutes
        "kwargs": {"source_name": "dataminr"},
    },
    "dataminr-detection-pipeline": {
        "task": "tasks.detection.run_detection_chain",
        "schedule": crontab(minute="*/15"),  # Every 15 minutes
        "kwargs": {"source_name": "dataminr"},
    },
}
```

### 7.2 Task Definitions

#### `tasks.ingestion.full_pipeline`
1. Authenticate with Dataminr API (get/refresh DmAuth token)
2. Retrieve alerts via cursor-based pagination
3. Process each alert (extract fields, resolve locations)
4. Write each processed alert to Apollo as a Detection (`status: "raw"`)
5. On success, trigger detection chain

#### `tasks.detection.run_detection_chain`
1. Query Apollo for recent raw detections (6-hour lookback)
2. Run BERT detector → write results to Apollo
3. Run Scoring detector → write results to Apollo (skip BERT duplicates)

### 7.3 Retry Logic

Mirrors `clear_poc` behaviour:
- **Network/API errors** (connection, timeout, HTTP 5xx): Retry up to 3 times with 60s delay
- **Code errors** (KeyError, ValueError, etc.): Fail immediately, do not retry
- **Rate limit (429)**: Exponential backoff with jitter, respect `Retry-After` header
- **Auth failure (401)**: Clear cached token, re-authenticate, retry once

---

## 8. Configuration & Environment Variables

```bash
# Dataminr API
DATAMINR_API_USER_ID=           # Required
DATAMINR_API_PASSWORD=          # Required

# Apollo API
APOLLO_GRAPHQL_ENDPOINT=http://localhost:4000/graphql
APOLLO_API_KEY=sk_live_...      # Required — admin/editor role

# Celery / Redis
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/1

# Detection
BERT_MODEL_PATH=./models/dataminr-bert  # Path to fine-tuned BERT model
BERT_CONFIDENCE_THRESHOLD=0.5
SCORING_MIN_DETECTION_SCORE=8

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json                 # json or console
```

---

## 9. Error Handling & Observability

### 9.1 Error Handling

| Scenario | Handling |
|---|---|
| Dataminr auth failure | Retry once with fresh credentials; alert on persistent failure |
| Dataminr 429 rate limit | Backoff with jitter; log warning |
| Apollo API unreachable | Retry with exponential backoff; buffer alerts locally |
| Apollo mutation fails | Log full error + input payload; continue processing remaining alerts |
| BERT model load failure | Fail task immediately; log error with model path |
| Location resolution failure | Proceed with `locationIds: []`; store unmatched location in `rawData.metadata` |

### 9.2 Logging

Structured JSON logs via `structlog`:

```json
{
  "timestamp": "2026-03-18T14:30:00Z",
  "level": "info",
  "event": "alert_ingested",
  "alert_id": "dm-12345",
  "source": "dataminr",
  "location": "Khartoum, Sudan",
  "location_matched": true,
  "apollo_detection_id": "clxyz..."
}
```

Key log events:
- `dataminr_auth_success` / `dataminr_auth_failure`
- `alerts_retrieved` (count, cursor position)
- `alert_processed` (per alert)
- `alert_ingested` (written to Apollo)
- `detection_started` / `detection_completed` (per detector run)
- `detection_created` (per detection written to Apollo)

### 9.3 Health & Metrics

- `/health` HTTP endpoint (lightweight Flask/FastAPI) for container health checks
- Celery task success/failure counters
- Dataminr API latency and error rate
- Apollo API latency and error rate
- Detection counts by type and shock category

---

## 10. Deployment

### Docker Compose (development)

```yaml
services:
  pipeline-worker:
    build: .
    command: celery -A src.celery_app worker --loglevel=info
    env_file: .env
    volumes:
      - ./models:/app/models  # BERT model mount
    depends_on:
      - redis

  pipeline-beat:
    build: .
    command: celery -A src.celery_app beat --loglevel=info
    env_file: .env
    depends_on:
      - redis

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
```

### Production considerations

- Run multiple worker instances for throughput (each caches BERT model independently ~500MB RAM)
- Use a persistent Redis or RabbitMQ broker
- Mount BERT model from a shared volume or download on startup
- Set `CELERY_TASK_ACKS_LATE=True` for at-least-once delivery
- Monitor with Flower (Celery) + Prometheus + Grafana
