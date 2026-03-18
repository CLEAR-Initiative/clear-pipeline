# CLEAR Pipeline

> **Status: In Development** — This project is under active development and not yet production-ready.

Data ingestion pipeline that retrieves signals from the Dataminr First Alert API, runs ML-based classification and clustering, and writes structured results (signals → events → alerts) into the CLEAR system via GraphQL.

## Architecture

```
Dataminr API → [poll_dataminr] → [process_signal] → CLEAR API (GraphQL)
                                      │
                              ┌───────┴───────┐
                              │  ML Pipeline   │
                              │  classify →    │
                              │  group →       │
                              │  assess        │
                              └────────────────┘
```

## Setup

```bash
# Install dependencies
pip install uv
uv pip install --system .

# Copy env and fill in values
cp .env.example .env

# Run with Docker
docker compose up -d

# Or run locally (requires Redis)
celery -A src.celery_app worker --beat --loglevel=info
```

## Configuration

See `.env.example` for all available settings. Key variables:

| Variable | Description |
|---|---|
| `DATAMINR_API_USER_ID` | Dataminr First Alert API user ID |
| `DATAMINR_API_PASSWORD` | Dataminr First Alert API password |
| `CLEAR_API_URL` | CLEAR GraphQL API endpoint |
| `CLEAR_API_KEY` | Service account API key (`sk_live_...`) |
| `ANTHROPIC_API_KEY` | API key for ML inference |
| `REDIS_URL` | Redis connection URL |
| `POLL_INTERVAL_SECONDS` | How often to poll Dataminr (default: 15) |
| `RELEVANCE_THRESHOLD` | Min relevance score for event creation (default: 0.5) |

## Pipeline Flow

1. **Poll** — Celery beat triggers `poll_dataminr` every N seconds
2. **Fetch** — Gets signals from Dataminr within time window (last synced → now)
3. **Ingest** — Each signal is mapped and saved via `createSignal` mutation
4. **Classify** — ML classifies: disaster types, relevance, severity
5. **Group** — If relevant, ML clusters into existing or new event
6. **Escalate** — If severity >= 4, assesses for alert creation (always `draft`)

```mermaid
sequenceDiagram
    participant Beat as Celery Beat
    participant Poll as poll_dataminr
    participant Redis
    participant DM as Dataminr API
    participant Proc as process_signal
    participant CLEAR as CLEAR API (GraphQL)
    participant Claude as Claude (Anthropic)

    Beat->>Poll: trigger every 15s

    Note over Poll,Redis: Determine time window
    Poll->>Redis: get last_synced
    alt no cached timestamp
        Poll->>CLEAR: query latest signal publishedAt
    end

    Note over Poll,DM: Fetch new signals
    Poll->>DM: POST /auth/1/userAuthorization
    DM-->>Poll: authorizationToken (DmAuth)
    Poll->>Redis: cache token (TTL from expirationTime)
    Poll->>DM: GET /alerts/1/alerts?alertversion=19
    DM-->>Poll: alerts[]
    Poll->>Redis: deduplicate (seen set)
    Poll->>Redis: update last_synced

    loop each new signal
        Poll->>Proc: dispatch process_signal task

        Note over Proc,CLEAR: Stage 1 — Ingest
        Proc->>CLEAR: createSignal mutation
        CLEAR-->>Proc: signal { id }

        Note over Proc,Claude: Stage 2 — Classify
        Proc->>Claude: classify(title, location, context)
        Claude-->>Proc: { disaster_types, relevance, severity }
        Proc->>Redis: cache classification

        alt relevance >= threshold
            Note over Proc,Claude: Stage 3 — Group
            Proc->>CLEAR: query recent events
            Proc->>Claude: group(signal, existing events)
            Claude-->>Proc: { match existing | create new }
            Proc->>CLEAR: addSignalToEvent or createEvent

            alt severity >= 4
                Note over Proc,Claude: Stage 4 — Escalate
                Proc->>Claude: assess(event, signals)
                Claude-->>Proc: { should_alert, title, body }
                Proc->>CLEAR: createAlert (status: draft)
            end
        end
    end
```

## Project Structure

```
src/
├── config.py           # Settings from .env
├── celery_app.py       # Celery app + beat schedule
├── tasks/
│   ├── poll.py         # poll_dataminr task
│   └── process.py      # process_signal task
├── clients/
│   ├── dataminr.py     # Dataminr API (auth + fetch)
│   ├── graphql.py      # CLEAR API mutations/queries
│   └── claude.py       # ML inference client
├── models/
│   ├── dataminr.py     # Dataminr response schemas
│   └── clear.py        # GraphQL inputs + ML output schemas
├── services/
│   ├── signal.py       # Signal field mapping + ingestion
│   ├── event.py        # Event grouping
│   ├── alert.py        # Alert escalation
│   └── geo.py          # Location resolution
└── prompts/
    ├── classify.py     # Signal classification
    ├── group.py        # Event grouping
    └── assess.py       # Alert assessment
```
