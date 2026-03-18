# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

CLEAR Pipeline ingests signals from the Dataminr First Alert API, runs a 3-stage ML classification pipeline (classify → group → assess) via Claude, and writes structured results (signals → events → alerts) into the CLEAR system via GraphQL. Orchestrated by Celery with Redis as broker.

## Commands

```bash
# Install dependencies
pip install uv
uv pip install --system ".[dev]"

# Run locally (requires Redis on localhost:6379)
celery -A src.celery_app worker --beat --loglevel=info

# Run with Docker
docker compose up -d

# Lint
ruff check src/ tests/
ruff format --check src/ tests/

# Run tests
pytest
pytest tests/test_foo.py           # single file
pytest tests/test_foo.py::test_bar # single test
```

## Architecture

**Two Celery tasks drive everything:**

1. **`poll_dataminr`** (beat-scheduled, every N seconds) — fetches new signals from Dataminr, deduplicates via Redis, fans out each signal to `process_signal.delay()`
2. **`process_signal`** — runs a single signal through 4 stages:
   - **Ingest** → map Dataminr fields to CLEAR schema, resolve coordinates to location ID, save via GraphQL `createSignal`
   - **Classify** → Claude classifies disaster types, relevance (0-1), severity (1-5), summary. Cached 24h in Redis
   - **Group** (if relevance ≥ threshold) → Claude decides: add to existing event or create new one
   - **Escalate** (if severity ≥ 4) → Claude assesses whether to create a draft alert for human review

**Key layers:**

| Layer | Path | Role |
|-------|------|------|
| Tasks | `src/tasks/` | Celery task definitions (poll + process) |
| Clients | `src/clients/` | External API wrappers (Dataminr OAuth2, CLEAR GraphQL, Claude) |
| Services | `src/services/` | Business logic (signal ingestion, event grouping, alert escalation, geo resolution) |
| Models | `src/models/` | Pydantic models — `dataminr.py` for API response schemas, `clear.py` for GraphQL inputs + ML output schemas |
| Prompts | `src/prompts/` | System/user prompt builders for each ML stage |
| Config | `src/config.py` | Pydantic BaseSettings, loads from `.env` |

## Important Patterns

- **Redis caching is pervasive**: auth tokens (3.5h), dedup sets (48h), classifications (24h), active events (1h), locations (6h), geo grid cells (6h). Cache invalidation happens on mutations.
- **Retry strategy**: poll task retries 3× with 30s cooldown; process task retries 2× with 10s; GraphQL client retries 3× with exponential backoff (2^attempt seconds).
- **Module-level singletons**: `_client` in claude.py, `_source_id_cache`/`_disaster_types_cache` in process.py — lazily initialized to avoid redundant API calls per worker.
- **All alerts are created with `status="draft"`** — human review required before publishing.
- **Geo resolution is a placeholder** in v0 — `resolve_location()` returns a best-match location but production should use PostGIS spatial queries.
- **Sudan-focused**: the classify prompt is scoped to humanitarian intelligence for Sudan.

## Code Style

- Python 3.12+, ruff for linting (line length 100, rules: E, F, I, W)
- Pydantic v2 for all data validation
- httpx for HTTP requests (async-capable but used synchronously)
- All timestamps are ISO-8601 UTC
