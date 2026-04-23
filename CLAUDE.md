# CLAUDE.md

Guidance for Claude Code working in `clear-pipeline`.

## What this repo is

Python data pipeline that polls Dataminr for humanitarian signals, classifies and groups them via Claude, and pushes events + draft alerts into the CLEAR API. Runs as Celery workers + a beat scheduler against Redis. Talks to `clear-api` over GraphQL.

Authoritative docs: [docs/DATA_PIPELINE.md](docs/DATA_PIPELINE.md) (single source of truth for pipeline behaviour), [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (sequence diagram), [docs/PRD.md](docs/PRD.md) (product requirements).

## Stack

- Python 3.12, dependencies via `uv` (see [pyproject.toml](pyproject.toml), [uv.lock](uv.lock))
- Celery 5 (broker + result backend = Redis)
- `anthropic>=0.52` for Claude calls
- `httpx` for GraphQL to `clear-api`
- `pydantic` v2 for models, `pydantic-settings` for config

## Commands

```bash
docker compose up                       # redis + pipeline worker
celery -A src.celery_app worker -B      # local: worker + beat
ruff check src/                         # lint
pytest                                  # tests (see tests/)
```

## Key entry points

- [src/celery_app.py](src/celery_app.py) — Celery app + beat schedule (poll every `POLL_INTERVAL_SECONDS=15`).
- [src/tasks/poll.py](src/tasks/poll.py) — Dataminr poll, dedup via Redis, fan-out to `process_signal`.
- [src/tasks/process.py](src/tasks/process.py) — per-signal pipeline: ingest → classify → (gate) → group → assess.
- [src/services/](src/services/) — one module per pipeline stage (`signal`, `event`, `alert`, `geo`).
- [src/prompts/](src/prompts/) — system + user prompts for the three Claude stages (`classify`, `group`, `assess`).

## The Claude chokepoint

**Every Claude call goes through one function: [src/clients/claude.py:22](src/clients/claude.py#L22) — `call_claude()`.** Three stages call it: `classify` (per signal), `group` (per relevant signal), `assess` (per event hitting severity ≥ 4). Whenever you instrument, rate-limit, cache, or swap the model, this is the single place to do it.

## Sibling repo: `clear-pipeline-insights`

Lives at `../clear-pipeline-insights/`. Next.js dashboard + ingest API for observing every Claude call this pipeline makes — cost, latency, prompts, responses. Built to support prompt iteration, $/env tracking, and eventually side-by-side comparison with Nikita's classifier.

- **Why we're building it:** [docs/PIPELINE_INSIGHTS_PROPOSAL.md](docs/PIPELINE_INSIGHTS_PROPOSAL.md) (lives in this repo — it's a pipeline architectural decision).
- **What it builds:** `../clear-pipeline-insights/SPEC.md`.

**How this repo will connect to it (not yet wired):** the pipeline POSTs telemetry to the dashboard's HTTP API. Two env vars only — `INSIGHTS_API_URL` and `INSIGHTS_INGEST_TOKEN`. The pipeline never has insights-DB credentials.

**The instrumentation work, when unblocked:**
1. Add `INSIGHTS_API_URL` + `INSIGHTS_INGEST_TOKEN` to [src/config.py](src/config.py) and `.env`.
2. New file `src/clients/insights.py` with `ensure_run()` and `record_call(...)` — synchronous HTTP POST wrapped in try/except that **swallows errors** (telemetry must never fail the pipeline).
3. Add `*_PROMPT_VERSION` constant to each file in [src/prompts/](src/prompts/).
4. Modify `call_claude()` to accept `stage`, `signal_id`, `event_id` kwargs, capture `response.usage` + latency + parse outcome, and call `record_call(...)`.
5. Thread `stage`/`signal_id`/`event_id` through callsites in [src/tasks/process.py](src/tasks/process.py), [src/services/event.py](src/services/event.py), [src/services/alert.py](src/services/alert.py).

This work is blocked on the insights API existing. Until then, do not pre-add stubs.

## Conventions

- Settings via `pydantic-settings` only ([src/config.py](src/config.py)) — never read env vars directly elsewhere.
- Logging via stdlib `logging` with module-level `logger = logging.getLogger(__name__)`.
- Redis is used for: dedup (`dataminr:seen:*`), classification cache (`classification:*`), active-events cache (`events:active`). Don't reinvent — see [src/services/event.py](src/services/event.py) and [src/tasks/process.py](src/tasks/process.py) for patterns.
- GraphQL calls go through [src/clients/graphql.py](src/clients/graphql.py) which already has retry/backoff. Do not bypass.
- The pipeline runs on Railway in prod. **Railway logs vanish on restart** — that's part of why we're building the insights dashboard. Do not rely on log scraping for analysis.
