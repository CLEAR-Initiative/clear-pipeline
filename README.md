# clear-pipeline

Dagster project that builds the CLEAR knowledge base from ReliefWeb
PDFs (weekly cron) and one-off manual document uploads. Ingest chain:
PDF → text → chunks → LLM contextualization + parameter extraction →
embeddings → clear-api `upsertKnowledgebaseChunks`. In parallel, a
domain-partitioned datapoint extraction pipeline writes structured
`report_datapoints` and rolls them up into `aggregated_datapoints` at
four tiers (weekly × A2, monthly × A1, yearly × country, all-time ×
country) — see [docs/humanitarian-datapoint-extraction.md](docs/humanitarian-datapoint-extraction.md).


## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

### The Task Worker

`drain_tasks` (group `tasks`) is the production Worker on clear-api's generic Task queue
(clear-api ADR-0010): it claims Tasks of every kind registered in
`clear_pipeline.defs.tasks.worker.HANDLERS`, runs each handler under a heartbeat, and
completes or fails it. The first handler is `event.impact_prior`. Adding a kind of work is a
handler (`@register_handler("<kind>")` returning a `TaskOutcome`), not a module and not a queue.

It needs one extra variable, because the pipeline user may not claim Tasks:

| Variable | Meaning |
|---|---|
| `CLEAR_WORKER_API_KEY` | An `sk_live_…` key of clear-api's `worker` service user (`scripts/create-worker-user.ts` there). Every Task call — claim, heartbeat, complete, fail and the handler's reads — uses it. |
| `TASK_POLL_INTERVAL_MINUTES` | How often `task_worker_sensor` drains (default 5). |
| `TASK_HEARTBEAT_MINUTES` | How often a running handler extends its lease (default 5; clear-api's lease is 15 minutes). |

The sensor ships STOPPED, like the other drains. The model for the ImpactPrior handler is the
`narrative` role (`LLM_NARRATIVE_*`); without one it falls back to a rule-based selection over
CLEAR's own Events and reports no usage.

Retry is clear-api's: a failed Task returns to PENDING while attempts remain (`TASK_MAX_ATTEMPTS`
there, default 3) and is FAILED after. clear-api adds no delay between attempts, so a drain run
stops claiming a kind as soon as one of its Tasks fails or is lost — the next sensor tick is the
backoff. Otherwise the same Task would be re-claimed at once and lose every attempt in seconds.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
