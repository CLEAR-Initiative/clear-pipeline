# clear-context-pipeline

> ⚠️ **Status: POC — experimentation phase.**
> This project is a proof-of-concept exploring structured humanitarian
> datapoint extraction, vector RAG, and pre-computed aggregations for
> the situation-analysis dashboard. APIs, schemas, prompts, and
> aggregation rules are all expected to change as we learn from real
> data. Not production-ready — do not build durable integrations
> against its outputs yet, and expect breaking changes between
> iterations. Schemas are versioned (`schemaVersion` on
> `report_datapoints` / `aggregated_datapoints`) so future stable
> releases can migrate cleanly.

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

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
