# Spec: Centralized observability hub for Dagster (and future CLEAR-Initiative services)

## 1. Problem

`clear-context-pipeline` runs Dagster on a shared VM (`/srv/app`, per
`.github/workflows/build-and-deploy.yml`), **co-located in the same Docker
Compose stack as `clear-api`, `clear-mvp`, and `clear-pipeline`**. Today the
only visibility into runs is Dagster's own Postgres-backed event log
(queryable one run at a time via the webserver) plus raw container stdout —
nothing is aggregated, graphed, or retained anywhere else. `config.py`
declares `sentry_dsn` / `logtail_source_token` settings that
were never wired up.

As the number of ingestion sources (currently Dataminr, GDACS, ACLED, IDMC,
plus non-polled ones) and services grows, there needs to be one place to see
whether a given run/job is healthy, whether the data it produced is
trustworthy (data quality), and whether the box it runs on is under
resource pressure — without opening the Dagster UI run-by-run.

## 2. Goals / decisions

- Self-hosted **Grafana + Loki + Prometheus + Alertmanager**, not a SaaS
  (fits the existing sovereignty-leaning, OpenTofu-first infra style; the
  scaffolded-but-unused `sentry_dsn`/`logtail_source_token` settings are
  superseded by this, not extended).
- Scoped as a **shared hub**: other CLEAR-Initiative services (`clear-api`,
  etc.) should be able to plug in later without hub-side changes, not just
  this pipeline.
- Includes **host/container hardware metrics** (CPU, mem, disk, OOM kills),
  not just Dagster's own run-level info.
- **No alert receivers wired yet** — dashboards and logs only for this pass;
  Alertmanager is deployed but dormant, ready to wire up later.

Because the app repos already share one Docker host, that host is the
natural place to also run the observability stack — Promtail can tail every
container's stdout via the Docker socket regardless of which repo/compose
project a container belongs to, which gets "other services" log visibility
for free, with zero code changes in those other repos.

## 3. Architecture

Two pillars plus a dashboard layer, both usable by any future service via
the same two rules ("log to stdout", "push metrics to the gateway"):

**Logs → Loki.** Promtail scrapes container stdout/stderr via Docker
service discovery against the shared Docker socket. No app code changes:
this already captures every `logging.getLogger(__name__)` call in
`providers/*.py` and Dagster's own structured log stream today. Any future
service gets this automatically just by running as a container on the same
host — no per-service config.

**Metrics → Prometheus.**
- `node_exporter` (host CPU/mem/disk/load) and `cAdvisor` (per-container
  CPU/mem/net), scraped directly, no app code required.
  **Known limitation:** Dagster's `DefaultRunLauncher` forks each run as a *subprocess* of the
  `dagster-daemon` container, so cAdvisor sees the daemon container's
  aggregate usage, not per-run isolation. True per-run attribution would
  need `DockerRunLauncher` (one container per run) — a separate, larger
  infra decision.
- `Pushgateway` for batch-style metrics Prometheus can't pull (Dagster runs
  aren't long-lived scrape targets): run outcome (success/failure,
  duration) and data-quality results (pass/fail, failed-record rate),
  pushed once per run. This is the deliberately source-agnostic contract —
  any future service, Dagster-based or not, pushes to the same Pushgateway
  with the same label convention (`service`, `job`, `status`) and requires
  no new hub-side wiring.

**Grafana** sits on top with Prometheus + Loki provisioned as code
(datasource YAML, not click-ops), so the stack is reproducible.

**Alertmanager** is deployed and wired to Prometheus now, but with an
empty/no-op route — ready for Slack/email later without redeploying the
core stack.

This is deliberately not per-record custom plumbing: Dagster already emits
exactly the "run ok/nok" and "data quality" signals natively via
`MaterializeResult(metadata={...})` (see `factory.py:97-98`,
`stages.py:268,411` — every stage already reports created/failed/alerted
counts this way) and, once added, `@dg.asset_check`. The new sensor's job is
only to read what Dagster already recorded and forward it to Prometheus in a
generic `(service, job, status)` shape — not to reinvent counting.

## 4. Dagster-side changes (this repo)

New package `src/clear_context_pipeline/defs/observability/` (auto-discovered
by `definitions.py`'s `load_from_defs_folder` — no registration needed):

- `metrics.py` — thin `prometheus_client` wrapper: `push_run_metrics(job,
  status, duration_seconds, **labels)` using `push_to_gateway`. No-ops if
  `settings.pushgateway_url` is unset (same "inert until configured"
  convention as `idmc_client_id` etc.).
- `sensors.py` — one `@dg.run_status_sensor` (un-pinned — fires for every
  job, not one per source) reacting to `DagsterRunStatus.SUCCESS` /
  `.FAILURE`. On fire: reads the run's duration and any
  `MaterializeResult`/`AssetCheckEvaluation` metadata already attached to
  the run's events, and calls `push_run_metrics`. Ships
  `default_status=dg.DefaultSensorStatus.STOPPED`, matching the existing
  convention in `poll_sensor.py` — nothing pushes until explicitly turned on
  per environment.
- `checks.py` — a small, representative set of `@dg.asset_check`s added
  generically inside `factory.py`'s `build_source_assets` (one check per
  polled source, parametrized off the existing `created`/`failed` counts
  already computed in the ingest loop) that fails when the per-run failure
  rate crosses a threshold. This starts the data-quality pattern without
  hand-writing a bespoke check per connector.

`config.py` additions (same empty-default `Settings` convention as the rest
of the file):
```python
pushgateway_url: str = ""
observability_service_name: str = "clear-context-pipeline"
```

`pyproject.toml`: add `prometheus-client` to main deps.

## 5. The hub itself

New `deploy/observability/` in this repo (the only writable location
available in this workspace — no separate infra repo is checked out here,
and the actual `/srv/app` compose file lives only on the VM):

- `docker-compose.yml` — grafana, loki, promtail, prometheus, pushgateway,
  alertmanager, node-exporter, cadvisor.
- `prometheus.yml`, `alertmanager.yml` (empty route), `loki-config.yml`,
  `promtail-config.yml`.
- `grafana/provisioning/datasources/*.yml` (Prometheus + Loki auto-added).
- `grafana/provisioning/dashboards/*.yml` plus **one** starter dashboard
  JSON: run success/failure rate by job, data-quality check pass rate, host
  CPU/mem, log volume by service — enough to prove the pipeline end-to-end,
  not a dashboard library.
- `README.md` — how to point this repo's containers at the hub
  (`PUSHGATEWAY_URL`), and an explicit note that deploying this onto/next to
  `/srv/app` on the shared VM (merging into or running alongside the
  existing compose project, sharing its Docker network/socket) is a manual
  follow-up step outside this repo's CI — access to the VM and deploy
  secrets is outside this workspace.

## 6. Non-goals for this pass

- Tracing (OpenTelemetry/Tempo) — not requested; the stack leaves room to
  add it later.
- `DockerRunLauncher` / true per-run hardware isolation.
- Alert receivers/routing — dashboards and logs only for now.
- Instrumenting `clear-api`/other repos directly — the hub and the two rules
  (push to Pushgateway, log to stdout) are what let them adopt this later
  with zero hub-side changes; not done in this pass.

## 7. Verification

- `docker compose -f deploy/observability/docker-compose.yml up -d`
  locally; confirm Grafana loads, Prometheus targets (self, node-exporter,
  cadvisor, pushgateway) show `UP`, and the Loki datasource shows live
  container logs.
- Enable the new `run_status_sensor` locally in Dagster dev, materialize
  `raw_idmc` (known-working from prior work on this branch), confirm a
  `clear_run_success`/`clear_dq_*` metric lands in Prometheus after the run
  and the run's log lines appear in Grafana's Loki explore view.
- No repo-wide lint/test command exists yet (no `ruff`/CI lint step
  configured) — run `uv run pytest` for anything under `tests/`, and
  spot-check the new modules with
  `python -c "import clear_context_pipeline.definitions"` to confirm
  `load_from_defs_folder` picks them up without error.
