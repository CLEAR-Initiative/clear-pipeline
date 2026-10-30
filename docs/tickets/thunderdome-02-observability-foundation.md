# Thunderdome — Observability Foundation

## Description

Thunderdome needs somewhere to push and query metrics, and a way to see
when something fails outright — neither fully exists today across the
CLEAR application. `docs/observability-hub-design.md` specifies what's
missing for `clear-context-pipeline` and how to close it against the
existing hub (OpenObserve + GlitchTip + Vector, run by `clear-infra`)
rather than stand up a second stack: host metrics, a Pushgateway for
Dagster's one-shot run metrics, and a GlitchTip project for
`clear-context-pipeline`.

`clear-api` already reports uncaught exceptions to its own GlitchTip
project (`src/utils/sentry.ts`, `SENTRY_DSN`, no-ops cleanly when unset) —
that half of the gap is closed for `clear-api` already. What's still
missing on the `clear-api` side is the same thing missing everywhere else:
no metrics (request latency, error rate per GraphQL operation) exist yet.

This ticket is "build what's missing, on both services" — the prerequisite
plumbing that Thunderdome's comparison surface (evaluation-stack
implementation) gets built on top of.

## Definition of Done

- Dagster run outcomes (success/failure, duration) and a representative set
  of data-quality asset checks are visible as metrics in OpenObserve after
  every run.
- Uncaught exceptions in `clear-context-pipeline` land in its own GlitchTip
  project instead of as raw log text in OpenObserve.
- `clear-api`'s request latency and per-operation error rate are visible as
  metrics in OpenObserve.
- Host CPU/mem/disk/load for the VM are visible in OpenObserve.

## Tasks

### `clear-context-pipeline`

Implemented on `feat/pushgateway-run-metrics`. Layout differs from the design
doc: no `defs/observability/` package.

- [x] `providers/metrics.py`: `push_run_metrics(job, success, duration_s,
      checks)` via `prometheus_client`; no-op if `pushgateway_url` is unset,
      push errors logged, never raised.
- [x] `defs/observability.py`: `run_success_metrics_sensor` and
      `run_failure_metrics_sensor` (`RUNNING` on deploy). Each pushes run
      outcome, duration and every GX gate (`ASSET_CHECK_EVALUATION`) result
      of the run.
- [x] `pushgateway_url` and `observability_service_name` in `config.py`;
      `sentry_dsn` was already wired (`providers/observability.py`).
- [x] `prometheus-client` in `pyproject.toml`.
- Dropped: per-source `@asset_check` on ingest counts. The existing GX gates
  (`gx_pipeline/factory.py`) are the data-quality signal.

### `clear-api`

- [ ] Expose a `/metrics` endpoint (`prom-client`, Node's long-running
      process can be scraped directly — no Pushgateway needed, that's only
      for Dagster's one-shot run model) with request count, latency
      histogram, and error rate labeled per GraphQL operation name.
      Mirrors the Apollo plugin pattern already used for
      `sentryApolloPlugin` (`src/utils/sentry.ts`).
- [ ] Confirm no change needed to `src/utils/sentry.ts` — GlitchTip
      reporting is already live.

### `clear-infra` (branch `feat/pushgateway-run-metrics`)

Done in `components/app-vm/files/`:

- [x] `pushgateway` service in `docker-compose.yml.tftpl` (no published port).
- [x] `vector.yaml.tftpl`: `prometheus_scrape` source on the Pushgateway
      (`honor_labels: true`) and `prometheus_remote_write` sink to OpenObserve.
- [x] `PUSHGATEWAY_URL` in `dagster.env.tftpl`. `SENTRY_DSN` was already wired
      (`glitchtip_dsn_dagster`).
- [ ] Verify OpenObserve's remote-write path
      (`/api/default/prometheus/api/v1/write`) on `dev-observe` after apply.
- [ ] If `feat/dagster-vm` merges, Dagster leaves the app VM: move the
      Pushgateway to the Dagster VM, or point `PUSHGATEWAY_URL` at the app VM.
- Not done: Vector `host_metrics` (host CPU/mem/disk/load).

## Data Requirements

None new — this ships metrics/error infrastructure, not business data.

## Technical Implementation

See `docs/observability-hub-design.md` in full (architecture diagram, one
shipper/two destinations, §3–§5) for the `clear-context-pipeline` half.
The `clear-api` `/metrics` endpoint follows the same "scrape target, not
push" shape as host metrics, just a second `prometheus_scrape` source in
Vector rather than a new destination.

## Out of Scope

Per design doc §6, applies to both services:

- Tracing / OpenTelemetry.
- `DockerRunLauncher` / true per-run hardware isolation (a Dagster
  deployment-model limitation, not fixable by swapping observability
  tools).
- Alert routing / receivers (OpenObserve's own basic rules are the
  deliverable; Alertmanager is the fallback if that stops being enough).
- Host-level OS logs (journald/syslog) and non-Docker log files.

## Notes / Resources / Questions

- Design doc: `docs/observability-hub-design.md` (this repo) — written
  against `clear-context-pipeline` only; the `clear-api` metrics task above
  extends the same plan, it isn't in that doc yet.
- The `clear-infra` half needs its own PR there; confirm who owns the
  Terraform changes — out of scope for whoever picks up the
  application-side halves.
- This ticket blocks the evaluation-stack implementation: the comparison
  surface needs a metrics destination to write to, and the offline-eval /
  shadow-comparison jobs need the same run-outcome visibility as any other
  job, on either service.

## Outcomes & Decisions

No decisions recorded yet beyond the design doc itself being accepted as
the plan of record for `clear-context-pipeline` — it supersedes the
original Grafana/Loki/Prometheus/Alertmanager proposal. The `clear-api`
metrics gap and its fix are new, not yet discussed outside this ticket.
