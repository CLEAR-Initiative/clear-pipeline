"""Run + data-quality metrics pushed to a Prometheus Pushgateway.

Vector scrapes the Pushgateway and remote-writes to OpenObserve (clear-infra).
**No-op when `PUSHGATEWAY_URL` is unset**, and a failed push is logged, never
raised: metrics must not fail a run. One group per job (a push replaces the
previous one); history lives in OpenObserve, one point per Vector scrape."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass

from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckOutcome:
    asset: str
    check: str
    passed: bool
    severity: str


def build_registry(
    job: str,
    success: bool,
    duration_s: float | None,
    checks: Iterable[CheckOutcome],
    finished_at: float | None = None,
):
    """Gauges for one finished run. Split from the push so it is testable.
    `finished_at` is the run's end time; falls back to now when unknown."""
    from prometheus_client import CollectorRegistry, Gauge

    reg = CollectorRegistry()
    labels = ["job", "service"]
    base = {"job": job, "service": settings.observability_service_name}
    Gauge("dagster_run_last_success", "1 if the last run of the job succeeded", labels, registry=reg).labels(**base).set(
        int(success)
    )
    Gauge("dagster_run_last_timestamp_seconds", "Unix time the last run finished", labels, registry=reg).labels(
        **base
    ).set(time.time() if finished_at is None else finished_at)
    if duration_s is not None:
        Gauge("dagster_run_last_duration_seconds", "Duration of the last run", labels, registry=reg).labels(
            **base
        ).set(duration_s)
    check_gauge = Gauge(
        "dagster_asset_check_last_passed",
        "1 if the asset check (GX gate) passed in the last run",
        [*labels, "asset", "check", "severity"],
        registry=reg,
    )
    for c in checks:
        check_gauge.labels(**base, asset=c.asset, check=c.check, severity=c.severity).set(int(c.passed))
    return reg


def _push(job: str, build) -> None:
    """Push `build()`'s registry under `job`. No-op without a URL; errors are
    logged, never raised."""
    url = (settings.pushgateway_url or "").strip()
    if not url:
        return
    try:
        from prometheus_client import push_to_gateway

        push_to_gateway(url, job=job, registry=build(), timeout=10)
    except Exception:  # noqa: BLE001 — never let metrics fail a run
        logger.warning("[metrics] Pushgateway push failed for job %s", job, exc_info=True)


def push_run_metrics(
    job: str,
    success: bool,
    duration_s: float | None,
    checks: Iterable[CheckOutcome] = (),
    finished_at: float | None = None,
) -> None:
    _push(job, lambda: build_registry(job, success, duration_s, checks, finished_at))


HEARTBEAT_JOB = "dagster_heartbeat"


def build_heartbeat_registry(running: int, oldest_age_s: float, now: float | None = None):
    """Liveness gauges for the Dagster daemon: when it last ticked, and how long
    the oldest in-progress run has been going."""
    from prometheus_client import CollectorRegistry, Gauge

    reg = CollectorRegistry()
    labels = ["service"]
    base = {"service": settings.observability_service_name}
    Gauge("dagster_heartbeat_timestamp_seconds", "Unix time of the last monitor tick", labels, registry=reg).labels(
        **base
    ).set(time.time() if now is None else now)
    Gauge("dagster_running_runs", "Runs STARTED or STARTING", labels, registry=reg).labels(**base).set(running)
    Gauge(
        "dagster_oldest_running_run_age_seconds", "Age of the oldest in-progress run (0 if none)", labels, registry=reg
    ).labels(**base).set(oldest_age_s)
    return reg


def push_heartbeat(running: int, oldest_age_s: float) -> None:
    _push(HEARTBEAT_JOB, lambda: build_heartbeat_registry(running, oldest_age_s))
