"""GlitchTip/Sentry error reporting for the pipeline.

`init_sentry()` is called at code-location import (definitions.py) so it runs in
every process that matters — the daemon (where the run/failure sensors fire), the
code server, and each run/step process (so an unhandled step exception is
reported). It is a **no-op when `SENTRY_DSN` is unset** (local/dev without the
secret), so it's always safe to call.

GlitchTip is Sentry-API-compatible, so the `sentry_sdk` client talks to it
directly via the DSN (provisioned per environment as the Dagster GlitchTip DSN)."""

from __future__ import annotations

import logging
import re
from typing import Any

from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

_initialized = False


def init_sentry() -> None:
    """Initialise GlitchTip/Sentry reporting once per process. No-op if the DSN
    is unset or the SDK isn't installed — observability must never break the run."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    dsn = (settings.sentry_dsn or "").strip()
    if not dsn:
        logger.info("[observability] SENTRY_DSN unset — GlitchTip reporting disabled")
        return
    try:
        import sentry_sdk
        from sentry_sdk.scrubber import DEFAULT_DENYLIST, EventScrubber

        sentry_sdk.init(
            dsn=dsn,
            environment=settings.sentry_env,
            # Errors only — no performance tracing (keeps GlitchTip volume + cost down).
            traces_sample_rate=0.0,
            # The httpx integration records every request's query string in a
            # breadcrumb, which carries secrets such as IDMC's client_id.
            event_scrubber=EventScrubber(denylist=DEFAULT_DENYLIST + ["http.query"]),
            # Group Dagster log-line events by job + step, not by run (#1526).
            before_send=group_dagster_log_events,
            # Unhandled exceptions + ERROR-level logs become GlitchTip events via
            # the SDK's default logging/excepthook integrations.
        )
        logger.info("[observability] GlitchTip reporting initialised (env=%s)", settings.sentry_env)
    except Exception:  # noqa: BLE001 — never let observability setup break the pipeline
        logger.exception("[observability] GlitchTip init failed — continuing without it")


# A run id, or any other UUID or long hex/number id, in a log message. Stripped
# before grouping so the same failure on the next run lands on the same issue.
_VOLATILE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\b[0-9a-f]{12,}\b|\b\d{4,}\b",
    re.IGNORECASE,
)


def _normalise(message: str) -> str:
    return _VOLATILE.sub("<id>", message)


def dagster_log_fingerprint(meta: Any) -> list[str] | None:
    """GlitchTip fingerprint for a Dagster log record, from its ``dagster_meta``.

    Dagster formats every log line as ``<job> - <run id> - <pid> - <step> -
    <event> - <message>``, so with default grouping one failed run makes one
    issue per line (~14), and the next run makes 14 more. This groups on job,
    step, event type and the message with ids removed: the same failure on the
    next run is another event on the same issue (and regresses it once
    resolved). The run id stays on the event as a tag. GlitchTip joins the
    parts with no separator, hence the ``key=`` prefixes."""
    if not isinstance(meta, dict):
        return None
    event = meta.get("dagster_event")
    event_type = getattr(getattr(event, "event_type", None), "value", None) or "log"
    return [
        "dagster-log|",
        f"job={meta.get('job_name') or meta.get('resource_name') or 'system'}|",
        f"step={meta.get('step_key') or '-'}|",
        f"event={event_type}|",
        _normalise(str(meta.get("orig_message") or "")),
    ]


def group_dagster_log_events(event: dict, hint: dict) -> dict:
    """``before_send`` hook: fingerprint Dagster log-line events (see
    :func:`dagster_log_fingerprint`) and tag them with job, step and run id.
    Every other event passes through unchanged. Never raises."""
    try:
        record = (hint or {}).get("log_record")
        meta = getattr(record, "dagster_meta", None)
        fingerprint = dagster_log_fingerprint(meta)
        if fingerprint and not event.get("fingerprint"):
            event["fingerprint"] = fingerprint
            tags = event.setdefault("tags", {})
            for tag, key in (("job", "job_name"), ("step", "step_key"), ("run_id", "run_id")):
                if meta.get(key):
                    tags.setdefault(tag, meta[key])
    except Exception:  # noqa: BLE001 — grouping must never drop an event
        logger.debug("[observability] could not fingerprint a Dagster log event", exc_info=True)
    return event


def capture_message(
    message: str, *, level: str = "error", fingerprint: list[str] | None = None, **tags: str
) -> None:
    """Send a message event to GlitchTip with optional tags and grouping
    fingerprint. No-op (with a log line) when sentry isn't initialised, so
    callers don't need to guard."""
    try:
        import sentry_sdk

        with sentry_sdk.new_scope() as scope:  # sentry-sdk 2.x API (push_scope is deprecated)
            for key, value in tags.items():
                scope.set_tag(key, value)
            if fingerprint:
                scope.fingerprint = fingerprint
            sentry_sdk.capture_message(message, level=level)
    except Exception:  # noqa: BLE001
        logger.warning("[observability] GlitchTip capture_message skipped: %s", message)
