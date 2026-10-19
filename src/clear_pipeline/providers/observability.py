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
            # Unhandled exceptions + ERROR-level logs become GlitchTip events via
            # the SDK's default logging/excepthook integrations.
        )
        logger.info("[observability] GlitchTip reporting initialised (env=%s)", settings.sentry_env)
    except Exception:  # noqa: BLE001 — never let observability setup break the pipeline
        logger.exception("[observability] GlitchTip init failed — continuing without it")


def capture_message(message: str, *, level: str = "error", **tags: str) -> None:
    """Send a message event to GlitchTip with optional tags. No-op (with a log
    line) when sentry isn't initialised, so callers don't need to guard."""
    try:
        import sentry_sdk

        with sentry_sdk.new_scope() as scope:  # sentry-sdk 2.x API (push_scope is deprecated)
            for key, value in tags.items():
                scope.set_tag(key, value)
            sentry_sdk.capture_message(message, level=level)
    except Exception:  # noqa: BLE001
        logger.warning("[observability] GlitchTip capture_message skipped: %s", message)
