"""
Centralised logging configuration — Better Stack (Logtail) + Sentry.

Env vars:
  LOGTAIL_SOURCE_TOKEN  — Better Stack ingest token
  SENTRY_DSN            — Sentry project DSN
  SENTRY_ENV            — Sentry environment
  LOG_LEVEL             — Python log level (default: INFO)
"""

import logging
import sys
import threading

_configured = False
_lock = threading.Lock()
_STDOUT_HANDLER: logging.Handler | None = None
_LOGTAIL_HANDLER: logging.Handler | None = None
_LEVEL: int = logging.INFO


def _make_stdout_handler(level: int) -> logging.Handler:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s [%(processName)s/%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    return handler


def _make_logtail_handler(token: str, level: int) -> logging.Handler | None:
    try:
        from logtail import LogtailHandler

        handler = LogtailHandler(source_token=token)
        handler.setLevel(level)
        return handler
    except Exception as e:
        logging.getLogger(__name__).warning(
            "[LOGGING] Failed to create Logtail handler: %s", e
        )
        return None


def attach_handlers_to(logger: logging.Logger) -> None:
    """Ensure our stdout + Logtail handlers are attached to `logger`.

    Called from Celery's after_setup_logger / after_setup_task_logger signals
    because Celery reconfigures loggers after worker boot — we need to
    re-attach our handlers or task logs won't be visible.
    """
    global _STDOUT_HANDLER, _LOGTAIL_HANDLER

    if _STDOUT_HANDLER is None:
        # Lazy init if setup_logging hasn't run yet
        setup_logging()

    logger.setLevel(_LEVEL)

    # Attach handlers if not already present (by identity)
    if _STDOUT_HANDLER and _STDOUT_HANDLER not in logger.handlers:
        logger.addHandler(_STDOUT_HANDLER)
    if _LOGTAIL_HANDLER and _LOGTAIL_HANDLER not in logger.handlers:
        logger.addHandler(_LOGTAIL_HANDLER)

    # Don't double-log via root
    logger.propagate = False


def setup_logging() -> None:
    global _configured, _STDOUT_HANDLER, _LOGTAIL_HANDLER, _LEVEL

    with _lock:
        if _configured:
            return
        _configured = True

        from src.config import settings

        level = getattr(logging, settings.log_level.upper(), logging.INFO)
        _LEVEL = level

        # ── Build shared handlers ────────────────────────────────────────
        _STDOUT_HANDLER = _make_stdout_handler(level)
        if settings.logtail_source_token:
            _LOGTAIL_HANDLER = _make_logtail_handler(settings.logtail_source_token, level)

        # ── Root logger ──────────────────────────────────────────────────
        root = logging.getLogger()
        root.setLevel(level)
        root.handlers.clear()
        root.addHandler(_STDOUT_HANDLER)
        if _LOGTAIL_HANDLER:
            root.addHandler(_LOGTAIL_HANDLER)

        # ── Sentry ───────────────────────────────────────────────────────
        if settings.sentry_dsn:
            try:
                import sentry_sdk
                from sentry_sdk.integrations.celery import CeleryIntegration
                from sentry_sdk.integrations.logging import LoggingIntegration

                sentry_sdk.init(
                    dsn=settings.sentry_dsn,
                    environment=settings.sentry_env,
                    traces_sample_rate=0.2,
                    integrations=[
                        CeleryIntegration(monitor_beat_tasks=True),
                        LoggingIntegration(
                            level=logging.INFO,
                            event_level=logging.ERROR,
                        ),
                    ],
                    before_send=_scrub_event,
                )
                root.info("[LOGGING] Sentry initialised (env=%s)", settings.sentry_env)
            except Exception as e:
                root.warning("[LOGGING] Failed to initialise Sentry: %s", e)
        else:
            root.info("[LOGGING] Sentry not configured (SENTRY_DSN empty)")

        if _LOGTAIL_HANDLER:
            root.info("[LOGGING] Logtail handler attached (Better Stack)")
        else:
            root.info("[LOGGING] Logtail not configured (LOGTAIL_SOURCE_TOKEN empty)")

        # Quieten noisy third-party loggers
        for noisy in ("httpx", "httpcore", "boto3", "botocore", "urllib3",
                      "celery.utils.functional"):
            logging.getLogger(noisy).setLevel(logging.WARNING)


def _scrub_event(event, hint):
    if event.get("request", {}).get("headers"):
        headers = event["request"]["headers"]
        for key in ("authorization", "cookie", "x-api-key"):
            headers.pop(key, None)
    return event
