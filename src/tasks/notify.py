"""Celery tasks: periodic alert digest notifications."""

import logging
from datetime import UTC, datetime, timedelta

import redis

from src.celery_app import app
from src.clients.graphql import get_published_alerts, notify_alert_digest
from src.config import settings

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

# Redis keys to track which alerts have been included in each digest
DAILY_CURSOR_KEY = "digest:daily:last_run"
WEEKLY_CURSOR_KEY = "digest:weekly:last_run"
MONTHLY_CURSOR_KEY = "digest:monthly:last_run"


def _get_alerts_since(since_iso: str | None, fallback_days: int) -> list[dict]:
    """Get published alerts with first signal created after the given timestamp."""
    all_alerts = get_published_alerts()
    if not all_alerts:
        return []

    if since_iso:
        cutoff = datetime.fromisoformat(since_iso.replace("Z", "+00:00"))
    else:
        cutoff = datetime.now(UTC) - timedelta(days=fallback_days)

    return [
        a for a in all_alerts
        if datetime.fromisoformat(
            a["event"]["firstSignalCreatedAt"].replace("Z", "+00:00")
        ) > cutoff
    ]


@app.task(
    name="src.tasks.notify.send_daily_digest",
    bind=True,
    max_retries=2,
    acks_late=True,
)
def send_daily_digest(self):
    """Send daily alert digest to subscribers with daily frequency."""
    try:
        last_run = _redis.get(DAILY_CURSOR_KEY)
        alerts = _get_alerts_since(last_run, fallback_days=1)

        if not alerts:
            logger.info("No new alerts for daily digest")
            return {"frequency": "daily", "alerts": 0, "notified": 0}

        alert_ids = [a["id"] for a in alerts]
        count = notify_alert_digest(alert_ids, "daily")

        _redis.set(DAILY_CURSOR_KEY, datetime.now(UTC).isoformat())
        logger.info("Daily digest sent: %d alerts, %d notifications", len(alert_ids), count)

        return {"frequency": "daily", "alerts": len(alert_ids), "notified": count}

    except Exception as exc:
        logger.error("send_daily_digest failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=60)


@app.task(
    name="src.tasks.notify.send_weekly_digest",
    bind=True,
    max_retries=2,
    acks_late=True,
)
def send_weekly_digest(self):
    """Send weekly alert digest to subscribers with weekly frequency."""
    try:
        last_run = _redis.get(WEEKLY_CURSOR_KEY)
        alerts = _get_alerts_since(last_run, fallback_days=7)

        if not alerts:
            logger.info("No new alerts for weekly digest")
            return {"frequency": "weekly", "alerts": 0, "notified": 0}

        alert_ids = [a["id"] for a in alerts]
        count = notify_alert_digest(alert_ids, "weekly")

        _redis.set(WEEKLY_CURSOR_KEY, datetime.now(UTC).isoformat())
        logger.info("Weekly digest sent: %d alerts, %d notifications", len(alert_ids), count)

        return {"frequency": "weekly", "alerts": len(alert_ids), "notified": count}

    except Exception as exc:
        logger.error("send_weekly_digest failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=60)


@app.task(
    name="src.tasks.notify.send_monthly_digest",
    bind=True,
    max_retries=2,
    acks_late=True,
)
def send_monthly_digest(self):
    """Send monthly alert digest to subscribers with monthly frequency."""
    try:
        last_run = _redis.get(MONTHLY_CURSOR_KEY)
        alerts = _get_alerts_since(last_run, fallback_days=30)

        if not alerts:
            logger.info("No new alerts for monthly digest")
            return {"frequency": "monthly", "alerts": 0, "notified": 0}

        alert_ids = [a["id"] for a in alerts]
        count = notify_alert_digest(alert_ids, "monthly")

        _redis.set(MONTHLY_CURSOR_KEY, datetime.now(UTC).isoformat())
        logger.info("Monthly digest sent: %d alerts, %d notifications", len(alert_ids), count)

        return {"frequency": "monthly", "alerts": len(alert_ids), "notified": count}

    except Exception as exc:
        logger.error("send_monthly_digest failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=60)
