"""
Periodic archival Celery tasks.

Tasks:
  - archive_stale_alerts: once per day, archive alerts whose linked event's
    lastSignalCreatedAt is older than the configured retention window
    (default 14 days). Calls the archiveStaleAlerts mutation on clear-api.
"""

import logging

from src.celery_app import app
from src.clients import graphql

logger = logging.getLogger(__name__)


@app.task(
    name="src.tasks.archive.archive_stale_alerts",
    bind=True,
    max_retries=2,
    acks_late=True,
)
def archive_stale_alerts(self, older_than_days: int = 14) -> dict:
    """Archive alerts whose event.lastSignalCreatedAt < now - N days."""
    logger.info("[ARCHIVE] archive_stale_alerts: older_than_days=%d", older_than_days)
    try:
        count = graphql.archive_stale_alerts(older_than_days=older_than_days)
        logger.info("[ARCHIVE] Archived %d stale alert(s)", count)
        return {"alerts_archived": count}
    except Exception as exc:
        logger.error("[ARCHIVE] archive_stale_alerts failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=300)
