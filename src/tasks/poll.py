"""Celery task: poll Dataminr for new signals and dispatch processing."""

import logging
from datetime import UTC, datetime

from src.celery_app import app
from src.clients.dataminr import fetch_signals, get_last_synced
from src.clients.graphql import GraphQLClientError, get_latest_signal_timestamp

logger = logging.getLogger(__name__)


@app.task(name="src.tasks.poll.poll_dataminr", bind=True, max_retries=3)
def poll_dataminr(self):
    """
    Poll Dataminr for new signals within the time window.

    Time window strategy:
    - Check Redis for last_synced timestamp
    - If not in Redis, query clear-api for latest signal's publishedAt
    - If no signals exist at all, use INITIAL_LOOKBACK_DAYS
    - Fetch all signals from that timestamp to now
    - Fan out each signal to process_signal task
    """
    try:
        # Determine start of time window
        since = get_last_synced()

        if since is None:
            # Try to get from clear-api
            latest = get_latest_signal_timestamp()
            if latest:
                since = datetime.fromisoformat(latest.replace("Z", "+00:00"))
                logger.info("[DATAMINR] Resuming from latest signal in DB: %s", since.isoformat())
            # If still None, fetch_signals will use INITIAL_LOOKBACK_DAYS default

        if since:
            logger.info("[DATAMINR] Polling for signals since %s", since.isoformat())
        else:
            logger.info("[DATAMINR] Polling (initial lookback)")

        signals = fetch_signals(since=since)

        if not signals:
            logger.info("[DATAMINR] No new signals")
            return {"signals_found": 0}

        # Fan out to process_signal tasks
        from src.tasks.process import process_signal

        for signal in signals:
            process_signal.delay(signal.model_dump(mode="json"))

        logger.info("[DATAMINR] Dispatched %d signals for processing", len(signals))
        return {"signals_found": len(signals)}

    except GraphQLClientError as exc:
        logger.error("[DATAMINR] poll_dataminr permanently failed (non-retryable): %s", exc)
        raise
    except Exception as exc:
        logger.error("[DATAMINR] poll_dataminr failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=30)
