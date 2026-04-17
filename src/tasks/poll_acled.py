"""Celery task: poll ACLED for new conflict events and dispatch processing."""

import logging
from datetime import UTC, datetime

from src.celery_app import app
from src.clients.acled import fetch_acled_events, get_last_synced
from src.clients.graphql import create_signal, get_data_sources

logger = logging.getLogger(__name__)

_acled_source_id: str | None = None


def _get_acled_source_id() -> str:
    """Get the ACLED data source ID from the CLEAR API (cached)."""
    global _acled_source_id
    if _acled_source_id is not None:
        return _acled_source_id

    from src.config import settings
    sources = get_data_sources()
    for src in sources:
        if src["name"] == settings.acled_source_name:
            _acled_source_id = src["id"]
            return _acled_source_id
    raise RuntimeError(
        f"Data source '{settings.acled_source_name}' not found in CLEAR API. "
        "Ensure it exists in the data_sources table."
    )


def _build_signal_input(event: dict, source_id: str) -> dict:
    """Convert a parsed ACLED event into a CLEAR CreateSignalInput dict."""
    # Parse event_date into ISO-8601
    event_date = event.get("event_date", "")
    try:
        published_at = datetime.strptime(event_date, "%Y-%m-%d").replace(tzinfo=UTC).isoformat()
    except (ValueError, TypeError):
        published_at = datetime.now(UTC).isoformat()

    input_data: dict = {
        "sourceId": source_id,
        "rawData": event["raw"],
        "publishedAt": published_at,
        "title": event["title"],
        "description": event.get("description"),
        "severity": event.get("severity"),
    }

    if event.get("source_url"):
        input_data["url"] = event["source_url"]

    # Pass lat/lng for server-side PostGIS geo-resolution
    if event.get("lat") is not None and event.get("lng") is not None:
        input_data["lat"] = event["lat"]
        input_data["lng"] = event["lng"]

    return input_data


@app.task(name="src.tasks.poll_acled.poll_acled", bind=True, max_retries=3)
def poll_acled(self):
    """
    Poll ACLED for new conflict events.

    - Fetches events for configured countries (default: Sudan)
    - Creates CLEAR signals from each event
    - Dispatches to process_acled_signal for classification + grouping
    """
    try:
        since = get_last_synced()

        if since:
            logger.info("[ACLED] Polling for events since last_synced=%s", since.isoformat())
        else:
            logger.info("[ACLED] No last_synced — using initial lookback")

        events = fetch_acled_events(since=since)

        if not events:
            logger.info("[ACLED] No new events to ingest")
            return {"events_found": 0, "signals_created": 0}

        source_id = _get_acled_source_id()
        logger.info("[ACLED] Creating signals using source_id=%s", source_id)

        created_count = 0
        failed_count = 0

        for event in events:
            try:
                input_data = _build_signal_input(event, source_id)
                created = create_signal(input_data)
                signal_id = created["id"]
                logger.info(
                    "[ACLED] Signal created: id=%s type=%s severity=%d title=%s",
                    signal_id,
                    event.get("event_type"),
                    event.get("severity", 0),
                    event.get("title", "")[:80],
                )

                from src.tasks.process import process_acled_signal
                process_acled_signal.delay(
                    signal_id=signal_id,
                    acled_event=event,
                )
                created_count += 1

            except Exception as e:
                failed_count += 1
                logger.error(
                    "[ACLED] Failed to ingest event %s: %s",
                    event.get("acled_id"),
                    e,
                    exc_info=True,
                )

        logger.info(
            "[ACLED] Poll complete: %d events found → %d signals created (%d failed)",
            len(events), created_count, failed_count,
        )
        return {"events_found": len(events), "signals_created": created_count, "failed": failed_count}

    except Exception as exc:
        logger.error("[ACLED] poll_acled failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=60)
