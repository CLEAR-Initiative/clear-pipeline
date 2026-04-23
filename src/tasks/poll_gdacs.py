"""Celery task: poll GDACS for new disaster events and dispatch processing."""

import logging
from datetime import UTC, datetime

from src.celery_app import app
from src.clients.gdacs import fetch_gdacs_events, get_last_synced
from src.clients.graphql import create_signal, get_data_sources

logger = logging.getLogger(__name__)

_gdacs_source_id: str | None = None


def _get_gdacs_source_id() -> str:
    """Get the GDACS data source ID from the CLEAR API (cached)."""
    global _gdacs_source_id
    if _gdacs_source_id is not None:
        return _gdacs_source_id

    from src.config import settings
    sources = get_data_sources()
    for src in sources:
        if src["name"] == settings.gdacs_source_name:
            _gdacs_source_id = src["id"]
            return _gdacs_source_id
    raise RuntimeError(
        f"Data source '{settings.gdacs_source_name}' not found in CLEAR API. "
        "Ensure it exists in the data_sources table."
    )


def _build_signal_input(event: dict, source_id: str) -> dict:
    """Convert a parsed GDACS event into a CLEAR CreateSignalInput dict."""
    input_data: dict = {
        "sourceId": source_id,
        # Dedup key — (sourceId, externalId) is unique, so re-ingesting the
        # same GDACS event (across poll rounds) returns the existing row.
        "externalId": f"gdacs:{event['gdacs_id']}",
        "rawData": event["raw"],
        "publishedAt": event.get("from_date") or datetime.now(UTC).isoformat(),
        "url": event.get("url"),
        "title": event["title"],
        "description": event.get("description"),
        "severity": event.get("severity"),
    }

    # Pass lat/lng for server-side PostGIS geo-resolution
    if event.get("lat") is not None and event.get("lng") is not None:
        input_data["lat"] = event["lat"]
        input_data["lng"] = event["lng"]

    return input_data


@app.task(name="src.tasks.poll_gdacs.poll_gdacs", bind=True, max_retries=3)
def poll_gdacs(self):
    """
    Poll GDACS for new disaster events.

    - Fetches events for the configured country (default: Sudan)
    - Creates CLEAR signals from each event
    - Dispatches each signal to the process_signal pipeline for classification + grouping
    """
    try:
        since = get_last_synced()

        if since:
            logger.info("[GDACS] Polling for events since last_synced=%s", since.isoformat())
        else:
            logger.info("[GDACS] No last_synced — using initial lookback")

        events = fetch_gdacs_events(since=since)

        if not events:
            logger.info("[GDACS] No new events to ingest")
            return {"events_found": 0, "signals_created": 0}

        source_id = _get_gdacs_source_id()
        logger.info("[GDACS] Creating signals using source_id=%s", source_id)

        created_count = 0
        failed_count = 0

        for event in events:
            try:
                input_data = _build_signal_input(event, source_id)
                created = create_signal(input_data)
                signal_id = created["id"]
                logger.info(
                    "[GDACS] Signal created: id=%s type=%s severity=%d title=%s",
                    signal_id,
                    event.get("event_type"),
                    event.get("severity", 0),
                    event.get("title", "")[:80],
                )

                # Dispatch to the standard processing pipeline
                from src.tasks.process import process_gdacs_signal
                process_gdacs_signal.delay(
                    signal_id=signal_id,
                    gdacs_event=event,
                    created_signal=created,
                )
                created_count += 1

            except Exception as e:
                failed_count += 1
                logger.error(
                    "[GDACS] Failed to ingest event %s: %s",
                    event.get("gdacs_id"),
                    e,
                    exc_info=True,
                )

        logger.info(
            "[GDACS] Poll complete: %d events found → %d signals created (%d failed)",
            len(events), created_count, failed_count,
        )
        return {"events_found": len(events), "signals_created": created_count, "failed": failed_count}

    except Exception as exc:
        logger.error("[GDACS] poll_gdacs failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=60)
