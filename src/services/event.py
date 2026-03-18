"""Event grouping service: cluster signals into events using Claude."""

import json
import logging
from datetime import UTC, datetime, timedelta

import redis

from src.clients.claude import call_claude
from src.clients.graphql import create_event, get_events, update_event
from src.config import settings
from src.models.clear import EventGroupingResult, SignalClassification
from src.prompts.group import SYSTEM_PROMPT, build_group_prompt

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

ACTIVE_EVENTS_CACHE_KEY = "events:active"
ACTIVE_EVENTS_TTL = 3600  # 1 hour


def _get_active_events() -> list[dict]:
    """Get active events from cache or fetch from CLEAR API."""
    cached = _redis.get(ACTIVE_EVENTS_CACHE_KEY)
    if cached:
        return json.loads(cached)

    events = get_events()
    _redis.setex(ACTIVE_EVENTS_CACHE_KEY, ACTIVE_EVENTS_TTL, json.dumps(events))
    return events


def _invalidate_events_cache() -> None:
    _redis.delete(ACTIVE_EVENTS_CACHE_KEY)


def group_signal(
    signal_id: str,
    signal_title: str | None,
    signal_description: str | None,
    signal_location_name: str | None,
    signal_origin_id: str | None,
    signal_timestamp: str,
    classification: SignalClassification,
) -> dict | None:
    """
    Use Claude to decide if a signal belongs to an existing event or creates a new one.

    Returns the event dict (created or updated) or None if grouping fails.
    """
    active_events = _get_active_events()

    prompt = build_group_prompt(
        title=signal_title,
        description=signal_description,
        location_name=signal_location_name,
        disaster_types=classification.disaster_types,
        severity=classification.severity,
        summary=classification.summary,
        timestamp=signal_timestamp,
        active_events=active_events,
    )

    result_data = call_claude(SYSTEM_PROMPT, prompt)
    result = EventGroupingResult.model_validate(result_data)

    now_iso = datetime.now(UTC).isoformat()

    if result.action == "add_to_existing" and result.event_id:
        logger.info("Adding signal %s to existing event %s", signal_id, result.event_id)
        # Add signal to existing event's signalIds
        updated = update_event(result.event_id, {
            "signalIds": [signal_id],
            "lastSignalCreatedAt": signal_timestamp,
        })
        _invalidate_events_cache()
        return updated

    elif result.action == "create_new":
        logger.info("Creating new event for signal %s: %s", signal_id, result.title)
        valid_from = signal_timestamp
        valid_to = (
            datetime.fromisoformat(signal_timestamp.replace("Z", "+00:00"))
            + timedelta(days=7)
        ).isoformat()

        event_input = {
            "signalIds": [signal_id],
            "title": result.title,
            "description": result.description,
            "validFrom": valid_from,
            "validTo": valid_to,
            "firstSignalCreatedAt": signal_timestamp,
            "lastSignalCreatedAt": signal_timestamp,
            "types": result.types or classification.disaster_types,
            "rank": classification.severity / 5.0,  # Normalize to 0-1
            "originId": signal_origin_id,
        }
        event = create_event(event_input)
        _invalidate_events_cache()
        return event

    else:
        logger.warning("Unexpected grouping action: %s", result.action)
        return None
