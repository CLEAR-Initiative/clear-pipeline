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
ACTIVE_EVENTS_TTL = 300  # 5 min (shorter to keep data fresh)


def _get_active_events() -> list[dict]:
    """Get active events from cache or fetch from CLEAR API.
    Only returns events from the last 7 days."""
    cached = _redis.get(ACTIVE_EVENTS_CACHE_KEY)
    if cached:
        return json.loads(cached)

    events = get_events()

    # Filter to events from the last 7 days
    cutoff = datetime.now(UTC) - timedelta(days=7)
    recent_events = []
    for e in events:
        try:
            last_signal = e.get("lastSignalCreatedAt") or e.get("validFrom", "")
            ts = datetime.fromisoformat(last_signal.replace("Z", "+00:00"))
            if ts >= cutoff:
                recent_events.append(e)
        except (ValueError, AttributeError):
            # Include events with unparseable dates (rather than silently drop)
            recent_events.append(e)

    _redis.setex(ACTIVE_EVENTS_CACHE_KEY, ACTIVE_EVENTS_TTL, json.dumps(recent_events))
    logger.info("Cached %d active events (out of %d total)", len(recent_events), len(events))
    return recent_events


def _invalidate_events_cache() -> None:
    _redis.delete(ACTIVE_EVENTS_CACHE_KEY)


def group_signal(
    signal_id: str,
    signal_title: str | None,
    signal_description: str | None,
    signal_location_name: str | None,
    signal_origin_id: str | None,
    signal_timestamp: str | None,
    classification: SignalClassification,
    signal_lat: float | None = None,
    signal_lng: float | None = None,
) -> dict | None:
    """
    Use Claude to decide if a signal belongs to an existing event or creates a new one.
    Every signal MUST end up in an event.

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
    ts = signal_timestamp or now_iso

    if result.action == "add_to_existing" and result.event_id:
        logger.info("Adding signal %s to existing event %s", signal_id, result.event_id)

        update_data: dict = {
            "signalIds": [signal_id],
            "lastSignalCreatedAt": ts,
        }

        # Update title and description if Claude provided new ones
        if result.title:
            update_data["title"] = result.title
        if result.description:
            update_data["description"] = result.description

        updated = update_event(result.event_id, update_data)
        _invalidate_events_cache()
        return updated

    elif result.action == "create_new":
        logger.info("Creating new event for signal %s: %s", signal_id, result.title)
        valid_from = ts
        try:
            valid_to = (
                datetime.fromisoformat(ts.replace("Z", "+00:00"))
                + timedelta(days=7)
            ).isoformat()
        except (ValueError, AttributeError):
            valid_to = (datetime.now(UTC) + timedelta(days=7)).isoformat()

        event_input: dict = {
            "signalIds": [signal_id],
            "title": result.title,
            "description": result.description,
            "validFrom": valid_from,
            "validTo": valid_to,
            "firstSignalCreatedAt": ts,
            "lastSignalCreatedAt": ts,
            "types": result.types or classification.disaster_types,
            "severity": classification.severity,
            "rank": classification.severity / 5.0,
            "originId": signal_origin_id,
        }
        if signal_lat is not None and signal_lng is not None:
            event_input["lat"] = signal_lat
            event_input["lng"] = signal_lng
        event = create_event(event_input)
        _invalidate_events_cache()
        return event

    else:
        # Fallback: if Claude returns unexpected action, create a new event anyway
        # to ensure every signal is associated with an event
        logger.warning(
            "Unexpected grouping action '%s' for signal %s — creating fallback event",
            result.action,
            signal_id,
        )
        try:
            valid_to = (
                datetime.fromisoformat(ts.replace("Z", "+00:00"))
                + timedelta(days=7)
            ).isoformat()
        except (ValueError, AttributeError):
            valid_to = (datetime.now(UTC) + timedelta(days=7)).isoformat()

        event = create_event({
            "signalIds": [signal_id],
            "title": signal_title or "Ungrouped Signal",
            "description": signal_description or classification.summary,
            "validFrom": ts,
            "validTo": valid_to,
            "firstSignalCreatedAt": ts,
            "lastSignalCreatedAt": ts,
            "types": classification.disaster_types,
            "severity": classification.severity,
            "rank": classification.severity / 5.0,
        })
        _invalidate_events_cache()
        return event
