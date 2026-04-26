"""Event grouping service: cluster signals into events using Claude."""

import json
import logging
from datetime import UTC, datetime, timedelta

import redis

from src.clients.claude import call_claude
from src.clients.graphql import create_event, get_events, update_event
from src.config import settings
from src.models.clear import EventGroupingResult, SignalClassification
from src.prompts.group import GROUP_PROMPT_VERSION, SYSTEM_PROMPT, build_group_prompt
from src.services.redis_lock import redis_lock
from src.services.event_grouping_v2 import group_signal_v2


def dispatch_group_signal(
    *,
    algo: str | None = None,
    **kwargs,
) -> dict | None:
    """Pick the grouping implementation based on `settings.grouping_algo`
    (or the `algo` override). `kwargs` are passed to the chosen function —
    extra kwargs unused by one variant are dropped cleanly below.
    """
    algo_sel = (algo or settings.grouping_algo or "v1").lower()
    if algo_sel == "v2":
        v2_keys = {
            "signal_id", "signal_title", "signal_description",
            "signal_timestamp", "classification", "created_signal",
        }
        return group_signal_v2(**{k: v for k, v in kwargs.items() if k in v2_keys})
    v1_keys = {
        "signal_id", "signal_title", "signal_description",
        "signal_location_name", "signal_origin_id", "signal_timestamp",
        "classification", "signal_lat", "signal_lng", "probability_radius_km",
        "created_signal",
    }
    return group_signal(**{k: v for k, v in kwargs.items() if k in v1_keys})

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
    probability_radius_km: float | None = None,
    created_signal: dict | None = None,
) -> dict | None:
    """
    Use Claude to decide if a signal belongs to an existing event or creates a new one.
    Every signal MUST end up in an event.

    Returns the event dict (created or updated) or None if grouping fails.
    """
    # Short-circuit: when a prior run already linked this signal to an
    # event, return that event instead of asking Claude to re-cluster.
    # Without this, a task retried after a downstream failure would ask
    # Claude to decide again and potentially create a second event.
    if created_signal:
        existing_events = created_signal.get("events") or []
        if existing_events:
            existing = existing_events[0]
            logger.info(
                "[GROUPING v1] Signal %s already linked to event %s — returning existing",
                signal_id, existing.get("id"),
            )
            return existing

    # Lock key approximates "signals that could plausibly cluster together":
    # (some location key + primary disaster type). Two workers hitting the
    # same bucket serialise, so the second one sees the first one's newly
    # created event instead of calling Claude with the same context twice.
    loc_key = (
        signal_origin_id
        or (signal_location_name or "").strip().lower()
        or "unknown"
    )
    type_key = classification.disaster_types[0] if classification.disaster_types else "ot"
    lock_key = f"group:v1:{loc_key}:{type_key}"

    with redis_lock(lock_key, ttl_seconds=30, wait_seconds=20) as acquired:
        if not acquired:
            logger.warning(
                "[GROUPING v1] Could not acquire %s within deadline — "
                "proceeding unlocked (duplicate event risk accepted).",
                lock_key,
            )
        return _group_signal_locked(
            signal_id=signal_id,
            signal_title=signal_title,
            signal_description=signal_description,
            signal_location_name=signal_location_name,
            signal_origin_id=signal_origin_id,
            signal_timestamp=signal_timestamp,
            classification=classification,
            signal_lat=signal_lat,
            signal_lng=signal_lng,
            probability_radius_km=probability_radius_km,
        )


def _group_signal_locked(
    signal_id: str,
    signal_title: str | None,
    signal_description: str | None,
    signal_location_name: str | None,
    signal_origin_id: str | None,
    signal_timestamp: str | None,
    classification: SignalClassification,
    signal_lat: float | None = None,
    signal_lng: float | None = None,
    probability_radius_km: float | None = None,
) -> dict | None:
    """The original body of `group_signal`, extracted so the public entry
    point can wrap it in a Redis lock."""
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

    result_data = call_claude(
        SYSTEM_PROMPT,
        prompt,
        stage="group",
        prompt_version=GROUP_PROMPT_VERSION,
        signal_id=signal_id,
    )
    result = EventGroupingResult.model_validate(result_data)

    now_iso = datetime.now(UTC).isoformat()
    ts = signal_timestamp or now_iso

    # Estimate population from GeoTIFF if Claude didn't extract it
    population = result.population_affected
    logger.info(
        "[EVENT] Population from Claude for signal %s: %s (lat=%s lng=%s radius=%s)",
        signal_id, population, signal_lat, signal_lng, probability_radius_km,
    )
    if population is None and signal_lat is not None and signal_lng is not None:
        from src.services.population import estimate_population_for_signal

        population = estimate_population_for_signal(
            lat=signal_lat,
            lng=signal_lng,
            probability_radius_km=probability_radius_km,
        )
        logger.info(
            "[EVENT] Population from GeoTIFF for signal %s: %s (radius=%.3f km)",
            signal_id, population, probability_radius_km or 1.0,
        )

    if result.action == "add_to_existing" and result.event_id:
        logger.info("Adding signal %s to existing event %s", signal_id, result.event_id)

        update_data: dict = {
            "signalIds": [signal_id],
            "lastSignalCreatedAt": ts,
        }

        # Bump event severity (and derived rank) to the MAX across signals —
        # otherwise event.severity stays frozen at the first signal's value.
        existing_event = next(
            (e for e in active_events if e.get("id") == result.event_id),
            None,
        )
        existing_severity = (existing_event or {}).get("severity") or 0
        max_severity = max(existing_severity, classification.severity)
        if max_severity > existing_severity:
            update_data["severity"] = max_severity
            update_data["rank"] = max_severity / 5.0
            logger.info(
                "[EVENT] Severity bumped on event %s: %d → %d",
                result.event_id, existing_severity, max_severity,
            )

        # Update title and description if Claude provided new ones
        if result.title:
            update_data["title"] = result.title
        if result.description:
            update_data["description"] = result.description
        if population is not None:
            update_data["populationAffected"] = str(population)

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
        if population is not None:
            event_input["populationAffected"] = str(population)
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
