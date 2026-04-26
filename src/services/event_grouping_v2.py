"""
District + type based event grouping (v2).

Replaces the old semantic-only grouping with a rule-based matcher:

1. Classify the signal via EventClassifier → (level_1, level_2, glide_code).
2. Resolve the signal's admin-2 district id.
3. Fetch active events (last 14 days) and filter to those whose locations
   resolve to the same district AND whose `types[]` contains a glide code
   belonging to the same level_2 group as the signal.
4. If matches exist: add the signal to the MOST RECENT match (by
   `lastSignalCreatedAt`).
5. Otherwise: create a new event.
6. In either case, ask Claude to rewrite `title` + `description` from the
   event's current signal set (including the newly added signal).

Claude is no longer used for clustering decisions — only for the narrative.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import redis

from src.clients import graphql
from src.clients.claude import call_claude
from src.clients.graphql import create_event, get_events, update_event
from src.config import settings
from src.models.clear import EventRewrite, SignalClassification
from src.prompts.rewrite import (
    REWRITE_PROMPT_VERSION,
    SYSTEM_PROMPT as REWRITE_SYSTEM,
    build_rewrite_prompt,
)
from src.services.admin_resolver import resolve_admin2, resolve_signal_admin2
from src.services.classifier_singleton import (
    code_to_level2_map,
    get_classifier,
    level2_to_codes_map,
)
from src.services.redis_lock import redis_lock

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

ACTIVE_EVENTS_CACHE_KEY_V2 = "events:active:v2"
ACTIVE_EVENTS_TTL_V2 = 300  # 5 min
ACTIVE_EVENTS_WINDOW_DAYS = 14  # Must match the archival window

IOM_DTM_METADATA_TYPE = "iom_dtm_displacement"


def _compute_event_severity(
    signals: list[dict],
    claude_fallback: int | None,
) -> int | None:
    """Event-level severity rule:
    - If EVERY signal has a non-null source severity → return round(mean).
    - Otherwise → return the Claude-estimated fallback (may itself be None).
    """
    if not signals:
        return claude_fallback
    severities = [s.get("severity") for s in signals]
    if all(s is not None for s in severities):
        mean = sum(severities) / len(severities)
        return max(1, min(5, round(mean)))
    return claude_fallback


def _resolve_population_displaced(
    claude_value: int | None,
    admin2_id: str | None,
) -> int | None:
    """Three-tier fallback:
      1. `claude_value` (Claude's max extraction across signal text).
      2. `location_metadata(type="iom_dtm_displacement")` for the event's
         admin-2, reading `data.population_displaced`.
      3. `settings.default_population_displaced` (1670 by default).
    Returns None only if all three fall through (shouldn't happen — default
    is always set).
    """
    if claude_value is not None and claude_value > 0:
        return int(claude_value)

    if admin2_id:
        try:
            rows = graphql.get_location_metadata(admin2_id, IOM_DTM_METADATA_TYPE)
            if rows:
                data = rows[0].get("data") or {}
                dtm_val = data.get("population_displaced")
                if dtm_val is not None:
                    logger.info(
                        "[GROUPING v2] populationDisplaced from DTM for admin2=%s: %s",
                        admin2_id, dtm_val,
                    )
                    return int(dtm_val)
        except Exception as e:
            logger.warning(
                "[GROUPING v2] DTM lookup failed for admin2=%s: %s",
                admin2_id, e,
            )

    default = settings.default_population_displaced
    logger.info("[GROUPING v2] populationDisplaced falling back to default: %s", default)
    return default


def _get_active_events_v2() -> list[dict]:
    """Events touched in the last 14 days (matches the archival cutoff so we
    don't cluster into an event that the nightly job is about to archive)."""
    cached = _redis.get(ACTIVE_EVENTS_CACHE_KEY_V2)
    if cached:
        return json.loads(cached)

    events = get_events()
    cutoff = datetime.now(UTC) - timedelta(days=ACTIVE_EVENTS_WINDOW_DAYS)
    recent: list[dict] = []
    for e in events:
        try:
            last = e.get("lastSignalCreatedAt") or e.get("validFrom") or ""
            ts = datetime.fromisoformat(last.replace("Z", "+00:00"))
            if ts >= cutoff:
                recent.append(e)
        except (ValueError, AttributeError):
            # Don't silently drop parseable-but-weird dates
            recent.append(e)

    _redis.setex(ACTIVE_EVENTS_CACHE_KEY_V2, ACTIVE_EVENTS_TTL_V2, json.dumps(recent))
    logger.info(
        "[GROUPING v2] Cached %d active events (of %d total, %dd window)",
        len(recent), len(events), ACTIVE_EVENTS_WINDOW_DAYS,
    )
    return recent


def _invalidate_events_cache_v2() -> None:
    _redis.delete(ACTIVE_EVENTS_CACHE_KEY_V2)


def _event_matches(event: dict, target_admin2: str, target_level2: str) -> bool:
    """True iff the event's primary location resolves to `target_admin2` AND
    its `types[]` contains a code whose level_2 == `target_level2`."""
    types: list[str] = event.get("types") or []
    if not types:
        return False

    code_l2 = code_to_level2_map()
    event_l2s = {code_l2.get(t) for t in types if code_l2.get(t)}
    if target_level2 not in event_l2s:
        return False

    # Event location — pick the first non-null, resolve its admin-2
    for key in ("originLocation", "generalLocation", "destinationLocation"):
        loc = event.get(key)
        if not loc:
            continue
        admin2 = resolve_admin2(loc)
        if admin2 == target_admin2:
            return True
    return False


def _most_recent(events: list[dict]) -> dict | None:
    def ts(e: dict) -> str:
        return e.get("lastSignalCreatedAt") or e.get("firstSignalCreatedAt") or ""

    return max(events, key=ts) if events else None


def _rewrite_event(
    event_id: str,
    location_name: str | None,
    level_2_type: str | None,
) -> tuple[EventRewrite | None, list[dict]]:
    """Fetch an event's full signal list and ask Claude for a fresh
    title/description + severity/displacement fallbacks.

    Returns (rewrite, signals) — `signals` is the list we fetched so callers
    can also compute event severity without re-fetching. Either element may
    be empty/None on failure.
    """
    event = graphql.get_event_with_signals(event_id)
    if not event:
        return None, []

    signals = event.get("signals") or []
    if not signals:
        return None, []

    prompt = build_rewrite_prompt(
        location_name=location_name,
        level_2_type=level_2_type,
        signals=signals,
    )

    try:
        result_data = call_claude(
            REWRITE_SYSTEM,
            prompt,
            stage="rewrite",
            prompt_version=REWRITE_PROMPT_VERSION,
            event_id=event_id,
        )
        return EventRewrite.model_validate(result_data), signals
    except Exception as e:
        logger.error(
            "[GROUPING v2] Rewrite failed for event %s: %s",
            event_id, e, exc_info=True,
        )
        return None, signals


def group_signal_v2(
    signal_id: str,
    signal_title: str | None,
    signal_description: str | None,
    signal_timestamp: str | None,
    classification: SignalClassification,
    created_signal: dict[str, Any],
) -> dict | None:
    """District+type grouping. Returns the event dict (created or updated)
    or None on failure.

    `created_signal` is the full createSignal GraphQL result, used to:
      - pick the primary location (origin > general > destination)
      - walk up to admin-2 via ancestorIds
      - detect "signal already linked to an event" (from a prior retried run)
    """

    # ── 0. Short-circuit if the signal is already grouped ──────────────
    # When clear-api returns an existing signal via its idempotent
    # createSignal, the row may already be linked to an event from a
    # previous pipeline run (e.g. the first attempt succeeded at
    # create_event but crashed on the follow-up update_event). Without
    # this guard, a retried task would create a second event and link the
    # same signal to both.
    existing_events = created_signal.get("events") or []
    if existing_events:
        existing = existing_events[0]
        logger.info(
            "[GROUPING v2] Signal %s is already linked to event %s — "
            "returning existing; skipping classification + grouping.",
            signal_id, existing.get("id"),
        )
        return existing

    # ── 1. Classify the signal ─────────────────────────────────────────
    classifier = get_classifier()
    text = " ".join(filter(None, [signal_title, signal_description]))
    pred = classifier.predict(text, top_k=1)
    top = pred["top_k"][0] if pred.get("top_k") else {}
    glide_code: str | None = top.get("id")
    level_1: str | None = top.get("type_level_1")
    level_2: str | None = top.get("type_level_2")
    confidence: float = float(pred.get("confidence") or 0.0)

    logger.info(
        "[GROUPING v2] Signal %s classified: l1=%s l2=%s code=%s confidence=%.3f",
        signal_id, level_1, level_2, glide_code, confidence,
    )

    if not level_2 or not glide_code:
        logger.warning("[GROUPING v2] Classifier produced no usable level_2 — creating isolated event")
        level_2 = level_2 or "other"
        glide_code = glide_code or "ot"

    # ── 2. Resolve admin-2 district ────────────────────────────────────
    admin2_id = resolve_signal_admin2(created_signal)
    if not admin2_id:
        logger.warning(
            "[GROUPING v2] Signal %s: could not resolve an admin-2 district — creating isolated event",
            signal_id,
        )

    # ── 3. Lock on (admin2, level_2) to serialise cache-read-then-create ─
    # Two concurrent workers with signals in the same district+type would
    # otherwise both see no match and both call create_event. The lock gates
    # the critical section; the loser waits, re-reads the cache, and picks up
    # the new event created by the winner.
    lock_key = f"group:v2:{admin2_id or 'none'}:{level_2}"

    # ── Common metadata ────────────────────────────────────────────────
    now_iso = datetime.now(UTC).isoformat()
    ts = signal_timestamp or now_iso

    # Pick the primary location name for the rewrite prompt
    primary = None
    for key in ("originLocation", "generalLocation", "destinationLocation"):
        if created_signal.get(key):
            primary = created_signal[key]
            break
    location_name = primary.get("name") if primary else None

    # If we don't have an admin2, there's nothing to race against — two
    # isolated events for different unknown locations don't conflict.
    if not admin2_id:
        return _match_and_act(
            signal_id=signal_id,
            signal_title=signal_title,
            signal_description=signal_description,
            classification=classification,
            admin2_id=None,
            level_2=level_2,
            glide_code=glide_code,
            ts=ts,
            location_name=location_name,
            primary=primary,
        )

    # ttl_seconds = 30 is enough to cover worst-case Claude rewrite latency.
    # wait_seconds = 20 gives the first worker plenty of room to finish and
    # invalidate the cache before we re-read it.
    with redis_lock(lock_key, ttl_seconds=30, wait_seconds=20) as acquired:
        if not acquired:
            logger.warning(
                "[GROUPING v2] Could not acquire %s within deadline — "
                "proceeding unlocked (duplicate event risk accepted).",
                lock_key,
            )
        return _match_and_act(
            signal_id=signal_id,
            signal_title=signal_title,
            signal_description=signal_description,
            classification=classification,
            admin2_id=admin2_id,
            level_2=level_2,
            glide_code=glide_code,
            ts=ts,
            location_name=location_name,
            primary=primary,
        )


def _match_and_act(
    *,
    signal_id: str,
    signal_title: str | None,
    signal_description: str | None,
    classification: SignalClassification,
    admin2_id: str | None,
    level_2: str,
    glide_code: str,
    ts: str,
    location_name: str | None,
    primary: dict | None,
) -> dict | None:
    """The race-prone section extracted so `group_signal_v2` can wrap it in
    a lock. Reads the active-events cache, picks a match (or creates one),
    then applies the rewrite pass."""

    active = _get_active_events_v2()
    matches: list[dict] = []
    if admin2_id:
        matches = [e for e in active if _event_matches(e, admin2_id, level_2)]

    # ── 4a. ADD to most recent matching event ──────────────────────────
    if matches:
        target = _most_recent(matches)
        if target is None:  # defensive; shouldn't hit given matches non-empty
            target = matches[0]
        target_id = target["id"]

        logger.info(
            "[GROUPING v2] Matched %d events; adding signal %s to most recent %s",
            len(matches), signal_id, target_id,
        )

        # First attach the signal so the rewrite sees the full set
        update_event(target_id, {
            "signalIds": [signal_id],
            "lastSignalCreatedAt": ts,
        })

        # Now rewrite + derive severity + displacement across the full set
        rewrite, signals = _rewrite_event(target_id, location_name, level_2)
        event_severity = _compute_event_severity(
            signals,
            rewrite.severity if rewrite else None,
        )
        pop_displaced = _resolve_population_displaced(
            claude_value=rewrite.population_displaced if rewrite else None,
            admin2_id=admin2_id,
        )

        final_update: dict = {}
        if rewrite:
            final_update["title"] = rewrite.title
            final_update["description"] = rewrite.description
        if event_severity is not None:
            final_update["severity"] = event_severity
            final_update["rank"] = event_severity / 5.0
        if pop_displaced is not None:
            final_update["populationDisplaced"] = str(pop_displaced)

        updated = update_event(target_id, final_update) if final_update else target
        _invalidate_events_cache_v2()
        return updated

    # ── 4b. CREATE new event ───────────────────────────────────────────
    logger.info(
        "[GROUPING v2] No match for signal %s (admin2=%s l2=%s) — creating new event",
        signal_id, admin2_id, level_2,
    )

    try:
        valid_to = (
            datetime.fromisoformat(ts.replace("Z", "+00:00"))
            + timedelta(days=ACTIVE_EVENTS_WINDOW_DAYS)
        ).isoformat()
    except (ValueError, AttributeError):
        valid_to = (datetime.now(UTC) + timedelta(days=ACTIVE_EVENTS_WINDOW_DAYS)).isoformat()

    # Event-level location: the admin-2 district we clustered on IS the
    # event's location. Stored in `locationId` (generalLocation) — the
    # semantically correct slot for "the event is happening here". Falls
    # back to the signal's primary location when we couldn't resolve an
    # admin-2 (shouldn't happen for signals with coords, but safe for
    # text-only signals whose location is still unknown).
    event_location_id = admin2_id or (primary.get("id") if primary else None)

    # Bootstrap title/description from the signal; Claude will polish below.
    boot_title = signal_title or f"{level_2.title()} in {location_name or 'unknown location'}"
    boot_desc = signal_description or classification.summary

    event_input: dict = {
        "signalIds": [signal_id],
        "title": boot_title,
        "description": boot_desc,
        "validFrom": ts,
        "validTo": valid_to,
        "firstSignalCreatedAt": ts,
        "lastSignalCreatedAt": ts,
        "types": [glide_code],
        "rank": 0.0,
        "locationId": event_location_id,
    }

    event = create_event(event_input)
    _invalidate_events_cache_v2()

    # Polish title/description + derive severity + displacement across the
    # event's full signal set (here, just the one we linked).
    rewrite, signals = _rewrite_event(event["id"], location_name, level_2)
    event_severity = _compute_event_severity(
        signals,
        rewrite.severity if rewrite else None,
    )
    pop_displaced = _resolve_population_displaced(
        claude_value=rewrite.population_displaced if rewrite else None,
        admin2_id=admin2_id,
    )

    final_update: dict = {}
    if rewrite:
        final_update["title"] = rewrite.title
        final_update["description"] = rewrite.description
    if event_severity is not None:
        final_update["severity"] = event_severity
        final_update["rank"] = event_severity / 5.0
    if pop_displaced is not None:
        final_update["populationDisplaced"] = str(pop_displaced)

    if final_update:
        update_event(event["id"], final_update)
        event.update({
            "title": final_update.get("title", event.get("title")),
            "description": final_update.get("description", event.get("description")),
        })
        _invalidate_events_cache_v2()

    return event
