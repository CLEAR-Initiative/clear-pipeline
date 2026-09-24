"""Event provider — grouping (district + type), event-type stats, and admin-2 resolution.

Consolidated from clear-pipeline event / event_grouping_v2 / event_type_stats /
admin_resolver, plus the EventRewrite result model.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TypedDict

import redis
from pydantic import BaseModel

from clear_pipeline.providers import clear_api as graphql
from clear_pipeline.providers.classify import (
    SignalClassification,
    code_to_level2_map,
    code_to_level3_map,
)
from clear_pipeline.providers.clear_api import (
    create_event,
    get_events,
    update_event,
)
from clear_pipeline.providers import insights
from clear_pipeline.providers.llm import make_llm_provider
from clear_pipeline.providers.prompts.rewrite import (
    REWRITE_PROMPT_VERSION,
    SYSTEM_PROMPT as REWRITE_SYSTEM,
)
from clear_pipeline.providers.prompts.rewrite import build_rewrite_prompt
from clear_pipeline.providers.redis_lock import redis_lock
from clear_pipeline.providers.signal import (
    earliest_onset_iso,
    extract_casualties_from_text,
    extract_event_start_from_text,
)
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)


class EventRewrite(BaseModel):
    """Output from the event-rewrite LLM call (text polish + fallbacks)."""

    title: str
    description: str
    severity: int | None = None
    population_displaced: int | None = None
    """Event onset (when the real-world event STARTED) as an ISO-8601 date
    (YYYY-MM-DD), inferred across the event's signals — the LLM fallback for
    `startedAt` when the regex extractor can't parse it. Null when no signal
    indicates a start date."""
    start_date: str | None = None


# ── Event-type stats (ACLED percentiles) ─────────────────────────────────────
_STATS_PATH = Path(__file__).resolve().parent / "data" / "acled_event_type_stats.json"


class _PercentileStats(TypedDict):
    max: float
    median: float
    min: float
    q25: float
    q75: float


class _EventTypeStats(TypedDict, total=False):
    event_count: int
    fatalities: _PercentileStats
    population_1km: _PercentileStats
    population_2km: _PercentileStats
    population_5km: _PercentileStats


def _normalize_key(s: str) -> str:
    """Normalise a level_3 / sub_event_type string for lookup.

    JSON keys are lowercase with " / " around slashes (e.g.
    "abduction / forced disappearance"); ACLED's API returns
    "Abduction/forced disappearance". Normalise both to the JSON form.
    """
    if not s:
        return ""
    lowered = s.strip().lower().replace("/", " / ")
    return " ".join(lowered.split())


def _load_stats() -> dict[str, _EventTypeStats]:
    try:
        with _STATS_PATH.open("r", encoding="utf-8") as f:
            raw: dict[str, _EventTypeStats] = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.error("[EVENT-STATS] Failed to load %s: %s", _STATS_PATH, e)
        return {}

    return {_normalize_key(k): v for k, v in raw.items()}


# Loaded once at module import — small file (~24 entries).
_STATS: dict[str, _EventTypeStats] = _load_stats()


class EventTypeStats(TypedDict):
    casualties: int | None
    population_affected: int | None


def get_stats_for_event_type(event_type: str) -> EventTypeStats:
    """Return (q75 fatalities, median population_1km) for a level_3 / sub-type.

    Both values may be None when the event_type is unknown to the stats file
    (e.g. natural-hazard types — flood, earthquake — currently have no
    entries) or when the stat itself is 0/None. Callers should treat None
    as "no fallback available" and skip the field rather than write 0.
    """
    if not event_type:
        return {"casualties": None, "population_affected": None}

    key = _normalize_key(event_type)
    stats = _STATS.get(key)
    if not stats:
        logger.debug(
            "[EVENT-STATS] No stats for event_type=%r (key=%r)",
            event_type, key,
        )
        return {"casualties": None, "population_affected": None}

    fatalities = stats.get("fatalities") or {}
    pop_1km = stats.get("population_1km") or {}

    q75_fatalities = fatalities.get("q75")
    median_pop = pop_1km.get("median")

    return {
        "casualties": int(q75_fatalities) if q75_fatalities and q75_fatalities > 0 else None,
        "population_affected": int(median_pop) if median_pop and median_pop > 0 else None,
    }

# ── Admin-2 resolution ───────────────────────────────────────────────────────


# location_id → level, cached across the worker process lifetime
_level_cache: dict[str, int] = {}
# location_id → admin-2 location id (or None if unresolvable)
_admin2_cache: dict[str, str | None] = {}


def resolve_admin2(location: dict[str, Any] | None) -> str | None:
    """Return the level-2 district id for a location dict, or None if unresolvable.

    The location dict must come from a GraphQL query that selects
    `id`, `level`, and `ancestorIds`. If a higher-level ancestor needs to be
    checked, we fetch it lazily and cache the result.
    """
    if not location or not location.get("id"):
        return None

    loc_id = location["id"]
    if loc_id in _admin2_cache:
        return _admin2_cache[loc_id]

    level = location.get("level")
    ancestor_ids: list[str] = location.get("ancestorIds") or []

    admin2_id: str | None = None

    if level == 2:
        admin2_id = loc_id
    elif level is not None and level > 2:
        # Walk ancestors looking for the level-2 one
        admin2_id = _find_level2_among(ancestor_ids)
    # level < 2: country/state is too broad for our clustering key; skip
    elif level is not None and level < 2:
        admin2_id = None
    else:
        # No level info — fall back to scanning ancestors
        admin2_id = _find_level2_among(ancestor_ids)

    _level_cache[loc_id] = level or -1
    _admin2_cache[loc_id] = admin2_id
    logger.debug(
        "[ADMIN2] %s (level=%s) → admin2=%s",
        location.get("name") or loc_id, level, admin2_id,
    )
    return admin2_id


def _find_level2_among(ids: list[str]) -> str | None:
    """Fetch unknown-level ancestors and return the id of the level-2 one."""
    if not ids:
        return None

    unknown = [i for i in ids if i not in _level_cache]
    if unknown:
        # Fetch each one — small N, fine for now. If this becomes a hot path,
        # add a bulk `locationsByIds` query.
        for lid in unknown:
            data = graphql.get_location_with_geometry(lid)
            if data:
                _level_cache[lid] = int(data.get("level") or -1)
            else:
                _level_cache[lid] = -1

    for lid in ids:
        if _level_cache.get(lid) == 2:
            return lid
    return None


def pick_primary_location(signal: dict[str, Any]) -> dict[str, Any] | None:
    """From a signal dict (result of createSignal), pick the single most
    specific resolved location to use for clustering. Priority:
    origin > general > destination."""
    for key in ("originLocation", "generalLocation", "destinationLocation"):
        loc = signal.get(key)
        if loc and loc.get("id"):
            return loc
    return None


def resolve_signal_admin2(created_signal: dict[str, Any]) -> str | None:
    """Convenience: pick the primary location off a createSignal result and
    walk it to its admin-2 district id."""
    loc = pick_primary_location(created_signal)
    if not loc:
        return None
    return resolve_admin2(loc)

# ── Grouping v2 (district + type) ────────────────────────────────────────────


_redis = redis.from_url(settings.redis_url, decode_responses=True)

ACTIVE_EVENTS_CACHE_KEY = "events:active:v2"
ACTIVE_EVENTS_TTL = 300  # 5 min
# How long an event stays "active" for incoming signals to attach to. Beyond
# this, a new signal creates a fresh event instead of merging into an old one.
# Shorter window = more distinct events, less risk of unrelated incidents
# getting grouped just because they share district+type.
ACTIVE_EVENTS_WINDOW_DAYS = 7


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


def _resolve_actual_casualties(
    created_signal: dict[str, Any] | None,
    signal_title: str | None,
    signal_description: str | None,
) -> int | None:
    """Pick the best 'actual' casualty count for a signal:

      1. The structured value the source/builder set (signal.casualties on
         the GraphQL result — e.g. ACLED's `fatalities`, or Dataminr's
         build-time regex extraction).
      2. Regex extraction from title + description as a backstop for sources
         whose builders don't run the extraction (GDACS, manual signals).

    Returns None when neither yields a number — the caller is expected to
    fall through to the per-event-type historical lookup.
    """
    if created_signal:
        source_value = created_signal.get("casualties")
        if source_value is not None:
            return source_value
    return extract_casualties_from_text(signal_title, signal_description)


def _stats_for_glide(glide_code: str | None) -> dict:
    """Resolve (q75 fatalities, median pop_1km) for a glide code via its
    level_3 sub-type. Returns {casualties, population_affected} with None
    for either field when the glide code's level_3 isn't in the stats file
    (e.g. natural-hazard types currently have no entries)."""
    if not glide_code:
        return {"casualties": None, "population_affected": None}
    code_l3 = code_to_level3_map()
    level_3 = code_l3.get(glide_code)
    if not level_3:
        return {"casualties": None, "population_affected": None}
    return get_stats_for_event_type(level_3)


def _resolve_signal_stats(
    actual_casualties: int | None,
    actual_population: int | None,
    glide_code: str | None,
) -> dict:
    """Per-signal stats with a 3-tier fallback chain:
      1. Raw-extracted actual from the source (ACLED fatalities, GDACS
         population_affected, Dataminr/manual regex).
      2. Per-event-type historical lookup via the signal's level_3 sub-type
         (q75 fatalities / median pop_1km).
      3. For populationAffected only: settings.default_population_affected
         as a last-resort constant so events always carry some estimate.

    Casualties stays None when both (1) and (2) produce nothing — there's
    no sensible global default for fatalities.
    """
    fallback = _stats_for_glide(glide_code)

    casualties: int | None
    if actual_casualties is not None:
        casualties = actual_casualties
    elif fallback["casualties"] is not None:
        casualties = fallback["casualties"]
    else:
        casualties = None

    population: int
    if actual_population is not None:
        population = actual_population
    elif fallback["population_affected"] is not None:
        population = fallback["population_affected"]
    else:
        population = settings.default_population_affected

    return {"casualties": casualties, "population_affected": population}


def _merge_event_stats(target: dict, resolved: dict) -> dict:
    """Compute the new (casualties, populationAffected) for an event after a
    new signal is attached.

    First-signal semantics is handled at the call site (new event creation
    passes resolved values through directly). For subsequent signals here,
    the rule is:
      - casualties: existing + new_resolved (sum across attached signals)
      - populationAffected: max(existing, new_resolved)

    Returns a dict suitable for splicing into update_event input — only
    populated when at least one of the two stats is non-zero. Note:
    casualties summing is delta-based; if the same celery task retries after
    a partial failure the running total may double-count. The signalEvents
    unique constraint prevents the underlying signal from actually attaching
    twice, but the population fields are absolute writes.
    """
    new_casualties = resolved["casualties"] or 0
    new_pop = resolved["population_affected"] or 0

    existing_casualties = target.get("casualties") or 0
    existing_pop_raw = target.get("populationAffected")
    try:
        existing_pop = int(existing_pop_raw) if existing_pop_raw is not None else 0
    except (TypeError, ValueError):
        existing_pop = 0

    out: dict = {}
    total_casualties = existing_casualties + new_casualties
    if total_casualties > 0:
        out["casualties"] = total_casualties

    max_pop = max(existing_pop, new_pop)
    if max_pop > 0:
        out["populationAffected"] = str(max_pop)
    return out


def _resolve_population_displaced(claude_value: int | None) -> int:
    """Two-tier fallback:
      1. `claude_value` (regex-style extraction across the signal text done
         by the rewrite pass).
      2. `settings.default_population_displaced` (1670 by default).

    The DTM-from-location-metadata tier was previously between these two,
    but DTM data is district-wide and event-agnostic — we'd attribute a
    whole-district displacement total to a single event, inflating the
    estimate. Better to fall straight through to the bounded default when
    the text doesn't tell us a number.
    """
    if claude_value is not None and claude_value > 0:
        return int(claude_value)

    default = settings.default_population_displaced
    logger.info("[GROUPING] populationDisplaced falling back to default: %s", default)
    return default


def _get_active_events() -> list[dict]:
    """Events touched in the last 14 days (matches the archival cutoff so we
    don't cluster into an event that the nightly job is about to archive)."""
    cached = _redis.get(ACTIVE_EVENTS_CACHE_KEY)
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

    _redis.setex(ACTIVE_EVENTS_CACHE_KEY, ACTIVE_EVENTS_TTL, json.dumps(recent))
    logger.info(
        "[GROUPING] Cached %d active events (of %d total, %dd window)",
        len(recent), len(events), ACTIVE_EVENTS_WINDOW_DAYS,
    )
    return recent


def _invalidate_events_cache() -> None:
    _redis.delete(ACTIVE_EVENTS_CACHE_KEY)


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
        with insights.scope(
            stage="signal.rewrite", prompt_version=REWRITE_PROMPT_VERSION, event_id=event_id,
        ):
            result = make_llm_provider("signal").complete_structured(
                system=REWRITE_SYSTEM, user=prompt, schema=EventRewrite
            )
        return result, signals
    except Exception as e:
        logger.error(
            "[GROUPING] Rewrite failed for event %s: %s",
            event_id, e, exc_info=True,
        )
        return None, signals


def group_signal(
    signal_id: str,
    signal_title: str | None,
    signal_description: str | None,
    signal_timestamp: str | None,
    classification: SignalClassification,
    created_signal: dict[str, Any],
    signal_actual_population_affected: int | None = None,
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
            "[GROUPING] Signal %s is already linked to event %s — "
            "returning existing; skipping classification + grouping.",
            signal_id, existing.get("id"),
        )
        return existing

    # ── 1. Reuse the classifier prediction ─────────────────────────────
    # classify_locally already ran the taxonomy classifier on the same
    # title + description (upstream, for the relevance gate) and carried its full
    # result on `classification`. Reuse it instead of running a SECOND inference
    # here — a re-prediction could pick a different glide/level_2 than the one that
    # admitted the signal, and doubles the model cost per signal.
    glide_code: str | None = classification.disaster_types[0] if classification.disaster_types else None
    level_1: str | None = classification.type_level_1
    level_2: str | None = classification.type_level_2
    confidence: float = classification.relevance

    logger.info(
        "[GROUPING] Signal %s classified: l1=%s l2=%s code=%s confidence=%.3f",
        signal_id, level_1, level_2, glide_code, confidence,
    )

    if not level_2 or not glide_code:
        logger.warning("[GROUPING] Classification has no usable level_2 — creating isolated event")
        level_2 = level_2 or "other"
        glide_code = glide_code or "ot"

    # ── 2. Resolve admin-2 district ────────────────────────────────────
    admin2_id = resolve_signal_admin2(created_signal)
    if not admin2_id:
        logger.warning(
            "[GROUPING] Signal %s: could not resolve an admin-2 district — creating isolated event",
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

    # Resolve the per-signal stats once. Casualties priority:
    #   1. Source-shipped structured field (signal.casualties — ACLED's
    #      `fatalities`, or whatever the signal-builder already extracted).
    #   2. Text-extraction from title + description (covers GDACS and
    #      manual-signal paths whose builders don't run the regex).
    #   3. Per-event-type historical lookup (q75 fatalities for the level_3
    #      sub-type) via `_resolve_signal_stats` below.
    # The text-extraction tier closes the gap where GDACS / manual signals
    # used to skip straight to the historical default even when the title
    # plainly said "23 killed".
    actual_casualties = _resolve_actual_casualties(
        created_signal, signal_title, signal_description,
    )
    resolved_stats = _resolve_signal_stats(
        actual_casualties=actual_casualties,
        actual_population=signal_actual_population_affected,
        glide_code=glide_code,
    )

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
            resolved_stats=resolved_stats,
        )

    # The critical section runs create/update_event (up to 3×60s) + the rewrite
    # LLM call (90s × tenacity retries) — several minutes worst case. ttl_seconds
    # must exceed that or the lock expires mid-section and a concurrent worker
    # creates a duplicate event for the same (admin2, level_2) — the exact race the
    # lock prevents. 360s matches the per-signal lock TTL and the drain's step
    # budget. wait_seconds = 20 still lets a fast first worker finish + invalidate
    # the cache before the next re-reads it.
    with redis_lock(lock_key, ttl_seconds=360, wait_seconds=20) as acquired:
        if not acquired:
            logger.warning(
                "[GROUPING] Could not acquire %s within deadline — "
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
            resolved_stats=resolved_stats,
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
    resolved_stats: dict,
) -> dict | None:
    """The race-prone section extracted so `group_signal` can wrap it in
    a lock. Reads the active-events cache, picks a match (or creates one),
    then applies the rewrite pass."""

    active = _get_active_events()
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
            "[GROUPING] Matched %d events; adding signal %s to most recent %s",
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
        )
        # Subsequent-signal stats: add casualties to the running total, take
        # max() for populationAffected. Per-signal values prefer raw-extracted
        # actuals (ACLED fatalities, GDACS population, Dataminr regex) and
        # fall back to the per-event-type stats lookup keyed off the signal's
        # glide. Skipped entirely when neither source produced a value.
        merged_stats = _merge_event_stats(target, resolved_stats)

        final_update: dict = {}
        if rewrite:
            final_update["title"] = rewrite.title
            final_update["description"] = rewrite.description
        if event_severity is not None:
            final_update["severity"] = event_severity
            final_update["rank"] = event_severity / 5.0
        if pop_displaced is not None:
            final_update["populationDisplaced"] = str(pop_displaced)
        final_update.update(merged_stats)
        # Onset: keep the EARLIEST across the event's signals — the new signal's
        # parsed onset, the LLM's (it saw the full set), and the event's current
        # startedAt. Only write when it moves the value earlier.
        new_onset = extract_event_start_from_text(
            signal_title, signal_description, classification.summary, reference_ts=ts,
        )
        onset = earliest_onset_iso(
            target.get("startedAt"), new_onset, rewrite.start_date if rewrite else None,
        )
        if onset is not None and onset != target.get("startedAt"):
            final_update["startedAt"] = onset

        updated = update_event(target_id, final_update) if final_update else target
        _invalidate_events_cache()
        return updated

    # ── 4b. CREATE new event ───────────────────────────────────────────
    logger.info(
        "[GROUPING] No match for signal %s (admin2=%s l2=%s) — creating new event",
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

    # First-signal stats: casualties + populationAffected come from the
    # already-resolved (actual-or-fallback) values for this signal. Either
    # may be None for non-conflict glides whose level_3 isn't in the lookup
    # AND whose source didn't ship a structured value — in which case we
    # leave the field unset.
    # Event onset (startedAt): the real-world start parsed from the signal text,
    # anchored to this signal's collection time so relative phrases resolve. The
    # LLM rewrite below is the fallback when the regex can't parse it.
    started_at = extract_event_start_from_text(
        signal_title, signal_description, classification.summary, reference_ts=ts,
    )

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
    if started_at is not None:
        event_input["startedAt"] = started_at
    if resolved_stats["casualties"] is not None:
        event_input["casualties"] = resolved_stats["casualties"]
    if resolved_stats["population_affected"] is not None:
        event_input["populationAffected"] = str(resolved_stats["population_affected"])

    event = create_event(event_input)
    _invalidate_events_cache()

    # Polish title/description + derive severity + displacement across the
    # event's full signal set (here, just the one we linked).
    rewrite, signals = _rewrite_event(event["id"], location_name, level_2)
    event_severity = _compute_event_severity(
        signals,
        rewrite.severity if rewrite else None,
    )
    pop_displaced = _resolve_population_displaced(
        claude_value=rewrite.population_displaced if rewrite else None,
    )

    # casualties + populationAffected were already set at create_event() time
    # from this first signal's glide-derived stats. They're maintained via
    # _merge_event_stats() in the update branch as more signals attach.
    final_update: dict = {}
    if rewrite:
        final_update["title"] = rewrite.title
        final_update["description"] = rewrite.description
    if event_severity is not None:
        final_update["severity"] = event_severity
        final_update["rank"] = event_severity / 5.0
    if pop_displaced is not None:
        final_update["populationDisplaced"] = str(pop_displaced)
    # Onset fallback: if the regex couldn't parse a start date, take the LLM's
    # (it saw the full signal text). Keep the earliest of the two either way.
    onset = earliest_onset_iso(started_at, rewrite.start_date if rewrite else None)
    if onset is not None and onset != started_at:
        final_update["startedAt"] = onset

    if final_update:
        update_event(event["id"], final_update)
        event.update({
            "title": final_update.get("title", event.get("title")),
            "description": final_update.get("description", event.get("description")),
        })
        _invalidate_events_cache()

    return event

