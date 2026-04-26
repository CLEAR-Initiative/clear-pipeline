"""
Resolve any CLEAR location to its admin-level-2 (district) ancestor.

The new grouping algorithm keys events on (admin_2_district_id, level_2_type).
Signals arrive with locations at various levels:
  - level 4 (point) — from signal lat/lng
  - level 0/1/2 — from Claude text extraction

This module walks up via `ancestorIds` to find the level-2 district, using
a process-wide cache to avoid re-fetching the same ancestor chains.
"""

from __future__ import annotations

import logging
from typing import Any

from src.clients import graphql

logger = logging.getLogger(__name__)

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
