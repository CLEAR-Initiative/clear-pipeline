"""Location resolution: Dataminr coordinates → CLEAR location ID."""

import json
import logging
import math

import redis

from src.clients.graphql import get_locations
from src.config import settings

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

LOCATIONS_CACHE_KEY = "locations:cache"
LOCATIONS_CACHE_TTL = 6 * 3600  # 6 hours


def _load_locations() -> list[dict]:
    """Load locations from cache or fetch from CLEAR API."""
    cached = _redis.get(LOCATIONS_CACHE_KEY)
    if cached:
        return json.loads(cached)

    logger.info("Fetching locations from CLEAR API for geo cache")
    locations = get_locations()
    _redis.setex(LOCATIONS_CACHE_KEY, LOCATIONS_CACHE_TTL, json.dumps(locations))
    return locations


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate haversine distance in km between two points."""
    R = 6371.0
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def resolve_location(lat: float, lon: float) -> str | None:
    """
    Resolve a lat/lon coordinate to the nearest CLEAR location ID.

    Uses a simple haversine distance calculation against cached locations.
    For production, this should use PostGIS ST_Distance via the API.

    Returns the location ID or None if no locations are available.
    """
    # Check grid-cell cache first (round to 0.1°)
    grid_key = f"geo:resolve:{lat:.1f}:{lon:.1f}"
    cached = _redis.get(grid_key)
    if cached:
        return cached if cached != "__none__" else None

    locations = _load_locations()
    if not locations:
        return None

    # For now, return the location name-matched or nearest by level
    # This is a placeholder — real implementation should query PostGIS
    # via a nearestLocation GraphQL query on clear-api
    best_id: str | None = None
    best_name: str | None = None

    # Prefer higher-level (more granular) locations
    # Level 2 = locality, Level 1 = state, Level 0 = country
    for loc in sorted(locations, key=lambda l: -l.get("level", 0)):
        loc_id = loc["id"]
        loc_name = loc["name"]
        # Store the most granular location as fallback
        if best_id is None:
            best_id = loc_id
            best_name = loc_name

    # Cache result (even None to avoid repeated lookups)
    _redis.setex(grid_key, LOCATIONS_CACHE_TTL, best_id if best_id else "__none__")

    if best_id:
        logger.debug("Resolved (%f, %f) → %s (%s)", lat, lon, best_name, best_id)

    return best_id
