"""Location resolution: extract and resolve locations from signal text using Claude."""

import logging
import re

from pydantic import BaseModel

from clear_pipeline.providers.llm import make_llm_provider
from clear_pipeline.providers.clear_api import get_locations, get_locations_by_level

logger = logging.getLogger(__name__)


class _LocationResolution(BaseModel):
    """Structured schema for the location-extraction call (replaces the old
    free-form JSON dict from call_claude)."""

    location_type: str = "general"
    origin_id: str | None = None
    destination_id: str | None = None
    location_id: str | None = None

# Cache location list to avoid repeated API calls
_locations_cache: list[dict] | None = None


def _get_locations() -> list[dict]:
    """Get locations from CLEAR API (cached in memory)."""
    global _locations_cache
    if _locations_cache is None:
        _locations_cache = get_locations()
    return _locations_cache


def invalidate_locations_cache() -> None:
    global _locations_cache, _country_ids_cache
    _locations_cache = None
    _country_ids_cache = None
    _scoped_locations_cache.clear()


# Deepest admin level offered to the LLM when a resolution is scoped to one
# country: country (0), state (1), district (2). L4 rows are mostly
# coordinate-named landmark points ("Point 12.89, 23.47") that would bloat the
# prompt without giving the model anything it can match a place name to.
SCOPED_MAX_LEVEL = 2

# Country name → that country's L0..SCOPED_MAX_LEVEL locations (cached).
_scoped_locations_cache: dict[str, list[dict]] = {}


def _country_scoped_locations(country: str) -> list[dict]:
    """The named country's L0 row plus its descendants down to
    ``SCOPED_MAX_LEVEL``, walked via ``parent`` links. Empty when the country
    isn't a known level-0 location."""
    key = _normalise_country_name(country)
    if key in _scoped_locations_cache:
        return _scoped_locations_cache[key]
    locations = _get_locations()
    by_id = {loc["id"]: loc for loc in locations if loc.get("id")}
    root = next(
        (loc for loc in locations
         if loc.get("level") == 0 and _normalise_country_name(loc.get("name") or "") == key),
        None,
    )
    scoped: list[dict] = []
    if root is not None:
        for loc in locations:
            if (loc.get("level") or 0) > SCOPED_MAX_LEVEL:
                continue
            node, seen = loc, set()
            while node is not None and node["id"] not in seen:
                if node["id"] == root["id"]:
                    scoped.append(loc)
                    break
                seen.add(node["id"])
                node = by_id.get((node.get("parent") or {}).get("id") or "")
    _scoped_locations_cache[key] = scoped
    return scoped


# Normalised admin-0 name → locations.id (cached in memory, like _locations_cache)
_country_ids_cache: dict[str, str] | None = None


def _normalise_country_name(name: str) -> str:
    """Casefold and drop the decorations that differ between a source's country
    label and the backfilled A0 name: a parenthetical long form ("Venezuela
    (Bolivarian Republic of)") and a leading "the"."""
    name = re.sub(r"\([^)]*\)", "", name).strip().casefold()
    return re.sub(r"^the\s+", "", name)


def resolve_country_only_location(name: str | None) -> str | None:
    """Return the level-0 ``locations.id`` when `name` is nothing but a country
    ("Sudan"), else None.

    Dataminr labels an alert it could only place at country level with the bare
    country name and puts the coordinates at that country's centroid. Sending
    those coordinates to clear-api creates an L4 point inside whichever district
    the centroid happens to fall in (Sudan's lands in Sheikan, North Kordofan),
    so callers use this to attach the signal to the country instead."""
    global _country_ids_cache
    if not name or not name.strip():
        return None
    if _country_ids_cache is None:
        _country_ids_cache = {
            _normalise_country_name(loc["name"]): loc["id"]
            for loc in get_locations_by_level(0)
            if loc.get("name") and loc.get("id")
        }
    return _country_ids_cache.get(_normalise_country_name(name))


# Bump whenever SYSTEM_PROMPT below changes (see CLASSIFY_PROMPT_VERSION for rationale).
LOCATION_PROMPT_VERSION = "location-v1"

SYSTEM_PROMPT = """You are a geolocation extraction assistant for a humanitarian crisis monitoring system focused on Sudan.

Your task is to extract location information from signal/alert text and match it to known locations in the database.

IMPORTANT RULES:
1. If the signal describes DISPLACEMENT or MOVEMENT of people (refugees, IDPs, migration, evacuation, fleeing):
   - Set "location_type" to "displacement"
   - Set "origin" to where people are COMING FROM
   - Set "destination" to where people are GOING TO
   - Leave "location" as null

2. For ALL OTHER signals (conflict, disease, flooding, drought, political events, etc.):
   - Set "location_type" to "general"
   - Set "location" to the most specific location mentioned
   - Leave "origin" and "destination" as null

3. Match locations to the provided location list by name. Return the exact location ID from the list.
   - Prefer the most granular match (district > state > country)
   - If a city/district is mentioned, match to that level
   - If only a state is mentioned, match to state level
   - If no specific location in Sudan is mentioned, use the country-level Sudan entry

4. If the signal is NOT about Sudan at all (e.g., about Chad, Ethiopia with no Sudan connection), return all fields as null.

Respond with valid JSON only. No explanations."""


def _build_prompt(
    title: str | None,
    description: str | None,
    dataminr_location_name: str | None,
    locations: list[dict],
) -> str:
    # Build compact location reference (name → id, grouped by level)
    countries = []
    states = []
    districts = []
    for loc in locations:
        entry = f'  "{loc["name"]}": "{loc["id"]}"'
        if loc["level"] == 0:
            countries.append(entry)
        elif loc["level"] == 1:
            states.append(entry)
        else:
            districts.append(entry)

    loc_ref = "KNOWN LOCATIONS:\n"
    loc_ref += "Countries:\n" + "\n".join(countries) + "\n"
    loc_ref += "States:\n" + "\n".join(states) + "\n"
    loc_ref += "Districts:\n" + "\n".join(districts)

    return f"""SIGNAL:
Title: {title or "(none)"}
Description: {description or "(none)"}
Dataminr location: {dataminr_location_name or "(none)"}

{loc_ref}

Extract location info and return JSON:
{{
  "location_type": "displacement" | "general",
  "origin_id": "<location_id or null>",
  "destination_id": "<location_id or null>",
  "location_id": "<location_id or null>"
}}"""


def resolve_signal_location(
    title: str | None,
    description: str | None,
    dataminr_location_name: str | None,
    *,
    country: str | None = None,
) -> dict:
    """
    Use Claude to extract location from signal text and resolve to CLEAR location IDs.

    ``country`` (a level-0 location name, e.g. "Sudan") restricts the candidate
    list to that country's admin areas down to district level — for sources
    known to cover a single country. Omitted, every location is offered.

    Returns dict with keys:
      - location_type: "displacement" | "general"
      - origin_id: str | None
      - destination_id: str | None
      - location_id: str | None
    """
    locations = _country_scoped_locations(country) if country else _get_locations()
    if not locations:
        logger.warning("No locations in CLEAR API — skipping location resolution")
        return {"location_type": "general", "origin_id": None, "destination_id": None, "location_id": None}

    prompt = _build_prompt(title, description, dataminr_location_name, locations)

    try:
        result = make_llm_provider("signal").complete_structured(
            system=SYSTEM_PROMPT, user=prompt, schema=_LocationResolution
        )

        # Validate returned IDs against known locations
        known_ids = {loc["id"] for loc in locations}
        origin_id = result.origin_id
        destination_id = result.destination_id
        location_id = result.location_id

        if origin_id and origin_id not in known_ids:
            logger.warning("Claude returned unknown origin_id: %s", origin_id)
            origin_id = None
        if destination_id and destination_id not in known_ids:
            logger.warning("Claude returned unknown destination_id: %s", destination_id)
            destination_id = None
        if location_id and location_id not in known_ids:
            logger.warning("Claude returned unknown location_id: %s", location_id)
            location_id = None

        return {
            "location_type": result.location_type,
            "origin_id": origin_id,
            "destination_id": destination_id,
            "location_id": location_id,
        }

    except Exception as e:
        logger.error("Location resolution failed: %s", e)
        return {"location_type": "general", "origin_id": None, "destination_id": None, "location_id": None}


def locate_in_country(title: str | None, description: str | None, country: str) -> str | None:
    """Best location id for a signal from a single-country source: the most
    specific admin area (state/district) Claude can match in ``country`` from
    the text, else the country's own level-0 row. None only when ``country``
    isn't a known level-0 location.

    Works on non-English text (the Arabic X posts of ``sudan-war-x``), which
    the regex geoparser can't read. A displacement signal is pinned to its
    origin — where the incident happened — falling back to its destination.
    """
    resolved = resolve_signal_location(title, description, None, country=country)
    location_id = (
        resolved["location_id"] or resolved["origin_id"] or resolved["destination_id"]
    )
    if location_id:
        return location_id
    return resolve_country_only_location(country)
