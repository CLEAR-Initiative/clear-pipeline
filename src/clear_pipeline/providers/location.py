"""Location resolution: extract and resolve locations from signal text using Claude."""

import logging
import re

from pydantic import BaseModel

from clear_pipeline.providers.llm import make_llm_provider
from clear_pipeline.providers.clear_api import (
    find_or_create_landmark_l4,
    get_location_with_geometry,
    get_locations,
    get_locations_by_level,
)
from clear_pipeline.providers.geoparser import geocode_place_name

logger = logging.getLogger(__name__)


class _LocationResolution(BaseModel):
    """Structured schema for the location-extraction call (replaces the old
    free-form JSON dict from call_claude)."""

    location_type: str = "general"
    origin_id: str | None = None
    destination_id: str | None = None
    location_id: str | None = None
    place_name: str | None = None

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
    _area_shapes_cache.clear()


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
LOCATION_PROMPT_VERSION = "location-v2"

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
   - If a city, town, village or neighbourhood is mentioned, return the district that contains it, even when a state name appears first (as a headline, hashtag or dateline). Many districts share their main town's name (Kadugli, Tendalti, Shendi).
   - Use a state only when no place inside it is named
   - If no specific location in Sudan is mentioned, or the places named span several states, use the country-level Sudan entry

4. Set "place_name" to the most specific named place your answer is based on (for displacement, the origin's): a town, village, neighbourhood or named facility, in its usual English spelling (e.g. "Tendalti", "El Fasher", "Um Sufaifa"). Use null when the text names only a state or the country.

5. If the signal is NOT about Sudan at all (e.g., about Chad, Ethiopia with no Sudan connection), return all fields as null.

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
  "location_id": "<location_id or null>",
  "place_name": "<most specific place named, or null>"
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
      - place_name: str | None — the most specific place named in the text
        (town, village, facility), in Latin script, for geocoding
    """
    locations = _country_scoped_locations(country) if country else _get_locations()
    if not locations:
        logger.warning("No locations in CLEAR API — skipping location resolution")
        return {"location_type": "general", "origin_id": None, "destination_id": None,
                "location_id": None, "place_name": None}

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
            "place_name": (result.place_name or "").strip() or None,
        }

    except Exception as e:
        logger.error("Location resolution failed: %s", e)
        return {"location_type": "general", "origin_id": None, "destination_id": None,
                "location_id": None, "place_name": None}


def locate_in_country(
    title: str | None,
    description: str | None,
    country: str,
    *,
    country_code: str | None = None,
) -> str | None:
    """Best location id for a signal from a single-country source: the most
    specific admin area (state/district) Claude can match in ``country`` from
    the text, else the country's own level-0 row. None only when ``country``
    isn't a known level-0 location.

    Works on non-English text (the Arabic X posts of ``sudan-war-x``), which
    the regex geoparser can't read. A displacement signal is pinned to its
    origin — where the incident happened — falling back to its destination.

    With ``country_code`` (ISO2), the place Claude names is also geocoded
    (gazetteer, then LocationIQ) and, when the point falls inside the area
    Claude picked, promoted to an L4 point — which clear-api parents to its
    containing district, so a post Claude could only place in a state still
    reaches district level.
    """
    resolved = resolve_signal_location(title, description, None, country=country)
    matched_id = (
        resolved["location_id"] or resolved["origin_id"] or resolved["destination_id"]
    )
    country_id = resolve_country_only_location(country)
    area_id = matched_id or country_id
    place_name = resolved.get("place_name")
    if (
        country_code
        and place_name
        and area_id
        # Claude chose the country itself: the places span several states, so
        # one town's point would pull the whole report into that district.
        # (No match at all still falls back to the country and is geocoded.)
        and not (matched_id and matched_id == country_id)
        and not _names_country_or_state(place_name, country)
    ):
        point_id = _geocode_within(place_name, area_id, country_code)
        if point_id:
            return point_id
    return area_id


def _name_key(name: str) -> str:
    """Spelling-tolerant key for comparing an English place name with an admin
    area's name: drop a leading article and a trailing "state", fold q/g
    ("Qadarif"/"Gedaref"), then keep consonants only ("Sinnar"/"Sennar")."""
    key = re.sub(r"^(al|el|ad|ed|an|as|at|ar|az|ash)[\s-]+", "", name.casefold().strip())
    key = re.sub(r"\s+state$", "", key).replace("q", "g")
    key = re.sub(r"[^a-z]", "", key)
    return re.sub(r"(.)\1+", r"\1", re.sub(r"[aeiouy]", "", key))


def _names_country_or_state(place_name: str, country: str) -> bool:
    """True when ``place_name`` is the country's or one of its states' own
    names. Many state capitals share their state's name (Khartoum, Gedaref,
    Kassala, Sennar), so geocoding a state name pins a state-wide post to the
    capital's district — or, for "North Kordofan", to whatever village fuzzily
    matches. A post that names only a state stays at the state."""
    key = _name_key(place_name)
    return bool(key) and any(
        _name_key(loc["name"]) == key
        for loc in _country_scoped_locations(country)
        if (loc.get("level") or 0) <= 1
    )

# locations.id → shapely geometry (None when the area has none). Cached.
_area_shapes_cache: dict[str, object | None] = {}


def _area_shape(location_id: str):
    if location_id not in _area_shapes_cache:
        from shapely.geometry import shape

        loc = get_location_with_geometry(location_id) or {}
        geometry = loc.get("geometry")
        _area_shapes_cache[location_id] = shape(geometry) if geometry else None
    return _area_shapes_cache[location_id]


def _geocode_within(place_name: str, area_id: str, country_code: str) -> str | None:
    """Geocode ``place_name`` inside ``country_code`` and promote it to an L4
    point, but only when the point lies inside ``area_id`` — the admin area
    Claude matched. Village names repeat across Sudan; a hit outside the
    area Claude read from the text is the wrong village, so each lookup tier
    is asked for a point inside it, and with none the area wins.
    Best-effort: any failure returns None and the caller keeps the area."""
    try:
        area = _area_shape(area_id)
        if area is None:
            logger.info("[locate] area %s has no geometry — can't confirm %r", area_id, place_name)
            return None

        from shapely.geometry import Point

        # Strictly inside (`contains`, not `covers`): a point on or over the
        # line can be parented to the neighbouring area's district. Passed
        # into the lookups, so a same-named place elsewhere is a miss and the
        # next tier is tried.
        geo = geocode_place_name(
            place_name,
            expected_country_codes={country_code.lower()},
            accept=lambda lat, lng: area.contains(Point(lng, lat)),
        )
        if geo is None:
            logger.info("[locate] %r has no match inside area %s — keeping the area",
                        place_name, area_id)
            return None

        promo = find_or_create_landmark_l4(
            name=place_name, lat=geo.lat, lng=geo.lng, kind=geo.kind,
        )
        if promo.get("abortedReason") or not promo.get("locationId"):
            return None
        logger.info(
            "[locate] %r → L4 %s (reused=%s) inside area %s",
            place_name, promo["locationId"], promo.get("reused"), area_id,
        )
        return promo["locationId"]
    except Exception as exc:  # noqa: BLE001 — geocoding is an enrichment, never a failure
        logger.warning("[locate] geocoding %r failed: %s", place_name, exc)
        return None
