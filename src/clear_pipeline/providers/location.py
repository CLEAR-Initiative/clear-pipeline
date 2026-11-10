"""Location resolution: extract and resolve locations from signal text using Claude."""

import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from pydantic import BaseModel

import httpx

from clear_pipeline.providers.llm import LLMUnavailableError, is_llm_unavailable, make_llm_provider
from clear_pipeline.providers.clear_api import (
    ClearApiError,
    GraphQLErrors,
    find_or_create_landmark_l4,
    get_location_with_geometry,
    get_locations,
    get_locations_by_level,
)
from clear_pipeline.providers.geoparser import (
    GeocodeTrace,
    arabic_lookup_applies,
    gazetteer_knows,
    geocode_place_name,
    latin_name_key,
)

logger = logging.getLogger(__name__)


# ── Geocode misses (the unresolved place-name queue) ─────────────────────────

@dataclass(frozen=True)
class GeocodeMiss:
    """A place name that `_geocode_within` could not place inside its area."""

    name: str
    name_ar: str | None
    country_code: str
    area_id: str
    #: "NO_MATCH": no candidate. "OUTSIDE_AREA": a candidate exists, but it
    #: lies outside the area.
    reason: str


# The open collector of the current call, or None. The locator knows nothing
# about signals: a caller that owns a signal id (the drain) opens a collector
# around `locate` and records what it finds. With no collector (a dry run, an
# evaluation, a test) a miss is only logged, as before.
_geocode_misses: ContextVar[list[GeocodeMiss] | None] = ContextVar("geocode_misses", default=None)


@contextmanager
def collect_geocode_misses() -> Iterator[list[GeocodeMiss]]:
    """Collect the geocode misses of the locate calls made inside the block."""
    misses: list[GeocodeMiss] = []
    token = _geocode_misses.set(misses)
    try:
        yield misses
    finally:
        _geocode_misses.reset(token)


def _note_miss(name: str, name_ar: str | None, country_code: str, area_id: str, reason: str) -> None:
    misses = _geocode_misses.get()
    if misses is not None:
        misses.append(GeocodeMiss(name, name_ar, country_code.upper(), area_id, reason))


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


class LocationsUnavailableError(RuntimeError):
    """clear-api couldn't serve the locations a signal is matched against. Like
    LLMUnavailableError, not a fault of the signal: a caller defers it rather
    than place it at country level, or nowhere, for good."""


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
LOCATION_PROMPT_VERSION = "location-v4"

_SYSTEM_PROMPT_TEMPLATE = """You are a geolocation extraction assistant for a humanitarian crisis monitoring system focused on {country}.

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
   - If a city, town, village, neighbourhood or named facility is mentioned, return the district that contains it, even when a state name appears first (as a headline, hashtag or dateline). Many districts share their main town's name (Kadugli, Tendalti, Shendi).
   - An incident at one site whose consequences reach several states (a strike on a power station that cuts electricity in four states) belongs to the site's district, not the country
   - Use a state only when no place inside it is named
   - If no specific location in {country} is mentioned, or the places named span several states, use the country-level {country} entry

4. Set "place_name" to the most specific named place your answer is based on (for displacement, the origin's): a town, village, neighbourhood or named facility, in its usual English spelling (e.g. "Tendalti", "El Fasher", "Um Sufaifa"). Use null when the text names only a state or the country.

5. If the signal is NOT about {country} at all (e.g., only about a neighbouring country, with no {country} connection), return all fields as null.

Respond with valid JSON only. No explanations."""


def system_prompt(country: str) -> str:
    """The location prompt for signals from ``country``: a non-Sudan source under
    the Sudan prompt would be told to drop everything (rule 5)."""
    return _SYSTEM_PROMPT_TEMPLATE.format(country=country)


# Unscoped resolutions (Dataminr) keep the Sudan prompt.
SYSTEM_PROMPT = system_prompt("Sudan")


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


def _signal_llm():
    """The `signal` role's provider; a missing or invalid config is no usable LLM."""
    try:
        return make_llm_provider("signal")
    except (RuntimeError, ValueError) as exc:
        raise LLMUnavailableError(f"signal LLM not configured: {exc}") from exc


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
    llm = _signal_llm()

    try:
        result = llm.complete_structured(
            system=system_prompt(country) if country else SYSTEM_PROMPT,
            user=prompt,
            schema=_LocationResolution,
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
        if is_llm_unavailable(e):
            # Every later signal would fail the same way: fail instead of pinning them all to the fallback.
            raise LLMUnavailableError(f"Location resolution: {e}") from e
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
    reaches district level. When Claude answers the country itself, nothing is
    geocoded: the post describes several places, and one place name (even a
    facility's) says nothing about the others.

    When Claude placed the post in a state, every place the post names is
    tried, not only the one the answer is based on: that one first, then the
    others (`named_places`, a separate model call, made only when the first
    gives no point). The first name that geocodes inside the state places the
    signal. Each name that does not is a geocode miss (see
    `collect_geocode_misses`). Elsewhere only the answer's own name is tried:
    with no area matched there is no area to check a name against (see
    `_tries_other_names`).
    """
    try:
        resolved = resolve_signal_location(title, description, None, country=country)
        country_id = resolve_country_only_location(country)
    except (GraphQLErrors, ClearApiError, httpx.HTTPError) as exc:
        # The gazetteer is unreachable: every later signal would fail the same way.
        raise LocationsUnavailableError(f"locations for {country!r}: {exc}") from exc
    matched_id = (
        resolved["location_id"] or resolved["origin_id"] or resolved["destination_id"]
    )
    area_id = matched_id or country_id
    place_name = resolved.get("place_name")
    if (
        country_code
        and area_id
        # Claude chose the country itself: the places span several states, so
        # one place's point would pull the whole report into its district. That
        # holds for a facility too: a post about strikes on Nyala Airport and
        # Merowe Dam names one of them as place_name and describes both states.
        # (No match at all still falls back to the country and is geocoded.)
        and not (matched_id and matched_id == country_id)
    ):
        other_names = _tries_other_names(matched_id, country)
        for name in _places_to_geocode(place_name, title, description, country, others=other_names):
            point_id = _geocode_within(
                name, area_id, country_code, text=description or title or ""
            )
            if point_id:
                return point_id
    return area_id


# The most names tried for one post: the answer's own, plus these.
MAX_OTHER_PLACE_NAMES = 4


def _tries_other_names(matched_id: str | None, country: str) -> bool:
    """Whether the other places a post names are tried after the answer's own:
    only when Claude placed the post in a state.

    - A state is where another name helps (it can take the post to district
      level, which grouping needs) and where it is safe (a name must geocode
      inside the state, so a same-named village elsewhere, or a name the model
      spelled like another town, is refused).
    - With no area matched, the area is the whole country: nothing refuses a
      wrong name. The 20–27 Oct dry run pinned a post about a camp in Chad to
      a Sudanese village called Chad, and a post about الضعين (Ed Daein), read
      as "Damazin", to Ed Damazin, 900 km away.
    - A district is already as fine as grouping needs. A less specific name
      (the district's own town) would only move the pin to a town centre the
      post is not about."""
    if not matched_id:
        return False
    return any(
        loc["id"] == matched_id and (loc.get("level") or 0) == 1
        for loc in _country_scoped_locations(country)
    )


def _places_to_geocode(
    place_name: str | None,
    title: str | None,
    description: str | None,
    country: str,
    *,
    others: bool = True,
):
    """The place names to geocode for a post, in order: the one the location
    answer is based on, then (with ``others``) the other places the post
    names. The others cost a model call, so they are asked for only when the
    caller comes back for more: the first name gave no point, or there was
    none. The country's and the states' own names are left out (see
    `_names_country_or_state`)."""
    tried: set[str] = set()
    if place_name and not _names_country_or_state(place_name, country):
        tried.add(latin_name_key(place_name))
        yield place_name
    if not others:
        return
    tried_others = 0
    for name in named_places(title, description, country):
        key = latin_name_key(name)
        if not key or key in tried or _names_country_or_state(name, country):
            continue
        if _names_region_or_feature(name, country):
            continue
        tried.add(key)
        yield name
        tried_others += 1
        if tried_others >= MAX_OTHER_PLACE_NAMES:
            return


class _PlaceNames(BaseModel):
    place_names: list[str] = []


# Bump whenever PLACE_NAMES_SYSTEM changes (see LOCATION_PROMPT_VERSION).
PLACE_NAMES_PROMPT_VERSION = "place-names-v1"

# A separate call, like the Arabic spelling: a new field on the location
# prompt made Haiku return all-null answers for about half of the Arabic posts
# (20–27 Oct dry run), so that prompt is left exactly as it is.
PLACE_NAMES_SYSTEM = """You list the places where the events in a post about {country} happened. Return every specific place in {country} that the post names as a site of what it reports: towns, villages, neighbourhoods, camps and named facilities (a hospital, a dam, a power station, an airport, a bridge, a market, a refinery). Give each in its usual English spelling, most specific first. Do not list {country} itself, its states or regions, other countries or places in them, rivers, roads, plains, mountains or deserts, or places that are only mentioned in passing (where a speaker is from, where a statement was made). Return an empty list when the post names no such place.

Respond with valid JSON only: {{"place_names": ["<name>", ...]}}"""


def named_places(title: str | None, description: str | None, country: str) -> list[str]:
    """Every specific place a post names as a site of what it reports, in
    English spelling, most specific first. Empty when it names none, for an
    empty post, and on any failure but an LLM outage: this is an enrichment,
    and the area the location answer chose stands without it."""
    text = "\n".join(part.strip() for part in (title, description) if part and part.strip())
    if not text:
        return []
    llm = _signal_llm()
    try:
        result = llm.complete_structured(
            system=PLACE_NAMES_SYSTEM.format(country=country),
            user=f"POST:\n{text}",
            schema=_PlaceNames,
        )
    except Exception as exc:  # noqa: BLE001 — an enrichment; the area stands
        if is_llm_unavailable(exc):
            raise LLMUnavailableError(f"Place names: {exc}") from exc
        logger.warning("[locate] listing the places of a post failed: %s", exc)
        return []
    names: list[str] = []
    for name in result.place_names or []:
        cleaned = (name or "").strip() if isinstance(name, str) else ""
        if cleaned and cleaned not in names:
            names.append(cleaned)
    return names


class _ArabicSpelling(BaseModel):
    place_name_ar: str | None = None


# Bump whenever ARABIC_SPELLING_SYSTEM changes (see LOCATION_PROMPT_VERSION).
ARABIC_SPELLING_PROMPT_VERSION = "arabic-spelling-v1"

# A separate, small call rather than another field on the location prompt:
# adding "place_name_ar" there made Haiku return all-null answers for about
# half of the Arabic posts in the 20–27 Oct dry run.
ARABIC_SPELLING_SYSTEM = """You copy place names out of Arabic text. Given a post and the English name of a place it mentions, return that place's name exactly as the post writes it in Arabic script, without attached prefixes (ب، و، ل، ف، ك). Return null if the post does not write it in Arabic.

Respond with valid JSON only: {"place_name_ar": "<name or null>"}"""

_ARABIC_LETTER = re.compile(r"[\u0621-\u064A]")


def arabic_spelling(text: str, place_name: str) -> str | None:
    """The post's own Arabic spelling of ``place_name``, for an exact match on
    GeoNames' Arabic names, which skips transliteration. None for posts
    without Arabic, on any failure, or when the answer isn't in the text — a
    spelling the model made up must not reach the gazetteer."""
    if not text or not _ARABIC_LETTER.search(text):
        return None
    llm = _signal_llm()
    try:
        result = llm.complete_structured(
            system=ARABIC_SPELLING_SYSTEM,
            user=f"POST:\n{text}\n\nENGLISH PLACE NAME: {place_name}",
            schema=_ArabicSpelling,
        )
        name = (result.place_name_ar or "").strip()
    except Exception as exc:  # noqa: BLE001 — an enrichment; the Latin path follows
        if is_llm_unavailable(exc):
            raise LLMUnavailableError(f"Arabic spelling: {exc}") from exc
        logger.warning("[locate] Arabic spelling of %r failed: %s", place_name, exc)
        return None
    if not name or not _ARABIC_LETTER.search(name) or name not in text:
        return None
    return name


def _names_country_or_state(place_name: str, country: str) -> bool:
    """True when ``place_name`` is the country's or one of its states' own
    names. Many state capitals share their state's name (Khartoum, Gedaref,
    Kassala, Sennar), so geocoding a state name pins a state-wide post to the
    capital's district — or, for "North Kordofan", to whatever village fuzzily
    matches. A post that names only a state stays at the state."""
    key = latin_name_key(place_name)
    return bool(key) and any(
        latin_name_key(loc["name"]) == key
        for loc in _country_scoped_locations(country)
        if (loc.get("level") or 0) <= 1
    )

# Words that turn a state's name into a region phrase ("South Blue Nile",
# "Blue Nile axis"), or stand alone for one ("the east").
_REGION_WORDS = frozenset({
    "north", "south", "east", "west", "central", "northern", "southern", "eastern", "western",
    "axis", "region", "state", "area", "province", "front", "sector", "the", "of",
})
# A river, a plain or a road is not a place a point can stand for.
_FEATURE_WORDS = frozenset({
    "river", "plain", "plains", "desert", "mountain", "mountains", "road", "highway", "border",
})


def _region_core(name: str) -> str:
    """``name`` without its region words, as a spelling key: "South Blue
    Nile" and "Blue Nile axis" both give the key of "Blue Nile"."""
    words = [w for w in re.split(r"[^a-z]+", name.casefold()) if w and w not in _REGION_WORDS]
    return latin_name_key(" ".join(words))


def _names_region_or_feature(place_name: str, country: str) -> bool:
    """True when ``place_name`` is a region phrase or a natural feature, not a
    place: a state's name with region words ("South Blue Nile", "Blue Nile
    axis"), the part that states share ("Kordofan", "Darfur"), or a river, a
    plain, a road. The gazetteer answers such a name with whatever village
    fuzzily matches inside the state. Checked for the other names a post
    gives, which a model lists more loosely than the one its answer is
    based on."""
    words = {w for w in re.split(r"[^a-z]+", place_name.casefold()) if w}
    if words & _FEATURE_WORDS:
        return True
    core = _region_core(place_name)
    if not core:
        return True
    return any(
        _region_core(loc["name"]) == core
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


def _geocode_within(
    place_name: str, area_id: str, country_code: str, *, text: str = ""
) -> str | None:
    """Geocode ``place_name`` inside ``country_code`` and promote it to an L4
    point, but only when the point lies inside ``area_id`` — the admin area
    Claude matched. Village names repeat across Sudan; a hit outside the
    area Claude read from the text is the wrong village, so each lookup tier
    is asked for a point inside it, and with none the area wins.
    Best-effort: any failure returns None and the caller keeps the area,
    except an LLM outage, which the caller defers on.

    A place-alias hit (clear-api ADR-0019) already is a CLEAR location: it is
    returned as it is, after the same area check, with no L4 promote.

    A miss goes to the open collector (`collect_geocode_misses`): every tier
    found nothing inside the area, or the L4 promote gave no location. Not a
    miss: an area with no geometry, and a lookup that failed (an outage
    proves nothing about the name)."""
    try:
        area = _area_shape(area_id)
        if area is None:
            logger.info("[locate] area %s has no geometry — can't confirm %r", area_id, place_name)
            return None

        from shapely.geometry import Point

        # The post's Arabic spelling costs a model call, so ask only once the
        # area is usable and the Arabic lookup would actually run.
        arabic_name = arabic_spelling(text, place_name) if arabic_lookup_applies(place_name) else None
        # Strictly inside (`contains`, not `covers`): a point on or over the
        # line can be parented to the neighbouring area's district. Passed
        # into the lookups, so a same-named place elsewhere is a miss and the
        # next tier is tried.
        trace = GeocodeTrace()
        geo = geocode_place_name(
            place_name,
            expected_country_codes={country_code.lower()},
            accept=lambda lat, lng: area.contains(Point(lng, lat)),
            arabic_name=arabic_name,
            within_location_id=area_id,
            trace=trace,
        )
        if geo is None:
            logger.info("[locate] %r has no match inside area %s — keeping the area",
                        place_name, area_id)
            # Only for an open collector: a dry run or an evaluation records
            # nothing, so it must not pay for the extra lookup below.
            if not trace.failed and _geocode_misses.get() is not None:
                # The gazetteer filters by area on the server, so ask it once
                # more whether the name exists elsewhere in the country.
                outside = trace.outside or gazetteer_knows(
                    place_name, country_code, arabic_name=arabic_name
                )
                _note_miss(place_name, arabic_name, country_code, area_id,
                           "OUTSIDE_AREA" if outside else "NO_MATCH")
            return None

        if geo.location_id:
            # A place alias: a person already mapped this spelling to a CLEAR
            # location, and its point passed the area check. Use it as it is;
            # promoting the point would create a second landmark beside it.
            logger.info("[locate] %r → alias location %s inside area %s",
                        place_name, geo.location_id, area_id)
            return geo.location_id

        promo = find_or_create_landmark_l4(
            name=place_name, lat=geo.lat, lng=geo.lng, kind=geo.kind,
        )
        if promo.get("abortedReason") or not promo.get("locationId"):
            # A point was found inside the area, but it gave no location.
            _note_miss(place_name, arabic_name, country_code, area_id, "NO_MATCH")
            return None
        logger.info(
            "[locate] %r → L4 %s (reused=%s) inside area %s",
            place_name, promo["locationId"], promo.get("reused"), area_id,
        )
        return promo["locationId"]
    except LLMUnavailableError:
        # An outage isn't a miss: keeping the area would be saved and never retried.
        raise
    except Exception as exc:  # noqa: BLE001 — geocoding is an enrichment, never a failure
        logger.warning("[locate] geocoding %r failed: %s", place_name, exc)
        return None
