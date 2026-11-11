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
    exact_gazetteer_match,
    gazetteer_knows,
    gazetteer_names_area,
    gazetteer_names_town,
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
LOCATION_PROMPT_VERSION = "location-v5"

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
   - If a city, town, village, neighbourhood or named facility is mentioned, return the district that contains it, even when a state name appears first (as a headline, hashtag or dateline). Many districts share their main town's name.
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

        origin_id = _known_id("origin_id", origin_id, known_ids, locations)
        destination_id = _known_id("destination_id", destination_id, known_ids, locations)
        location_id = _known_id("location_id", location_id, known_ids, locations)

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


def _known_id(field: str, value: str | None, known_ids: set[str], locations: list[dict]) -> str | None:
    """``value`` when it is an id from the list. A model sometimes answers
    with a location's name where the prompt asks for its id ("Tendalti",
    "Al Khiwai"): that name is mapped to its id, by exact match with case
    ignored, among countries, states and districts. When a state and a
    district share the name (Khartoum, Kassala), the state is taken: a
    district has to be confirmed by a named place anyway (see
    `locate_in_country`). Anything else is dropped, as before."""
    if not value or value in known_ids:
        return value or None
    key = value.strip().casefold()
    named = [
        loc for loc in locations
        if (loc.get("level") or 0) <= SCOPED_MAX_LEVEL and (loc.get("name") or "").strip().casefold() == key
    ]
    if named:
        top_level = min((loc.get("level") or 0) for loc in named)
        top = [loc for loc in named if (loc.get("level") or 0) == top_level]
        if len(top) == 1:
            logger.info("Claude returned the name %r as %s: mapped to %s", value, field, top[0]["id"])
            return top[0]["id"]
    logger.warning("Claude returned unknown %s: %s", field, value)
    return None


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
    text = description or title or ""
    arabic_names: dict[str, str | None] = {}
    district = _scoped_location(matched_id, country, level=2)
    unconfirmed_district: dict | None = None
    if district is not None:
        # The model often answers with a district the post does not name.
        # Grouping keys on the district, so a guessed one splits an incident.
        # Keep it only when code confirms it.
        confirmed = _confirm_district(
            district, place_name, country, country_code, text=text, arabic_names=arabic_names
        )
        if confirmed:
            return confirmed
        state_id = (district.get("parent") or {}).get("id")
        if not state_id:
            return matched_id
        logger.info(
            "[locate] district %r is not confirmed by place %r: falling back to its state",
            district.get("name"), place_name,
        )
        matched_id = area_id = state_id
        unconfirmed_district = district
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
        other_names = (
            # A displacement report is pinned to its origin, and the answer's
            # own name is the origin's. The other names of the post include
            # where people fled to, which can lie in the same state.
            resolved.get("location_type") != "displacement"
            and _tries_other_names(matched_id, country)
            # Without the state's outline no name can be confirmed inside it.
            and _area_has_geometry(area_id)
        )
        own_name_tried = False
        for name in _places_to_geocode(
            place_name, title, description, country, others=other_names, country_code=country_code
        ):
            own_name_tried = own_name_tried or name == place_name
            point_id = _geocode_within(
                name, area_id, country_code, text=text, arabic_names=arabic_names
            )
            if point_id:
                return point_id
        # No place of the post gave a point inside the state. A district the
        # model chose is still kept when the post itself names it: "محلية
        # الخوي" with a village the gazetteer does not hold is a post about
        # Al Khiwai. Asked only now: a point in the state is the better answer.
        # The whole post is read, title and description: a source can name
        # the district in the title only.
        whole_post = "\n".join(part.strip() for part in (title, description) if part and part.strip())
        if unconfirmed_district is not None and _post_names_district(
            unconfirmed_district, whole_post, country, country_code, arabic_names
        ):
            logger.info("[locate] the post names district %r: keeping it", unconfirmed_district.get("name"))
            return unconfirmed_district["id"]
        # No name of the post is inside the area. An exact match outside it is
        # accepted only for a state the model chose (never for the country),
        # and only for the place the answer is based on: the other names of a
        # post are listed more loosely (see `_exact_match_elsewhere`).
        if (
            own_name_tried
            and place_name
            and _scoped_location(matched_id, country, level=1)
            # With no outline nothing was looked up inside the area either.
            and _area_has_geometry(area_id)
        ):
            point_id = _place_elsewhere(
                place_name, area_id, country, country_code, text=text, arabic_names=arabic_names
            )
            if point_id:
                return point_id
    return area_id


def _scoped_location(location_id: str | None, country: str, *, level: int) -> dict | None:
    """The country's location with this id, when it is at ``level``."""
    if not location_id:
        return None
    return next(
        (loc for loc in _country_scoped_locations(country)
         if loc["id"] == location_id and (loc.get("level") or 0) == level),
        None,
    )


def _confirm_district(
    district: dict,
    place_name: str | None,
    country: str,
    country_code: str | None,
    *,
    text: str,
    arabic_names: dict[str, str | None],
) -> str | None:
    """The location to store for a district the model chose, or None when
    nothing in code confirms the district.

    Two things confirm it:

    - The place the answer names geocodes inside the district. The point is
      returned (an L4 inside the district, or a place alias's location).
    - The place the answer names is the district's own name ("Kadugli" for
      Kadugli district; `latin_name_key` forgives the spelling). The district
      is returned. An Arabic spelling reaches the district on the first path:
      the gazetteer matches the post's own spelling.

    The 3–10 Nov posts stored in Tendalti, Kadugli and Shendi named Kazgeil,
    Wad Madani, El Obeid, Kutum: places the model's district does not hold.
    A district with no place name has nothing to check and is not kept.

    (A district that the post itself names is kept too, but only after the
    post's places were tried inside the state: see `locate_in_country`.)"""
    if not place_name:
        return None
    own_name = latin_name_key(place_name) == latin_name_key(district.get("name") or "")
    if country_code and _names_a_place(place_name, country, country_code):
        outer = _geocode_misses.get()
        if outer is None:
            point_id = _geocode_within(
                place_name, district["id"], country_code, text=text, arabic_names=arabic_names
            )
        else:
            # A miss inside a district that is then dropped is not a miss:
            # the name is tried again inside the state, which records its own.
            with collect_geocode_misses() as held:
                point_id = _geocode_within(
                    place_name, district["id"], country_code, text=text, arabic_names=arabic_names
                )
            if own_name and not point_id:
                outer.extend(held)
        if point_id:
            return point_id
    return district["id"] if own_name else None


# Words in a district's name that say what kind of area it is, or which state
# it is in: "Madeinat Kassala" (Kassala town), "Reifi Aroma" (rural Aroma),
# "At Tadamon - SK". A post writes the name without them.
_DISTRICT_KIND_WORDS = frozenset({"madeinat", "reifi"})


def _district_core_name(name: str) -> str:
    """A district's name as a post writes it: no state suffix, no kind word."""
    core = re.sub(r"\s+-\s+.*$", "", name.strip())
    words = core.split()
    if len(words) > 1 and words[0].casefold() in _DISTRICT_KIND_WORDS:
        core = " ".join(words[1:])
    return core


# What a post writes before or after a name to say it means the town or the
# district, not the state of the same name: "مدينة كسلا", "محلية الخرطوم",
# "Kassala town".
_DISTRICT_MARK_AR = r"(?:محلية|مدينة|بمحلية|بمدينة|ومحلية|ومدينة|لمحلية|لمدينة)"
_DISTRICT_MARK_LATIN = r"(?:city|town|locality|district)"


def _post_names_district(
    district: dict,
    text: str,
    country: str,
    country_code: str | None,
    arabic_names: dict[str, str | None],
) -> bool:
    """True when the post itself names the district the model chose.

    - In Latin script: the district's name is in the text, as a whole word.
    - In Arabic: the post writes the district's name (`arabic_spelling`, a
      model call that only copies from the text), and the gazetteer holds
      that spelling as an exact match for this district: a place alias that
      points at the district, or a GeoNames match inside it whose Latin name
      agrees with the district's. Without the gazetteer's word a spelling
      the model picked from the text proves nothing.

    A district that shares its name with a state (Khartoum, Kassala, Sennar)
    needs more: "flooding across Khartoum state" names the state. The name
    then counts only with a word that says it means the town or the district
    ("مدينة كسلا", "محلية الخرطوم", "Kassala town").

    False on any failure but an LLM outage: an unconfirmed district falls
    back to its state, which is the safe side."""
    name = _district_core_name(district.get("name") or "")
    if not text or len(latin_name_key(name)) < 3:
        return False
    shared = _names_country_or_state(name, country)
    folded = text.casefold()
    latin = re.escape(name.casefold())
    if shared:
        if re.search(rf"(?<![a-z]){latin}\s+{_DISTRICT_MARK_LATIN}(?![a-z])", folded) or re.search(
            rf"(?<![a-z]){_DISTRICT_MARK_LATIN}\s+of\s+{latin}(?![a-z])", folded
        ):
            return True
    elif re.search(rf"(?<![a-z]){latin}(?![a-z])", folded):
        return True
    if not country_code or not arabic_lookup_applies(name):
        return False
    try:
        if name not in arabic_names:
            arabic_names[name] = arabic_spelling(text, name)
        arabic_name = arabic_names[name]
        if not arabic_name:
            return False
        if shared and not re.search(rf"{_DISTRICT_MARK_AR}\s+{re.escape(arabic_name)}", text):
            return False
        hit = exact_gazetteer_match(
            name, country_code, arabic_name=arabic_name, within_location_id=district["id"]
        )
    except LLMUnavailableError:
        raise
    except Exception as exc:  # noqa: BLE001 — a failed lookup confirms nothing
        logger.info("[locate] checking that the post names %r failed: %s", name, exc)
        return False
    if hit is None:
        return False
    # An alias is returned whatever area was asked about: it must be this district's.
    return hit.location_id is None or hit.location_id == district["id"]


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
    country_code: str | None = None,
):
    """The place names to geocode for a post, in order: the one the location
    answer is based on, then (with ``others``) the other places the post
    names. The others cost a model call, so they are asked for only when the
    caller comes back for more: the first name gave no point, or there was
    none. The country's and the states' own names are left out, and so is a
    region phrase or a natural feature (see `_names_a_place`)."""
    # Repeats are told by spelling, not by `latin_name_key`: that key drops
    # vowels to compare two spellings of one place, so it would also skip
    # "Tandilti" after "Tendalti" missed, and the gazetteer can answer those
    # two queries differently. Another spelling is another chance.
    tried: set[str] = set()
    if place_name and _names_a_place(place_name, country, country_code):
        tried.add(_spelling_key(place_name))
        yield place_name
    if not others:
        return
    tried_others = 0
    for name in named_places(title, description, country):
        key = _spelling_key(name)
        if not key or key in tried or not _names_a_place(name, country, country_code):
            continue
        tried.add(key)
        yield name
        tried_others += 1
        if tried_others >= MAX_OTHER_PLACE_NAMES:
            return


def _spelling_key(name: str) -> str:
    """The same spelling, whatever its case, hyphens or spaces."""
    return re.sub(r"[^a-z0-9]", "", name.casefold())


def _area_has_geometry(area_id: str) -> bool:
    try:
        return _area_shape(area_id) is not None
    except Exception:  # noqa: BLE001 — unknown is "no": the answer's own name is still tried
        return False


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
    "n", "s", "e", "w",  # "N. Kordofan"
    "axis", "region", "state", "area", "province", "front", "sector", "the", "of", "and",
})
# A river, a plain or a road is not a place a point can stand for.
_FEATURE_WORDS = frozenset({
    "river", "plain", "plains", "desert", "mountain", "mountains", "hills", "road", "highway", "border",
})
# The same in Arabic, for a name the model copied from the post: a direction,
# or a word for a region ("ولاية" state, "إقليم" region, "محور" axis).
_ARABIC_REGION_WORDS = frozenset({
    "شمال", "جنوب", "شرق", "غرب", "وسط", "ولاية", "إقليم", "اقليم", "منطقة", "محور",
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
    fuzzily matches inside the state, or with the region's centre point."""
    if not re.search(r"[a-z]", place_name.casefold()):
        return _names_arabic_region(place_name)
    scoped = _country_scoped_locations(country)
    # A district's own name is a place, whatever words it shares with a
    # state: "Khartoum North" is a city, not the north of Khartoum state.
    key = latin_name_key(place_name)
    if key and any(latin_name_key(loc["name"]) == key for loc in scoped if (loc.get("level") or 0) == 2):
        return False
    words = {w for w in re.split(r"[^a-z]+", place_name.casefold()) if w}
    if words & _FEATURE_WORDS:
        return True
    core = _region_core(place_name)
    if not core:
        return True
    return any(_region_core(loc["name"]) == core for loc in scoped if (loc.get("level") or 0) <= 1)


def _names_arabic_region(place_name: str) -> bool:
    """`_names_region_or_feature` for a name with no Latin letters. True for
    a direction alone ("شرق"), and for a region several states share, with or
    without a direction ("دارفور", "وسط دارفور", "ولاية شمال كردفان").

    Any other Arabic name is a place as far as code can tell: the states'
    Arabic names are not known here, and many districts are a direction and
    a name ("شرق النيل" is Sharg An Neel). The gazetteer tells those apart:
    it refuses a match that is a state (`geoparser._names_an_area`)."""
    words = [w for w in re.split(r"[\s_]+", place_name.strip()) if w]
    if not any(_ARABIC_LETTER.search(w) for w in words):
        return True  # no letters at all: nothing to look up
    core = " ".join(w for w in words if w not in _ARABIC_REGION_WORDS)
    regions = {s for spellings in _REGION_SPELLINGS.values() for s in spellings if _ARABIC_LETTER.search(s)}
    return not core or core in regions


def _names_a_place(place_name: str, country: str, country_code: str | None = None) -> bool:
    """False for a name that no point stands for: the country's or a state's
    own name, a region phrase, a natural feature. The check is the same for
    the name the location answer is based on and for the other names of the
    post. Until #1429 the answer's own name was only checked against the
    states' names, and "Darfur" was geocoded: the gazetteer holds a region of
    that name, with a centre point inside North Darfur.

    A name that reads as a region phrase is still a place when the gazetteer
    knows a town by exactly that name: "Khartoum North" is a city (Bahri),
    and its district has another name, so `_names_region_or_feature` cannot
    tell. Asked only for such a name, and only with ``country_code``."""
    if _names_country_or_state(place_name, country):
        return False
    if not _names_region_or_feature(place_name, country):
        return True
    return bool(country_code) and gazetteer_names_town(place_name, country_code)


# ── an exact match outside the model's area (#1414) ──────────────────────────

# Outside the area a match counts only for a place that a same-named hamlet
# or a misread name rarely lands on by chance: a town of at least this many
# people, or a place of one of the feature codes below.
ELSEWHERE_MIN_POPULATION = 5000
# Seats of government. The only codes a Latin spelling may match: it comes
# from the model, which misreads names ("Damazin" for الضعين, Ed Daein).
_SEAT_CODES = frozenset({"PPLC", "PPLA", "PPLA2", "PPLA3", "PPLA4"})
# The post's own Arabic spelling may also match a district by its name
# (GeoNames lists the districts, with Arabic names): الخوي, الفاو, البرقيق.
_ARABIC_CODES = _SEAT_CODES | {"ADM2"}

# The regions several states share a name with, as posts write them. A post
# that says "Kordofan" with no north, south or west is placed by the model in
# one of the three states; the place it names decides which.
_REGION_SPELLINGS = {
    "kordofan": ("kordofan", "كردفان"),
    "darfur": ("darfur", "دارفور"),
}
_DIRECTIONS = {
    "north": ("north", "northern", "شمال"),
    "south": ("south", "southern", "جنوب"),
    "east": ("east", "eastern", "شرق"),
    "west": ("west", "western", "غرب"),
    "central": ("central", "وسط"),
}
_ANY_DIRECTION = "|".join(word for words in _DIRECTIONS.values() for word in words)


def _region_mentions(text: str, region: str, directions: str) -> bool:
    """True when ``text`` names the region after one of ``directions`` (a
    regex alternation). The Arabic form may carry "ولاية" (state)."""
    return any(
        re.search(rf"(?:{directions})[\s_]+(?:ولاية[\s_]+)?{spelling}", text.casefold())
        for spelling in _REGION_SPELLINGS[region]
    )


def _names_bare_region(text: str, region: str) -> bool:
    """True when ``text`` names the region with no direction: "Kordofan",
    "كردفان", but not only "South Kordofan" or "جنوب كردفان"."""
    if not text:
        return False
    folded = text.casefold()
    for spelling in _REGION_SPELLINGS[region]:
        # Drop every mention with a direction, then see whether one is left.
        bare = re.sub(rf"(?:{_ANY_DIRECTION})[\s_]+(?:ولاية[\s_]+)?{spelling}", " ", folded)
        if spelling in bare:
            return True
    return False


def _region_of_state(state: dict, states: list[dict]) -> tuple[str, list[dict]] | None:
    """The region ``state`` shares its name with, and the other states of that
    region: ("kordofan", [North Kordofan, West Kordofan]) for South Kordofan.
    None for a state that shares its name with no other."""
    core = _region_core(state.get("name") or "")
    siblings = [s for s in states if s["id"] != state["id"] and _region_core(s.get("name") or "") == core]
    region = next((r for r in _REGION_SPELLINGS if latin_name_key(r) == core), None)
    return (region, siblings) if core and siblings and region else None


def _text_names_state(text: str, state: dict, region: tuple[str, list[dict]] | None) -> bool:
    """True when the post names the state the model chose. The model's state
    is then what the post says, and a same-named place in another state is
    another place: a camp called Babanusa in South Darfur is not the town in
    West Kordofan, and Al Firdous in Khartoum is not the one in East Darfur.

    - Its English name is in the text.
    - A state of a region: the region with the state's direction is in the
      text ("جنوب كردفان" for South Kordofan). The bare region is not enough.
    - Any other state: the post writes its name in Arabic (`arabic_spelling`,
      a model call; the caller asks only once a match elsewhere is found)."""
    name = (state.get("name") or "").strip()
    if not text or not name:
        return False
    if name.casefold() in text.casefold():
        return True
    if region is not None:
        words = [w for w in re.split(r"[^a-z]+", name.casefold()) if w in _DIRECTIONS]
        return bool(words) and _region_mentions(text, region[0], "|".join(_DIRECTIONS[words[0]]))
    return arabic_spelling(text, name) is not None


def _place_elsewhere(
    place_name: str,
    area_id: str,
    country: str,
    country_code: str,
    *,
    text: str = "",
    arabic_names: dict[str, str | None] | None = None,
) -> str | None:
    """The location for a place that no lookup found inside ``area_id``, when
    the gazetteer holds a safe exact match for it elsewhere in the country
    (see `_exact_match_elsewhere`). None otherwise, and on any failure but an
    LLM outage. The miss the area lookup recorded for the name is withdrawn:
    a name placed by this rule is not a miss."""
    try:
        if arabic_names is not None and place_name in arabic_names:
            arabic_name = arabic_names[place_name]
        else:
            arabic_name = arabic_spelling(text, place_name) if arabic_lookup_applies(place_name) else None
        # One lookup tells whether the name exists anywhere: most misses end here.
        if not gazetteer_knows(place_name, country_code, arabic_name=arabic_name):
            return None
        geo = _exact_match_elsewhere(place_name, arabic_name, area_id, country, country_code, text=text)
        if geo is None:
            return None
        if geo.location_id:
            location_id = geo.location_id
        else:
            promo = find_or_create_landmark_l4(name=place_name, lat=geo.lat, lng=geo.lng, kind=geo.kind)
            if promo.get("abortedReason") or not promo.get("locationId"):
                return None
            location_id = promo["locationId"]
        logger.info("[locate] %r has no match inside area %s: placed at %s by an exact match "
                    "elsewhere in %s (%r)", place_name, area_id, location_id, country, geo.display_name)
        misses = _geocode_misses.get()
        if misses is not None:
            misses[:] = [m for m in misses if not (m.name == place_name and m.area_id == area_id)]
        return location_id
    except LLMUnavailableError:
        raise
    except Exception as exc:  # noqa: BLE001 — an enrichment: the area stands
        logger.warning("[locate] looking for %r outside its area failed: %s", place_name, exc)
        return None


def _exact_match_elsewhere(
    place_name: str,
    arabic_name: str | None,
    area_id: str,
    country: str,
    country_code: str,
    *,
    text: str = "",
):
    """An exact gazetteer match for the place outside ``area_id`` (a state the
    model chose), when that match is safe to use. None otherwise.

    The area check exists because village names repeat across a country. It
    also refuses the right place when the model chose the wrong state: Wad
    Madani, El Obeid, Al Khiwai. This rule accepts a match outside the area
    when all of these hold:

    - The post does not name the model's state itself (`_text_names_state`).
      Checked last.
    - The match is exact. A fuzzy match elsewhere is another place.
    - It is unambiguous: the name has an exact match in one state only, and
      inside that state in one district only. Every exact match counts
      toward "more than one", whatever its size.
    - It is a place of some size: `ELSEWHERE_MIN_POPULATION` people, a seat
      of government, or (Arabic only) a district by its own name.
    - It matches the post's own Arabic spelling, and agrees with the Latin
      name, as inside an area. The model's Latin spelling is used alone only
      for a post with no Arabic text. In an Arabic post a Latin name the
      model cannot point to in the text is a reading nothing checks: الضعين
      (Ed Daein) read as "Damazin" is refused.

    A post that names "Kordofan" or "Darfur" with no direction is first
    checked against the other states of that region: the region is what the
    post says, so there a match needs no size, and the Latin spelling is
    tried when the Arabic one (found in the post) matches nothing."""
    scoped = _country_scoped_locations(country)
    states = [loc for loc in scoped if (loc.get("level") or 0) == 1]
    area = next((s for s in states if s["id"] == area_id), None)
    if area is None:
        return None
    region = _region_of_state(area, states)
    # A name the model copied in Arabic script has no second reading to agree
    # with, so it always needs the size.
    unchecked = not latin_name_key(place_name)
    if not arabic_name and not unchecked and _ARABIC_LETTER.search(text or ""):
        return None

    def match_in(location_id: str, arabic: str | None, *, any_size: bool):
        return exact_gazetteer_match(
            place_name, country_code, arabic_name=arabic, within_location_id=location_id,
            min_population=None if any_size else ELSEWHERE_MIN_POPULATION,
            feature_codes=_ARABIC_CODES if arabic else _SEAT_CODES,
        )

    # A lookup that fails raises (see `exact_gazetteer_match`), and the
    # caller gives up: one unanswered state could hold a second match.
    def holders(candidates: list[dict], arabic: str | None) -> list[dict]:
        return [c for c in candidates if match_in(c["id"], arabic, any_size=True) is not None]

    def unique_match(candidates: list[dict], arabic: str | None, *, any_size: bool):
        """(match, ambiguous). The match, when one state of ``candidates``
        holds the name, and one district of that state. ``ambiguous``: more
        than one state, or more than one district, holds it."""
        in_states = holders(candidates, arabic)
        if len(in_states) != 1:
            return None, len(in_states) > 1
        districts = [
            loc for loc in scoped
            if (loc.get("level") or 0) == 2 and (loc.get("parent") or {}).get("id") == in_states[0]["id"]
        ]
        in_districts = holders(districts, arabic)
        if len(in_districts) != 1:
            return None, len(in_districts) > 1
        return match_in(in_districts[0]["id"], arabic, any_size=any_size), False

    hit = None
    if region and _names_bare_region(text, region[0]):
        for arabic in ([arabic_name, None] if arabic_name and not unchecked else [arabic_name or None]):
            hit, ambiguous = unique_match(region[1], arabic, any_size=not unchecked)
            # The Latin spelling is for an Arabic one that matches nothing.
            # One that matches several places is ambiguous, and stays so.
            if hit is not None or ambiguous:
                break
    if hit is None:
        others = [s for s in states if s["id"] != area_id]
        hit, _ = unique_match(others, arabic_name or None, any_size=False)
    # Asked last: for a state outside a region it costs a model call.
    if hit is None or _text_names_state(text, area, region):
        return None
    return hit


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
    place_name: str,
    area_id: str,
    country_code: str,
    *,
    text: str = "",
    arabic_names: dict[str, str | None] | None = None,
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
    miss: an area with no geometry, a lookup that failed (an outage proves
    nothing about the name), and a name that is an area's own (a state, a
    region, a mountain range: `GeocodeTrace.area`)."""
    try:
        area = _area_shape(area_id)
        if area is None:
            logger.info("[locate] area %s has no geometry — can't confirm %r", area_id, place_name)
            return None

        from shapely.geometry import Point

        # The post's Arabic spelling costs a model call, so ask only once the
        # area is usable and the Arabic lookup would actually run.
        # `arabic_names` keeps the answer for a name that is tried in a second
        # area (a district, then its state).
        if not arabic_lookup_applies(place_name):
            arabic_name = None
        elif arabic_names is not None and place_name in arabic_names:
            arabic_name = arabic_names[place_name]
        else:
            arabic_name = arabic_spelling(text, place_name)
            if arabic_names is not None:
                arabic_names[place_name] = arabic_name
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
            # A name the gazetteer knows as a state or a region is not a
            # miss either: a reviewer has no place to give it.
            if (
                not trace.failed
                and not trace.area
                and _geocode_misses.get() is not None
                # The area lookup can miss a state's own name: its centre
                # point lies outside a district that was searched.
                and not gazetteer_names_area(place_name, country_code, arabic_name=arabic_name)
            ):
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
