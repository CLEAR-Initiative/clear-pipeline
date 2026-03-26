"""Location resolution: extract and resolve locations from signal text using Claude."""

import logging

from src.clients.claude import call_claude
from src.clients.graphql import get_locations

logger = logging.getLogger(__name__)

# Cache location list to avoid repeated API calls
_locations_cache: list[dict] | None = None


def _get_locations() -> list[dict]:
    """Get locations from CLEAR API (cached in memory)."""
    global _locations_cache
    if _locations_cache is None:
        _locations_cache = get_locations()
    return _locations_cache


def invalidate_locations_cache() -> None:
    global _locations_cache
    _locations_cache = None


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
) -> dict:
    """
    Use Claude to extract location from signal text and resolve to CLEAR location IDs.

    Returns dict with keys:
      - location_type: "displacement" | "general"
      - origin_id: str | None
      - destination_id: str | None
      - location_id: str | None
    """
    locations = _get_locations()
    if not locations:
        logger.warning("No locations in CLEAR API — skipping location resolution")
        return {"location_type": "general", "origin_id": None, "destination_id": None, "location_id": None}

    prompt = _build_prompt(title, description, dataminr_location_name, locations)

    try:
        result = call_claude(SYSTEM_PROMPT, prompt)

        # Validate returned IDs against known locations
        known_ids = {loc["id"] for loc in locations}
        origin_id = result.get("origin_id")
        destination_id = result.get("destination_id")
        location_id = result.get("location_id")

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
            "location_type": result.get("location_type", "general"),
            "origin_id": origin_id,
            "destination_id": destination_id,
            "location_id": location_id,
        }

    except Exception as e:
        logger.error("Location resolution failed: %s", e)
        return {"location_type": "general", "origin_id": None, "destination_id": None, "location_id": None}
