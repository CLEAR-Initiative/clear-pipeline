"""Signal creation: map Dataminr payload → CLEAR signal and persist via GraphQL."""

import logging
import re

from src.clients.graphql import create_signal
from src.models.dataminr import DataminrSignal
from src.services.location import resolve_signal_location

logger = logging.getLogger(__name__)

# Map Dataminr alertType.name to severity 1-5
DATAMINR_SEVERITY_MAP: dict[str, int] = {
    "flash": 5,
    "urgent": 4,
    "alert": 3,
    "watch": 2,
}


def _estimate_severity_from_dataminr(signal: DataminrSignal) -> int | None:
    """Extract severity from Dataminr alertType, or return None if absent."""
    if signal.alertType and signal.alertType.name:
        name = signal.alertType.name.lower().strip()
        return DATAMINR_SEVERITY_MAP.get(name)
    return None


# Match common phrasings of fatality counts in news/alert text:
#   "12 killed", "at least 5 dead", "3 fatalities", "killed 8 people",
#   "death toll of 14", "leaving 6 dead". We deliberately stay narrow on
#   the verb list to avoid false positives ("injured", "displaced" are
#   tracked separately and don't belong here).
_NUM = r"(\d{1,5})"
_CASUALTY_PATTERNS: list[re.Pattern[str]] = [
    re.compile(rf"\b(?:at least|over|more than|nearly|around|about)?\s*{_NUM}\s+(?:people\s+)?(?:were\s+|are\s+)?(?:killed|dead|deceased|fatalities)\b", re.IGNORECASE),
    re.compile(rf"\b(?:killed|leaving|left)\s+(?:at least\s+|over\s+|more than\s+|nearly\s+)?{_NUM}\s+(?:people|dead|civilians|soldiers)?\b", re.IGNORECASE),
    re.compile(rf"\bdeath toll\s+(?:of|at|reaches?|rose to|climbed to|stands at)\s+{_NUM}\b", re.IGNORECASE),
    re.compile(rf"\b{_NUM}\s+(?:civilians?|soldiers?|militants?|protesters?)\s+(?:were\s+)?killed\b", re.IGNORECASE),
]


def extract_casualties_from_text(*texts: str | None) -> int | None:
    """Best-effort fatality count parsed from free-text headlines/descriptions.

    Returns the maximum number found across all matched patterns (multiple
    sources sometimes mention different running totals; the upper bound is
    the most useful for severity assessment). Returns None if no pattern
    matches.
    """
    best: int | None = None
    for text in texts:
        if not text:
            continue
        for pat in _CASUALTY_PATTERNS:
            for m in pat.finditer(text):
                try:
                    val = int(m.group(1))
                except (ValueError, IndexError):
                    continue
                if val < 0 or val > 100_000:
                    continue
                if best is None or val > best:
                    best = val
    return best


def build_signal_input(signal: DataminrSignal, source_id: str) -> dict:
    """Map a Dataminr signal to a CLEAR CreateSignalInput dict."""
    # Build description from subHeadline fields
    description_parts = []
    if signal.subHeadline:
        if signal.subHeadline.title:
            description_parts.append(signal.subHeadline.title)
        if signal.subHeadline.subHeadlines:
            description_parts.append(signal.subHeadline.subHeadlines)
    description = " — ".join(description_parts) if description_parts else None

    # URL from publicPost
    url = None
    if signal.publicPost and signal.publicPost.href:
        url = signal.publicPost.href

    # Full raw payload as JSON
    raw_data = signal.model_dump(mode="json")

    # Estimate severity from Dataminr alertType (1-5 or None)
    severity = _estimate_severity_from_dataminr(signal)

    input_data: dict = {
        "sourceId": source_id,
        # Idempotent ingestion key — the clear-api upsert behaviour keys on
        # (sourceId, externalId), so re-ingesting the same Dataminr alert
        # returns the existing row instead of creating a duplicate.
        "externalId": f"dataminr:{signal.alertId}",
        "rawData": raw_data,
        "publishedAt": signal.alertTimestamp,
        "url": url,
        "title": signal.headline,
        "description": description,
    }
    if severity is not None:
        input_data["severity"] = severity

    # Dataminr has no structured casualties field; parse it from the headline
    # and description text. Best-effort — only set when a match is found.
    casualties = extract_casualties_from_text(signal.headline, description)
    if casualties is not None:
        input_data["casualties"] = casualties

    # Check if Dataminr provides coordinates
    has_coords = False
    dataminr_location_name = None
    if signal.estimatedEventLocation:
        dataminr_location_name = signal.estimatedEventLocation.name
        if signal.estimatedEventLocation.coordinates:
            coords = signal.estimatedEventLocation.coordinates
            if len(coords) >= 2:
                input_data["lat"] = coords[0]
                input_data["lng"] = coords[1]
                has_coords = True

    if has_coords:
        # Dataminr has coordinates — let the API's PostGIS geo-resolution handle it.
        # Still use Claude to determine if it's displacement (for origin/destination).
        loc_result = resolve_signal_location(
            title=signal.headline,
            description=description,
            dataminr_location_name=dataminr_location_name,
        )
        if loc_result["location_type"] == "displacement":
            # Displacement: set origin/destination from Claude, lat/lng stays for API fallback
            if loc_result["origin_id"]:
                input_data["originId"] = loc_result["origin_id"]
            if loc_result["destination_id"]:
                input_data["destinationId"] = loc_result["destination_id"]
            logger.info(
                "Displacement signal (with coords): origin=%s destination=%s",
                loc_result["origin_id"],
                loc_result["destination_id"],
            )
        else:
            # General with coords: let PostGIS resolve locationId from lat/lng
            logger.info("General signal: using lat/lng for PostGIS resolution")
    else:
        # No coordinates — use Claude to resolve location from text
        loc_result = resolve_signal_location(
            title=signal.headline,
            description=description,
            dataminr_location_name=dataminr_location_name,
        )
        if loc_result["location_type"] == "displacement":
            if loc_result["origin_id"]:
                input_data["originId"] = loc_result["origin_id"]
            if loc_result["destination_id"]:
                input_data["destinationId"] = loc_result["destination_id"]
            logger.info(
                "Displacement signal (no coords): origin=%s destination=%s",
                loc_result["origin_id"],
                loc_result["destination_id"],
            )
        else:
            if loc_result["location_id"]:
                input_data["locationId"] = loc_result["location_id"]
            logger.info("General signal (no coords): locationId=%s", loc_result["location_id"])

    return input_data


def ingest_signal(signal: DataminrSignal, source_id: str) -> dict:
    """Build and persist a CLEAR signal from a Dataminr payload. Returns the created signal."""
    input_data = build_signal_input(signal, source_id)
    result = create_signal(input_data)
    logger.info(
        "Created signal id=%s title=%s location=%s",
        result["id"],
        result.get("title", "")[:60],
        result.get("generalLocation", {}).get("name") if result.get("generalLocation") else
        result.get("originLocation", {}).get("name") if result.get("originLocation") else "none",
    )
    return result
