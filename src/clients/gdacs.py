"""GDACS API client — fetches disaster events for a country within a time window.

GDACS is a public API (no authentication required).
Base URL: https://www.gdacs.org/gdacsapi
Key endpoint: /api/Events/geteventlist/search
"""

import logging
from datetime import UTC, datetime, timedelta

import httpx
import redis

from src.config import settings

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

# GDACS event types → CLEAR glide numbers
GDACS_TYPE_MAP: dict[str, str] = {
    "EQ": "eq",   # Earthquake
    "TC": "tc",   # Tropical cyclone
    "FL": "fl",   # Flood
    "VO": "vo",   # Volcano
    "DR": "dr",   # Drought
    "WF": "wf",   # Wild fire
    "TS": "ts",   # Tsunami
}

# GDACS alert level → severity (1-5)
GDACS_SEVERITY_MAP: dict[str, int] = {
    "Red": 5,
    "Orange": 4,
    "Green": 2,
}


def _parse_event(raw: dict) -> dict | None:
    """Parse a raw GDACS event dict into a normalized signal-like dict.

    Returns None if the event lacks required fields.
    """
    # GDACS returns events in various nested formats depending on the endpoint.
    # The search endpoint typically nests data under "properties" in a GeoJSON-like structure.
    props = raw.get("properties", raw)

    event_id = props.get("eventid") or raw.get("eventid")
    event_type = props.get("eventtype") or raw.get("eventtype")
    if not event_id or not event_type:
        return None

    # Extract coordinates — may be in geometry.coordinates or direct lat/lng
    lat = None
    lng = None
    geo = raw.get("geometry")
    if geo and geo.get("type") == "Point" and geo.get("coordinates"):
        coords = geo["coordinates"]
        if len(coords) >= 2:
            lng, lat = coords[0], coords[1]  # GeoJSON is [lng, lat]
    if lat is None:
        lat = props.get("lat") or props.get("geo_lat")
        lng = props.get("lng") or props.get("geo_lng") or props.get("lon")

    name = props.get("name") or props.get("eventname", "")
    description = props.get("description") or props.get("htmldescription", "")
    alert_level = props.get("alertlevel", "Green")
    severity = GDACS_SEVERITY_MAP.get(alert_level, 2)
    from_date = props.get("fromdate") or props.get("datestart")
    to_date = props.get("todate") or props.get("dateend")
    country = props.get("country", "")
    # url can be a dict with {geometry, report, details} or a plain string
    url_field = props.get("url", "")
    if isinstance(url_field, dict):
        url = url_field.get("report") or url_field.get("details") or ""
    else:
        url = url_field or props.get("link", "")
    glide = props.get("glide", "")
    iso3 = props.get("iso3", "")

    glide_type = GDACS_TYPE_MAP.get(event_type, "ot")

    # Extract population affected from severitydata or direct fields
    severity_data = props.get("severitydata", {}) or {}
    population_affected = (
        props.get("numaffected")
        or props.get("totalaffected")
        or severity_data.get("numaffected")
        or severity_data.get("totalaffected")
        or None
    )

    # Build title
    title = f"GDACS {alert_level} alert: {name}" if name else f"GDACS {alert_level} {event_type} alert"

    return {
        "gdacs_id": f"{event_type}-{event_id}",
        "title": title,
        "description": description[:500] if description else None,
        "severity": severity,
        "alert_level": alert_level,
        "event_type": event_type,
        "glide_type": glide_type,
        "glide": glide,
        "lat": float(lat) if lat else None,
        "lng": float(lng) if lng else None,
        "country": country,
        "iso3": iso3,
        "from_date": from_date,
        "to_date": to_date,
        "url": url,
        "population_affected": int(population_affected) if population_affected else None,
        "raw": raw,
    }


def _fetch_for_country(country: str, since: datetime, now: datetime) -> list[dict]:
    """Fetch raw events from GDACS for a single country."""
    params: dict = {
        "country": country,
        "fromDate": since.strftime("%Y-%m-%dT%H:%M:%S"),
        "toDate": now.strftime("%Y-%m-%dT%H:%M:%S"),
        "pageSize": 100,
        "pageNumber": 1,
    }

    url = f"{settings.gdacs_base_url}/api/Events/geteventlist/search"
    logger.info("Fetching GDACS events for %s: %s", country, url)

    try:
        resp = httpx.get(url, params=params, headers={"Accept": "application/json"}, timeout=30)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.error("GDACS API request failed for %s: %s", country, e)
        return []

    # GDACS may return empty body, XML, or HTML instead of JSON
    content_type = resp.headers.get("content-type", "")
    if not resp.text.strip():
        logger.warning("GDACS returned empty response for %s (status=%d)", country, resp.status_code)
        return []
    if "json" not in content_type and not resp.text.strip().startswith(("{", "[")):
        logger.warning("GDACS returned non-JSON response for %s: content-type=%s, body=%s", country, content_type, resp.text[:200])
        return []

    try:
        data = resp.json()
    except Exception as e:
        logger.error("GDACS JSON parse failed for %s: %s, body=%s", country, e, resp.text[:200])
        return []

    raw_events: list[dict] = []
    if isinstance(data, dict):
        if "features" in data:
            raw_events = data["features"]
        elif "events" in data:
            raw_events = data["events"]
        elif isinstance(data.get("result"), list):
            raw_events = data["result"]
    elif isinstance(data, list):
        raw_events = data

    logger.info("GDACS returned %d raw events for %s", len(raw_events), country)
    return raw_events


def fetch_gdacs_events(since: datetime | None = None) -> list[dict]:
    """
    Fetch GDACS events for all configured countries within a time window.

    Deduplicates against Redis seen-set.
    Returns a list of normalized event dicts.
    """
    if since is None:
        since = datetime.now(UTC) - timedelta(days=settings.initial_lookback_days)

    now = datetime.now(UTC)
    countries = [c.strip() for c in settings.gdacs_countries.split(",") if c.strip()]

    all_raw: list[dict] = []
    for country in countries:
        all_raw.extend(_fetch_for_country(country, since, now))

    # Parse and deduplicate
    events: list[dict] = []
    for raw in all_raw:
        parsed = _parse_event(raw)
        if not parsed:
            continue

        dedup_key = f"gdacs:seen:{parsed['gdacs_id']}"
        if _redis.exists(dedup_key):
            continue

        _redis.setex(dedup_key, settings.dedup_ttl_hours * 3600, "1")
        events.append(parsed)

    if events:
        set_last_synced(now)

    logger.info(
        "GDACS: %d new events after dedup (out of %d raw, %d countries)",
        len(events), len(all_raw), len(countries),
    )
    return events


def get_last_synced() -> datetime | None:
    val = _redis.get("gdacs:last_synced")
    if val:
        return datetime.fromisoformat(val)
    return None


def set_last_synced(ts: datetime) -> None:
    _redis.set("gdacs:last_synced", ts.isoformat())
