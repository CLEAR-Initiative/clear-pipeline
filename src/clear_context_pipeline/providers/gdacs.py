"""GDACS API client — fetches disaster events for a country within a time window.

GDACS is a public API (no authentication required).
Base URL: https://www.gdacs.org/gdacsapi
Key endpoint: /api/Events/geteventlist/search

Ported from clear-pipeline for the Dagster consolidation.
"""

import logging
from datetime import UTC, datetime, timedelta

import httpx
import redis

from clear_context_pipeline.signals.config import settings
from clear_context_pipeline.providers.signal import enrich_with_geoparser

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
    logger.info("[GDACS] Fetching events for country=%s url=%s", country, url)

    try:
        resp = httpx.get(url, params=params, headers={"Accept": "application/json"}, timeout=30)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.error("[GDACS] API request failed for %s: %s", country, e)
        return []

    # GDACS may return empty body, XML, or HTML instead of JSON
    content_type = resp.headers.get("content-type", "")
    if not resp.text.strip():
        logger.warning("[GDACS] empty response for %s (status=%d)", country, resp.status_code)
        return []
    if "json" not in content_type and not resp.text.strip().startswith(("{", "[")):
        logger.warning("[GDACS] non-JSON response for %s: content-type=%s, body=%s", country, content_type, resp.text[:200])
        return []

    try:
        data = resp.json()
    except Exception as e:
        logger.error("[GDACS] JSON parse failed for %s: %s, body=%s", country, e, resp.text[:200])
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

    logger.info("[GDACS] returned %d raw events for %s", len(raw_events), country)
    return raw_events


def fetch_gdacs_events(since: datetime | None = None) -> list[dict]:
    """
    Fetch GDACS events for all configured countries within a time window.

    Deduplicates against Redis seen-set.
    Returns a list of normalized event dicts.
    """
    if since is None:
        since = datetime.now(UTC) - timedelta(days=settings.initial_lookback_days)
        logger.info("[GDACS] No 'since' provided, using initial lookback of %d days", settings.initial_lookback_days)

    now = datetime.now(UTC)
    countries = [c.strip() for c in settings.gdacs_countries.split(",") if c.strip()]

    logger.info("[GDACS] Fetch window: %s → %s", since.isoformat(), now.isoformat())
    logger.info("[GDACS] Configured countries: %s", countries)

    all_raw: list[dict] = []
    for country in countries:
        raw = _fetch_for_country(country, since, now)
        logger.info("[GDACS] %s: %d raw events fetched", country, len(raw))
        all_raw.extend(raw)

    logger.info("[GDACS] Total raw events across all countries: %d", len(all_raw))

    # Parse + dedup. Seen-set is READ here only; marked (mark_seen) after
    # createSignal via the connector's post_create, and the watermark is advanced
    # by the ingest asset after a clean batch — so a record whose persistence
    # fails stays un-seen + inside the window and the next poll re-fetches it.
    events: list[dict] = []
    parse_failed = 0
    deduped = 0
    batch_ids: set[str] = set()
    for raw in all_raw:
        parsed = _parse_event(raw)
        if not parsed:
            parse_failed += 1
            continue

        gdacs_id = parsed["gdacs_id"]
        if gdacs_id in batch_ids or _redis.exists(f"gdacs:seen:{gdacs_id}"):
            deduped += 1
            continue
        batch_ids.add(gdacs_id)
        events.append(parsed)

    logger.info(
        "[GDACS] Result: %d new events (parse_failed=%d, already_seen=%d) out of %d raw",
        len(events), parse_failed, deduped, len(all_raw),
    )
    return events


def mark_seen(gdacs_id: str) -> None:
    """Mark an event ingested (Redis seen-set) — called only after createSignal
    is confirmed, so a failed persistence leaves the event eligible for re-poll."""
    _redis.setex(f"gdacs:seen:{gdacs_id}", settings.dedup_ttl_hours * 3600, "1")


def get_last_synced() -> datetime | None:
    val = _redis.get("gdacs:last_synced")
    if val:
        return datetime.fromisoformat(val)
    return None


def set_last_synced(ts: datetime) -> None:
    _redis.set("gdacs:last_synced", ts.isoformat())


def build_gdacs_signal_input(event: dict, source_id: str) -> dict:
    """Convert a parsed GDACS event into a CLEAR CreateSignalInput dict."""
    input_data: dict = {
        "sourceId": source_id,
        # Dedup key — (sourceId, externalId) is unique, so re-ingesting the
        # same GDACS event (across poll rounds) returns the existing row.
        "externalId": f"gdacs:{event['gdacs_id']}",
        "rawData": event["raw"],
        "publishedAt": event.get("from_date") or datetime.now(UTC).isoformat(),
        "url": event.get("url"),
        "title": event["title"],
        "description": event.get("description"),
        "severity": event.get("severity"),
    }

    # Pass lat/lng for server-side PostGIS geo-resolution. Set before the
    # geoparser call below so the same-A2 safety check has source coords.
    if event.get("lat") is not None and event.get("lng") is not None:
        input_data["lat"] = event["lat"]
        input_data["lng"] = event["lng"]

    # Text-based geoparser: enrich `geoparsedData` and, when a landmark
    # resolves cleanly, promote it to a reusable L4 (overriding the default
    # "signal-title L4" branch in clear-api). Best-effort.
    enrich_with_geoparser(
        input_data,
        title=event["title"],
        description=event.get("description"),
        log_tag=f"gdacs:{event.get('gdacs_id')}",
    )

    return input_data
