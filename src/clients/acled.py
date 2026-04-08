"""ACLED (Armed Conflict Location & Event Data Project) API client.

Authentication: session cookies via POST /user/login (24h expiry).
Endpoint: /api/acled/read with country + event_date filters.

Docs: https://acleddata.com/api-documentation/
"""

import json
import logging
from datetime import UTC, datetime, timedelta

import httpx
import redis

from src.config import settings

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

ACLED_COOKIE_KEY = "acled:cookies"

# ACLED event_type / disorder_type → CLEAR glide numbers
ACLED_EVENT_TYPE_MAP: dict[str, str] = {
    "Battles": "ba",
    "Explosions/Remote violence": "rv",
    "Violence against civilians": "vc",
    "Protests": "pr",
    "Riots": "ri",
    "Strategic developments": "pv",
}

ACLED_DISORDER_TYPE_MAP: dict[str, str] = {
    "Political violence": "pv",
    "Violence against civilians": "vc",
    "Demonstrations": "pr",
    "Strategic developments": "pv",
}


def _authenticate() -> dict | None:
    """Authenticate with ACLED and return cookie dict (or None on failure).

    Caches cookies in Redis for 23 hours.
    """
    cached = _redis.get(ACLED_COOKIE_KEY)
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            _redis.delete(ACLED_COOKIE_KEY)

    if not settings.acled_username or not settings.acled_password:
        logger.error("ACLED credentials not configured (ACLED_USERNAME, ACLED_PASSWORD)")
        return None

    logger.info("Authenticating with ACLED API")

    try:
        with httpx.Client(follow_redirects=True, timeout=30) as client:
            # Initial GET to establish session cookies
            client.get(settings.acled_base_url)

            # Login
            login_url = f"{settings.acled_base_url}/user/login"
            resp = client.post(
                login_url,
                params={"_format": "json"},
                headers={"Content-Type": "application/json"},
                content=json.dumps({
                    "name": settings.acled_username,
                    "pass": settings.acled_password,
                }),
            )

            if resp.status_code != 200:
                logger.error(
                    "ACLED login failed: status=%d body=%s",
                    resp.status_code,
                    resp.text[:300],
                )
                return None

            try:
                data = resp.json()
            except Exception as e:
                logger.error("ACLED login response not JSON: %s", e)
                return None

            if "current_user" not in data:
                logger.error("ACLED login response missing current_user: %s", data)
                return None

            cookies = dict(client.cookies)
            logger.info(
                "ACLED authenticated as %s",
                data["current_user"].get("name", "?"),
            )

            _redis.setex(ACLED_COOKIE_KEY, settings.acled_token_ttl, json.dumps(cookies))
            return cookies

    except httpx.HTTPError as e:
        logger.error("ACLED auth request failed: %s", e)
        return None


def _parse_event(raw: dict) -> dict | None:
    """Normalize a raw ACLED event into our signal-like dict."""
    event_id = raw.get("event_id_cnty") or raw.get("event_id") or raw.get("data_id")
    event_date = raw.get("event_date")
    if not event_id or not event_date:
        return None

    event_type = raw.get("event_type", "") or ""
    disorder_type = raw.get("disorder_type", "") or ""

    # Map to glide type
    glide_type = (
        ACLED_EVENT_TYPE_MAP.get(event_type)
        or ACLED_DISORDER_TYPE_MAP.get(disorder_type)
        or "ot"
    )

    # Coordinates
    lat = None
    lng = None
    try:
        if raw.get("latitude"):
            lat = float(raw["latitude"])
        if raw.get("longitude"):
            lng = float(raw["longitude"])
    except (ValueError, TypeError):
        pass

    fatalities = 0
    try:
        fatalities = int(raw.get("fatalities", 0) or 0)
    except (ValueError, TypeError):
        pass

    # Severity estimation: higher for events with fatalities or civilian violence
    severity = 2
    if fatalities >= 10:
        severity = 5
    elif fatalities >= 3:
        severity = 4
    elif fatalities >= 1:
        severity = 3
    elif event_type in ("Battles", "Explosions/Remote violence"):
        severity = 3
    elif disorder_type == "Violence against civilians":
        severity = 3

    location = raw.get("location") or raw.get("admin2") or raw.get("admin1") or ""
    admin1 = raw.get("admin1", "")
    admin2 = raw.get("admin2", "")
    country = raw.get("country", "")

    notes = raw.get("notes", "") or ""
    actors = []
    for key in ("actor1", "actor2"):
        if raw.get(key):
            actors.append(raw[key])
    actor_str = " vs ".join(actors) if actors else ""

    title_parts = [event_type or "Conflict event"]
    if location:
        title_parts.append(f"in {location}")
    if actor_str:
        title_parts.append(f"({actor_str})")
    title = " ".join(title_parts)

    description_parts = []
    if notes:
        description_parts.append(notes)
    if fatalities > 0:
        description_parts.append(f"Fatalities: {fatalities}.")
    description = " ".join(description_parts)[:500] if description_parts else title

    return {
        "acled_id": f"acled-{event_id}",
        "title": title,
        "description": description,
        "severity": severity,
        "event_type": event_type,
        "disorder_type": disorder_type,
        "glide_type": glide_type,
        "fatalities": fatalities,
        "lat": lat,
        "lng": lng,
        "country": country,
        "admin1": admin1,
        "admin2": admin2,
        "location": location,
        "event_date": event_date,
        "source_url": raw.get("source_scale") or raw.get("source", ""),
        "raw": raw,
    }


def _fetch_for_country(
    cookies: dict,
    country: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """Fetch raw ACLED events for a single country within a date range."""
    url = f"{settings.acled_base_url}/api/acled/read"
    all_events: list[dict] = []
    page = 0

    with httpx.Client(cookies=cookies, timeout=60) as client:
        while page < 20:  # Safety cap: 20 pages × 5000 = 100k events
            page += 1
            params = {
                "country": country,
                "event_date": f"{start_date}|{end_date}",
                "event_date_where": "BETWEEN",
                "_format": "json",
                "limit": 5000,
                "page": page,
            }

            logger.info("Fetching ACLED page %d for %s (%s to %s)", page, country, start_date, end_date)

            try:
                resp = client.get(url, params=params)
                resp.raise_for_status()
            except httpx.HTTPError as e:
                logger.error("ACLED API request failed for %s: %s", country, e)
                break

            try:
                data = resp.json()
            except Exception as e:
                logger.error("ACLED JSON parse failed: %s, body=%s", e, resp.text[:300])
                break

            # Response can be direct list or {success, data: [...]}
            events: list[dict] = []
            if isinstance(data, list):
                events = data
            elif isinstance(data, dict):
                if not data.get("success", True):
                    logger.error("ACLED API error: %s", data.get("error"))
                    break
                events = data.get("data", [])

            logger.info("ACLED page %d returned %d events", page, len(events))

            if not events:
                break

            all_events.extend(events)

            if len(events) < 5000:
                break

    return all_events


def fetch_acled_events(since: datetime | None = None) -> list[dict]:
    """
    Fetch ACLED events for all configured countries within a time window.

    Deduplicates against Redis seen-set.
    """
    if since is None:
        since = datetime.now(UTC) - timedelta(days=settings.initial_lookback_days)

    now = datetime.now(UTC)
    start_date = since.strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    cookies = _authenticate()
    if not cookies:
        return []

    countries = [c.strip() for c in settings.acled_countries.split(",") if c.strip()]

    all_raw: list[dict] = []
    for country in countries:
        raw = _fetch_for_country(cookies, country, start_date, end_date)
        all_raw.extend(raw)

    # Parse and deduplicate
    events: list[dict] = []
    for raw in all_raw:
        parsed = _parse_event(raw)
        if not parsed:
            continue

        dedup_key = f"acled:seen:{parsed['acled_id']}"
        if _redis.exists(dedup_key):
            continue

        _redis.setex(dedup_key, settings.dedup_ttl_hours * 3600, "1")
        events.append(parsed)

    if events:
        set_last_synced(now)

    logger.info(
        "ACLED: %d new events after dedup (out of %d raw, %d countries)",
        len(events), len(all_raw), len(countries),
    )
    return events


def get_last_synced() -> datetime | None:
    val = _redis.get("acled:last_synced")
    if val:
        return datetime.fromisoformat(val)
    return None


def set_last_synced(ts: datetime) -> None:
    _redis.set("acled:last_synced", ts.isoformat())
