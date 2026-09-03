"""ACLED (Armed Conflict Location & Event Data Project) API client.

Authentication: session cookies via POST /user/login (24h expiry).
Endpoint: /api/acled/read with country + event_date filters.

Docs: https://acleddata.com/api-documentation/

Ported from clear-pipeline (src/clients/acled.py + src/tasks/poll_acled.py) as a
pure provider module for the Dagster consolidation.
"""

import json
import logging
from datetime import UTC, datetime, timedelta

import httpx
import redis

from clear_pipeline.providers.signal import enrich_with_geoparser
from clear_pipeline.signals.config import settings

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
            cookies = json.loads(cached)
            logger.info("[ACLED] Using cached session cookies (%d cookies)", len(cookies))
            return cookies
        except json.JSONDecodeError:
            _redis.delete(ACLED_COOKIE_KEY)

    if not settings.acled_username or not settings.acled_password:
        logger.error("[ACLED] credentials not configured (ACLED_USERNAME, ACLED_PASSWORD)")
        return None

    logger.info("[ACLED] Authenticating fresh session for user=%s", settings.acled_username)

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
    sub_event_type = raw.get("sub_event_type", "") or ""
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
        "sub_event_type": sub_event_type,
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

    logger.info(
        "[ACLED] Fetching events for country=%s date_range=%s|%s url=%s",
        country, start_date, end_date, url,
    )

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

            logger.info("[ACLED] GET page=%d params=%s", page, params)

            try:
                resp = client.get(url, params=params)
                logger.info(
                    "[ACLED] page=%d status=%d content-type=%s body_size=%d",
                    page, resp.status_code, resp.headers.get("content-type", "?"), len(resp.content),
                )
                resp.raise_for_status()
            except httpx.HTTPError as e:
                logger.error("[ACLED] API request failed for %s page %d: %s", country, page, e)
                if hasattr(e, "response") and e.response is not None:
                    logger.error("[ACLED] response body: %s", e.response.text[:500])
                break

            if not resp.text.strip():
                logger.warning("[ACLED] empty response body for %s page %d", country, page)
                break

            try:
                data = resp.json()
            except Exception as e:
                logger.error("[ACLED] JSON parse failed: %s, body=%s", e, resp.text[:500])
                break

            # Response can be direct list or {success, data: [...]}
            events: list[dict] = []
            if isinstance(data, list):
                events = data
                logger.info("[ACLED] page=%d (direct list format)", page)
            elif isinstance(data, dict):
                logger.info("[ACLED] page=%d (wrapped format) keys=%s", page, list(data.keys()))
                if not data.get("success", True):
                    logger.error("[ACLED] API error response: %s", data)
                    break
                events = data.get("data", [])
                if "count" in data:
                    logger.info("[ACLED] API reports total count=%s", data["count"])
                if "messages" in data:
                    logger.info("[ACLED] API messages: %s", data["messages"])
            else:
                logger.error("[ACLED] unexpected response type: %s", type(data).__name__)
                break

            logger.info("[ACLED] page %d returned %d events for %s", page, len(events), country)

            if not events:
                break

            all_events.extend(events)

            if len(events) < 5000:
                break

    logger.info("[ACLED] Total fetched %d events for %s across %d pages", len(all_events), country, page)
    return all_events


def fetch_acled_events(since: datetime | None = None) -> list[dict]:
    """
    Fetch ACLED events for all configured countries within a time window.

    Deduplicates against Redis seen-set.
    """
    if since is None:
        since = datetime.now(UTC) - timedelta(days=settings.initial_lookback_days)
        logger.info("[ACLED] No 'since' provided, using initial lookback of %d days", settings.initial_lookback_days)

    now = datetime.now(UTC)
    # ACLED releases data in weekly batches where `event_date` lags publication by
    # days, and the server-side filter is on `event_date` — so an event published
    # after our watermark but DATED before it would be filtered out forever.
    # Overscan the window by the publication lag; the Redis seen-set + clear-api's
    # (sourceId, externalId) idempotency dedup the re-fetched rows.
    query_since = since - timedelta(days=settings.acled_publication_lag_days)
    start_date = query_since.strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    logger.info("[ACLED] Fetch window: %s → %s", start_date, end_date)

    cookies = _authenticate()
    if not cookies:
        logger.error("[ACLED] Authentication failed, aborting fetch")
        return []

    countries = [c.strip() for c in settings.acled_countries.split(",") if c.strip()]
    logger.info("[ACLED] Configured countries: %s", countries)

    all_raw: list[dict] = []
    for country in countries:
        raw = _fetch_for_country(cookies, country, start_date, end_date)
        logger.info("[ACLED] %s: %d raw events fetched", country, len(raw))
        all_raw.extend(raw)

    logger.info("[ACLED] Total raw events across all countries: %d", len(all_raw))

    # Parse + dedup. The seen-set is only READ here — it is MARKED (mark_seen)
    # after createSignal succeeds (via the connector's post_create), and the
    # watermark is advanced by the ingest asset after a clean batch. That way a
    # record whose persistence fails stays un-seen and inside the poll window, so
    # the next poll re-fetches it instead of losing it (clear-api's
    # (sourceId, externalId) upsert dedups the re-fetch).
    events: list[dict] = []
    parse_failed = 0
    deduped = 0
    batch_ids: set[str] = set()
    for raw in all_raw:
        parsed = _parse_event(raw)
        if not parsed:
            parse_failed += 1
            continue

        acled_id = parsed["acled_id"]
        if acled_id in batch_ids or _redis.exists(f"acled:seen:{acled_id}"):
            deduped += 1
            continue
        batch_ids.add(acled_id)
        events.append(parsed)

    logger.info(
        "[ACLED] Result: %d new events (parse_failed=%d, already_seen=%d) out of %d raw",
        len(events), parse_failed, deduped, len(all_raw),
    )
    return events


def mark_seen(acled_id: str) -> None:
    """Mark an event ingested (Redis seen-set) — called only after createSignal
    is confirmed, so a failed persistence leaves the event eligible for re-poll."""
    _redis.setex(f"acled:seen:{acled_id}", settings.dedup_ttl_hours * 3600, "1")


def get_last_synced() -> datetime | None:
    val = _redis.get("acled:last_synced")
    if val:
        return datetime.fromisoformat(val)
    return None


def set_last_synced(ts: datetime) -> None:
    _redis.set("acled:last_synced", ts.isoformat())


def build_acled_signal_input(event: dict, source_id: str) -> dict:
    """Convert a parsed ACLED event into a CLEAR CreateSignalInput dict."""
    # Parse event_date into ISO-8601
    event_date = event.get("event_date", "")
    try:
        published_at = datetime.strptime(event_date, "%Y-%m-%d").replace(tzinfo=UTC).isoformat()
    except (ValueError, TypeError):
        published_at = datetime.now(UTC).isoformat()

    input_data: dict = {
        "sourceId": source_id,
        # Dedup key — ACLED retroactively updates events across rounds and
        # sometimes re-emits them; (sourceId, externalId) ensures we only
        # ever store one row per event.
        "externalId": f"acled:{event['acled_id']}",
        "rawData": event["raw"],
        "publishedAt": published_at,
        "title": event["title"],
        "description": event.get("description"),
        "severity": event.get("severity"),
        # ACLED ships `fatalities` directly; pass through as casualties.
        # Event-level casualties is derived from a per-sub_event_type lookup
        # in the grouping layer; this signal-level value is just the raw count.
        "casualties": event.get("fatalities") or None,
    }

    if event.get("source_url"):
        input_data["url"] = event["source_url"]

    # Pass lat/lng for server-side PostGIS geo-resolution. Set before the
    # geoparser call below so the same-A2 safety check has source coords.
    if event.get("lat") is not None and event.get("lng") is not None:
        input_data["lat"] = event["lat"]
        input_data["lng"] = event["lng"]

    # Text-based geoparser: enrich `geoparsedData` and, when a landmark
    # resolves cleanly, promote it to a reusable L4 (overriding the default
    # "signal-title L4" branch in clear-api). Best-effort; failures are
    # swallowed and the source coords win.
    enrich_with_geoparser(
        input_data,
        title=event["title"],
        description=event.get("description"),
        log_tag=f"acled:{event.get('acled_id')}",
    )

    return input_data
