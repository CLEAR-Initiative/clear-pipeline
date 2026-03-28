"""Dataminr First Alert API client with Redis-cached auth tokens and rate limiting.

Supports both the current API (api.dataminr.com) and the legacy API
(firstalert-api.dataminr.com) as a fallback when the current API fails.
"""

import logging
import time
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import httpx
import redis

from src.config import settings
from src.models.dataminr import DataminrAlertsResponse, DataminrSignal, EstimatedEventLocation

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

# Rate limit: track requests per minute via Redis
RATE_LIMIT_KEY = "dataminr:rate_limit"
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX_REQUESTS = 20  # max requests per window (conservative)


def _wait_for_rate_limit() -> None:
    """Block until we're under the rate limit."""
    while True:
        current = _redis.get(RATE_LIMIT_KEY)
        if current is None or int(current) < RATE_LIMIT_MAX_REQUESTS:
            break
        ttl = _redis.ttl(RATE_LIMIT_KEY)
        wait = max(ttl, 1)
        logger.warning("Dataminr rate limit reached (%s/%d), waiting %ds", current, RATE_LIMIT_MAX_REQUESTS, wait)
        time.sleep(wait)


def _record_request() -> None:
    """Increment the rate limit counter."""
    pipe = _redis.pipeline()
    pipe.incr(RATE_LIMIT_KEY)
    pipe.expire(RATE_LIMIT_KEY, RATE_LIMIT_WINDOW)
    pipe.execute()


def _request_with_rate_limit(method: str, url: str, token: str, auth_scheme: str = "Bearer", **kwargs) -> httpx.Response:
    """Make an HTTP request with rate limiting and retry on 429."""
    _wait_for_rate_limit()
    _record_request()

    headers = {
        "Accept": "application/json",
        "Authorization": f"{auth_scheme} {token}",
        **(kwargs.pop("headers", {})),
    }

    resp = httpx.request(method, url, headers=headers, timeout=kwargs.pop("timeout", 60), **kwargs)

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "30"))
        logger.warning("Dataminr 429 rate limited, backing off %ds", retry_after)
        time.sleep(retry_after)
        _wait_for_rate_limit()
        _record_request()
        resp = httpx.request(method, url, headers=headers, timeout=60)

    return resp


# ═══════════════════════════════════════════════════════════════════════════════
# Current API (api.dataminr.com)
# ═══════════════════════════════════════════════════════════════════════════════

def _get_token() -> str:
    """Get a valid Dataminr access token (current API), refreshing from API if expired."""
    cached = _redis.get("dataminr:token")
    if cached:
        return cached

    logger.info("Fetching new Dataminr access token (current API)")
    _wait_for_rate_limit()
    _record_request()

    resp = httpx.post(
        settings.dataminr_auth_url,
        data={
            "grant_type": "api_key",
            "client_id": settings.dataminr_client_id,
            "client_secret": settings.dataminr_client_secret,
        },
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    logger.debug("Dataminr auth response keys: %s", list(body.keys()))
    token = body.get("dmaToken") or body.get("token") or body.get("access_token")
    if not token:
        raise RuntimeError(f"No token in Dataminr auth response: {list(body.keys())}")
    _redis.setex("dataminr:token", settings.dataminr_token_ttl, token)
    logger.info("Dataminr token cached (TTL=%ds)", settings.dataminr_token_ttl)
    return token


def _fetch_signals_current(since: datetime) -> list[DataminrSignal]:
    """Fetch signals using the current API (api.dataminr.com)."""
    token = _get_token()

    all_signals: list[DataminrSignal] = []
    latest_seen: datetime | None = None
    url: str | None = settings.dataminr_alerts_url
    page = 0

    while url and page < settings.max_pages_per_poll:
        page += 1
        logger.info("Fetching Dataminr page %d (url=%s...)", page, url[:80])

        resp = _request_with_rate_limit("GET", url, token)

        if resp.status_code == 401:
            _redis.delete("dataminr:token")
            token = _get_token()
            resp = _request_with_rate_limit("GET", url, token)

        resp.raise_for_status()
        data = DataminrAlertsResponse.model_validate(resp.json())

        for signal in data.alerts:
            try:
                ts = datetime.fromisoformat(signal.alertTimestamp.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue

            if latest_seen is None or ts > latest_seen:
                latest_seen = ts

            if ts < since:
                logger.info("Reached signals before %s, stopping pagination", since.isoformat())
                url = None
                break

            dedup_key = f"dataminr:seen:{signal.alertId}"
            if _redis.exists(dedup_key):
                continue

            _redis.setex(dedup_key, settings.dedup_ttl_hours * 3600, "1")
            all_signals.append(signal)

        else:
            next_page = data.nextPage
            if next_page:
                if next_page.startswith("http"):
                    url = next_page
                elif next_page.startswith("/"):
                    if not next_page.startswith("/firstalert"):
                        next_page = f"/firstalert{next_page}"
                    parsed = urlparse(settings.dataminr_alerts_url)
                    base_url = f"{parsed.scheme}://{parsed.netloc}"
                    url = f"{base_url}{next_page}"
                else:
                    url = f"{settings.dataminr_alerts_url}?nextPage={next_page}"
            else:
                url = None

    if page >= settings.max_pages_per_poll:
        logger.warning("Hit max page cap (%d), some older signals may be missed", settings.max_pages_per_poll)

    if latest_seen:
        set_last_synced(latest_seen)
        logger.info("Updated last_synced to %s", latest_seen.isoformat())

    logger.info("Fetched %d new signals from Dataminr current API (across %d pages)", len(all_signals), page)
    return all_signals


# ═══════════════════════════════════════════════════════════════════════════════
# Legacy API (firstalert-api.dataminr.com)
# ═══════════════════════════════════════════════════════════════════════════════

def _get_legacy_token() -> str:
    """Get a valid Dataminr access token (legacy API)."""
    cached = _redis.get("dataminr:legacy_token")
    if cached:
        return cached

    if not settings.dataminr_legacy_user_id or not settings.dataminr_legacy_password:
        raise RuntimeError(
            "Legacy API credentials not configured. "
            "Set DATAMINR_LEGACY_USER_ID and DATAMINR_LEGACY_PASSWORD in .env"
        )

    logger.info("Fetching new Dataminr access token (legacy API)")
    _wait_for_rate_limit()
    _record_request()

    auth_url = f"{settings.dataminr_legacy_base_url}/auth/1/userAuthorization"
    resp = httpx.post(
        auth_url,
        data={
            "grant_type": "api_key",
            "scope": "first_alert_api",
            "api_user_id": settings.dataminr_legacy_user_id,
            "api_password": settings.dataminr_legacy_password,
        },
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    token = body.get("authorizationToken")
    if not token:
        raise RuntimeError(f"No authorizationToken in legacy auth response: {list(body.keys())}")

    # Compute TTL from expirationTime (ms since epoch) or default 1h
    expiration_time = body.get("expirationTime")
    if expiration_time:
        expires_in = max(int(expiration_time / 1000 - datetime.now(UTC).timestamp()), 300)
    else:
        expires_in = 3600

    _redis.setex("dataminr:legacy_token", expires_in, token)
    logger.info("Dataminr legacy token cached (TTL=%ds)", expires_in)
    return token


def _convert_legacy_alert(alert: dict) -> DataminrSignal | None:
    """Convert a legacy API alert dict into a DataminrSignal model.

    Legacy format differences:
    - estimatedEventLocation is an array [name, lat, lng, radius, mgrs]
    - eventTime is milliseconds since epoch (not ISO string)
    - alertId field name is the same
    """
    alert_id = alert.get("alertId")
    if not alert_id:
        return None

    # Convert eventTime (ms epoch) to ISO string
    event_time = alert.get("eventTime")
    if event_time:
        alert_timestamp = datetime.fromtimestamp(event_time / 1000, tz=UTC).isoformat()
    else:
        alert_timestamp = datetime.now(UTC).isoformat()

    # Convert location array to object
    location = None
    loc_data = alert.get("estimatedEventLocation", [])
    if isinstance(loc_data, list) and len(loc_data) >= 3:
        try:
            lat = float(loc_data[1])
            lng = float(loc_data[2])
            location = EstimatedEventLocation(
                name=loc_data[0] if loc_data[0] else None,
                coordinates=[lat, lng],
                probabilityRadius=float(loc_data[3]) if len(loc_data) > 3 and loc_data[3] else None,
                MGRS=loc_data[4] if len(loc_data) > 4 else None,
            )
        except (ValueError, IndexError):
            pass

    return DataminrSignal(
        alertId=alert_id,
        alertTimestamp=alert_timestamp,
        estimatedEventLocation=location,
        headline=alert.get("headline"),
        subHeadline=alert.get("subHeadline"),
        publicPost=alert.get("publicPost"),
        alertType=alert.get("alertType"),
        dataminrAlertUrl=alert.get("firstAlertURL"),
        listsMatched=alert.get("alertLists"),
        alertTopics=alert.get("alertTopics"),
        linkedAlerts=alert.get("linkedAlerts"),
        termsOfUse=alert.get("termsOfUse"),
    )


def _fetch_signals_legacy(since: datetime) -> list[DataminrSignal]:
    """Fetch signals using the legacy API (firstalert-api.dataminr.com)."""
    token = _get_legacy_token()

    all_signals: list[DataminrSignal] = []
    latest_seen: datetime | None = None
    alerts_url = f"{settings.dataminr_legacy_base_url}/alerts/1/alerts"
    page = 0
    cursor_from: str | None = None

    while page < settings.max_pages_per_poll:
        page += 1

        params: dict = {"alertversion": str(settings.dataminr_alert_version)}
        if cursor_from:
            params["from"] = cursor_from

        logger.info("Fetching Dataminr legacy page %d", page)

        resp = _request_with_rate_limit("GET", alerts_url, token, auth_scheme="DmAuth", params=params)

        if resp.status_code == 401:
            _redis.delete("dataminr:legacy_token")
            token = _get_legacy_token()
            resp = _request_with_rate_limit("GET", alerts_url, token, auth_scheme="DmAuth", params=params)

        resp.raise_for_status()
        data = resp.json()
        alerts = data.get("alerts", [])
        cursor_to = data.get("to")

        if not alerts:
            break

        for raw_alert in alerts:
            signal = _convert_legacy_alert(raw_alert)
            if not signal:
                continue

            try:
                ts = datetime.fromisoformat(signal.alertTimestamp.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue

            if latest_seen is None or ts > latest_seen:
                latest_seen = ts

            if ts < since:
                logger.info("Reached signals before %s, stopping legacy pagination", since.isoformat())
                cursor_to = None
                break

            dedup_key = f"dataminr:seen:{signal.alertId}"
            if _redis.exists(dedup_key):
                continue

            _redis.setex(dedup_key, settings.dedup_ttl_hours * 3600, "1")
            all_signals.append(signal)

        if not cursor_to:
            break
        cursor_from = cursor_to

    if latest_seen:
        set_last_synced(latest_seen)
        logger.info("Updated last_synced to %s (legacy)", latest_seen.isoformat())

    logger.info("Fetched %d new signals from Dataminr legacy API (across %d pages)", len(all_signals), page)
    return all_signals


# ═══════════════════════════════════════════════════════════════════════════════
# Public API — auto-fallback from current to legacy
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_signals(since: datetime | None = None) -> list[DataminrSignal]:
    """
    Fetch signals from Dataminr within a time window.

    Uses the current API by default, falling back to legacy API on failure.
    Set DATAMINR_USE_LEGACY=true to force legacy API.

    Deduplicates against Redis seen-set.
    Respects rate limits with backoff on 429.
    """
    if since is None:
        since = datetime.now(UTC) - timedelta(days=settings.initial_lookback_days)

    if settings.dataminr_use_legacy:
        logger.info("Using Dataminr legacy API (forced via config)")
        return _fetch_signals_legacy(since)

    try:
        return _fetch_signals_current(since)
    except Exception as e:
        logger.warning("Current Dataminr API failed (%s), falling back to legacy API", e)

        # Check if legacy credentials are configured
        if not settings.dataminr_legacy_user_id or not settings.dataminr_legacy_password:
            logger.error("Legacy API credentials not configured, cannot fallback")
            raise

        try:
            return _fetch_signals_legacy(since)
        except Exception as legacy_err:
            logger.error("Legacy Dataminr API also failed: %s", legacy_err)
            raise


def get_last_synced() -> datetime | None:
    """Get the last synced timestamp from Redis."""
    val = _redis.get("dataminr:last_synced")
    if val:
        return datetime.fromisoformat(val)
    return None


def set_last_synced(ts: datetime) -> None:
    """Store the last synced timestamp in Redis."""
    _redis.set("dataminr:last_synced", ts.isoformat())
