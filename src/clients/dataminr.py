"""Dataminr First Alert API client with Redis-cached auth tokens and rate limiting."""

import logging
import time
from datetime import UTC, datetime, timedelta

import httpx
import redis

from src.config import settings
from src.models.dataminr import DataminrAlertsResponse, DataminrSignal

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


def _request_with_rate_limit(method: str, url: str, token: str, **kwargs) -> httpx.Response:
    """Make an HTTP request with rate limiting and retry on 429."""
    _wait_for_rate_limit()
    _record_request()

    resp = httpx.request(
        method,
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            **(kwargs.pop("headers", {})),
        },
        timeout=kwargs.pop("timeout", 60),
        **kwargs,
    )

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "30"))
        logger.warning("Dataminr 429 rate limited, backing off %ds", retry_after)
        time.sleep(retry_after)
        _wait_for_rate_limit()
        _record_request()
        resp = httpx.request(
            method,
            url,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            },
            timeout=60,
        )

    return resp


def _get_token() -> str:
    """Get a valid Dataminr access token, refreshing from API if expired."""
    cached = _redis.get("dataminr:token")
    if cached:
        return cached

    logger.info("Fetching new Dataminr access token")
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


def fetch_signals(since: datetime | None = None) -> list[DataminrSignal]:
    """
    Fetch signals from Dataminr within a time window.

    If `since` is None, fetches from INITIAL_LOOKBACK_DAYS ago.
    Follows nextPage pagination until all pages are consumed.
    Deduplicates against Redis seen-set.
    Respects rate limits with backoff on 429.
    """
    token = _get_token()

    if since is None:
        since = datetime.now(UTC) - timedelta(days=settings.initial_lookback_days)

    all_signals: list[DataminrSignal] = []
    latest_seen: datetime | None = None  # Track latest timestamp from API (even deduped)
    url: str | None = settings.dataminr_alerts_url
    page = 0

    while url and page < settings.max_pages_per_poll:
        page += 1
        logger.info("Fetching Dataminr page %d (url=%s...)", page, url[:80])

        resp = _request_with_rate_limit("GET", url, token)

        if resp.status_code == 401:
            # Token expired mid-fetch — clear cache and retry once
            _redis.delete("dataminr:token")
            token = _get_token()
            resp = _request_with_rate_limit("GET", url, token)

        resp.raise_for_status()
        data = DataminrAlertsResponse.model_validate(resp.json())

        for signal in data.alerts:
            # Filter by time window
            try:
                ts = datetime.fromisoformat(signal.alertTimestamp.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue

            # Track the latest timestamp we've seen from the API
            if latest_seen is None or ts > latest_seen:
                latest_seen = ts

            if ts < since:
                # Reached signals older than our window — stop paginating
                logger.info("Reached signals before %s, stopping pagination", since.isoformat())
                url = None
                break

            # Deduplicate
            dedup_key = f"dataminr:seen:{signal.alertId}"
            if _redis.exists(dedup_key):
                continue

            _redis.setex(dedup_key, settings.dedup_ttl_hours * 3600, "1")
            all_signals.append(signal)

        else:
            # Only follow nextPage if we didn't break out of the loop
            next_page = data.nextPage
            if next_page:
                # nextPage may be a relative path or cursor — ensure it's a full URL
                if next_page.startswith("http"):
                    url = next_page
                elif next_page.startswith("/"):
                    # Dataminr returns paths like /v1/alerts?... which need /firstalert prefix
                    if not next_page.startswith("/firstalert"):
                        next_page = f"/firstalert{next_page}"
                    url = f"https://api.dataminr.com{next_page}"
                else:
                    url = f"{settings.dataminr_alerts_url}?nextPage={next_page}"
            else:
                url = None

    if page >= settings.max_pages_per_poll:
        logger.warning("Hit max page cap (%d), some older signals may be missed", settings.max_pages_per_poll)

    # Always update last_synced based on the latest timestamp seen from API
    if latest_seen:
        set_last_synced(latest_seen)
        logger.info("Updated last_synced to %s", latest_seen.isoformat())

    logger.info("Fetched %d new signals from Dataminr (across %d pages)", len(all_signals), page)
    return all_signals


def get_last_synced() -> datetime | None:
    """Get the last synced timestamp from Redis."""
    val = _redis.get("dataminr:last_synced")
    if val:
        return datetime.fromisoformat(val)
    return None


def set_last_synced(ts: datetime) -> None:
    """Store the last synced timestamp in Redis."""
    _redis.set("dataminr:last_synced", ts.isoformat())
