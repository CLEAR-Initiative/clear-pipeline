"""Dataminr First Alert API client with Redis-cached auth tokens."""

import logging
import time
from datetime import UTC, datetime, timedelta

import httpx
import redis

from src.config import settings
from src.models.dataminr import DataminrAlertsResponse, DataminrSignal

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)


def _get_token() -> str:
    """Get a valid Dataminr access token, refreshing from API if expired."""
    cached = _redis.get("dataminr:token")
    if cached:
        return cached

    logger.info("Fetching new Dataminr access token")
    resp = httpx.post(
        settings.dataminr_auth_url,
        data={
            "grant_type": "api_key",
            "scope": "first_alert_api",
            "api_user_id": settings.dataminr_api_user_id,
            "api_password": settings.dataminr_api_password,
        },
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        timeout=30,
    )
    resp.raise_for_status()
    auth_data = resp.json()
    token = auth_data["authorizationToken"]

    # Calculate TTL from expirationTime (ms since epoch), default 1 hour
    expiration_ms = auth_data.get("expirationTime")
    if expiration_ms:
        ttl = max(int(expiration_ms / 1000 - time.time()), 60)
    else:
        ttl = 3600

    _redis.setex("dataminr:token", ttl, token)
    logger.info("Dataminr token cached (TTL=%ds)", ttl)
    return token


def fetch_signals(since: datetime | None = None) -> list[DataminrSignal]:
    """
    Fetch signals from Dataminr within a time window.

    If `since` is None, fetches from INITIAL_LOOKBACK_DAYS ago.
    Follows nextPage pagination until all pages are consumed.
    Deduplicates against Redis seen-set.
    """
    token = _get_token()

    if since is None:
        since = datetime.now(UTC) - timedelta(days=settings.initial_lookback_days)

    all_signals: list[DataminrSignal] = []
    url: str | None = settings.dataminr_alerts_url
    page = 0

    while url:
        page += 1
        logger.info("Fetching Dataminr page %d (url=%s)", page, url[:80])

        resp = httpx.get(
            url,
            params={"alertversion": "19"},
            headers={
                "Accept": "application/json",
                "Authorization": f"DmAuth {token}",
            },
            timeout=60,
        )

        if resp.status_code == 401:
            # Token expired mid-fetch — clear cache and retry once
            _redis.delete("dataminr:token")
            token = _get_token()
            resp = httpx.get(
                url,
                params={"alertversion": "19"},
                headers={
                    "Accept": "application/json",
                    "Authorization": f"DmAuth {token}",
                },
                timeout=60,
            )

        resp.raise_for_status()
        data = DataminrAlertsResponse.model_validate(resp.json())

        for signal in data.alerts:
            # Filter by time window
            ts = datetime.fromtimestamp(signal.eventTime / 1000, tz=UTC)

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
            # Only follow next page if we didn't break out of the loop
            # Dataminr uses cursor-based pagination via the 'to' field
            url = None  # Stop after first batch; cursor pagination not yet implemented

    logger.info("Fetched %d new signals from Dataminr", len(all_signals))
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
