"""Dataminr First Alert API client with Redis-cached auth tokens."""

import json
import logging
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
    token = resp.json()["access_token"]
    _redis.setex("dataminr:token", settings.dataminr_token_ttl, token)
    logger.info("Dataminr token cached (TTL=%ds)", settings.dataminr_token_ttl)
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
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            },
            timeout=60,
        )

        if resp.status_code == 401:
            # Token expired mid-fetch — clear cache and retry once
            _redis.delete("dataminr:token")
            token = _get_token()
            resp = httpx.get(
                url,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {token}",
                },
                timeout=60,
            )

        resp.raise_for_status()
        data = DataminrAlertsResponse.model_validate(resp.json())

        for signal in data.alerts:
            # Filter by time window
            try:
                ts = datetime.fromisoformat(signal.alertTimestamp.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue

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
            url = data.nextPage

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
