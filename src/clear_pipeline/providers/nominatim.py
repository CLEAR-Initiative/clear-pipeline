"""Nominatim-protocol geocoder client.

Talks to a Nominatim-compatible HTTP API (currently LocationIQ; could swap
to MapTiler / self-hosted Nominatim by changing settings only). All callers
go through a persistent cache (clear-api's `nominatim_cache` table) — only
true cache misses hit the upstream geocoder. Negative results (no_result,
error) are cached too so we don't re-ask the same dead question.

Three layers of protection:
  - **Cache** (persistent, via clear-api): primary lookup path. Successful
    geocodes cached ~6 months; empty results ~30 days; errors 1 hour.
  - **Rate limit** (Redis-shared across workers): minimum interval between
    upstream calls — defaults to 1 req/sec, well under LocationIQ's 2/sec
    burst limit. Workers sleep until the next slot is available.
  - **Circuit breaker** (Redis-shared): after N consecutive failures, stop
    calling the upstream entirely for a configurable window. While open,
    `search()` / `reverse()` return None and callers fall back to coord-
    based resolution.

All operations are best-effort enrichment, not critical-path. Failure to
geocode never raises to the caller — it returns None and the caller can
decide what to do (typically: skip the enrichment, keep the source's
coords).
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any

import httpx
import redis

from clear_pipeline.providers import clear_api as graphql
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

# ─── Redis keys for shared rate-limit and circuit state ──────────────────
_RATE_LIMIT_KEY = "nominatim:last_call_ts"
_CIRCUIT_FAILURES_KEY = "nominatim:circuit:failures"
_CIRCUIT_OPEN_UNTIL_KEY = "nominatim:circuit:open_until"

# Outbound HTTP timeout. Generous — LocationIQ's median response is <500ms
# but tail latency can spike during their incident windows.
_HTTP_TIMEOUT_SECONDS = 10.0

# Module-level Redis client — same pattern other clear-pipeline services
# (event_grouping_v2, notify) use. Connection is lazy.
_redis = redis.from_url(settings.redis_url, decode_responses=True)

# Track whether we've already warned about the missing API key in this
# process. We log on first attempted call (not at import time) so the
# warning lives next to whichever signal first tried to geocode — much
# easier to correlate than a one-shot startup log.
_warned_no_api_key = False


def _warn_missing_api_key_once() -> None:
    """Emit a single WARNING per worker process when LOCATIONIQ_API_KEY is
    missing. Pre-fix this failure mode was completely silent — every call
    returned None and no log line appeared, making it look like the
    geoparser was misbehaving when the actual issue was config."""
    global _warned_no_api_key
    if _warned_no_api_key:
        return
    _warned_no_api_key = True
    logger.warning(
        "[nominatim] LOCATIONIQ_API_KEY is not set — geocoder is DISABLED. "
        "Every geoparser lookup will return None and fall back to source "
        "coords. Set the env var on the worker process to enable geocoding."
    )


def _normalize_query(query: str) -> str:
    """Normalisation must be identical on every call site so the cache key
    is stable. Whitespace collapsed, lowercased, edges trimmed."""
    return " ".join(query.lower().strip().split())


def _compute_query_hash(endpoint: str, normalised_query: str, params_signature: str = "") -> str:
    """SHA-256 over `<endpoint>:<normalised_query>[:<params>]`. The params
    signature lets us distinguish queries that differ only by call params
    (e.g. different country bias, different limit)."""
    payload = f"{endpoint}:{normalised_query}"
    if params_signature:
        payload = f"{payload}:{params_signature}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ─── Circuit breaker ──────────────────────────────────────────────────────


def _is_circuit_open() -> bool:
    """True if the breaker is tripped and we should skip upstream calls."""
    open_until = _redis.get(_CIRCUIT_OPEN_UNTIL_KEY)
    if open_until is None:
        return False
    try:
        return time.time() < float(open_until)
    except (TypeError, ValueError):
        return False


def _record_success() -> None:
    """Reset the failure counter on any successful call."""
    _redis.delete(_CIRCUIT_FAILURES_KEY)


def _record_failure() -> None:
    """Increment the failure counter; trip the breaker once the threshold
    is crossed."""
    failures = _redis.incr(_CIRCUIT_FAILURES_KEY)
    # Belt-and-braces: never let the counter grow unboundedly even if the
    # reset path fails.
    _redis.expire(_CIRCUIT_FAILURES_KEY, 60 * 60)
    if failures >= settings.geocoder_circuit_failure_threshold:
        open_until = time.time() + settings.geocoder_circuit_open_seconds
        _redis.set(_CIRCUIT_OPEN_UNTIL_KEY, open_until, ex=settings.geocoder_circuit_open_seconds + 60)
        logger.warning(
            "[nominatim] circuit breaker TRIPPED after %d failures — "
            "skipping upstream calls for %ds",
            failures, settings.geocoder_circuit_open_seconds,
        )


# ─── Rate limiter ─────────────────────────────────────────────────────────


def _wait_for_rate_limit_slot() -> None:
    """Block until at least `geocoder_min_interval_seconds` has passed since
    the last upstream call (across all workers).

    The Redis state is intentionally simple — last-call timestamp only.
    Two workers racing on the same slot is acceptable because LocationIQ's
    rate limit (2/sec burst) tolerates the occasional double-fire; we
    target 1/sec sustained to leave headroom.
    """
    min_interval = settings.geocoder_min_interval_seconds
    last_call_raw = _redis.get(_RATE_LIMIT_KEY)
    last_call = 0.0
    if last_call_raw is not None:
        try:
            last_call = float(last_call_raw)
        except (TypeError, ValueError):
            last_call = 0.0

    now = time.time()
    elapsed = now - last_call
    if elapsed < min_interval:
        sleep_for = min_interval - elapsed
        logger.debug("[nominatim] rate-limit sleep %.3fs", sleep_for)
        time.sleep(sleep_for)

    _redis.set(_RATE_LIMIT_KEY, time.time(), ex=60 * 60)


# ─── Cache helpers ────────────────────────────────────────────────────────


def _cache_lookup(query_hash: str) -> dict | None:
    """Returns the cached response payload if a usable entry exists.
    Returns the literal dict {'status': 'no_result'} for cached negative
    results so the caller can short-circuit without re-asking upstream.
    Returns None when there is no cache entry at all (a real miss)."""
    try:
        entry = graphql.get_nominatim_cache_entry(query_hash)
    except Exception as exc:
        logger.warning("[nominatim] cache lookup failed (treating as miss): %s", exc)
        return None
    if not entry:
        return None
    # `responseJson` arrives as a parsed dict/list because clear-api stores
    # it as JSONB and the GraphQL JSON scalar passes it through.
    return {"status": entry["status"], "responseJson": entry.get("responseJson")}


def _cache_write(
    *,
    query_hash: str,
    query: str,
    endpoint: str,
    response_json: Any,
    status: str,
) -> None:
    """Persist a response to the cache. Picks the TTL from settings based
    on status. Failures are logged but never propagate."""
    if status == "ok":
        ttl = settings.geocoder_cache_ttl_ok_seconds
    elif status == "no_result":
        ttl = settings.geocoder_cache_ttl_no_result_seconds
    else:
        ttl = settings.geocoder_cache_ttl_error_seconds
    try:
        graphql.upsert_nominatim_cache(
            query_hash=query_hash,
            query=query,
            endpoint=endpoint,
            response_json=response_json if response_json is not None else {},
            status=status,
            ttl_seconds=ttl,
        )
    except Exception as exc:
        logger.warning("[nominatim] cache write failed (continuing): %s", exc)


# ─── Public API ───────────────────────────────────────────────────────────


def search(
    query: str,
    *,
    country_codes: str | None = "sd",
    limit: int = 5,
) -> list[dict] | None:
    """Forward geocoding — name → ranked list of candidates.

    Returns the raw Nominatim result array (list of dicts with `lat`, `lon`,
    `display_name`, `class`, `type`, `importance`, etc.) on success.
    Returns None for any non-success path: no cache hit + upstream failure,
    cached no_result, cached error, circuit open, or empty result list.

    Callers decide what to do with None — typically: skip geocoder
    enrichment for this signal and fall back to source-supplied coords.
    """
    if not query or not query.strip():
        return None
    if not settings.locationiq_api_key:
        # Geocoder disabled by config. Equivalent to a permanent circuit-open.
        _warn_missing_api_key_once()
        return None

    normalised = _normalize_query(query)
    params_sig = f"cc={country_codes or ''}&limit={limit}"
    qhash = _compute_query_hash("search", normalised, params_sig)

    # 1. Cache lookup
    cached = _cache_lookup(qhash)
    if cached is not None:
        if cached["status"] == "ok":
            return cached["responseJson"]
        # 'no_result' or 'error' — cached negative. Log because a stale
        # negative on a query LocationIQ would now happily answer is very
        # hard to spot without this line — the caller just sees a None
        # and assumes OSM has no coverage. TTLs: no_result ~30 days,
        # error ~1 hour (see settings.geocoder_cache_ttl_*_seconds).
        logger.info(
            "[nominatim] cached negative short-circuit: query=%r status=%s (hash=%s)",
            query, cached["status"], qhash[:8],
        )
        return None

    # 2. Circuit breaker
    if _is_circuit_open():
        logger.warning("[nominatim] circuit open, skipping search for %r", query)
        return None

    # 3. Outbound call
    _wait_for_rate_limit_slot()
    params: dict[str, Any] = {
        "q": query,
        "format": "json",
        "limit": limit,
        "key": settings.locationiq_api_key,
        "addressdetails": 1,
    }
    if country_codes:
        params["countrycodes"] = country_codes
    headers = {"User-Agent": settings.geocoder_user_agent}

    try:
        resp = httpx.get(
            f"{settings.locationiq_base_url.rstrip('/')}/search",
            params=params,
            headers=headers,
            timeout=_HTTP_TIMEOUT_SECONDS,
        )
    except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPError) as exc:
        logger.warning("[nominatim] network error for %r: %s", query, exc)
        _record_failure()
        return None

    if resp.status_code == 200:
        try:
            results = resp.json()
        except json.JSONDecodeError:
            logger.warning("[nominatim] unparseable 200 response for %r", query)
            _record_failure()
            return None
        if not isinstance(results, list) or not results:
            _cache_write(query_hash=qhash, query=query, endpoint="search",
                         response_json=[], status="no_result")
            _record_success()
            return None
        _cache_write(query_hash=qhash, query=query, endpoint="search",
                     response_json=results, status="ok")
        _record_success()
        return results

    # 404 from LocationIQ's /search means no results — treat as cacheable no_result.
    if resp.status_code == 404:
        _cache_write(query_hash=qhash, query=query, endpoint="search",
                     response_json={"http_status": 404}, status="no_result")
        _record_success()
        return None

    # 429 / 5xx — degraded geocoder, cache as error briefly so we back off.
    logger.warning("[nominatim] HTTP %s for %r: %s", resp.status_code, query, resp.text[:200])
    _cache_write(query_hash=qhash, query=query, endpoint="search",
                 response_json={"http_status": resp.status_code, "body_snippet": resp.text[:500]},
                 status="error")
    _record_failure()
    return None


def reverse(lat: float, lng: float) -> dict | None:
    """Reverse geocoding — coordinate → containing place.

    Returns the raw Nominatim object on success (dict with `display_name`,
    `address` block, `lat`, `lon`, etc.). Returns None on failure.

    The cache key includes the coordinates rounded to 5 decimal places
    (~1m precision) so two reverse queries at nearly the same point share
    the cache entry. Anything finer than that would defeat caching for
    drifting GPS reports.
    """
    if not settings.locationiq_api_key:
        return None

    rounded_query = f"{lat:.5f},{lng:.5f}"
    qhash = _compute_query_hash("reverse", rounded_query)

    cached = _cache_lookup(qhash)
    if cached is not None:
        if cached["status"] == "ok":
            return cached["responseJson"]
        return None

    if _is_circuit_open():
        logger.warning("[nominatim] circuit open, skipping reverse for %s", rounded_query)
        return None

    _wait_for_rate_limit_slot()
    params = {
        "lat": lat,
        "lon": lng,
        "format": "json",
        "key": settings.locationiq_api_key,
        "addressdetails": 1,
    }
    headers = {"User-Agent": settings.geocoder_user_agent}

    try:
        resp = httpx.get(
            f"{settings.locationiq_base_url.rstrip('/')}/reverse",
            params=params,
            headers=headers,
            timeout=_HTTP_TIMEOUT_SECONDS,
        )
    except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPError) as exc:
        logger.warning("[nominatim] reverse network error for %s: %s", rounded_query, exc)
        _record_failure()
        return None

    if resp.status_code == 200:
        try:
            data = resp.json()
        except json.JSONDecodeError:
            logger.warning("[nominatim] unparseable 200 reverse response for %s", rounded_query)
            _record_failure()
            return None
        if not isinstance(data, dict) or "error" in data:
            _cache_write(query_hash=qhash, query=rounded_query, endpoint="reverse",
                         response_json=data, status="no_result")
            _record_success()
            return None
        _cache_write(query_hash=qhash, query=rounded_query, endpoint="reverse",
                     response_json=data, status="ok")
        _record_success()
        return data

    if resp.status_code == 404:
        _cache_write(query_hash=qhash, query=rounded_query, endpoint="reverse",
                     response_json={"http_status": 404}, status="no_result")
        _record_success()
        return None

    logger.warning("[nominatim] reverse HTTP %s for %s: %s",
                   resp.status_code, rounded_query, resp.text[:200])
    _cache_write(query_hash=qhash, query=rounded_query, endpoint="reverse",
                 response_json={"http_status": resp.status_code, "body_snippet": resp.text[:500]},
                 status="error")
    _record_failure()
    return None
