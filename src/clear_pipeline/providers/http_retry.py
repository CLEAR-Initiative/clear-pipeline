"""Shared HTTP GET with rate-limit (429) + transient (5xx) retry.

The location-metadata ingests fan out several assets that hit the SAME upstream
API (all 7 HAPI endpoints share one HAPI rate limit), so 429s are expected under
parallelism. This wrapper retries them with backoff, honouring a numeric
``Retry-After`` header when the server sends one, so a burst self-heals instead
of failing the asset. 4xx other than 429 are caller bugs — raised immediately.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_MAX_BACKOFF_SECONDS = 60.0


def _retry_after_seconds(resp: httpx.Response) -> float | None:
    """Parse a numeric ``Retry-After`` (seconds). HTTP-date form is ignored
    (we fall back to exponential backoff) to keep this dependency-free."""
    raw = resp.headers.get("Retry-After")
    if raw and raw.strip().isdigit():
        return min(float(raw.strip()), _MAX_BACKOFF_SECONDS)
    return None


def get(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 60.0,
    retries: int = 5,
    backoff_base: float = 1.5,
) -> httpx.Response:
    """GET ``url`` with retry on 429 / 5xx / transport errors.

    Returns the successful response (already ``raise_for_status``-clean). On a
    non-retryable status (4xx ≠ 429) raises immediately; after the last retry
    re-raises the final error.
    """
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            resp = httpx.get(url, params=params, headers=headers, timeout=timeout)
        except httpx.TransportError as exc:  # connect/read timeouts etc.
            last_exc = exc
            if attempt == retries - 1:
                raise
            wait = min(backoff_base * (2 ** attempt), _MAX_BACKOFF_SECONDS)
            logger.warning("[HTTP] %s transport error (%s), retry %d/%d in %.1fs",
                           url, exc, attempt + 1, retries, wait)
            time.sleep(wait)
            continue

        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt == retries - 1:
                resp.raise_for_status()
            wait = _retry_after_seconds(resp) or min(backoff_base * (2 ** attempt), _MAX_BACKOFF_SECONDS)
            logger.warning("[HTTP] %s → %d (rate-limited/transient), retry %d/%d in %.1fs",
                           url, resp.status_code, attempt + 1, retries, wait)
            time.sleep(wait)
            continue

        resp.raise_for_status()  # raises on other 4xx (caller bug)
        return resp

    # Loop only exits via return/raise above; this satisfies type-checkers.
    if last_exc:
        raise last_exc
    raise RuntimeError(f"get({url!r}) exhausted retries without a response")
