"""Simple Redis distributed lock — context-manager style.

Why: event-grouping has a race window between the cache read and the
create_event call. Two Celery workers processing signals with the same
(admin2, level_2) at the same moment both see "no matching event" and both
create one → duplicate events.

Strategy:
  - Each grouping key gets a Redis lock (SET NX EX = acquire-if-not-exists
    with an auto-expiry so a crashed holder doesn't deadlock everything).
  - Losing the race waits and retries; winner does the cache-read → match →
    create sequence, then releases the lock.
  - Release is guarded by a per-acquire token so a slow holder whose lock
    already expired doesn't accidentally release someone else's.

The lock doesn't replace the DB uniqueness constraints from Wave 1 — it
just avoids write contention and pointless duplicate-event cleanup work.
"""

from __future__ import annotations

import logging
import secrets
import time
from contextlib import contextmanager
from typing import Iterator

import redis

from src.config import settings

logger = logging.getLogger(__name__)

_redis: redis.Redis | None = None

# Lua script: release a lock only if the token matches. Atomic so we don't
# delete a lock that has already expired and been re-acquired by someone else.
_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


def _client() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.from_url(settings.redis_url, decode_responses=True)
    return _redis


@contextmanager
def redis_lock(
    key: str,
    *,
    ttl_seconds: int = 30,
    wait_seconds: float = 5.0,
    poll_interval: float = 0.1,
) -> Iterator[bool]:
    """Acquire a distributed lock on `key`.

    Yields `True` if the lock was acquired, `False` if the wait timed out.
    Callers should typically skip their work (or retry their task later) when
    the lock can't be acquired — competing for the same key means a peer is
    doing the same work already.

    Usage:
        with redis_lock(f"group:{admin2}:{level_2}", ttl_seconds=20) as acquired:
            if not acquired:
                return  # peer is handling it; bail out
            ... do the contested work ...
    """
    client = _client()
    token = secrets.token_hex(16)
    deadline = time.monotonic() + wait_seconds
    acquired = False

    while True:
        if client.set(key, token, nx=True, ex=ttl_seconds):
            acquired = True
            break
        if time.monotonic() >= deadline:
            logger.warning(
                "[LOCK] Failed to acquire %s within %.1fs — yielding",
                key, wait_seconds,
            )
            break
        time.sleep(poll_interval)

    try:
        yield acquired
    finally:
        if acquired:
            try:
                client.eval(_RELEASE_SCRIPT, 1, key, token)
            except Exception as exc:
                logger.warning("[LOCK] Failed to release %s: %s", key, exc)
