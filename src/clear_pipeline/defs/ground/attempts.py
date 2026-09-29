"""Per-message failure counters shared by the ground drains (`stages.py`,
`transcribe.py`).

clear-api has no failure status on `ground_messages`, so a message that
keeps failing stays in its drain's queue. These counters are what stop the
drain from paying for it again: once a message reaches `max_attempts` it is
*parked*, and the drain skips it before making any LLM/S3/Whisper call.

A parked message stays parked until its counter expires (`ATTEMPTS_TTL`),
then gets a fresh set of attempts. The TTL is set only when the counter is
created, so repeated failures don't keep pushing the expiry back.
"""

import redis

ATTEMPTS_TTL_SECONDS = 7 * 86400


def parked_ids(r: redis.Redis, keys_by_id: dict[str, str], max_attempts: int) -> set[str]:
    """Ids whose counter has reached `max_attempts`. One MGET per page."""
    if not keys_by_id:
        return set()
    ids = list(keys_by_id)
    values = r.mget([keys_by_id[i] for i in ids])
    return {i for i, v in zip(ids, values) if v is not None and int(v) >= max_attempts}


def record_failure(r: redis.Redis, key: str) -> int:
    """Count one failed attempt and return the new total."""
    attempts = int(r.incr(key))
    if attempts == 1:
        r.expire(key, ATTEMPTS_TTL_SECONDS)
    return attempts


def park(r: redis.Redis, key: str, max_attempts: int) -> None:
    """Park a message that can never succeed (no retry would help)."""
    r.set(key, max_attempts, ex=ATTEMPTS_TTL_SECONDS)
