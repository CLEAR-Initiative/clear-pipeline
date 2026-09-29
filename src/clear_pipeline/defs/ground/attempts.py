"""Per-message failure handling shared by the ground drains (`stages.py`,
`transcribe.py`).

Redis counts a message's failed attempts toward `max_attempts`. Once
they're used up, the drain gives up on it (`give_up`): it marks the message
failed in clear-api (`markGroundMessagesFailed`), which durably takes it
out of the drain's queue and shows the failure in the review inbox, where a
reviewer can retry it. The counter is then cleared, so a retried message
starts with a fresh set of attempts.

*Parking* is only the fallback for when that mark call itself fails: the
counter is left at `max_attempts` and the drain skips the message before
any paid call (`parked_ids`). Each run retries the mark for the page's
parked messages in one batched call (`mark_parked`, no paid call), so a
park lasts only until clear-api accepts the mark. The same sweep marks
messages the pre-marker Redis stopgap parked.
"""

import logging

import redis

from clear_pipeline.providers.clear_api import mark_ground_messages_failed

logger = logging.getLogger(__name__)

# Counter lifetime: how long a fallback-parked message is skipped, and how
# long a counter lingers for a message that failed a few times and then
# succeeded. Set only when the counter is created, so repeated failures
# don't keep pushing the expiry back.
ATTEMPTS_TTL_SECONDS = 7 * 86400


def parked_ids(r: redis.Redis, keys_by_id: dict[str, str], max_attempts: int) -> set[str]:
    """Ids whose counter has reached `max_attempts` — messages parked
    because marking them failed in clear-api didn't work. One MGET per
    page."""
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


def error_text(exc: BaseException) -> str:
    """What a reviewer sees as the failure reason (clear-api truncates it)."""
    return f"{type(exc).__name__}: {exc}"


def give_up(
    r: redis.Redis,
    key: str,
    *,
    message_id: str,
    stage: str,
    error: str,
    max_attempts: int,
) -> bool:
    """Mark a message failed for `stage` ("ENRICH" | "TRANSCRIBE") in
    clear-api and clear its counter. Returns True once marked.

    Never raises: if the mark call fails, logs it and parks the message in
    Redis instead (returns False), so one bad message can't crash the
    drain."""
    try:
        mark_ground_messages_failed([{"messageId": message_id, "stage": stage, "error": error}])
    except Exception:  # noqa: BLE001 — parking is the fallback
        logger.exception(
            "[ground] couldn't mark message %s failed (%s) in clear-api — parking it in Redis",
            message_id, stage,
        )
        r.set(key, max_attempts, ex=ATTEMPTS_TTL_SECONDS)
        return False
    r.delete(key)
    return True


# The exception text isn't kept in Redis, so a mark retried from the park
# can only say that the attempts ran out.
PARKED_ERROR = "Gave up after repeated failures (reason not retained: marked from the Redis park)"


def mark_parked(r: redis.Redis, keys_by_id: dict[str, str], *, stage: str) -> bool:
    """Retry the mark for parked messages (`keys_by_id` holds only parked
    ids) in one batched call, clearing their counters once it lands.
    Returns True once marked; on failure, logs it and leaves them parked.
    Never raises."""
    if not keys_by_id:
        return True
    try:
        mark_ground_messages_failed([
            {"messageId": mid, "stage": stage, "error": PARKED_ERROR} for mid in keys_by_id
        ])
    except Exception:  # noqa: BLE001 — they stay parked; next run retries
        logger.exception(
            "[ground] couldn't mark %d parked message(s) failed (%s) — still parked",
            len(keys_by_id), stage,
        )
        return False
    r.delete(*keys_by_id.values())
    return True
