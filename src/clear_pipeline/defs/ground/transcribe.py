"""Hotline voice-note transcription drain — see `defs/ground/__init__.py`
for the overview.

Structurally identical to `defs/ground/stages.py`'s `ground_hotline_enrich`
drain (single-flight Redis lock, per-run attempt cap, per-item exception
isolation with a Redis attempt counter; a message that exhausts it is
marked failed in clear-api — see `attempts.py`) — a separate asset/sensor rather
than folded into that one so a slow/expensive transcription doesn't block
enrichment throughput for text-only messages on the same source.

No `from __future__ import annotations` — Dagster inspects the `context`
annotation on assets (same reason `defs/ground/stages.py` omits it).
"""

import logging
import os

import dagster as dg
import redis

from clear_pipeline.defs.ground.attempts import (
    error_text,
    give_up,
    mark_parked,
    parked_ids,
    record_failure,
)
from clear_pipeline.defs.signals.poll_sensor import build_poll_sensor
from clear_pipeline.providers.clear_api import (
    ground_messages_for_classification,
    pipeline_ground_source_ids,
    upsert_ground_message_transcripts,
)
from clear_pipeline.providers.llm import TRANSIENT_LLM_ERRORS
from clear_pipeline.providers.redis_lock import redis_lock
from clear_pipeline.providers.s3 import s3_client
from clear_pipeline.providers.stt import transcribe_audio
from clear_pipeline.signals.config import settings

_redis = redis.from_url(settings.redis_url)
logger = logging.getLogger(__name__)


def _s3_client():
    return s3_client()


_MESSAGE_LOCK_TTL_SECONDS = 120
# Same rationale as ground_hotline_enrich's _FETCH_LIMIT (see stages.py):
# groundMessagesForClassification(awaitingTranscript: true) returns the
# oldest-first voice notes with no transcript, up to 2000, and has no
# cursor — so it's one fetch per source, bounded by the per-run cap.
_FETCH_LIMIT = 2000
# Lower than ground_hotline_enrich's 200: an audio upload + transcription
# call runs longer and costs more per item than a short structured-output
# text completion. Counts failures too, like stages.py.
_MAX_ATTEMPTED_PER_RUN = 100
_MAX_MESSAGE_ATTEMPTS = 5
_STAGE = "TRANSCRIBE"  # GroundPipelineStage for markGroundMessagesFailed

_HOTLINE_SOURCE_KIND = "hotline"


def _transcribe_one_message(msg: dict) -> str:
    """Fetch + transcribe every voice attachment on one message. Raises on
    failure — the caller isolates it (see `_process_one_message`)."""
    s3 = _s3_client()
    bucket = os.environ["S3_BUCKET"]
    parts = []
    for key in msg["voiceMediaKeys"]:
        audio_bytes = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        filename = key.rsplit("/", 1)[-1]
        parts.append(transcribe_audio(audio_bytes, filename))
    return "\n\n".join(p for p in parts if p)


# Per-message drain outcomes.
_PROCESSED = "processed"  # transcribed + wrote transcript → done
_REQUEUE = "requeue"      # transient (lock contention / retryable failure) → retry next run
_DROP_FAILED = "drop_failed"  # permanently bad (exhausted retries) → marked failed


def _process_one_message(msg: dict) -> str:
    lock_key = f"ground:transcribe:{msg['id']}"
    with redis_lock(lock_key, ttl_seconds=_MESSAGE_LOCK_TTL_SECONDS, wait_seconds=0) as acquired:
        if not acquired:
            return _REQUEUE  # a peer holds it — leave untranscribed

        transcript = _transcribe_one_message(msg)
        upsert_ground_message_transcripts([{"messageId": msg["id"], "transcript": transcript}])
        return _PROCESSED


def _drain_ground_transcribe(context) -> dg.MaterializeResult:
    with redis_lock("ground_transcribe:drain", ttl_seconds=3600, wait_seconds=0) as acquired:
        if not acquired:
            context.log.info("[ground:transcribe] another drain holds the lock — skipping this run")
            return dg.MaterializeResult(metadata={"skipped_concurrent": True})
        return _drain_ground_transcribe_locked(context)


def _attempts_key(message_id: str) -> str:
    return f"ground:transcribe:attempts:{message_id}"


def _drain_ground_transcribe_locked(context) -> dg.MaterializeResult:
    source_ids = pipeline_ground_source_ids(kind=_HOTLINE_SOURCE_KIND, is_active=True)

    attempted = processed = requeued = failed = parked = not_ready = 0
    capped = False
    for source_id in source_ids:
        if capped:
            break
        page = ground_messages_for_classification(
            source_id, limit=_FETCH_LIMIT, awaiting_transcript=True,
        )
        skip = parked_ids(
            _redis, {m["id"]: _attempts_key(m["id"]) for m in page}, _MAX_MESSAGE_ATTEMPTS,
        )
        # Parked = marking failed on an earlier run (or parked by the
        # pre-marker stopgap). Retry the mark for all of them in one call —
        # no paid call — so a park lasts only until clear-api accepts it.
        # They're skipped this run either way (still in this page).
        parked_marked = mark_parked(
            _redis, {mid: _attempts_key(mid) for mid in skip}, stage=_STAGE,
        )

        for msg in page:
            mid = msg["id"]
            if not msg.get("voiceMediaKeys"):
                # hasVoice but the audio isn't in S3 yet — hotline ingest
                # creates the row first and stores media after. Not a
                # failure: skip without spending an attempt.
                not_ready += 1
                continue
            if mid in skip:
                if parked_marked:
                    failed += 1  # marked failed just now
                else:
                    parked += 1
                continue
            if attempted >= _MAX_ATTEMPTED_PER_RUN:
                context.log.warning(
                    "[ground:transcribe] hit per-run cap of %d attempted — remainder drains next run",
                    _MAX_ATTEMPTED_PER_RUN,
                )
                capped = True
                break
            attempted += 1
            try:
                outcome = _process_one_message(msg)
            except TRANSIENT_LLM_ERRORS:
                outcome = _REQUEUE
            except Exception as exc:  # noqa: BLE001 — isolate one message's failure
                attempts = record_failure(_redis, _attempts_key(mid))
                if attempts >= _MAX_MESSAGE_ATTEMPTS:
                    context.log.exception(
                        "[ground:transcribe] message %s failed %d× — marking it failed", mid, attempts,
                    )
                    give_up(
                        _redis, _attempts_key(mid), message_id=mid, stage=_STAGE,
                        error=error_text(exc), max_attempts=_MAX_MESSAGE_ATTEMPTS,
                    )
                    outcome = _DROP_FAILED
                else:
                    context.log.warning(
                        "[ground:transcribe] message %s failed (attempt %d/%d) — retrying next run",
                        mid, attempts, _MAX_MESSAGE_ATTEMPTS,
                    )
                    outcome = _REQUEUE

            if outcome == _PROCESSED:
                processed += 1
            elif outcome == _DROP_FAILED:
                # Marked failed in clear-api (or parked, if that call
                # failed — same as ground_hotline_enrich). The server also
                # drops it from the enrichment queue, so the inbox shows it
                # as "transcription failed" until a reviewer retries it.
                failed += 1
            else:
                requeued += 1

    context.log.info(
        "[ground:transcribe] processed=%d requeued=%d failed=%d parked=%d not_ready=%d",
        processed, requeued, failed, parked, not_ready,
    )
    return dg.MaterializeResult(
        metadata={
            "processed": processed,
            "requeued": requeued,
            "failed": failed,
            "parked": parked,
            "not_ready": not_ready,
        }
    )


@dg.asset(
    name="ground_transcribe",
    group_name="ground",
    description="Drain hotline voice notes awaiting transcription → Whisper transcribe → write transcript.",
)
def ground_transcribe(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return _drain_ground_transcribe(context)


# Same reasoning as ground_hotline_enrich_sensor: hotline messages arrive
# via a webhook route, not a polled ingest asset, so this is sensor-only.
ground_transcribe_job = dg.define_asset_job(
    name="ground_transcribe_job", selection=[ground_transcribe]
)
ground_transcribe_sensor = build_poll_sensor(
    name="ground_transcribe_sensor",
    job=ground_transcribe_job,
    default_interval_minutes=settings.manual_poll_interval_minutes,
)
