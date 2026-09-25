"""Hotline enrichment drain — see `defs/ground/__init__.py` for the overview.

Modeled on `defs/signals/stages.py`'s `classify_group` drain
(`_drain_signals_locked` / `_process_one_signal`): single-flight Redis lock,
a per-run processing cap (cost guardrail), and per-item exception isolation
with a Redis attempt counter so a persistently-bad message still leaves the
queue instead of poisoning the oldest-first head. One difference: there's
no batch-and-requery loop here — see the `_FETCH_LIMIT` comment below for
why.

No `from __future__ import annotations` — Dagster inspects the `context`
annotation on assets (same reason `defs/signals/stages.py` omits it).
"""

import logging

import dagster as dg
import redis

from clear_pipeline.defs.ground.prompts import (
    HOTLINE_ENRICH_PROMPT_VERSION,
    HOTLINE_ENRICH_SYSTEM_PROMPT,
    build_hotline_enrich_prompt,
)
from clear_pipeline.defs.ground.schemas import HotlineEnrichment
from clear_pipeline.defs.signals.poll_sensor import build_poll_sensor
from clear_pipeline.providers.clear_api import (
    find_or_create_landmark_l4,
    ground_messages_for_classification,
    pipeline_ground_source_ids,
    upsert_ground_message_classifications,
    upsert_ground_thread_drafts,
)
from clear_pipeline.providers.geoparser import geoparse_signal
from clear_pipeline.providers.llm import TRANSIENT_LLM_ERRORS, LLMProvider, make_llm_provider
from clear_pipeline.providers.redis_lock import redis_lock
from clear_pipeline.providers.signal import geoparse_to_dict
from clear_pipeline.signals.config import settings

_redis = redis.from_url(settings.redis_url)
logger = logging.getLogger(__name__)

_MESSAGE_LOCK_TTL_SECONDS = 120
# groundMessagesForClassification has no cursor — it always returns the
# SAME oldest-first N messages up to `limit` (server clamps to 2000; see
# ground.resolver.ts), unlike pending_signals/pending_translations, which
# shrink as items leave a queue. So there's no batch-and-requery loop here:
# fetch the full backlog per source in one call (2000, the server's own
# cap), then bound actual LLM spend with _MAX_PROCESSED_PER_RUN below —
# the same "cost guardrail" defs/signals/stages.py applies via
# settings.signal_max_signals_per_run.
_FETCH_LIMIT = 2000
_MAX_PROCESSED_PER_RUN = 200
# Bound per-message retries so a transient failure (LLM/clear-api blip) is
# retried instead of dropped, but a persistently-bad message still leaves
# the queue (mirrors _MAX_SIGNAL_ATTEMPTS in defs/signals/stages.py).
_MAX_MESSAGE_ATTEMPTS = 5
_LLM_MAX_TOKENS = 400  # short structured output — headline + a few enum fields

_HOTLINE_SOURCE_KIND = "hotline"


def _enrich_one_message(llm: LLMProvider, msg: dict) -> HotlineEnrichment:
    """Classify + suggest a headline/severity/disaster-type for one message.
    Raises on failure — the caller isolates it (see `_process_one_message`).

    Uses the voice-note transcript in place of `text` when one exists — the
    caller (`_drain_hotline_enrich_locked`) already holds a message with an
    untranscribed voice note out of `pending`, so by the time this runs,
    `transcript` is either the real content or the message never had one."""
    user = build_hotline_enrich_prompt(
        msg.get("transcript") or msg["text"],
        sender_ref=msg["senderRef"],
        sent_at=msg["sentAt"],
        has_media=msg["hasMedia"],
    )
    return llm.complete_structured(
        system=HOTLINE_ENRICH_SYSTEM_PROMPT,
        user=user,
        schema=HotlineEnrichment,
        max_tokens=_LLM_MAX_TOKENS,
        cache_key=HOTLINE_ENRICH_PROMPT_VERSION,
    )


def _geoparse_one_message(text: str) -> str | None:
    """Best-effort location candidate for a message's free text. Returns a
    `locations` row id, or None on any failure/no-match. Unlike
    `providers.signal.enrich_with_geoparser`, there's no source lat/lng to
    scope the search by — hotline messages carry no coordinates — so this
    calls `geoparse_signal` + `find_or_create_landmark_l4` directly instead
    of reusing that helper's country-scoping wrapper."""
    try:
        geo_result = geoparse_signal(None, text, expected_country_codes=None)
    except Exception:  # noqa: BLE001 — best-effort, never blocks enrichment
        logger.warning("[ground:enrich] geoparser failed (continuing without location)", exc_info=True)
        return None
    if geo_result is None:
        return None
    logger.info(
        "[ground:enrich] geoparsed: candidate=%r kind=%s importance=%.2f",
        geo_result.candidate, geo_result.kind, geo_result.importance,
    )
    try:
        promo = find_or_create_landmark_l4(
            name=geo_result.candidate, lat=geo_result.lat, lng=geo_result.lng,
            kind=geo_result.kind,
        )
        return promo.get("locationId")
    except Exception:  # noqa: BLE001 — promotion is best-effort
        logger.warning("[ground:enrich] L4 promotion failed", exc_info=True)
        return None
    finally:
        # Debug-only — there's no geoparsedData column on ground_threads
        # (unlike signals), so the structured result isn't persisted.
        logger.debug("[ground:enrich] geoparse_to_dict=%s", geoparse_to_dict(geo_result))


# Per-message drain outcomes.
_PROCESSED = "processed"  # enriched + wrote draft + classification → done
_REQUEUE = "requeue"      # transient (lock contention / retryable failure) → retry next run
_DROP_FAILED = "drop_failed"  # permanently bad (no threadId, or exhausted retries) → mark FAILED


def _process_one_message(llm: LLMProvider, msg: dict) -> str:
    if not msg.get("threadId"):
        # V1 ingest always creates a placeholder thread per message, so this
        # shouldn't happen — but never let one poison the oldest-first head.
        logger.warning("[ground:enrich] message %s has no threadId — dropping", msg["id"])
        return _DROP_FAILED

    lock_key = f"ground:message:{msg['id']}"
    with redis_lock(lock_key, ttl_seconds=_MESSAGE_LOCK_TTL_SECONDS, wait_seconds=0) as acquired:
        if not acquired:
            return _REQUEUE  # a peer holds it — leave unclassified

        enrichment = _enrich_one_message(llm, msg)
        location_id = _geoparse_one_message(msg.get("transcript") or msg["text"])

        # Write the draft FIRST, classification LAST: classification-non-null
        # is what stops this message being selected next run, so writing it
        # last means a crash between the two writes leaves the message still
        # "pending" (retried) instead of silently losing the computed draft.
        upsert_ground_thread_drafts([{
            "threadId": msg["threadId"],
            "draftTitle": enrichment.title,
            "draftSeverity": enrichment.severity,
            "draftLocationId": location_id,
            "draftDisasterType": enrichment.disaster_type,
        }])
        upsert_ground_message_classifications([{
            "messageId": msg["id"],
            "classification": enrichment.classification,
            "uncertaintyMarker": enrichment.uncertainty_marker,
        }])
        return _PROCESSED


def _drain_hotline_enrich(context) -> dg.MaterializeResult:
    with redis_lock("ground_hotline_enrich:drain", ttl_seconds=3600, wait_seconds=0) as acquired:
        if not acquired:
            context.log.info("[ground:enrich] another drain holds the lock — skipping this run")
            return dg.MaterializeResult(metadata={"skipped_concurrent": True})
        return _drain_hotline_enrich_locked(context)


def _drain_hotline_enrich_locked(context) -> dg.MaterializeResult:
    llm = make_llm_provider("signal")  # cheap/Haiku-tier role — short messages, high volume
    source_ids = pipeline_ground_source_ids(kind=_HOTLINE_SOURCE_KIND, is_active=True)

    processed = requeued = failed = 0
    capped = False
    for source_id in source_ids:
        if capped:
            break
        page = ground_messages_for_classification(source_id, limit=_FETCH_LIMIT)
        # Hold a message with an untranscribed voice note out of enrichment —
        # its `text` is usually empty, so enriching now would waste an LLM
        # call on no content. ground_transcribe (transcribe.py) drains it
        # first; once `transcript` lands, the next tick picks it up here.
        pending = [
            m
            for m in page
            if m.get("classification") is None
            and not (m.get("voiceMediaKeys") and m.get("transcript") is None)
        ]

        for msg in pending:
            if processed >= _MAX_PROCESSED_PER_RUN:
                context.log.warning(
                    "[ground:enrich] hit per-run cap of %d processed — remainder drains next run",
                    _MAX_PROCESSED_PER_RUN,
                )
                capped = True
                break
            try:
                outcome = _process_one_message(llm, msg)
            except TRANSIENT_LLM_ERRORS:
                outcome = _REQUEUE
            except Exception:  # noqa: BLE001 — isolate one message's failure
                mid = msg["id"]
                attempts = _redis.incr(f"ground:attempts:{mid}")
                _redis.expire(f"ground:attempts:{mid}", 86400)
                if attempts >= _MAX_MESSAGE_ATTEMPTS:
                    context.log.exception(
                        "[ground:enrich] message %s failed %d× — giving up", mid, attempts,
                    )
                    outcome = _DROP_FAILED
                else:
                    context.log.warning(
                        "[ground:enrich] message %s failed (attempt %d/%d) — retrying next run",
                        mid, attempts, _MAX_MESSAGE_ATTEMPTS,
                    )
                    outcome = _REQUEUE

            if outcome == _PROCESSED:
                processed += 1
            elif outcome == _DROP_FAILED:
                # Nothing marks it out of the queue (no status column on
                # groundMessages) — a dropped message is logged and left
                # NULL. An operator has to intervene; there's no other
                # queue-exit path for a message the enrichment can't
                # process at all.
                failed += 1
            else:
                requeued += 1

    context.log.info(
        "[ground:enrich] processed=%d requeued=%d failed=%d", processed, requeued, failed,
    )
    return dg.MaterializeResult(
        metadata={"processed": processed, "requeued": requeued, "failed": failed}
    )


@dg.asset(
    name="ground_hotline_enrich",
    group_name="ground",
    description="Drain hotline messages awaiting classification → LLM classify/title/severity/disaster-type + geoparse location → write classification + thread draft.",
)
def ground_hotline_enrich(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return _drain_hotline_enrich(context)


# Hotline messages arrive via a webhook route, not a polled Dagster ingest
# asset — nothing to be eager on, so this is sensor-only (same reason
# `manual` signals need signals_drain_sensor instead of eager automation).
ground_hotline_enrich_job = dg.define_asset_job(
    name="ground_hotline_enrich_job", selection=[ground_hotline_enrich]
)
ground_hotline_enrich_sensor = build_poll_sensor(
    name="ground_hotline_enrich_sensor",
    job=ground_hotline_enrich_job,
    default_interval_minutes=settings.manual_poll_interval_minutes,
)
