"""Hotline enrichment drain — see `defs/ground/__init__.py` for the overview.

Modeled on `defs/signals/stages.py`'s `classify_group` drain
(`_drain_signals_locked` / `_process_one_signal`): single-flight Redis lock,
a per-run attempt cap (cost guardrail), and per-item exception isolation
with a Redis attempt counter. clear-api has no failure status on
ground_messages, so a message that exhausts its attempts stays in the
queue; it is *parked* (skipped before any LLM call) rather than removed —
see `attempts.py`. One difference: there's no batch-and-requery loop here —
see the `_FETCH_LIMIT` comment below for why.

No `from __future__ import annotations` — Dagster inspects the `context`
annotation on assets (same reason `defs/signals/stages.py` omits it).
"""

import logging

import dagster as dg
import redis

from clear_pipeline.defs.ground.attempts import park, parked_ids, record_failure
from clear_pipeline.defs.ground.prompts import (
    HOTLINE_ENRICH_PROMPT_VERSION,
    HOTLINE_ENRICH_SYSTEM_PROMPT,
    build_hotline_enrich_prompt,
)
from clear_pipeline.defs.ground.schemas import HotlineEnrichment
from clear_pipeline.defs.signals.poll_sensor import build_poll_sensor
from clear_pipeline.providers.clear_api import (
    ground_messages_for_classification,
    pipeline_ground_source_ids,
    resolve_location,
    upsert_ground_message_classifications,
    upsert_ground_thread_drafts,
)
from clear_pipeline.providers.geoparser import geoparse_signal
from clear_pipeline.providers.llm import (
    TRANSIENT_LLM_ERRORS,
    LLMProvider,
    make_llm_provider,
)
from clear_pipeline.providers.redis_lock import redis_lock
from clear_pipeline.providers.signal import geoparse_to_dict
from clear_pipeline.signals.config import settings

_redis = redis.from_url(settings.redis_url)
logger = logging.getLogger(__name__)

_MESSAGE_LOCK_TTL_SECONDS = 120
# groundMessagesForClassification(unclassifiedOnly: true) returns the
# oldest-first unclassified messages up to `limit` (server clamps to 2000;
# see ground.resolver.ts). The window shrinks as messages are classified,
# but there's no cursor, so there's no batch-and-requery loop here: fetch
# up to 2000 per source in one call, then bound spend with
# _MAX_ATTEMPTED_PER_RUN below — the same "cost guardrail"
# defs/signals/stages.py applies via settings.signal_max_signals_per_run.
_FETCH_LIMIT = 2000
# Counts every message handed to _process_one_message, failures included,
# so a failure storm is bounded the same as a success run.
_MAX_ATTEMPTED_PER_RUN = 200
# Bound per-message retries so a transient failure (LLM/clear-api blip) is
# retried, but a persistently-bad message is parked instead of re-billed
# every tick (see attempts.py).
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


def _hotline_country_codes() -> set[str]:
    return {
        c.strip().lower()
        for c in settings.ground_hotline_country_codes.split(",")
        if c.strip()
    }


def _geoparse_one_message(text: str) -> str | None:
    """Best-effort location suggestion for a message's free text. Returns
    an EXISTING `locations` row id, or None on any failure/no-match.

    Unlike `providers.signal.enrich_with_geoparser`, there are no source
    coordinates to scope by, so the geocode is scoped to the hotline's
    configured country (settings.ground_hotline_country_codes) — otherwise
    a same-named place in another POC country can outrank the right one.

    It deliberately does NOT call `find_or_create_landmark_l4`: the result
    is only a draft for an ERM to review, and an L4 row is permanent and
    shared with signals/events. Instead the candidate is matched to an
    existing admin location (L0-L3) by name. That loses landmark precision
    ("Nyala Airport" finds nothing) until promotion can create the L4."""
    try:
        geo_result = geoparse_signal(
            None, text, expected_country_codes=_hotline_country_codes() or None,
        )
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
        return resolve_location(name=geo_result.candidate)
    except Exception:  # noqa: BLE001 — location lookup is best-effort
        logger.warning("[ground:enrich] location lookup failed", exc_info=True)
        return None
    finally:
        # Debug-only — there's no geoparsedData column on ground_threads
        # (unlike signals), so the structured result isn't persisted.
        logger.debug("[ground:enrich] geoparse_to_dict=%s", geoparse_to_dict(geo_result))


# Per-message drain outcomes.
_PROCESSED = "processed"  # enriched + wrote draft + classification → done
_REQUEUE = "requeue"      # transient (lock contention / retryable failure) → retry next run
_DROP_FAILED = "drop_failed"  # permanently bad (no threadId, or exhausted retries) → parked


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


def _attempts_key(message_id: str) -> str:
    return f"ground:attempts:{message_id}"


def _drain_hotline_enrich_locked(context) -> dg.MaterializeResult:
    llm = make_llm_provider("signal")  # cheap/Haiku-tier role — short messages, high volume
    source_ids = pipeline_ground_source_ids(kind=_HOTLINE_SOURCE_KIND, is_active=True)

    attempted = processed = requeued = failed = parked = 0
    capped = False
    for source_id in source_ids:
        if capped:
            break
        page = ground_messages_for_classification(
            source_id, limit=_FETCH_LIMIT, unclassified_only=True,
        )
        # Hold a voice note out of enrichment until it has a transcript —
        # its `text` is usually empty, so enriching now would waste an LLM
        # call on no content and the message would never be re-enriched.
        # `hasVoice` (not `voiceMediaKeys`) because it's set when the row
        # is created, before the media lands. ground_transcribe
        # (transcribe.py) drains it first; the next tick picks it up here.
        pending = [
            m for m in page if not (m.get("hasVoice") and m.get("transcript") is None)
        ]
        skip = parked_ids(
            _redis, {m["id"]: _attempts_key(m["id"]) for m in pending}, _MAX_MESSAGE_ATTEMPTS,
        )

        for msg in pending:
            mid = msg["id"]
            if mid in skip:
                parked += 1
                continue
            if attempted >= _MAX_ATTEMPTED_PER_RUN:
                context.log.warning(
                    "[ground:enrich] hit per-run cap of %d attempted — remainder drains next run",
                    _MAX_ATTEMPTED_PER_RUN,
                )
                capped = True
                break
            attempted += 1
            try:
                outcome = _process_one_message(llm, msg)
            except TRANSIENT_LLM_ERRORS:
                outcome = _REQUEUE
            except Exception:  # noqa: BLE001 — isolate one message's failure
                attempts = record_failure(_redis, _attempts_key(mid))
                if attempts >= _MAX_MESSAGE_ATTEMPTS:
                    context.log.exception(
                        "[ground:enrich] message %s failed %d× — parking it", mid, attempts,
                    )
                    outcome = _DROP_FAILED
                else:
                    context.log.warning(
                        "[ground:enrich] message %s failed (attempt %d/%d) — retrying next run",
                        mid, attempts, _MAX_MESSAGE_ATTEMPTS,
                    )
                    outcome = _REQUEUE
            else:
                if outcome == _DROP_FAILED:
                    # Deterministic (no threadId) — no retry will help.
                    park(_redis, _attempts_key(mid), _MAX_MESSAGE_ATTEMPTS)

            if outcome == _PROCESSED:
                processed += 1
            elif outcome == _DROP_FAILED:
                # Still classification NULL in clear-api (no status column
                # on groundMessages), so it stays in the page — the parked
                # counter is what keeps later runs from paying for it again.
                # An operator has to intervene.
                failed += 1
            else:
                requeued += 1

    context.log.info(
        "[ground:enrich] processed=%d requeued=%d failed=%d parked=%d",
        processed, requeued, failed, parked,
    )
    return dg.MaterializeResult(
        metadata={
            "processed": processed, "requeued": requeued, "failed": failed, "parked": parked,
        }
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
