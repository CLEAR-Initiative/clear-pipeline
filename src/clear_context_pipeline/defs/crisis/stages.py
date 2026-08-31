"""Crisis-enrichment drain — the queue consumer.

One Dagster asset (``enrich_crises``) drains clear-api's ``pendingCrises`` queue:
per PENDING crisis it runs ``enrich_one_crisis`` (gather events → RAG-ground →
generate narrative/scenarios/needs → compute population → write back → mark
ENRICHED) and enqueues crisis translation. Crises materialise no ingest asset,
so a sensor ticks the asset on an interval (there is no eager upstream).

Mirrors ``defs/signals/stages.py``: a single-flight drain lock keeps overlapping
sensor + manual runs serial, a per-crisis lock + attempt counter isolate one
crisis's failure, and a per-run cap bounds LLM spend (each crisis fires three
Claude calls).

No ``from __future__ import annotations`` — Dagster inspects the ``context``
annotation on assets.
"""

import logging

import dagster as dg
import redis

from clear_context_pipeline.defs.crisis.enrich import EMPTY, ENRICHED, enrich_one_crisis
from clear_context_pipeline.defs.signals.poll_sensor import build_poll_sensor
from clear_context_pipeline.providers.clear_api import (
    enqueue_translation,
    get_locations_by_level,
    mark_crisis_enriched,
    pending_crises,
)
from clear_context_pipeline.providers.redis_lock import redis_lock
from clear_context_pipeline.providers.translate import configured_target_locales
from clear_context_pipeline.signals.config import settings

_redis = redis.from_url(settings.redis_url)
logger = logging.getLogger(__name__)

_CRISIS_LOCK_TTL_SECONDS = 360      # covers the 3 sequential Claude calls per crisis
_DRAIN_LOCK_TTL_SECONDS = 3600      # generous single-flight guard
_MAX_BATCHES = 50
# Crises are heavier than signals (3 LLM calls + a RAG search each), so a smaller
# batch and a lower per-run cap than the signal drain.
_BATCH_SIZE = 50
_MAX_CRISES_PER_RUN = 25
# Bound per-crisis retries so a transient clear-api/LLM blip is retried, but a
# persistently-bad crisis is force-marked ENRICHED and leaves the queue instead
# of poisoning the oldest-first head.
_MAX_CRISIS_ATTEMPTS = 5

# Per-crisis drain outcomes.
_REQUEUE = "requeue"  # a peer holds the crisis lock — leave PENDING, retry next run


def _enqueue_translations(crisis_id: str) -> None:
    """Enqueue the crisis for (re)translation at every configured target locale.
    Idempotent per (crisis, locale); the translate stage skips unchanged fields,
    so enqueuing on every enrichment is safe. Never fails the drain."""
    for locale in configured_target_locales():
        try:
            enqueue_translation("crisis", crisis_id, locale)
        except Exception:  # noqa: BLE001 — enqueue must never fail enrichment
            logger.warning(
                "[enrich_crises] enqueue_translation failed for crisis %s (%s) — translation skipped",
                crisis_id, locale, exc_info=True,
            )


def _process_one_crisis(crisis: dict, a0_ids: set[str]) -> str:
    crisis_id = crisis["id"]
    with redis_lock(f"crisis:enrich:{crisis_id}", ttl_seconds=_CRISIS_LOCK_TTL_SECONDS, wait_seconds=0) as acquired:
        if not acquired:
            return _REQUEUE  # a peer holds it (or is enriching it) — leave PENDING
        outcome = enrich_one_crisis(crisis, a0_ids=a0_ids)
        if outcome == ENRICHED:
            _enqueue_translations(crisis_id)
        return outcome


def _drain_crises(context) -> dg.MaterializeResult:
    """Drain PENDING crises under a single-flight lock.

    The sensor + any manual run can overlap; a global drain lock makes runs
    serial. A skipped run is harmless — the holder drains the queue and the next
    tick re-runs. The per-crisis lock is the backstop for the rare TTL-expiry
    case."""
    with redis_lock("enrich_crises:drain", ttl_seconds=_DRAIN_LOCK_TTL_SECONDS, wait_seconds=0) as acquired:
        if not acquired:
            context.log.info("[enrich_crises] another drain holds the lock — skipping this run")
            return dg.MaterializeResult(metadata={"skipped_concurrent": True})
        return _drain_crises_locked(context)


def _drain_crises_locked(context) -> dg.MaterializeResult:
    # A0 id set for RAG country scoping — resolved once per run, reused across
    # every crisis (the locations tree changes rarely).
    try:
        a0_ids = {loc["id"] for loc in get_locations_by_level(0) if loc.get("id")}
    except Exception:  # noqa: BLE001 — country scoping is a refinement, not a gate
        context.log.warning("[enrich_crises] admin-0 lookup failed — RAG falls back to event-type scoping", exc_info=True)
        a0_ids = set()

    enriched = empty = requeued = failed = 0
    for _ in range(_MAX_BATCHES):
        batch = pending_crises(first=_BATCH_SIZE)  # oldest-first
        if not batch:
            break
        made_progress = False
        for crisis in batch:
            try:
                outcome = _process_one_crisis(crisis, a0_ids)
            except Exception:  # noqa: BLE001 — isolate one crisis's failure
                # Transient (clear-api/LLM blip) vs persistent: retry up to
                # _MAX_CRISIS_ATTEMPTS, then force-mark ENRICHED so a genuinely
                # bad crisis leaves the queue instead of poisoning the head.
                cid = crisis["id"]
                attempts = _redis.incr(f"crisis:attempts:{cid}")
                _redis.expire(f"crisis:attempts:{cid}", 86400)
                if attempts >= _MAX_CRISIS_ATTEMPTS:
                    context.log.exception(
                        "[enrich_crises] crisis %s failed %d× — force-marking ENRICHED", cid, attempts,
                    )
                    try:
                        mark_crisis_enriched(cid)  # drop it from the queue
                        made_progress = True
                    except Exception:  # noqa: BLE001 — clear-api likely down; leave PENDING to retry
                        context.log.warning("[enrich_crises] could not force-mark crisis %s — leaving PENDING", cid)
                    failed += 1
                else:
                    context.log.warning(
                        "[enrich_crises] crisis %s failed (attempt %d/%d) — leaving PENDING for retry",
                        cid, attempts, _MAX_CRISIS_ATTEMPTS,
                    )
                    requeued += 1
                continue
            if outcome == ENRICHED:
                enriched += 1
                made_progress = True
            elif outcome == EMPTY:
                empty += 1
                made_progress = True
            else:  # _REQUEUE — transient, stays PENDING for the next run
                requeued += 1

        # Cost guardrail: cap LLM spend per run (3 Claude calls per crisis). The
        # remainder stays PENDING and drains on the next tick.
        if enriched >= _MAX_CRISES_PER_RUN:
            context.log.warning(
                "[enrich_crises] hit per-run cap of %d enriched crises — stopping; remainder drains next run",
                _MAX_CRISES_PER_RUN,
            )
            break
        # Stop when a batch advanced nothing — every crisis was a transient
        # requeue (lock contention or a recoverable clear-api outage).
        if not made_progress:
            break

    context.log.info(
        "[enrich_crises] enriched=%d empty=%d requeued=%d failed=%d",
        enriched, empty, requeued, failed,
    )
    return dg.MaterializeResult(
        metadata={"enriched": enriched, "empty": empty, "requeued": requeued, "failed": failed}
    )


@dg.asset(
    name="enrich_crises",
    group_name="crisis",
    description="Drain PENDING crises → RAG-grounded narrative/scenarios/needs + population → mark ENRICHED (+ enqueue crisis translation).",
)
def enrich_crises(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return _drain_crises(context)


# ── drain trigger ─────────────────────────────────────────────────────────
# Crises have no upstream ingest asset (they're created by analyst rollup /
# crisis detection), so eager automation never fires for them. This sensor ticks
# the drain on an interval. Concurrent runs are safe: the drain lock + per-crisis
# lock + PENDING→ENRICHED marker make it idempotent.
enrich_crises_job = dg.define_asset_job(name="enrich_crises_job", selection=[enrich_crises])
crisis_enrich_sensor = build_poll_sensor(
    name="crisis_enrich_sensor",
    job=enrich_crises_job,
    default_interval_minutes=settings.manual_poll_interval_minutes,
)
