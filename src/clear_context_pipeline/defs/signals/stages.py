"""Shared, source-agnostic drain stages — the horizontal pipeline.

Per-source ingest (factory.py) all feeds ONE set of stage assets that process
signals from every source through a chain of durable clear-api queues:

    [signals NEW] ─► classify_group ─► [events pending alert] ─► alert
                          │
                          ├─► enqueue_translation(event) ──┐
                          └─► enqueue_translation(location)─┴─► [translationQueue] ─► translate

Each stage is one Dagster asset that drains its queue and is eager on the
previous stage; classify_group is eager on every ``raw_<source>`` ingest (and a
sensor also ticks it so analyst-created ``manual`` signals — which materialise no
ingest asset — get processed). Per-signal projection is dispatched by source via
``CONNECTORS_BY_SOURCE``, so one stage handles every source uniformly.

There is a single grouping algorithm (local classifier → district+type grouping →
LLM rewrite). Alerts are decided at the EVENT level: the alert stage escalates
events surfaced by ``eventsPendingAlert`` (severity-gated), with no LLM assess gate.

No ``from __future__ import annotations`` — Dagster inspects the ``context``
annotation on assets.
"""

import logging

import dagster as dg
import redis

from clear_context_pipeline.defs.signals import lake
from clear_context_pipeline.defs.signals.connectors import (
    CONNECTORS,
    CONNECTORS_BY_SOURCE,
    DRAINED_SOURCES,
    SignalView,
)
from clear_context_pipeline.defs.signals.poll_sensor import build_poll_sensor
from clear_context_pipeline.providers.alert import escalate_to_alert
from clear_context_pipeline.providers.classify import classify_locally
from clear_context_pipeline.providers.clear_api import (
    enqueue_translation,
    events_pending_alert,
    get_crisis_canonical,
    get_event_canonical,
    get_location_canonical,
    mark_signals_processed,
    mark_translated,
    pending_signals,
    pending_translations,
)
from clear_context_pipeline.providers.event import group_signal
from clear_context_pipeline.providers.redis_lock import redis_lock
from clear_context_pipeline.providers.signal import extract_population_affected_from_text
from clear_context_pipeline.providers.translate import (
    LOCKED,
    TRANSLATED,
    configured_target_locales,
    translate_and_upsert,
)
from clear_context_pipeline.signals.config import settings

_redis = redis.from_url(settings.redis_url)
logger = logging.getLogger(__name__)

_SIGNAL_LOCK_TTL_SECONDS = 360
_MAX_BATCHES = 50
_BATCH_SIZE = 200
# Bound per-signal retries so a transient failure (S3 blip, clear-api 5xx) is
# retried instead of dropped, but a persistently-bad signal still leaves the queue.
_MAX_SIGNAL_ATTEMPTS = 5
_ALERT_MIN_SEVERITY = 4  # events at/above this with no alert surface in eventsPendingAlert

_EAGER = dg.AutomationCondition.eager()
# classify_group re-runs whenever ANY polled source ingests.
_INGEST_DEPS = [f"raw_{c.source}" for c in CONNECTORS if c.polled]


# ══════════════════════════════════════════════════════════════════════════════
# Stage 1 — classify + group (source-agnostic)
# ══════════════════════════════════════════════════════════════════════════════

def _project(created: dict):
    """Rehydrate + project a NEW signal via its source connector.
    Returns ``(connector, SignalView)`` on success, or a *permanent-skip reason*
    string that the caller marks out of the queue (never leaves it NEW forever):
      - ``"unknown_source"`` — source not in the drained registry
      - ``"no_blob"``        — polled signal with no rawS3Key (legacy Celery row)
    """
    source_name = (created.get("source") or {}).get("name")
    connector = CONNECTORS_BY_SOURCE.get(source_name or "")
    if connector is None or source_name not in DRAINED_SOURCES:
        return "unknown_source"
    if connector.polled:
        key = created.get("rawS3Key")
        if not key:
            return "no_blob"
        s3 = lake.s3_client()
        body = s3.get_object(Bucket=settings.s3_bucket, Key=key)["Body"].read()
        record = connector.parse(body)
    else:
        record = None  # manual — project from the row
    return connector, connector.project(record, created)


def _event_location_id(created: dict) -> str | None:
    """The signal's resolved location id (origin > general > destination) — the
    location the event is pinned to, for location translation enqueue."""
    for key in ("originLocation", "generalLocation", "destinationLocation"):
        loc = created.get(key)
        if loc and loc.get("id"):
            return loc["id"]
    return None


def _enqueue_translations(entity_type: str, entity_id: str) -> None:
    """Enqueue an entity for (re)translation at every configured target locale.
    Idempotent per (entity, locale); the translate stage's staleness check skips
    unchanged fields, so enqueuing on every group is safe. Never fails the drain."""
    for locale in configured_target_locales():
        try:
            enqueue_translation(entity_type, entity_id, locale)
        except Exception:  # noqa: BLE001 — enqueue must never fail grouping, but log the loss
            logger.warning(
                "[classify_group] enqueue_translation failed for %s %s (%s) — translation skipped",
                entity_type, entity_id, locale, exc_info=True,
            )


def _group(view: SignalView, created: dict) -> dict | None:
    """Classify + group one signal into an event (create or add-to-existing).
    Returns the event dict, or None when below the relevance threshold."""
    classification = classify_locally(
        title=view.title, description=view.description, source_severity=created.get("severity"),
    )
    if classification.relevance < settings.relevance_threshold:
        return None

    actual_pop = extract_population_affected_from_text(
        view.title, view.description, created.get("description"),
    )
    return group_signal(
        signal_id=created["id"],
        signal_title=view.title,
        # The real prose description — NOT the title. `view.description` is what the
        # relevance classifier above saw, so grouping stays consistent with it.
        signal_description=view.description,
        signal_timestamp=view.timestamp,
        classification=classification,
        created_signal=created,
        signal_actual_population_affected=actual_pop,
    )


# Per-signal drain outcomes. A signal must NOT sit at NEW forever, or — since
# pendingSignals is oldest-first with no offset — a batch of permanently-stuck
# rows at the queue head stalls everything behind it (acute at cutover, when every
# legacy Celery row lacks rawS3Key). So only genuinely-transient outcomes leave a
# signal NEW; permanent ones are marked terminal and leave the queue.
_PROCESSED = "processed"        # classified + grouped → mark PROCESSED
_REQUEUE = "requeue"           # transient (lock contention) → leave NEW, retry next run
_DROP_DONE = "drop_done"       # legacy no-blob row (already processed by Celery) → mark PROCESSED
_DROP_FAILED = "drop_failed"   # unknown/undrained source → mark FAILED (anomaly)


def _process_one_signal(created: dict) -> str:
    projected = _project(created)
    if isinstance(projected, str):
        # Permanent skip — mark it out of the queue so it can't poison the head.
        return _DROP_DONE if projected == "no_blob" else _DROP_FAILED
    connector, view = projected
    lock_key = f"signal:{connector.source}:{view.external_id}"
    with redis_lock(lock_key, ttl_seconds=_SIGNAL_LOCK_TTL_SECONDS, wait_seconds=0) as acquired:
        if not acquired:
            return _REQUEUE  # a peer holds it (or is processing it) — leave NEW
        event = _group(view, created)
        if event:
            _enqueue_translations("event", event["id"])
            loc_id = _event_location_id(created)
            if loc_id:
                _enqueue_translations("location", loc_id)
        return _PROCESSED


_DRAIN_LOCK_TTL_SECONDS = 3600  # generous; a normal drain finishes in seconds


def _drain_signals(context) -> dg.MaterializeResult:
    """Drain NEW signals from ALL sources under a single-flight lock.

    classify_group is triggered by BOTH eager automation (4 ingest assets) and the
    interval sensor, so runs would otherwise overlap whenever a drain takes longer
    than the sensor interval. Overlapping drains, combined with batch-level status
    marking, let a second run re-group a signal off a stale ``pending_signals``
    snapshot (double-counted casualties). A global drain lock makes runs serial;
    a skipped run is harmless — the holder drains the whole queue, and the next
    trigger re-runs. The per-signal lock + group_signal's "already linked"
    short-circuit remain the backstop for the rare TTL-expiry case."""
    with redis_lock("classify_group:drain", ttl_seconds=_DRAIN_LOCK_TTL_SECONDS, wait_seconds=0) as acquired:
        if not acquired:
            context.log.info("[classify_group] another drain holds the lock — skipping this run")
            return dg.MaterializeResult(metadata={"skipped_concurrent": True})
        return _drain_signals_locked(context)


def _drain_signals_locked(context) -> dg.MaterializeResult:
    processed = dropped = requeued = failed = 0
    for _ in range(_MAX_BATCHES):
        batch = pending_signals(first=_BATCH_SIZE)  # ALL sources, oldest-first
        if not batch:
            break
        done_ids: list[str] = []
        failed_ids: list[str] = []
        for created in batch:
            try:
                outcome = _process_one_signal(created)
            except Exception:  # noqa: BLE001 — isolate one signal's failure
                # Transient (S3/clear-api blip) vs persistent: retry up to
                # _MAX_SIGNAL_ATTEMPTS (leave NEW), then mark FAILED so a genuinely
                # bad signal still leaves the queue instead of poisoning the head.
                sid = created["id"]
                attempts = _redis.incr(f"signal:attempts:{sid}")
                _redis.expire(f"signal:attempts:{sid}", 86400)
                if attempts >= _MAX_SIGNAL_ATTEMPTS:
                    context.log.exception(
                        "[classify_group] signal %s failed %d× — marking FAILED", sid, attempts
                    )
                    failed_ids.append(sid)
                    failed += 1
                else:
                    context.log.warning(
                        "[classify_group] signal %s failed (attempt %d/%d) — leaving NEW for retry",
                        sid, attempts, _MAX_SIGNAL_ATTEMPTS,
                    )
                    requeued += 1
                continue
            if outcome == _PROCESSED:
                done_ids.append(created["id"])
                processed += 1
            elif outcome == _DROP_DONE:
                done_ids.append(created["id"])  # mark PROCESSED — leaves the queue
                dropped += 1
            elif outcome == _DROP_FAILED:
                failed_ids.append(created["id"])  # mark FAILED — leaves the queue
                failed += 1
            else:  # _REQUEUE — transient, stays NEW for the next run
                requeued += 1
        mark_signals_processed(done_ids, "PROCESSED")
        mark_signals_processed(failed_ids, "FAILED")
        # Cost guardrail: cap LLM spend per run (each processed signal makes a
        # rewrite call). The remainder stays NEW and drains on the next run.
        if processed >= settings.signal_max_signals_per_run:
            context.log.warning(
                "[classify_group] hit per-run cap of %d processed signals — "
                "stopping; remainder drains next run", settings.signal_max_signals_per_run,
            )
            break
        # Stop only when the batch advanced NOTHING — i.e. every row was a
        # transient requeue (lock contention). Permanent drops go into done/failed,
        # so a cutover backlog of legacy no-blob rows drains instead of deadlocking.
        if not done_ids and not failed_ids:
            break

    context.log.info(
        "[classify_group] processed=%d dropped=%d requeued=%d failed=%d",
        processed, dropped, requeued, failed,
    )
    return dg.MaterializeResult(
        metadata={"processed": processed, "dropped": dropped, "requeued": requeued, "failed": failed}
    )


@dg.asset(
    name="classify_group",
    deps=_INGEST_DEPS,
    group_name="signals",
    automation_condition=_EAGER,
    description="Drain NEW signals from ALL sources → classify → group into events (+ enqueue event/location translation).",
)
def classify_group(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return _drain_signals(context)


# ══════════════════════════════════════════════════════════════════════════════
# Stage 2 — alert (event-level, severity-gated by eventsPendingAlert)
# ══════════════════════════════════════════════════════════════════════════════

@dg.asset(
    name="alert",
    deps=["classify_group"],
    group_name="signals",
    automation_condition=_EAGER,
    description="Drain events pending alert (severity>=4, no alert yet) → escalate to alerts.",
)
def alert(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    # Staleness is enforced at the QUERY: eventsPendingAlert only returns events
    # whose latest signal is within alert_max_signal_age_hours, so the historical
    # backlog and backdated backfill (old ACLED events replayed on ingest) never
    # surface here — they're grouped into their event but not alerted. Every event
    # this returns is recent + severe → publish an alert. No archived-suppression
    # path (creating an alert row, even archived, still fanned out emails).
    alerted = failed = 0
    for _ in range(_MAX_BATCHES):
        events = events_pending_alert(
            first=_BATCH_SIZE,
            min_severity=_ALERT_MIN_SEVERITY,
            max_age_hours=settings.alert_max_signal_age_hours,
        )
        if not events:
            break
        made_progress = False
        for event in events:
            try:
                escalate_to_alert(event)  # published
            except Exception:  # noqa: BLE001 — isolate one event's failure
                context.log.exception("[alert] event %s failed", event.get("id"))
                failed += 1
                continue
            alerted += 1
            made_progress = True  # this event now has an alert → leaves the queue
        # Terminate when a batch produced no new alerts (every event errored).
        if not made_progress:
            break

    context.log.info("[alert] alerted=%d failed=%d", alerted, failed)
    return dg.MaterializeResult(metadata={"alerted": alerted, "failed": failed})


# ══════════════════════════════════════════════════════════════════════════════
# Stage 3 — translate
# ══════════════════════════════════════════════════════════════════════════════

_CANONICAL_FETCH = {
    "event": get_event_canonical,
    "crisis": get_crisis_canonical,
    "location": get_location_canonical,
}


@dg.asset(
    name="translate",
    deps=["classify_group", "alert"],
    group_name="signals",
    automation_condition=_EAGER,
    description="Drain the translation queue → translate entity fields into target locales → upsertTranslations.",
)
def translate(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return _drain_translations(context)


def _drain_translations(context) -> dg.MaterializeResult:
    translated = cleared = requeued = failed = 0
    seen: set[tuple[str, str]] = set()  # entities attempted this run — never re-invoke
    for _ in range(_MAX_BATCHES):
        queue = pending_translations(first=_BATCH_SIZE)
        if not queue:
            break
        # Collapse per-(entity, locale) rows to one translate call per entity —
        # translate_and_upsert handles every configured locale + clears the rows.
        entities: dict[tuple[str, str], set[str]] = {}
        for item in queue:
            entities.setdefault((item["entityType"], item["entityId"]), set()).add(item["locale"])

        made_progress = False
        for (entity_type, entity_id), locales in entities.items():
            if (entity_type, entity_id) in seen:
                continue  # already attempted this run — don't refetch / re-call the model
            seen.add((entity_type, entity_id))

            fetch = _CANONICAL_FETCH.get(entity_type)
            if fetch is None:
                # Unknown entityType shouldn't be enqueued — drop it so it can't
                # poison the oldest-first queue head.
                context.log.warning("[translate] unknown entityType %r — dropping", entity_type)
                _clear_translation_rows(entity_type, entity_id, locales)
                cleared += 1
                made_progress = True
                continue
            try:
                canonical = fetch(entity_id)
                if canonical is None:
                    # Entity deleted / not found — drop its queue rows.
                    _clear_translation_rows(entity_type, entity_id, locales)
                    cleared += 1
                    made_progress = True
                    continue
                outcome = translate_and_upsert(entity_type, entity_id, canonical)
            except Exception:  # noqa: BLE001 — isolate one entity's failure
                context.log.exception("[translate] %s %s failed", entity_type, entity_id)
                failed += 1
                continue
            if outcome == LOCKED:
                requeued += 1  # a peer holds it — rows stay queued, NOT progress
            else:
                # TRANSLATED / NOOP / UNPARSEABLE all cleared the rows.
                if outcome == TRANSLATED:
                    translated += 1
                else:
                    cleared += 1
                made_progress = True
        # Stop only when a batch cleared nothing — every entity was locked
        # (transient) or already seen this run.
        if not made_progress:
            break

    context.log.info(
        "[translate] translated=%d cleared=%d requeued=%d failed=%d",
        translated, cleared, requeued, failed,
    )
    return dg.MaterializeResult(
        metadata={"translated": translated, "cleared": cleared, "requeued": requeued, "failed": failed}
    )


def _clear_translation_rows(entity_type: str, entity_id: str, locales: set[str]) -> None:
    for locale in locales:
        try:
            mark_translated(entity_type, entity_id, locale)
        except Exception:  # noqa: BLE001 — queue cleanup must not fail the drain
            pass


# ── manual-signal trigger ─────────────────────────────────────────────────────
# Manual signals materialise no ingest asset, so eager automation never fires for
# them. This sensor ticks classify_group on an interval to drain them (and acts as
# a safety net for any source). Concurrent runs are safe: the per-signal Redis lock
# + NEW→PROCESSED status markers make the drain idempotent.
classify_group_job = dg.define_asset_job(name="classify_group_job", selection=[classify_group])
signals_drain_sensor = build_poll_sensor(
    name="signals_drain_sensor",
    job=classify_group_job,
    default_interval_minutes=settings.manual_poll_interval_minutes,
)
