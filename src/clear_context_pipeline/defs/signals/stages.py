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
from clear_context_pipeline.providers.alert import escalate_to_alert, is_stale_signal
from clear_context_pipeline.providers.classify import classify_locally
from clear_context_pipeline.providers.clear_api import (
    enqueue_translation,
    events_pending_alert,
    get_crisis_canonical,
    get_event_canonical,
    get_location_canonical,
    mark_signals_processed,
    pending_signals,
    pending_translations,
)
from clear_context_pipeline.providers.event import group_signal
from clear_context_pipeline.providers.redis_lock import redis_lock
from clear_context_pipeline.providers.signal import extract_population_affected_from_text
from clear_context_pipeline.providers.translate import (
    configured_target_locales,
    translate_and_upsert,
)
from clear_context_pipeline.signals.config import settings

_redis = redis.from_url(settings.redis_url)

_SIGNAL_LOCK_TTL_SECONDS = 360
_MAX_BATCHES = 50
_BATCH_SIZE = 200
_ALERT_MIN_SEVERITY = 4  # events at/above this with no alert surface in eventsPendingAlert

_EAGER = dg.AutomationCondition.eager()
# classify_group re-runs whenever ANY polled source ingests.
_INGEST_DEPS = [f"raw_{c.source}" for c in CONNECTORS if c.polled]


# ══════════════════════════════════════════════════════════════════════════════
# Stage 1 — classify + group (source-agnostic)
# ══════════════════════════════════════════════════════════════════════════════

def _project(created: dict):
    """Rehydrate + project a NEW signal via its source connector.
    Returns (connector, SignalView) or None to skip (unknown/ingest-only source,
    or a polled signal with no lake blob — e.g. a legacy Celery row)."""
    source_name = (created.get("source") or {}).get("name")
    connector = CONNECTORS_BY_SOURCE.get(source_name or "")
    if connector is None or source_name not in DRAINED_SOURCES:
        return None
    if connector.polled:
        key = created.get("rawS3Key")
        if not key:
            return None
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
        except Exception:  # noqa: BLE001 — translation enqueue must never fail grouping
            pass


def _group(view: SignalView, created: dict) -> dict | None:
    """Classify + group one signal into an event (create or add-to-existing).
    Returns the event dict, or None when below the relevance threshold."""
    classification = classify_locally(
        title=view.title, description=view.description, source_severity=created.get("severity"),
    )
    if classification.relevance < settings.relevance_threshold:
        return None

    actual_pop = extract_population_affected_from_text(
        view.title, created.get("title"), created.get("description"),
    )
    return group_signal(
        signal_id=created["id"],
        signal_title=view.title,
        signal_description=created.get("title"),
        signal_timestamp=view.timestamp,
        classification=classification,
        created_signal=created,
        signal_actual_population_affected=actual_pop,
    )


def _process_one_signal(created: dict) -> str:
    projected = _project(created)
    if projected is None:
        return "skipped"
    connector, view = projected
    lock_key = f"signal:{connector.source}:{view.external_id}"
    with redis_lock(lock_key, ttl_seconds=_SIGNAL_LOCK_TTL_SECONDS, wait_seconds=0) as acquired:
        if not acquired:
            return "skipped"
        event = _group(view, created)
        if event:
            _enqueue_translations("event", event["id"])
            loc_id = _event_location_id(created)
            if loc_id:
                _enqueue_translations("location", loc_id)
        return "processed"


def _drain_signals(context) -> dg.MaterializeResult:
    """Drain NEW signals from ALL sources, oldest-first, in bounded batches."""
    processed = skipped = failed = 0
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
                context.log.exception("[classify_group] signal %s failed", created.get("id"))
                failed_ids.append(created["id"])
                failed += 1
                continue
            if outcome == "processed":
                done_ids.append(created["id"])
                processed += 1
            else:
                skipped += 1
        mark_signals_processed(done_ids, "PROCESSED")
        mark_signals_processed(failed_ids, "FAILED")
        if not done_ids and not failed_ids:
            break

    context.log.info("[classify_group] processed=%d skipped=%d failed=%d", processed, skipped, failed)
    return dg.MaterializeResult(metadata={"processed": processed, "skipped": skipped, "failed": failed})


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
    alerted = suppressed = failed = 0
    for _ in range(_MAX_BATCHES):
        events = events_pending_alert(first=_BATCH_SIZE, min_severity=_ALERT_MIN_SEVERITY)
        if not events:
            break
        made_progress = False
        for event in events:
            try:
                # Suppress alerts for backdated events (replay/backfill).
                if is_stale_signal(event.get("validFrom")):
                    suppressed += 1
                    continue
                escalate_to_alert(event)
            except Exception:  # noqa: BLE001 — isolate one event's failure
                context.log.exception("[alert] event %s failed", event.get("id"))
                failed += 1
                continue
            alerted += 1
            made_progress = True  # this event now has an alert → leaves the queue
        # A batch that produced no new alerts is stale-only — stop (those events
        # keep re-appearing in eventsPendingAlert but must not spin the loop).
        if not made_progress:
            break

    context.log.info("[alert] alerted=%d suppressed=%d failed=%d", alerted, suppressed, failed)
    return dg.MaterializeResult(metadata={"alerted": alerted, "suppressed": suppressed, "failed": failed})


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
    translated = skipped = failed = 0
    for _ in range(_MAX_BATCHES):
        queue = pending_translations(first=_BATCH_SIZE)
        if not queue:
            break
        # Collapse per-(entity, locale) rows to one translate call per entity —
        # translate_and_upsert handles every configured locale + clears the rows.
        entities: dict[tuple[str, str], None] = {}
        for item in queue:
            entities[(item["entityType"], item["entityId"])] = None

        made_progress = False
        for entity_type, entity_id in entities:
            fetch = _CANONICAL_FETCH.get(entity_type)
            if fetch is None:
                context.log.warning("[translate] unknown entityType %r — skipping", entity_type)
                skipped += 1
                continue
            try:
                canonical = fetch(entity_id)
                if canonical is None:
                    skipped += 1
                    continue
                result = translate_and_upsert(entity_type, entity_id, canonical)
            except Exception:  # noqa: BLE001 — isolate one entity's failure
                context.log.exception("[translate] %s %s failed", entity_type, entity_id)
                failed += 1
                continue
            if result:
                translated += 1
            made_progress = True  # upsert/skip both clear the queue rows
        if not made_progress:
            break

    context.log.info("[translate] translated=%d skipped=%d failed=%d", translated, skipped, failed)
    return dg.MaterializeResult(metadata={"translated": translated, "skipped": skipped, "failed": failed})


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
