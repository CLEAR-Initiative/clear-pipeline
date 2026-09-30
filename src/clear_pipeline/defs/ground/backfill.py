"""One-off backfill: enrichment drafts for hotline threads that have none.

`ground_hotline_enrich` (stages.py) only takes messages with no
classification. Messages classified before that drain existed therefore
never get a draft, so the inbox's Add to CLEAR modal opens them with the
raw message as the title and no severity. This asset finds those threads
and writes a draft for them, using the same LLM call + geoparse as the
drain.

It writes the draft only. The existing classification is left alone: a
reviewer may already be triaging by it, and changing it would move the
entry between inbox tabs.

Manual-only (no sensor or schedule): materialize it from the Dagster UI.
Idempotent: a thread that gets a draft drops out of the next run, so it
can be re-run until `remaining` is 0. Failures are logged and skipped, not
retried or marked failed; this isn't a queue.

No `from __future__ import annotations` — Dagster inspects the `context`
annotation on assets (same reason stages.py omits it).
"""

import dagster as dg

from clear_pipeline.defs.ground.stages import (
    _FETCH_LIMIT,
    _HOTLINE_SOURCE_KIND,
    _MESSAGE_LOCK_TTL_SECONDS,
    _enrich_one_message,
    _geoparse_one_message,
)
from clear_pipeline.providers.clear_api import (
    ground_messages_for_classification,
    ground_thread_drafts_for_source,
    pipeline_ground_source_ids,
    upsert_ground_thread_drafts,
)
from clear_pipeline.providers.llm import make_llm_provider
from clear_pipeline.providers.redis_lock import redis_lock

# Same cost guardrail as the drain's _MAX_ATTEMPTED_PER_RUN. Re-run for more.
_MAX_ATTEMPTED_PER_RUN = 200


def _backfill_candidates(source_id: str) -> list[dict]:
    """One message per draftless unverified thread, among messages that
    are already classified (the drain handles the rest). Uses the thread's
    oldest such message, as the V1 inbox shows the thread by it."""
    draftless = {
        t["id"]
        for t in ground_thread_drafts_for_source(source_id)
        if t.get("reviewState") == "unverified" and t.get("draftTitle") is None
    }
    if not draftless:
        return []
    picked: dict[str, dict] = {}
    for m in ground_messages_for_classification(source_id, limit=_FETCH_LIMIT):
        tid = m.get("threadId")
        if (
            tid in draftless
            and tid not in picked
            and m.get("classification") is not None
            # A voice note is enriched from its transcript; without one the
            # draft would be written from empty text.
            and not (m.get("hasVoice") and m.get("transcript") is None)
        ):
            picked[tid] = m
    return list(picked.values())


def _backfill_locked(context) -> dg.MaterializeResult:
    llm = make_llm_provider("signal")
    candidates = [
        m
        for source_id in pipeline_ground_source_ids(kind=_HOTLINE_SOURCE_KIND, is_active=True)
        for m in _backfill_candidates(source_id)
    ]

    written = skipped = failed = 0
    for msg in candidates[:_MAX_ATTEMPTED_PER_RUN]:
        # Same per-message lock as the drain, so the two never write the
        # same thread at once.
        lock_key = f"ground:message:{msg['id']}"
        with redis_lock(lock_key, ttl_seconds=_MESSAGE_LOCK_TTL_SECONDS, wait_seconds=0) as acquired:
            if not acquired:
                skipped += 1
                continue
            try:
                enrichment = _enrich_one_message(llm, msg)
                location_id = _geoparse_one_message(msg.get("transcript") or msg["text"])
                upsert_ground_thread_drafts([{
                    "threadId": msg["threadId"],
                    "draftTitle": enrichment.title,
                    "draftSeverity": enrichment.severity,
                    "draftLocationId": location_id,
                    "draftDisasterType": enrichment.disaster_type,
                }])
                written += 1
            except Exception:  # noqa: BLE001 — isolate one thread's failure
                context.log.exception("[ground:backfill] message %s failed — skipping", msg["id"])
                failed += 1

    remaining = max(len(candidates) - _MAX_ATTEMPTED_PER_RUN, 0) + skipped + failed
    context.log.info(
        "[ground:backfill] written=%d skipped=%d failed=%d remaining=%d",
        written, skipped, failed, remaining,
    )
    return dg.MaterializeResult(
        metadata={"written": written, "skipped": skipped, "failed": failed, "remaining": remaining}
    )


@dg.asset(
    name="ground_hotline_backfill_drafts",
    group_name="ground",
    description=(
        "One-off: write enrichment drafts for hotline threads classified before "
        "ground_hotline_enrich existed (they never got one). Leaves the classification "
        "as is. Manual-only; re-run until `remaining` is 0."
    ),
)
def ground_hotline_backfill_drafts(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with redis_lock("ground_hotline_backfill:run", ttl_seconds=3600, wait_seconds=0) as acquired:
        if not acquired:
            context.log.info("[ground:backfill] another backfill holds the lock — skipping this run")
            return dg.MaterializeResult(metadata={"skipped_concurrent": True})
        return _backfill_locked(context)
