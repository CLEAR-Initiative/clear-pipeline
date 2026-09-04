"""Per-source INGEST factory — turn a polled ``SignalSource`` connector into its
ingest asset + poll sensor.

The pipeline is stage-based: ingest is per-source (each source has its own API and
poll cadence), but classify → group → alert → translate are shared, source-agnostic
stage assets (see ``stages.py``). So this factory only builds the ingest half; a
non-polled (manual) source builds nothing here — its signals are created directly
in clear-api and picked up by the shared classify_group stage.

**Add a data source = add a connector to ``connectors.CONNECTORS``.**

Ingest absorbs the Celery ``poll_<source>`` + ``ingest_signal`` steps: poll → write
raw blob to the lake → ``createSignal(status=NEW, rawS3Key=…)``. The poll cadence is
a sensor (``poll_sensor.py``), default from the source's ``poll_interval_minutes``
and editable live from the Dagster UI. Ships STOPPED for the big-bang cutover.

No ``from __future__ import annotations`` — Dagster inspects the ``context``
annotation on the ingest asset.
"""

from datetime import UTC, datetime

import dagster as dg

from clear_pipeline.defs.signals import lake
from clear_pipeline.defs.signals.connectors import SignalSource
from clear_pipeline.defs.signals.poll_sensor import build_poll_sensor
from clear_pipeline.providers.clear_api import create_signal, update_signal_content
from clear_pipeline.signals.config import settings


def build_source_assets(connector: SignalSource) -> list:
    """Return a polled source's ingest defs — ``[raw_<src> asset, poll_sensor]``.
    A non-polled (manual) source returns ``[]`` (no ingest; the shared stages pick
    up its clear-api signals)."""
    if not connector.polled:
        return []

    src = connector.source
    ingest_name = f"raw_{src}"

    @dg.asset(
        name=ingest_name,
        group_name=src,
        description=f"Poll {src} → write raw blobs to the S3 lake + createSignal(status=NEW).",
    )
    def _ingest(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        # Timestamp captured BEFORE polling — the watermark is advanced to this
        # only after a clean batch, so nothing published during the poll is skipped.
        poll_started = datetime.now(UTC)

        # No cross-source fallback: when this source has no watermark (first run,
        # or Redis was wiped), pass since=None so its poll uses ITS OWN initial
        # lookback. Seeding from the GLOBAL latest-signal timestamp would make a
        # newly-enabled source (e.g. ACLED after Dataminr has run) skip its entire
        # backfill history.
        since = connector.last_synced()
        records = connector.poll(since)
        if not records:
            context.log.info("[%s] no new signals", src)
            return dg.MaterializeResult(metadata={"signals_created": 0})

        s3 = lake.s3_client()
        bucket = settings.s3_bucket
        api_source_id = connector.api_source_id()
        created = failed = 0
        for record in records:
            # Per-record isolation: one bad record (a 4xx createSignal, a geoparser
            # blow-up in to_signal_input) must NOT abort the batch and strand the
            # rest. seen-marking (post_create) happens only AFTER createSignal AND
            # any content-update call both succeed, so a failed record stays
            # re-pollable. clear-api's (sourceId, externalId) get-or-create makes a
            # plain re-fetch idempotent — but for a source that revises records in
            # place (to_content_update_input returning non-None), createSignal
            # alone would silently no-op on the revision; the follow-up update is
            # what actually applies it.
            try:
                key = lake.raw_key(
                    src, connector.published_at(record), connector.external_id(record)
                )
                lake.write_raw(s3, bucket, key, connector.raw_bytes(record))
                input_data = connector.to_signal_input(record, api_source_id)
                input_data["rawS3Key"] = key
                created_signal = create_signal(input_data)
                # Every connector implements to_content_update_input — one that
                # doesn't revise records in place returns None explicitly (its own
                # implementation, not a fallback), so the update call below never
                # runs for it.
                update_input = connector.to_content_update_input(input_data, created_signal)
                if update_input is not None:
                    update_signal_content(update_input)
                connector.post_create(record)
                created += 1
            except Exception:  # noqa: BLE001 — isolate one record's failure
                context.log.exception("[%s] failed to ingest a record", src)
                failed += 1

        # Advance the watermark ONLY when every record persisted. On any failure we
        # leave it, so the next poll re-fetches the whole window and retries the
        # failed records (idempotent) instead of skipping past them.
        if failed == 0 and created:
            connector.set_watermark(poll_started)
        elif failed:
            context.log.warning(
                "[%s] %d record(s) failed — watermark held for retry next poll", src, failed
            )

        context.log.info("[%s] created %d signals (failed %d)", src, created, failed)
        return dg.MaterializeResult(metadata={"signals_created": created, "failed": failed})

    ingest_job = dg.define_asset_job(name=f"{src}_ingest", selection=[_ingest])
    poll_sensor = build_poll_sensor(
        name=f"{src}_poll_sensor",
        job=ingest_job,
        default_interval_minutes=connector.poll_interval_minutes,
    )
    return [_ingest, poll_sensor]
