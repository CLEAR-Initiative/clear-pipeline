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

from datetime import datetime

import dagster as dg

from clear_context_pipeline.defs.signals import lake
from clear_context_pipeline.defs.signals.connectors import SignalSource
from clear_context_pipeline.defs.signals.poll_sensor import build_poll_sensor
from clear_context_pipeline.providers.clear_api import create_signal, get_latest_signal_timestamp
from clear_context_pipeline.signals.config import settings


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
        since = connector.last_synced()
        if since is None:
            latest = get_latest_signal_timestamp()
            if latest:
                since = datetime.fromisoformat(latest.replace("Z", "+00:00"))

        records = connector.poll(since)  # advances the watermark internally
        if not records:
            context.log.info("[%s] no new signals", src)
            return dg.MaterializeResult(metadata={"signals_created": 0})

        s3 = lake.s3_client()
        bucket = settings.s3_bucket
        api_source_id = connector.api_source_id()
        created = 0
        for record in records:
            key = lake.raw_key(
                src, connector.published_at(record), connector.external_id(record)
            )
            lake.write_raw(s3, bucket, key, connector.raw_bytes(record))
            input_data = connector.to_signal_input(record, api_source_id)
            input_data["rawS3Key"] = key
            create_signal(input_data)
            # Post-create hook (idempotent): darfur24 marks the article seen only
            # after the signal is confirmed; no-op for the others.
            connector.post_create(record)
            created += 1

        context.log.info("[%s] created %d signals", src, created)
        return dg.MaterializeResult(metadata={"signals_created": created})

    ingest_job = dg.define_asset_job(name=f"{src}_ingest", selection=[_ingest])
    poll_sensor = build_poll_sensor(
        name=f"{src}_poll_sensor",
        job=ingest_job,
        default_interval_minutes=connector.poll_interval_minutes,
    )
    return [_ingest, poll_sensor]
