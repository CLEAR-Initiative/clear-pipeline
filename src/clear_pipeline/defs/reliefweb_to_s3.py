"""Weekly ReliefWeb -> S3 ingest (ADR-0013 multi-source KB).

ReliefWeb is a KB *source*: the ``ReliefWebConnector`` (providers/kb_sources)
owns the API query + PDF download, and the ``reliefweb_fetch`` asset here wraps
it — computing the rolling window (first-ingest 90d backfill vs 7d weekly delta)
and staging each PDF to S3. The shared helpers the connector reuses
(``_fetch_window``, ``_upload_pdf``, ``_s3_object_exists``, ``_is_first_ingest``)
live in this module.

``reliefweb_fetch`` runs on its own ``reliefweb_fetch_schedule``; ``kb_fetch_sensor``
then launches the source-neutral downstream (``reliefweb_kb`` group) for the
country that just got new data. Each source has its own fetch + schedule; the
downstream is shared (ADR-0013).

S3 layout under ``reliefweb/``::

  reports/<iso3>/<format-slug>/<YYYY>-W<WW>.staged.jsonl       # fetch marker + staged docs
  pdfs/<iso3>/<format-slug>/<report_id>/<filename>             # the PDFs

``date.created`` (when ReliefWeb indexed the report) drives the rolling
weekly window — ``date.original`` would let back-dated publications land
outside it. Country filter is ``primary_country.iso3`` so only reports
where the target country is the primary focus are returned, excluding
regional roll-ups and cross-country refugee reports; using ISO3 rather
than name avoids the South-Sudan / Sudan collision. The ISO-week S3 key
means re-runs in the same week overwrite cleanly.

Credentials come from environment variables (``.env`` at project root):
``RELIEFWEB_APPNAME``, ``S3_ENDPOINT``, ``S3_REGION``, ``S3_BUCKET``,
``S3_ACCESS_KEY_ID``, ``S3_SECRET_ACCESS_KEY``.
"""

import json
import os
import tempfile
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import botocore.exceptions
import dagster as dg
import requests
from dotenv import load_dotenv

from clear_pipeline.defs.reliefweb_partitions import (
    FORMAT_NAME,
    country_partitions,
    list_pipeline_iso3s,
    reports_prefix,
)

load_dotenv(dotenv_path=Path(__file__).resolve().parents[3] / ".env")

RELIEFWEB_REPORTS_URL = "https://api.reliefweb.int/v2/reports"
PAGE_SIZE = 1000  # API hard cap
HTTP_TIMEOUT = 60
PDF_DOWNLOAD_TIMEOUT = 120  # PDFs are bigger than JSON; allow more time.
# In-memory threshold before SpooledTemporaryFile spills to disk. Keeps
# typical (1–10 MB) PDFs purely in RAM; rare large ones (>32 MB) spill
# to /tmp instead of blowing up the worker.
SPOOL_MAX_BYTES = 32 * 1024 * 1024

# Country scope is now a Dagster PARTITION, not a module constant: each asset
# reads `iso3 = context.partition_key` and the S3 layout + `FORMAT_NAME` filter
# live in `reliefweb_partitions`. Filtering by `primary_country.iso3` (not
# `country.iso3`) still restricts results to reports where the partition's country
# is the PRIMARY focus — excluding regional dashboards and cross-country refugee
# reports that only tag it secondarily; ISO3 avoids the South-Sudan collision.

# Rolling ingest window. On the first run (no report window in S3 yet) the
# fetch reaches back the wider initial lookback, matching what the datapoint
# aggregation backfills (KB_AGGREGATION_INITIAL_LOOKBACK_DAYS); routine runs
# use the weekly delta. Overridable via env.
_DEFAULT_LOOKBACK_DAYS = 7
_DEFAULT_INITIAL_LOOKBACK_DAYS = 90


# ────────────────────────────────────────────────────────────────────────
# Shared helpers
# ────────────────────────────────────────────────────────────────────────

def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Missing env var {name}. Add it to .env or export it before running.",
        )
    return value


def _s3_client():
    from clear_pipeline.providers.s3 import s3_client

    return s3_client()


def _iso_seconds(dt: datetime) -> str:
    """ISO-8601 with second precision. ReliefWeb's date-range parser
    rejects the microsecond portion of ``datetime.isoformat()``
    ("Invalid range 'from' value for field 'date.created'") so we strip
    it before serialising."""
    return dt.replace(microsecond=0).isoformat()


def _week_tag(at: datetime) -> str:
    """ISO ``YYYY-WWW`` tag used in every weekly S3 key."""
    iso_year, iso_week, _ = at.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _is_first_ingest(s3, bucket: str, iso3: str) -> bool:
    """True when no report window has been written yet for THIS country — the
    country's own first ingest. Country-scoped (keys off the partition's S3
    prefix, not any downstream DB), so a newly-onboarded country correctly gets
    the wider initial lookback even after other countries are established, and
    it's correct whether the run is KB-only or the full datapoints pipeline."""
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=reports_prefix(iso3), MaxKeys=1)
    return int(resp.get("KeyCount", 0)) == 0


def _s3_object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as err:
        # Both `404` (object missing) and `403` (no permission to
        # confirm) come through here. We treat 404 as "needs upload" and
        # let any other code propagate so a real auth misconfig fails
        # loudly instead of triggering a silent re-upload.
        code = err.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


# ────────────────────────────────────────────────────────────────────────
# ReliefWeb report query (used by the connector's fetch)
# ────────────────────────────────────────────────────────────────────────

def _fetch_window(
    context: dg.AssetExecutionContext, appname: str, start: datetime, end: datetime,
    iso3: str,
) -> list[dict]:
    """Page through ``/v2/reports`` for the [start, end] window, for one country."""
    reports: list[dict] = []
    offset = 0
    while True:
        body = {
            "filter": {
                "operator": "AND",
                "conditions": [
                    {
                        "field": "date.created",
                        "value": {"from": _iso_seconds(start), "to": _iso_seconds(end)},
                    },
                    {"field": "primary_country.iso3", "value": iso3},
                    {"field": "format.name", "value": FORMAT_NAME},
                ],
            },
            "sort": ["date.created:asc"],
            "profile": "full",
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        resp = requests.post(
            RELIEFWEB_REPORTS_URL,
            params={"appname": appname},
            json=body,
            timeout=HTTP_TIMEOUT,
        )
        if not resp.ok:
            # ReliefWeb returns a JSON body with the failure reason
            # ({"error":{"message":"..."}}) — surface it before
            # `raise_for_status` discards everything but the URL.
            context.log.error(
                "reliefweb POST failed status=%s body=%s", resp.status_code, resp.text[:1000],
            )
            resp.raise_for_status()
        payload = resp.json()

        batch = payload.get("data") or []
        total = int(payload.get("totalCount") or 0)
        reports.extend(batch)
        context.log.info(
            "fetched %d/%d reports (offset=%d)", len(reports), total, offset,
        )

        if not batch or len(reports) >= total or len(batch) < PAGE_SIZE:
            break
        offset += len(batch)

    return reports


# ────────────────────────────────────────────────────────────────────────
# Shared PDF download helper (used by the ReliefWeb KB connector)
# ────────────────────────────────────────────────────────────────────────

def _upload_pdf(
    s3, bucket: str, url: str, key: str,
) -> int:
    """Stream a single PDF from ReliefWeb's CDN to S3. Returns the byte
    count written. Uses a spooled temp file so PDFs up to
    ``SPOOL_MAX_BYTES`` stay in RAM and only the rare large ones spill
    to disk."""
    with requests.get(url, stream=True, timeout=PDF_DOWNLOAD_TIMEOUT) as resp:
        resp.raise_for_status()
        with tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_BYTES) as buf:
            for chunk in resp.iter_content(chunk_size=64 * 1024):
                if chunk:
                    buf.write(chunk)
            size = buf.tell()
            buf.seek(0)
            s3.upload_fileobj(
                buf, bucket, key, ExtraArgs={"ContentType": "application/pdf"},
            )
    return size



# ────────────────────────────────────────────────────────────────────────
# Multi-source KB connector fetch (ADR-0013)
# ────────────────────────────────────────────────────────────────────────
#
# `reliefweb_fetch` is ReliefWeb's per-source fetch asset: it runs on
# `reliefweb_fetch_schedule` and, once it stages new data, `kb_fetch_sensor`
# launches the shared source-neutral downstream (`reliefweb_kb` group) for the
# country. It lives in its own `kb_fetch` group so the downstream job (which
# selects `reliefweb_kb`) loads its output from storage rather than re-running it.


@dg.asset(group_name="kb_fetch", partitions_def=country_partitions)
def reliefweb_fetch(context: dg.AssetExecutionContext) -> list[dict]:
    """Fetch ReliefWeb reports for this country partition via the
    `ReliefWebConnector`, staging each PDF to S3 and returning the staged
    `KbRawDocument`s (as dicts) for the downstream to process.

    A country's FIRST ingest (no report window in S3 yet for its iso3) reaches
    back the wider initial lookback (90d) so the datapoint aggregation has a full
    window to backfill; routine runs take the 7-day weekly delta. Both knobs are
    env-overridable.

    The connector stages PDFs to the SAME S3 keys the legacy assets use (pinned
    byte-for-byte by `test_reliefweb_paths_byte_identical_to_legacy`), HEAD-
    skipping ones already present — so running this against an established
    country re-stages nothing.
    """
    # Instantiate the connector directly (rather than via the registry) so this
    # asset has no import-order dependency on the connector module having been
    # imported for its registration side effect. The registry + `all_connectors`
    # are what the phase-2b fetch SENSOR enumerates to stay source-agnostic.
    from clear_pipeline.providers.kb_sources.reliefweb import ReliefWebConnector

    iso3 = context.partition_key
    bucket = _require_env("S3_BUCKET")
    s3 = _s3_client()

    # Same window rule as the legacy ingest asset, keyed off whether THIS country
    # already has a report window in S3 (country-scoped first-ingest detection).
    if _is_first_ingest(s3, bucket, iso3):
        lookback_days = int(
            os.environ.get("KB_INGEST_INITIAL_LOOKBACK_DAYS", str(_DEFAULT_INITIAL_LOOKBACK_DAYS)),
        )
        window_label = "initial-backfill"
    else:
        lookback_days = int(
            os.environ.get("KB_INGEST_LOOKBACK_DAYS", str(_DEFAULT_LOOKBACK_DAYS)),
        )
        window_label = "weekly"

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=lookback_days)
    context.log.info(
        "reliefweb_fetch: country=%s mode=%s window=[%s, %s]",
        iso3, window_label, start.isoformat(), end.isoformat(),
    )

    docs = ReliefWebConnector().fetch(
        country_iso3=iso3,
        window_start=start.isoformat(),
        window_end=end.isoformat(),
    )
    records = [asdict(d) for d in docs]

    # First-ingest marker. `_is_first_ingest` keys off ANY object under the
    # country's reports prefix; the legacy `reliefweb_weekly_reports_in_s3` wrote
    # the raw report JSONL there as that marker. The connector returns
    # `KbRawDocument`s instead, so persist the staged-doc records under the same
    # prefix — under a distinct `.staged.jsonl` key so it never clobbers the
    # legacy `<week>.jsonl` while both coexist pre-cutover. This keeps first-
    # ingest detection correct: the next run sees the country as established and
    # takes the weekly delta instead of re-running the 90-day backfill.
    marker_key = f"{reports_prefix(iso3)}{_week_tag(end)}.staged.jsonl"
    body = b"\n".join(
        json.dumps(r, separators=(",", ":")).encode("utf-8") for r in records
    )
    if body:
        body += b"\n"
    s3.put_object(
        Bucket=bucket, Key=marker_key, Body=body, ContentType="application/x-ndjson",
    )

    context.log.info(
        "reliefweb_fetch: staged %d document(s) for %s; marker -> s3://%s/%s",
        len(records), iso3, bucket, marker_key,
    )
    context.add_output_metadata(
        {
            "country": dg.MetadataValue.text(iso3),
            "source": dg.MetadataValue.text("reliefweb"),
            "mode": dg.MetadataValue.text(window_label),
            "window_from": dg.MetadataValue.text(start.isoformat()),
            "window_to": dg.MetadataValue.text(end.isoformat()),
            "staged_documents": dg.MetadataValue.int(len(records)),
            "marker_s3_key": dg.MetadataValue.text(marker_key),
        },
    )
    return records


# ────────────────────────────────────────────────────────────────────────
# Per-source fetch job + schedule, and the sensor-triggered source-neutral
# downstream (ADR-0013)
# ────────────────────────────────────────────────────────────────────────

# `clear/weight: heavy` caps these against the run queue's tag_concurrency_limits
# (deploy/dagster.yaml) so the per-country fan-out serialises to one heavy run at
# a time instead of starving the in-process code server's heartbeat on Railway.
_HEAVY_RUN_TAGS = {"clear/weight": "heavy"}

# Per-source fetch. Each KB source (ReliefWeb today; manual upload + others next)
# gets its OWN fetch job + schedule so a source polls — and fails — in isolation,
# never blocking another source or the downstream.
reliefweb_fetch_job = dg.define_asset_job(
    name="reliefweb_fetch_job",
    selection=[reliefweb_fetch],
    tags=_HEAVY_RUN_TAGS,
)

# The source-neutral downstream: pdf_text → chunks → enriched_chunks → upsert,
# plus figures, datapoints, datapoint_aggregations (the whole `reliefweb_kb`
# group). It does NOT select the fetch assets — `pdf_text`/`figures` load the
# staged docs from `reliefweb_fetch`'s latest materialization. Selected by group
# so new downstream assets are picked up automatically. The figure→KB sync keeps
# its own job/schedule (separate `reliefweb_kb_figures` group).
kb_downstream_job = dg.define_asset_job(
    name="kb_downstream_job",
    selection=dg.AssetSelection.groups("kb"),
    tags=_HEAVY_RUN_TAGS,
)

# Tuesday 06:00 UTC — by the time we run, all of "last week" is settled in
# ReliefWeb's index. Partitioned by country, so the schedule fans out ONE fetch
# run per live country partition; `run_key` is per-country-per-week so a re-tick
# in the same week is deduped. This runs ONLY the fetch — `kb_fetch_sensor` below
# starts the downstream once the staged data lands.
@dg.schedule(
    name="reliefweb_fetch_schedule",
    job=reliefweb_fetch_job,
    cron_schedule="0 6 * * TUE",
    execution_timezone="UTC",
)
def reliefweb_fetch_schedule(context: dg.ScheduleEvaluationContext):
    iso3s = context.instance.get_dynamic_partitions(country_partitions.name)
    if not iso3s:
        # The sync sensor seeds the partition set from pipelineCountries; until
        # it has ticked once there is nothing to run.
        return dg.SkipReason(
            "no country partitions registered yet — the partition sensor seeds "
            "them from clear-api pipelineCountries",
        )
    week = _week_tag(context.scheduled_execution_time)
    return [
        dg.RunRequest(partition_key=iso3, run_key=f"{iso3}-{week}")
        for iso3 in iso3s
    ]


# Source-neutral downstream trigger. The moment ANY KB source stages new data —
# a `*_fetch` asset materializes for a country — launch the downstream for that
# country partition. Cursor-tracked over fetch materializations so it processes
# only new batches and never backfills history; the downstream's per-report
# skip-if-exists (pdf_text / enrich / datapoints) makes a re-trigger of an
# already-processed report a no-op, so even a replayed materialization is safe.
#
# Adding a PULL source = append its fetch AssetKey here (so this sensor launches
# the downstream when it stages data). The downstream discovers the staged docs
# from S3 itself (`read_latest_staged_documents`), so `kb_pdf_text` needs no edit.
# PUSH sources (manual upload) have no fetch asset — clear-api launches
# `kb_downstream_job` directly — so they are not listed here.
_FETCH_ASSET_KEYS = [dg.AssetKey("reliefweb_fetch")]


@dg.sensor(name="kb_fetch_sensor", job=kb_downstream_job, minimum_interval_seconds=30)
def kb_fetch_sensor(context: dg.SensorEvaluationContext):
    # Cursor = the max materialization storage_id seen so far (one int across all
    # fetch assets). `None` means never-primed: the first tick records the current
    # high-water mark and triggers NOTHING, so enabling the sensor never backfills
    # pre-existing fetch runs. The cursor is ALWAYS persisted afterwards (even "0"
    # on an empty instance) so priming happens exactly once — the first fetch after
    # enabling then trips the downstream.
    priming = context.cursor is None
    start = 0 if priming else int(context.cursor)
    high = start
    run_requests: list[dg.RunRequest] = []
    for key in _FETCH_ASSET_KEYS:
        result = context.instance.fetch_materializations(
            dg.AssetRecordsFilter(asset_key=key, after_storage_id=start or None),
            limit=5000,
            ascending=True,
        )
        for record in result.records:
            high = max(high, record.storage_id)
            if priming:
                continue
            mat = record.event_log_entry.asset_materialization
            partition = mat.partition if mat else None
            if not partition:
                continue
            run_requests.append(
                dg.RunRequest(
                    partition_key=partition,
                    run_key=f"kb-{key.to_python_identifier()}-{partition}-{record.storage_id}",
                ),
            )
    context.update_cursor(str(high))
    if priming:
        return dg.SkipReason(
            "primed cursor at the current high-water mark — will trigger the "
            "downstream on the next fetch materialization",
        )
    if not run_requests:
        return dg.SkipReason("no new KB fetch materializations since last tick")
    return run_requests


# Keep the dynamic partition set in lockstep with clear-api's `pipelineCountries`
# so onboarding a country there is all it takes to add it to the KB pipeline.
# ADD-ONLY: registering a partition is safe, but REMOVING one would discard that
# country's materialization history, so a de-listed country is logged for a human
# rather than dropped. The sensor does NOT launch runs — a new country is picked
# up by the Tuesday schedule (its first run is the initial-lookback ingest), or an
# operator can backfill it on demand from the Dagster UI.
@dg.sensor(
    name="reliefweb_country_partition_sensor",
    minimum_interval_seconds=3600,
)
def reliefweb_country_partition_sensor(context: dg.SensorEvaluationContext):
    desired = set(list_pipeline_iso3s())
    existing = set(context.instance.get_dynamic_partitions(country_partitions.name))
    stale = sorted(existing - desired)
    if stale:
        context.log.warning(
            "country partitions no longer in pipelineCountries (kept to preserve "
            "history — remove manually if intended): %s",
            ", ".join(stale),
        )
    new = sorted(desired - existing)
    if not new:
        return dg.SkipReason("country partition set already matches pipelineCountries")
    context.log.info("registering new country partitions: %s", ", ".join(new))
    return dg.SensorResult(
        dynamic_partitions_requests=[country_partitions.build_add_request(new)],
    )
