"""Weekly ReliefWeb -> S3 ingest, split into three assets.

  1. ``reliefweb_weekly_reports_in_s3`` — pull the last 7 days of report
     *metadata* for ``COUNTRY_ISO3``, write as JSONL, and pass the report
     list downstream.
  2. ``reliefweb_weekly_pdf_manifest`` — derive the flat list of
     ``(report_id, filename, url, s3_key)`` entries for every PDF
     attached to those reports, and write the manifest as a JSONL object
     in S3 so PDFs can be re-downloaded later without re-fetching
     metadata.
  3. ``reliefweb_weekly_pdfs_in_s3`` — consume the manifest, HEAD each
     planned key, and stream-upload only the missing PDFs to S3. Pure
     side effect.

Why the split: PDF downloads are the slow, network-flaky step. Keeping
them isolated means a transient ReliefWeb CDN error only invalidates
step 3, so a retry doesn't re-pull the API metadata. It also lets the
manifest stand on its own — a downstream consumer can use it as an index
of "what's in S3 for week W" without scanning the bucket.

S3 layout under ``reliefweb/``::

  reports/<iso3>/<format-slug>/<YYYY>-W<WW>.jsonl              # step 1 — metadata
  pdfs/<iso3>/<format-slug>/<YYYY>-W<WW>.manifest.jsonl        # step 2 — manifest
  pdfs/<iso3>/<format-slug>/<report_id>/<filename>             # step 3 — the PDFs

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
from datetime import datetime, timedelta, timezone
from pathlib import Path

import botocore.exceptions
import dagster as dg
import requests
from dotenv import load_dotenv

from clear_context_pipeline.defs.reliefweb_partitions import (
    FORMAT_NAME,
    country_partitions,
    list_pipeline_iso3s,
    pdf_key,
    pdf_prefix,
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
    from clear_context_pipeline.providers.s3 import s3_client

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
# Step 1 — pull report metadata
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


@dg.asset(group_name="reliefweb", partitions_def=country_partitions)
def reliefweb_weekly_reports_in_s3(
    context: dg.AssetExecutionContext,
) -> list[dict]:
    """Fetch a rolling window of ReliefWeb report metadata for this run's
    country partition and write it to S3 as JSONL. Routine runs take the
    7-day weekly delta; the country's FIRST run (no report window in S3 yet for
    that iso3) reaches back the wider initial lookback (90d) so the datapoint
    aggregation has a full window to backfill over.

    Returns the report list so the downstream PDF-manifest asset can
    consume it without a round-trip back to S3."""
    iso3 = context.partition_key
    appname = _require_env("RELIEFWEB_APPNAME")
    bucket = _require_env("S3_BUCKET")
    s3 = _s3_client()

    # This COUNTRY's first run (no report window in S3 yet for its iso3) reaches
    # back the wider initial lookback so a freshly-onboarded country seeds the
    # window the aggregation backfills; routine runs use the weekly delta.
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
        "reliefweb ingest: country=%s mode=%s window=[%s, %s]",
        iso3, window_label, start.isoformat(), end.isoformat(),
    )
    reports = _fetch_window(context, appname, start, end, iso3)

    key = f"{reports_prefix(iso3)}{_week_tag(end)}.jsonl"
    body = b"\n".join(
        json.dumps(r, separators=(",", ":")).encode("utf-8") for r in reports
    )
    if body:
        body += b"\n"
    s3.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson",
    )
    context.log.info(
        "uploaded %d reports (%d bytes) -> s3://%s/%s",
        len(reports), len(body), bucket, key,
    )

    context.add_output_metadata(
        {
            "country": dg.MetadataValue.text(iso3),
            "format": dg.MetadataValue.text(FORMAT_NAME),
            "mode": dg.MetadataValue.text(window_label),
            "window_from": dg.MetadataValue.text(start.isoformat()),
            "window_to": dg.MetadataValue.text(end.isoformat()),
            "report_count": dg.MetadataValue.int(len(reports)),
            "s3_bucket": dg.MetadataValue.text(bucket),
            "s3_key": dg.MetadataValue.text(key),
            "bytes_written": dg.MetadataValue.int(len(body)),
        },
    )
    return reports


# ────────────────────────────────────────────────────────────────────────
# Step 2 — build the PDF manifest
# ────────────────────────────────────────────────────────────────────────

@dg.asset(group_name="reliefweb", partitions_def=country_partitions)
def reliefweb_weekly_pdf_manifest(
    context: dg.AssetExecutionContext,
    reliefweb_weekly_reports_in_s3: list[dict],
) -> list[dict]:
    """Flatten every ``fields.file[]`` entry into a download manifest
    and write it to S3 as JSONL.

    Each manifest entry::

        {"report_id": str, "filename": str, "url": str, "s3_key": str}

    The download step keys off ``s3_key`` so renaming this convention in
    one place automatically retargets future uploads."""
    iso3 = context.partition_key
    bucket = _require_env("S3_BUCKET")

    manifest: list[dict] = []
    reports_with_pdfs = 0
    for report in reliefweb_weekly_reports_in_s3:
        files = (report.get("fields") or {}).get("file") or []
        if not files:
            continue
        reports_with_pdfs += 1
        report_id = str(report.get("id") or "unknown")
        for f in files:
            url = f.get("url")
            filename = f.get("filename")
            if not url or not filename:
                # Defensive: skip malformed entries but keep marching.
                context.log.warning(
                    "report %s has an attachment with no url/filename — skipping (%s)",
                    report_id, f,
                )
                continue
            manifest.append(
                {
                    "report_id": report_id,
                    "filename": filename,
                    "url": url,
                    "s3_key": pdf_key(iso3, report_id, filename),
                },
            )

    end = datetime.now(timezone.utc)
    manifest_key = f"{pdf_prefix(iso3)}{_week_tag(end)}.manifest.jsonl"
    body = b"\n".join(
        json.dumps(entry, separators=(",", ":")).encode("utf-8") for entry in manifest
    )
    if body:
        body += b"\n"
    _s3_client().put_object(
        Bucket=bucket, Key=manifest_key, Body=body, ContentType="application/x-ndjson",
    )
    context.log.info(
        "manifest: %d PDFs across %d reports -> s3://%s/%s",
        len(manifest), reports_with_pdfs, bucket, manifest_key,
    )

    context.add_output_metadata(
        {
            "pdf_count": dg.MetadataValue.int(len(manifest)),
            "reports_with_pdfs": dg.MetadataValue.int(reports_with_pdfs),
            "s3_bucket": dg.MetadataValue.text(bucket),
            "s3_manifest_key": dg.MetadataValue.text(manifest_key),
        },
    )
    return manifest


# ────────────────────────────────────────────────────────────────────────
# Step 3 — download the PDFs
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


@dg.asset(group_name="reliefweb", partitions_def=country_partitions)
def reliefweb_weekly_pdfs_in_s3(
    context: dg.AssetExecutionContext,
    reliefweb_weekly_pdf_manifest: list[dict],
) -> dg.MaterializeResult:
    """Download every manifest entry and put it in S3.

    Idempotent: existing keys are HEAD-checked and skipped. Failures on
    individual PDFs are logged and counted but never abort the run — the
    manifest will still be in S3 so a re-run picks up where this one
    left off."""
    iso3 = context.partition_key
    bucket = _require_env("S3_BUCKET")
    s3 = _s3_client()

    uploaded = 0
    skipped = 0
    failed = 0
    total_bytes = 0
    failures: list[str] = []

    for i, entry in enumerate(reliefweb_weekly_pdf_manifest, 1):
        key = entry["s3_key"]
        if _s3_object_exists(s3, bucket, key):
            skipped += 1
            continue
        try:
            size = _upload_pdf(s3, bucket, entry["url"], key)
            uploaded += 1
            total_bytes += size
            if i % 10 == 0 or i == len(reliefweb_weekly_pdf_manifest):
                context.log.info(
                    "progress %d/%d (uploaded=%d skipped=%d failed=%d)",
                    i, len(reliefweb_weekly_pdf_manifest), uploaded, skipped, failed,
                )
        except Exception as err:  # noqa: BLE001 — log + continue is intentional
            failed += 1
            failures.append(f"{entry['report_id']}/{entry['filename']}: {err}")
            context.log.error(
                "failed to upload %s -> s3://%s/%s : %s",
                entry["url"], bucket, key, err,
            )

    context.log.info(
        "PDFs done: uploaded=%d skipped=%d failed=%d total_bytes=%d",
        uploaded, skipped, failed, total_bytes,
    )

    metadata: dict = {
        "manifest_entries": dg.MetadataValue.int(len(reliefweb_weekly_pdf_manifest)),
        "uploaded": dg.MetadataValue.int(uploaded),
        "skipped_already_present": dg.MetadataValue.int(skipped),
        "failed": dg.MetadataValue.int(failed),
        "bytes_uploaded": dg.MetadataValue.int(total_bytes),
        "s3_bucket": dg.MetadataValue.text(bucket),
        "s3_prefix": dg.MetadataValue.text(pdf_prefix(iso3)),
    }
    if failures:
        # First few only — the run log carries the full list.
        metadata["failure_sample"] = dg.MetadataValue.md(
            "\n".join(f"- `{line}`" for line in failures[:10]),
        )
    return dg.MaterializeResult(metadata=metadata)


# ────────────────────────────────────────────────────────────────────────
# Job + schedule
# ────────────────────────────────────────────────────────────────────────

reliefweb_weekly_job = dg.define_asset_job(
    name="reliefweb_weekly_to_s3",
    selection=[
        reliefweb_weekly_reports_in_s3,
        reliefweb_weekly_pdf_manifest,
        reliefweb_weekly_pdfs_in_s3,
    ],
)

# The full weekly pipeline: ReliefWeb ingest → PDF text → chunks →
# LLM enrichment → embedding + knowledgebase upsert. Selected by
# group name so adding assets to either group picks them up
# automatically. Runs end-to-end on the schedule below.
reliefweb_weekly_kb_job = dg.define_asset_job(
    name="reliefweb_weekly_knowledgebase",
    selection=dg.AssetSelection.groups("reliefweb", "reliefweb_kb"),
)

# Monday 06:00 UTC — by the time we run, all of "last week" is settled in
# ReliefWeb's index. The assets are partitioned by country, so the schedule
# fans out ONE run per live country partition (rather than a single global run).
# `run_key` is per-country-per-week so a re-tick in the same week is deduped.
# Points at the KB-inclusive job so the weekly cron builds the full knowledge
# base for every country, not just the S3 ingest.
@dg.schedule(
    name="reliefweb_weekly_schedule",
    job=reliefweb_weekly_kb_job,
    cron_schedule="0 6 * * MON",
    execution_timezone="UTC",
)
def reliefweb_weekly_schedule(context: dg.ScheduleEvaluationContext):
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


# Keep the dynamic partition set in lockstep with clear-api's `pipelineCountries`
# so onboarding a country there is all it takes to add it to the KB pipeline.
# ADD-ONLY: registering a partition is safe, but REMOVING one would discard that
# country's materialization history, so a de-listed country is logged for a human
# rather than dropped. The sensor does NOT launch runs — a new country is picked
# up by the Monday schedule (its first run is the initial-lookback ingest), or an
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
