"""Extract text from the week's ReliefWeb PDFs.

Reads each PDF the manifest points at from S3, extracts text
page-by-page with pdfplumber, and writes one JSONL per report back to
S3. Downstream chunk / enrich stages read from S3 so a failure in
either can be replayed without re-hitting the PDFs.

S3 layout:
    reliefweb/kb/text/<iso3>/<format-slug>/<report_id>.jsonl

Each line is::

    {"report_id": str, "page_num": int (1-indexed), "text": str}
"""

import json
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dotenv import load_dotenv

from clear_context_pipeline.defs.knowledgebase._pdf_extract import (
    extract_pages,
    extract_pages_pypdf,
)

load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

# Spawn context so each extraction worker starts from a clean interpreter
# (only io + pdfplumber via _pdf_extract), NOT a fork of this Dagster process
# with all of Dagster/boto3 resident. That lean baseline is what lets the
# heavier weekly sitreps parse within the container's per-process memory
# instead of OOM-killing the worker.
_MP_SPAWN = multiprocessing.get_context("spawn")

# Match the country / format scope used by reliefweb_to_s3 so the S3
# paths stay parallel to the existing tree — a future switch to another
# country / format is one env var, not a scavenger hunt.
COUNTRY_ISO3 = "sdn"
FORMAT_SLUG = "situation-report"

S3_TEXT_PREFIX = f"reliefweb/kb/text/{COUNTRY_ISO3}/{FORMAT_SLUG}"


def _s3_client():
    from clear_context_pipeline.providers.s3 import s3_client

    return s3_client()


def _text_key(report_id: str) -> str:
    return f"{S3_TEXT_PREFIX}/{report_id}.jsonl"


def _extract_pages_isolated(pdf_bytes: bytes, fn=extract_pages) -> list[dict]:
    """Parse one PDF in its own short-lived worker process.

    `fn` is the extraction function to run in the worker — pdfplumber's
    ``extract_pages`` by default, or ``extract_pages_pypdf`` for the lighter
    fallback when pdfplumber OOMs.

    A fresh single-worker executor PER PDF: a pdfplumber crash or OOM on one
    pathological file kills only that child (the OS reclaims its memory) and
    raises here for the caller to skip. Crucially it can't leave a broken pool
    that cascades into skipping every *subsequent* PDF — which is exactly what
    a single long-lived pool did when one heavy sitrep near the top of the
    batch OOM-killed its worker and took the rest of the week down with it.
    """
    with ProcessPoolExecutor(max_workers=1, mp_context=_MP_SPAWN) as pool:
        return pool.submit(fn, pdf_bytes).result()


def _existing_page_count(s3, bucket: str, text_key: str) -> int | None:
    """Page count of a report's already-extracted text JSONL in S3, or None if
    it hasn't been extracted yet (or the object can't be read).

    Lets the asset skip re-parsing PDFs a prior run already handled —
    re-extraction is wasted work on every re-run and re-OOMs the graphics-
    dense sitreps that exhaust pdfplumber. A read failure returns None so we
    fall back to re-extracting rather than dropping the report.
    """
    try:
        body = s3.get_object(Bucket=bucket, Key=text_key)["Body"].read()
    except Exception:  # noqa: BLE001 — not cached / unreadable → re-extract
        return None
    count = sum(1 for line in body.splitlines() if line.strip())
    return count or None


def _report_summary(
    report_id: str,
    entries: list[dict],
    reports_by_id: dict[str, dict],
    text_key: str,
    num_pages: int,
) -> dict:
    """Slim per-report record the downstream chunk/enrich assets consume.
    Built identically whether the text was freshly extracted or reused from a
    prior run's S3 object."""
    report_meta = reports_by_id.get(report_id) or {}
    fields = report_meta.get("fields") or {}
    report_title = fields.get("title") or entries[0].get("filename", report_id)
    # `date` on a report is `{original, created, changed}` — prefer the
    # publication date; fall back to created when publishers left it blank.
    report_dates = fields.get("date") or {}
    published_at = report_dates.get("original") or report_dates.get("created")
    return {
        "report_id": report_id,
        "report_title": report_title,
        "source_url": fields.get("url") or entries[0]["url"],
        "s3_key": entries[0]["s3_key"],
        "published_at": published_at,
        "s3_text_key": text_key,
        "num_pages": num_pages,
    }


@dg.asset(
    group_name="reliefweb_kb",
    # Ordering-only dep: this asset reads each PDF back from S3 (by key from
    # the manifest), so the upload MUST finish first. Without it, pdf_text and
    # reliefweb_weekly_pdfs_in_s3 both depend only on the manifest and run in
    # parallel — text extraction then races ahead of the upload and hits
    # NoSuchKey. Invisible at 7-day volume; exposed by the 90-day initial run.
    deps=["reliefweb_weekly_pdfs_in_s3"],
)
def reliefweb_weekly_pdf_text(
    context: AssetExecutionContext,
    reliefweb_weekly_pdf_manifest: list[dict],
    reliefweb_weekly_reports_in_s3: list[dict],
) -> list[dict]:
    """Per-report page text as JSONL in S3.

    Returns a slim summary list ``[{report_id, s3_text_key, num_pages,
    report_title, source_url, published_at, s3_pdf_key}]`` that
    downstream assets consume — the actual page text stays in S3 to
    keep the Dagster IO manager payload small.
    """
    bucket = os.environ["S3_BUCKET"]
    s3 = _s3_client()

    # Manifest is one row per attachment; a report may have multiple
    # PDFs. Group by report_id and process each report as a unit so
    # chunk indices are contiguous within a report.
    by_report: dict[str, list[dict]] = {}
    for entry in reliefweb_weekly_pdf_manifest:
        by_report.setdefault(entry["report_id"], []).append(entry)

    # Report metadata by id — pulled from the upstream report list so
    # downstream assets have `report_title` / `published_at` without
    # a second S3 fetch. `date.original` is the publication date the
    # sitrep header shows; `date.created` is ReliefWeb's index time and
    # is used as a fallback when publishers omit the original date.
    reports_by_id: dict[str, dict] = {}
    for report in reliefweb_weekly_reports_in_s3:
        report_id = str(report.get("id") or "")
        if report_id:
            reports_by_id[report_id] = report

    summaries: list[dict] = []
    reused = 0
    for report_id, entries in by_report.items():
        text_key = _text_key(report_id)

        # Idempotency: a report already extracted by a prior run is reused,
        # not re-parsed. Re-extraction is wasted work on every re-run and
        # re-OOMs the handful of graphics-dense sitreps that exhaust
        # pdfplumber — a report that parsed once shouldn't have to survive
        # every subsequent run. Still emit its summary so downstream sees it.
        cached_pages = _existing_page_count(s3, bucket, text_key)
        if cached_pages is not None:
            summaries.append(
                _report_summary(report_id, entries, reports_by_id, text_key, cached_pages)
            )
            reused += 1
            context.log.info(
                "report %s already extracted (%d pages) — reusing s3://%s/%s",
                report_id, cached_pages, bucket, text_key,
            )
            continue

        # A report's PDFs are extracted in manifest order and their
        # pages concatenated. `page_num` is 1-indexed within the
        # combined stream — imperfect (loses per-file provenance) but
        # citation to "report_id + page N" is what the chatbot needs
        # and users read reports as a single logical document anyway.
        all_pages: list[dict] = []
        page_offset = 0

        for entry in entries:
            try:
                obj = s3.get_object(Bucket=bucket, Key=entry["s3_key"])
                pdf_bytes = obj["Body"].read()
            except Exception as exc:  # noqa: BLE001
                # Missing S3 objects are surfaced but don't abort the
                # week — a re-run of `reliefweb_weekly_pdfs_in_s3` will
                # refill them and a subsequent kb build catches up.
                context.log.warning(
                    "s3 fetch failed for %s (%s) — skipping: %s",
                    entry["s3_key"], report_id, exc,
                )
                continue

            try:
                pages = _extract_pages_isolated(pdf_bytes)
            except Exception as exc:  # noqa: BLE001
                # pdfplumber's worker died — almost always an OOM on a
                # graphics-dense page. Retry with pypdf, which reads text
                # without pdfplumber's per-object model and survives pages
                # that exhaust it (lower layout fidelity, but far better than
                # losing the report's text entirely).
                context.log.warning(
                    "pdfplumber failed for %s (%s) — retrying with pypdf: %s",
                    entry["filename"], report_id, exc,
                )
                try:
                    pages = _extract_pages_isolated(pdf_bytes, extract_pages_pypdf)
                    context.log.info(
                        "pypdf fallback extracted %d page(s) for %s (%s)",
                        len(pages), entry["filename"], report_id,
                    )
                except Exception as exc2:  # noqa: BLE001
                    context.log.warning(
                        "pypdf fallback also failed for %s (%s) — skipping: %s",
                        entry["filename"], report_id, exc2,
                    )
                    continue

            for p in pages:
                all_pages.append({
                    "page_num": p["page_num"] + page_offset,
                    "text": p["text"],
                })
            page_offset += len(pages)

        if not all_pages:
            context.log.warning(
                "no extractable text for report %s — skipping",
                report_id,
            )
            continue

        body = b"\n".join(
            json.dumps(
                {"report_id": report_id, "page_num": p["page_num"], "text": p["text"]},
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
            for p in all_pages
        ) + b"\n"
        s3.put_object(
            Bucket=bucket, Key=text_key, Body=body,
            ContentType="application/x-ndjson",
        )

        summaries.append(
            _report_summary(report_id, entries, reports_by_id, text_key, len(all_pages))
        )
        context.log.info(
            "extracted %d pages for report %s → s3://%s/%s",
            len(all_pages), report_id, bucket, text_key,
        )

    context.add_output_metadata({
        "reports_processed": dg.MetadataValue.int(len(summaries)),
        "reports_reused": dg.MetadataValue.int(reused),
        "reports_skipped": dg.MetadataValue.int(len(by_report) - len(summaries)),
        "s3_prefix": dg.MetadataValue.text(f"s3://{bucket}/{S3_TEXT_PREFIX}/"),
    })
    return summaries
