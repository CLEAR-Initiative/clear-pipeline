"""Infographic capture — detect, crop, transcribe, and store report figures.

For each ReliefWeb PDF in the week's manifest, this asset:

  1. **Detects** figure regions per page (``pdf_regions.detect_figure_regions`` —
     embedded images, ruled tables, or a whole-page composite panel).
  2. **Crops + renders** each region to a downscaled PNG (``pdf_render``) and
     uploads it to S3 (``figures_prefix(iso3)/<report_id>/<page>-<hash>.png``),
     deduping repeated banners/logos by content hash within a report.
  3. **Transcribes** each crop with a vision model (``vision.transcribe_figure``)
     into structured, embeddable JSON (kind / title / rows / groups / callouts).
  4. **Enriches** each figure with the same retrieval params as text chunks
     (locations / event types / need sectors / time range) via the ``extraction``
     LLM, and resolves the figure's cited source to a ``data_sources`` id.
  5. **Upserts** the report's figures to clear-api (``upsertReportFigures`` —
     replace-on-reingest), making them retrievable via ``reportFigures(...)``.

Cost/precision posture (spec §9): region detection is free; only flagged regions
pay for a vision call (~$0.006/page). The whole stage is **opt-in** via
``KB_CAPTURE_FIGURES=1`` — it renders graphics-dense pages (the memory-heavy
ones), so it stays off until deliberately enabled, and never blocks the text KB.

S3 layout:
    reliefweb/kb/figures/<iso3>/<format-slug>/<report_id>.jsonl   (record)
    reliefweb/kb/figures/<iso3>/<format-slug>/<report_id>/<page>-<hash>.png
"""

import io
import json
import os
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dotenv import load_dotenv

from clear_context_pipeline.defs.knowledgebase.enrich import (
    EXTRACTION_SYSTEM,
    ExtractedParameters,
    _resolve_location,
)
from clear_context_pipeline.defs.reliefweb_partitions import (
    country_partitions,
    figures_prefix,
)
from clear_context_pipeline.providers import clear_api, make_llm_provider
from clear_context_pipeline.providers.pdf_regions import detect_figure_regions, pad_bbox
from clear_context_pipeline.providers.pdf_render import (
    figure_s3_key,
    image_hash,
    render_region_png,
)
from clear_context_pipeline.providers.vision import (
    FigureTranscription,
    flatten_transcription as _transcription_text,
    transcribe_figure,
)

load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

# Opt-in: rendering + vision only run when explicitly enabled. Off, the asset is
# a no-op that emits an empty summary, so it can sit in the graph without paying
# render/vision cost or risking OOM on dense pages until deliberately switched on.
_CAPTURE_FIGURES = os.environ.get("KB_CAPTURE_FIGURES", "0").strip().lower() in ("1", "true", "yes")
# Runaway guard: a malformed PDF that flags every page shouldn't fan out into
# hundreds of vision calls. Head-N per report.
_MAX_FIGURES_PER_REPORT = int(os.environ.get("KB_MAX_FIGURES_PER_REPORT", "40"))
# The model that did the transcription — recorded on every figure row for
# provenance + re-extraction decisions (mirrors report_datapoints.extractedByModel).
_VISION_MODEL_TAG = os.environ.get("LLM_VISION_MODEL", os.environ.get("LLM_EXTRACTION_MODEL", "haiku"))


def _s3_client():
    from clear_context_pipeline.providers.s3 import s3_client

    return s3_client()


def _record_key(iso3: str, report_id: str) -> str:
    return f"{figures_prefix(iso3)}/{report_id}.jsonl"


def _already_captured(s3, bucket: str, key: str) -> bool:
    """True when a prior run already wrote this report's figures record — lets a
    resume skip the expensive render+vision work. A read failure is treated as
    'not captured' so we re-capture rather than silently skip."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:  # noqa: BLE001 — not present / unreadable → re-capture
        return False


def _enrich_figure(llm_extract, t: FigureTranscription) -> ExtractedParameters:
    """Extract retrieval params (locations / time / event types / sectors) from a
    figure's transcription text. Empty text → empty params (no LLM call)."""
    blob = _transcription_text(t)
    if not blob:
        return ExtractedParameters()
    return llm_extract.complete_structured(
        system=EXTRACTION_SYSTEM,
        user=f"Figure transcription:\n\n{blob}",
        schema=ExtractedParameters,
        max_tokens=1200,
    )


def _resolve_source_id(t: FigureTranscription, report: dict) -> str | None:
    """Resolve the figure's attribution to a ``data_sources`` id. Prefer the
    source printed ON the figure (the vision pass captured it); fall back to the
    report's publisher so a figure without its own credit still attributes to the
    report's org. Best-effort — a resolver hiccup yields None, not a failure."""
    name = (t.source or "").strip() or (report.get("publisher_name") or "").strip()
    if not name:
        return None
    try:
        return clear_api.resolve_data_source(
            name=name, homepage=report.get("publisher_homepage"),
        )
    except Exception:  # noqa: BLE001 — attribution is best-effort
        return None


def _capture_pdf(
    context: AssetExecutionContext,
    pdf_bytes: bytes,
    *,
    report_id: str,
    iso3: str,
    page_offset: int,
    seen_hashes: set[str],
    remaining: int,
) -> tuple[list[dict], int]:
    """Detect + render + transcribe every figure in one PDF. Returns
    ``(figures, n_nonempty_pages)`` — the raw per-figure dicts (image already
    uploaded to S3, transcription attached but NOT yet enriched) and the count of
    NON-EMPTY pages, which the caller advances ``page_offset`` by so figure
    pageNumbers line up with pdf_text's chunk page numbers across a report's
    multiple PDFs. Per-page best-effort: one bad page contributes nothing rather
    than aborting the report."""
    import pdfplumber

    bucket = os.environ["S3_BUCKET"]
    s3 = _s3_client()
    prefix = figures_prefix(iso3)
    out: list[dict] = []
    # Count of NON-EMPTY pages — the value the caller advances page_offset by, so
    # figure pageNumbers line up with pdf_text's chunk page numbers. pdf_text
    # numbers pages via extract_pages, which DROPS blank/image-only pages and
    # offsets by that non-empty count; advancing here by the physical page count
    # instead drifts every subsequent figure on a multi-PDF report. `extract_text`
    # non-empty is the cheap proxy for extract_pages' (text+tables) test — they
    # diverge only for the rare text-less-but-ruled-table page.
    n_nonempty = 0

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages):
            page_num = i + 1 + page_offset  # physical page index within PDF (1-based) + offset
            try:
                page_text = (page.extract_text() or "").strip()
            except Exception:  # noqa: BLE001 — context only; fine to skip
                page_text = ""
            if page_text:
                n_nonempty += 1

            # Keep iterating (to finish the page count) even after the per-report
            # figure cap is hit — just stop capturing more figures.
            if len(out) >= remaining:
                continue
            try:
                regions = detect_figure_regions(page)
            except Exception as exc:  # noqa: BLE001
                context.log.warning("figure detect failed p%d of %s: %s", page_num, report_id, exc)
                continue
            if not regions:
                continue

            for region in regions:
                if len(out) >= remaining:
                    break
                try:
                    bbox = region.bbox
                    if not region.is_full_page:
                        bbox = pad_bbox(bbox, float(page.width), float(page.height))
                    png = render_region_png(page, bbox, is_full_page=region.is_full_page)
                    h = image_hash(png)
                    if h in seen_hashes:
                        continue  # repeated banner/logo across pages
                    seen_hashes.add(h)

                    s3_key = figure_s3_key(prefix, report_id, page_num, png)
                    s3.put_object(Bucket=bucket, Key=s3_key, Body=png, ContentType="image/png")

                    transcription = transcribe_figure(
                        png_bytes=png,
                        kind_hint=region.kind_hint,
                        page_context=page_text or None,
                    )
                    out.append({
                        "page_num": page_num,
                        "bbox": list(bbox) if not region.is_full_page else [],
                        "is_full_page": region.is_full_page,
                        "s3_key": s3_key,
                        "kind_hint": region.kind_hint,
                        "transcription": transcription,
                    })
                except Exception as exc:  # noqa: BLE001 — isolate per-region
                    context.log.warning(
                        "figure capture failed p%d of %s (%s): %s",
                        page_num, report_id, region.kind_hint, exc,
                    )
    return out, n_nonempty


@dg.asset(
    group_name="reliefweb_kb",
    # Ordering-only dep: PDFs must be uploaded before we read them back (same
    # NoSuchKey race pdf_text guards against). pdf_text also carries the
    # per-report metadata (title/url/publisher/country) we attach to each figure.
    deps=["reliefweb_weekly_pdfs_in_s3"],
    partitions_def=country_partitions,
)
def reliefweb_weekly_figures(
    context: AssetExecutionContext,
    reliefweb_weekly_pdf_manifest: list[dict],
    reliefweb_weekly_pdf_text: list[dict],
) -> list[dict]:
    """Per-report captured figures: cropped PNGs in S3 + structured transcription
    upserted to clear-api's ``report_figures``. Returns a slim summary list.

    No-op (empty summary) unless ``KB_CAPTURE_FIGURES=1`` — the stage is opt-in
    so the text KB never waits on (or OOMs from) figure rendering."""
    iso3 = context.partition_key
    bucket = os.environ["S3_BUCKET"]

    if not _CAPTURE_FIGURES:
        context.log.info("KB_CAPTURE_FIGURES not set — skipping figure capture for %s", iso3)
        context.add_output_metadata({"enabled": dg.MetadataValue.bool(False)})
        return []

    s3 = _s3_client()
    llm_extract = make_llm_provider("extraction")

    # PDF attachments grouped by report, in manifest order (matches pdf_text's
    # concatenation so pageNumber lines up across a report's multiple PDFs).
    by_report: dict[str, list[dict]] = {}
    for entry in reliefweb_weekly_pdf_manifest:
        by_report.setdefault(entry["report_id"], []).append(entry)

    # Per-report metadata (title/url/publisher/country) from the text stage.
    meta_by_report = {r["report_id"]: r for r in reliefweb_weekly_pdf_text}

    summaries: list[dict] = []
    total_figures = 0
    reused = 0
    for report_id, entries in by_report.items():
        report = meta_by_report.get(report_id)
        if not report:
            # No text summary → the report was skipped upstream (unreadable PDF).
            # Nothing to attribute a figure to; skip it here too.
            continue

        record_key = _record_key(iso3, report_id)
        if _already_captured(s3, bucket, record_key):
            reused += 1
            context.log.info("report %s figures already captured — reusing", report_id)
            # Still surface a summary so downstream counts are stable.
            summaries.append({"report_id": report_id, "s3_record_key": record_key, "reused": True})
            continue

        seen_hashes: set[str] = set()
        raw_figures: list[dict] = []
        page_offset = 0
        for entry in entries:
            if len(raw_figures) >= _MAX_FIGURES_PER_REPORT:
                break
            try:
                obj = s3.get_object(Bucket=bucket, Key=entry["s3_key"])
                pdf_bytes = obj["Body"].read()
            except Exception as exc:  # noqa: BLE001
                context.log.warning("s3 fetch failed for %s (%s): %s", entry["s3_key"], report_id, exc)
                continue
            try:
                captured, n_nonempty = _capture_pdf(
                    context, pdf_bytes,
                    report_id=report_id, iso3=iso3, page_offset=page_offset,
                    seen_hashes=seen_hashes,
                    remaining=_MAX_FIGURES_PER_REPORT - len(raw_figures),
                )
                raw_figures.extend(captured)
                # Advance by this PDF's NON-EMPTY page count — the same measure
                # pdf_text uses — so the next attachment's pageNumbers stay aligned
                # with the chunk page numbers.
                page_offset += n_nonempty
            except Exception as exc:  # noqa: BLE001 — one bad PDF, not the report
                context.log.warning("figure capture failed for %s (%s): %s", entry["s3_key"], report_id, exc)

        # Enrich + build the GraphQL figure inputs.
        figures_input: list[dict] = []
        for fig in raw_figures:
            t: FigureTranscription | None = fig["transcription"]
            kind = (t.kind if t else fig.get("kind")) or fig_kind_from_hint(fig)
            params = ExtractedParameters()
            source_id = None
            if t:
                try:
                    params = _enrich_figure(llm_extract, t)
                except Exception as exc:  # noqa: BLE001 — enrichment is best-effort
                    context.log.warning("figure enrich failed for %s p%s: %s", report_id, fig["page_num"], exc)
                source_id = _resolve_source_id(t, report)

            resolved_ids: list[str] = []
            unresolved_pcodes: list[str] = []
            for ref in params.locations:
                loc_id = _resolve_location(ref)
                if loc_id:
                    resolved_ids.append(loc_id)
                elif ref.pcode:
                    unresolved_pcodes.append(ref.pcode)

            figures_input.append({
                "pageNumber": fig["page_num"],
                "bbox": fig["bbox"],
                "isFullPage": fig["is_full_page"],
                "s3Key": fig["s3_key"],
                "kind": kind,
                "title": t.title if t else None,
                "description": t.description if t else None,
                "transcription": t.model_dump() if t else None,
                "sourceId": source_id,
                "locationIds": resolved_ids,
                "locationPcodes": unresolved_pcodes,
                "eventTypes": params.event_types,
                "needSectors": list(params.need_sectors),
                "timeRangeStart": params.time_range_start,
                "timeRangeEnd": params.time_range_end,
            })

        # #142-B: never upsert an empty figures list. `upsertReportFigures` deletes-
        # then-inserts per report, so sending [] would WIPE the report's previously-
        # captured figures — and a render/detect failure that captures nothing looks
        # identical to a genuinely figure-less report. Err toward preservation: skip
        # the upsert AND the record marker, so the report is re-attempted next run
        # rather than marked done with zero figures. (A figure that rendered but whose
        # vision transcription failed is still non-empty here — kind falls back to the
        # structural hint — so it's stored; only a wholly-empty capture is skipped.)
        if not figures_input:
            context.log.info(
                "report %s captured no figures — skipping upsert (not wiping) + record",
                report_id,
            )
            summaries.append({"report_id": report_id, "s3_record_key": None, "figures_written": 0})
            continue

        # Persist a record of what we captured (idempotency marker + audit) BEFORE
        # the upsert.
        record_body = b"\n".join(
            json.dumps(f, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            for f in figures_input
        ) + b"\n"
        s3.put_object(Bucket=bucket, Key=record_key, Body=record_body, ContentType="application/x-ndjson")

        try:
            result = clear_api.upsert_report_figures(
                report_id=report_id,
                report_title=report["report_title"],
                source_url=report["source_url"],
                extracted_by_model=_VISION_MODEL_TAG,
                figures=figures_input,
            )
            written = int(result.get("count", len(figures_input)))
        except Exception as exc:  # noqa: BLE001 — surface, don't abort the batch
            context.log.warning("upsert_report_figures failed for %s: %s", report_id, exc)
            written = 0

        total_figures += written
        summaries.append({
            "report_id": report_id,
            "s3_record_key": record_key,
            "figures_written": written,
        })
        context.log.info("captured %d figure(s) for report %s", written, report_id)

    context.add_output_metadata({
        "enabled": dg.MetadataValue.bool(True),
        "reports_processed": dg.MetadataValue.int(len(summaries)),
        "reports_reused": dg.MetadataValue.int(reused),
        "figures_written": dg.MetadataValue.int(total_figures),
        "s3_prefix": dg.MetadataValue.text(f"s3://{bucket}/{figures_prefix(iso3)}/"),
    })
    return summaries


def fig_kind_from_hint(fig: dict) -> str:
    """Fallback kind when vision transcription is unavailable: map the structural
    hint to a stored kind. 'page'/'image' → infographic; 'table' → table."""
    hint = fig.get("kind_hint") or ""
    return "table" if hint == "table" else "infographic"
