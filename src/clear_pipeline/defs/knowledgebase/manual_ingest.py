"""Manual document ingest into the knowledge base.

Alternative to the weekly ReliefWeb job for one-off documents (e.g. a
partner-shared report, an internal briefing). Skips the ReliefWeb API
poll — the caller pre-uploads a PDF to S3 and passes its key plus a
few metadata fields via ``RunConfig``. Everything downstream is
identical: extract → chunk → LLM enrich → embed → clear-api upsert.

Launch from Dagster UI:
  Jobs → process_manual_document_job → Launchpad → paste config → Materialize.

Launch from CLI:
  dagster job execute -j process_manual_document_job -c config.yaml

Sample config.yaml::

  ops:
    process_manual_document:
      config:
        s3_key: "reliefweb/manual-uploads/2026/nrc-yemen-brief.pdf"
        report_id: "manual:nrc-yemen-brief-2026-07"
        report_title: "NRC Yemen protection briefing (July 2026)"
        source_url: "https://internal.nrc.no/…"
        published_at: "2026-07-01"

report_id is the dedup key for ``knowledgebase.report_id`` — re-running
with the same report_id atomically replaces the previous version.
Prefix with ``manual:`` (or similar) to keep manual uploads distinct
from ReliefWeb ids at a glance.
"""

import json
import logging
import os
from pathlib import Path

import dagster as dg
from dagster import OpExecutionContext
from dotenv import load_dotenv

from clear_pipeline.defs.knowledgebase.chunks import (
    CHUNK_OVERLAP_TOKENS,
    CHUNK_TOKENS,
    _slice_into_chunks,
)
from clear_pipeline.defs.knowledgebase.enrich import (
    _resolve_location,
    _run_context,
    _run_extraction,
)
from clear_pipeline.defs.knowledgebase.pdf_text import _extract_pages_isolated
from clear_pipeline.providers import (
    clear_api,
    load_guardrails,
    make_embedding_provider,
    make_llm_provider,
)

# Figure capture shares the weekly asset's helpers so the manual path captures
# figures identically (detect → crop → vision → enrich → report_figures), and
# shares upsert.py's figure→KB merge so a manual upload's infographics are
# searchable in the SAME run rather than waiting for the weekly sync. (No
# circular import: neither figures nor upsert imports manual_ingest.)
from clear_pipeline.defs.knowledgebase.enrich import ExtractedParameters
from clear_pipeline.defs.knowledgebase.figures import (
    _CAPTURE_FIGURES,
    _MAX_FIGURES_PER_REPORT,
    _VISION_MODEL_TAG,
    _capture_pdf,
    _enrich_figure,
    _resolve_source_id,
    fig_kind_from_hint,
)
from clear_pipeline.defs.knowledgebase.upsert import merge_report_figures_to_kb
from clear_pipeline.providers.vision import FigureTranscription

load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

# Voyage's public batch limit — matches upsert.py so the two flows
# throttle identically when both are running (via the client-side
# RPM guard in providers/embedding.py).
EMBED_BATCH_SIZE = 128

# Separate S3 prefix for manual-flow debug artefacts. Keeps them out
# of `reliefweb/kb/…` so weekly runs and manual runs don't stomp on
# each other's outputs when we glance at the bucket.
S3_MANUAL_DEBUG_PREFIX = "reliefweb/kb/manual/enriched"

logger = logging.getLogger(__name__)


class ManualDocumentConfig(dg.Config):
    """RunConfig for one manually-uploaded document.

    Emitted to the UI Launchpad as a config form so a non-technical
    user can trigger a re-ingest by pasting the S3 key + report
    metadata.
    """
    s3_key: str
    """Pre-uploaded PDF's S3 key (relative to $S3_BUCKET)."""
    report_id: str
    """Stable dedup key. Re-running with the same id replaces the
    previous version. Prefix with e.g. `manual:` to distinguish from
    ReliefWeb ids at a glance."""
    report_title: str
    """Human-readable title — shown on search hits."""
    source_url: str
    """Canonical URL for the source document, if any. Free-text
    identifier is fine when there's no public link."""
    published_at: str
    """Publication date the citation should show. Accepts ISO date or
    datetime; clear-api's DateTime scalar parses either."""
    team_id: str = ""
    """Team the upload is attributed to (Cap 1, team-scoped upload). Empty for
    a global-admin upload or a scripted launch. Carried for provenance/logging;
    the KB write path doesn't persist it per-row (yet)."""
    uploaded_by_user_id: str = ""
    """clear-api user id of the uploader, for provenance/logging. Empty for a
    scripted launch."""


def _s3_client():
    from clear_pipeline.providers.s3 import s3_client

    return s3_client()


@dg.op(
    description=(
        "End-to-end ingest of one manually-uploaded PDF. Reads the PDF "
        "from S3, splits into chunks, runs the LLM contextualization + "
        "parameter extraction + location resolution, then embeds each "
        "chunk and hands the batch to clear-api's "
        "upsertKnowledgebaseChunks mutation. Re-running with the same "
        "report_id replaces the previous ingest atomically."
    ),
)
def process_manual_document(
    context: OpExecutionContext,
    config: ManualDocumentConfig,
) -> dict:
    """Single-op ingest for a manually uploaded PDF.

    The weekly path splits into four assets because each has a
    reasonable individual failure mode (e.g. transient S3 outage
    without touching the LLM budget). The manual path processes one
    document per run so the granularity buys us less — a single op
    keeps the launch UX simple and the failure surface obvious.

    Enriched chunk JSONL is still written to S3
    (``reliefweb/kb/manual/enriched/<report_id>.jsonl``) as a debug
    artefact so a failed upsert can be replayed without redoing the
    LLM work.
    """
    bucket = os.environ["S3_BUCKET"]
    s3 = _s3_client()
    guardrails = load_guardrails()

    # Provenance for a team-scoped upload (Cap 1) — logged so the ingest run
    # records who uploaded on behalf of which team even though the KB rows don't
    # carry it per-row.
    if config.uploaded_by_user_id or config.team_id:
        context.log.info(
            "manual upload %s attributed to user=%s team=%s",
            config.report_id, config.uploaded_by_user_id or "-", config.team_id or "-",
        )

    # ── 1. Fetch the PDF and extract text per page ───────────────
    context.log.info("fetching %s from s3://%s", config.s3_key, bucket)
    try:
        obj = s3.get_object(Bucket=bucket, Key=config.s3_key)
        pdf_bytes = obj["Body"].read()
    except Exception as exc:
        raise dg.Failure(
            description=f"S3 fetch failed for {config.s3_key}: {exc}",
        ) from exc

    try:
        pages = _extract_pages_isolated(pdf_bytes)
    except Exception as exc:
        raise dg.Failure(
            description=f"pdfplumber failed on {config.s3_key}: {exc}",
        ) from exc

    if not pages:
        raise dg.Failure(
            description=(
                f"No extractable text in {config.s3_key} — scanned PDF "
                "without OCR? Empty document?"
            ),
        )
    context.log.info("extracted %d pages", len(pages))

    # ── 2. Chunk ──────────────────────────────────────────────────
    chunks = _slice_into_chunks(
        pages, chunk_tokens=CHUNK_TOKENS, overlap_tokens=CHUNK_OVERLAP_TOKENS,
    )
    if not chunks:
        raise dg.Failure(description=f"chunker produced no chunks for {config.s3_key}")
    if len(chunks) > guardrails.max_chunks_per_report:
        context.log.warning(
            "%d chunks > KB_MAX_CHUNKS_PER_REPORT (%d) — truncating",
            len(chunks), guardrails.max_chunks_per_report,
        )
        chunks = chunks[: guardrails.max_chunks_per_report]
    context.log.info("chunked into %d chunks", len(chunks))

    # ── 3. Enrich (contextualize + extract + resolve) ────────────
    # doc_text is built unconditionally — even when the KB
    # contextualization step is skipped (`KB_SKIP_CONTEXTUALIZATION`),
    # downstream datapoint extraction still needs the concatenated
    # doc text to run its six-domain LLM calls. The skip flag only
    # gates the vector-side contextualization prefix.
    doc_text = "\n\n".join(
        f"[page {p['page_num']}]\n{p['text']}" for p in pages
    )
    if guardrails.skip_contextualization:
        context.log.warning("KB_SKIP_CONTEXTUALIZATION set — embedding raw chunks")
        llm_context = None
    else:
        llm_context = make_llm_provider("context")
    llm_extract = make_llm_provider("extraction")

    enriched: list[dict] = []
    for chunk in chunks:
        try:
            ctx_prefix = (
                _run_context(llm_context, doc_text, chunk["text"], cache_key=config.report_id)
                if llm_context
                else ""
            )
            embedded_text = (
                f"{ctx_prefix}\n\n{chunk['text']}" if ctx_prefix else chunk["text"]
            )
            params = _run_extraction(llm_extract, embedded_text)

            resolved_ids: list[str] = []
            unresolved_pcodes: list[str] = []
            for ref in params.locations:
                location_id = _resolve_location(ref)
                if location_id:
                    resolved_ids.append(location_id)
                elif ref.pcode:
                    unresolved_pcodes.append(ref.pcode)

            enriched.append({
                "report_id": config.report_id,
                "chunk_index": chunk["chunk_index"],
                "page_start": chunk["page_start"],
                "page_end": chunk["page_end"],
                "chunk_text": chunk["text"],
                "context_prefix": ctx_prefix,
                "embedded_text": embedded_text,
                "location_ids": resolved_ids,
                "location_pcodes": unresolved_pcodes,
                "time_range_start": params.time_range_start,
                "time_range_end": params.time_range_end,
                "event_types": params.event_types,
                "need_sectors": list(params.need_sectors),
            })
        except Exception as exc:  # noqa: BLE001
            context.log.warning(
                "enrich failed for chunk %d: %s — skipping",
                chunk["chunk_index"], exc,
            )

    if not enriched:
        raise dg.Failure(
            description=(
                "All chunks failed enrichment. Check LLM provider config "
                "and rate limits."
            ),
        )

    # Persist the enriched batch to S3 as a debug / replay artefact.
    debug_key = f"{S3_MANUAL_DEBUG_PREFIX}/{config.report_id}.jsonl"
    body = b"\n".join(
        json.dumps(e, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        for e in enriched
    ) + b"\n"
    s3.put_object(Bucket=bucket, Key=debug_key, Body=body, ContentType="application/x-ndjson")
    context.log.info("wrote enriched debug snapshot to s3://%s/%s", bucket, debug_key)

    # ── 4. Embed + upsert via clear-api ──────────────────────────
    embedder = make_embedding_provider()
    provider_name = embedder.provider_name
    model_name = embedder.model

    embeddings: list[list[float]] = []
    for i in range(0, len(enriched), EMBED_BATCH_SIZE):
        batch = enriched[i : i + EMBED_BATCH_SIZE]
        results = embedder.embed([e["embedded_text"] for e in batch], input_type="document")
        embeddings.extend(r.embedding for r in results)

    if len(embeddings) != len(enriched):
        raise dg.Failure(
            description=(
                f"Embedding count mismatch: got {len(embeddings)} for "
                f"{len(enriched)} enriched chunks."
            ),
        )

    chunk_inputs: list[dict] = []
    for row, vec in zip(enriched, embeddings, strict=True):
        chunk_inputs.append({
            "chunkIndex": row["chunk_index"],
            "pageStart": row["page_start"],
            "pageEnd": row["page_end"],
            "chunkText": row["chunk_text"],
            "contextPrefix": row["context_prefix"],
            "embeddedText": row["embedded_text"],
            "embeddingProvider": provider_name,
            "embeddingModel": model_name,
            "embedding": vec,
            "locationIds": row.get("location_ids") or [],
            "locationPcodes": row.get("location_pcodes") or [],
            "timeRangeStart": row.get("time_range_start"),
            "timeRangeEnd": row.get("time_range_end"),
            "eventTypes": row.get("event_types") or [],
            "needSectors": row.get("need_sectors") or [],
        })

    try:
        result = clear_api.upsert_knowledgebase_chunks(
            report_id=config.report_id,
            report_title=config.report_title,
            source_url=config.source_url,
            s3_key=config.s3_key,
            published_at=config.published_at,
            chunks=chunk_inputs,
            team_id=config.team_id or None,
            uploaded_by_user_id=config.uploaded_by_user_id or None,
        )
    except clear_api.ClearApiError as exc:
        # 4xx from clear-api — payload shape is wrong. Fail loud so the
        # user can fix and re-launch.
        raise dg.Failure(
            description=f"clear-api rejected upsert for {config.report_id}: {exc}",
        ) from exc

    context.log.info(
        "manual ingest complete for %s: deleted=%d inserted=%d (%s/%s)",
        config.report_id, result["chunksDeleted"], result["chunksInserted"],
        provider_name, model_name,
    )

    return {
        "report_id": config.report_id,
        "report_title": config.report_title,
        "source_url": config.source_url,
        "published_at": config.published_at,
        "s3_key": config.s3_key,
        # Attribution carried downstream so the figure merge can tag figures too.
        "team_id": config.team_id,
        "uploaded_by_user_id": config.uploaded_by_user_id,
        # `doc_text` travels through Dagster's IO manager to feed the
        # datapoint-extraction op downstream — the alternative (re-
        # download + re-extract from S3 in the next op) would double
        # the pdfplumber cost per manual upload for no gain.
        "doc_text": doc_text,
        "chunks_deleted": result["chunksDeleted"],
        "chunks_inserted": result["chunksInserted"],
        "chunks_enriched": len(enriched),
        "chunks_dropped": len(chunks) - len(enriched),
        "embedding_provider": provider_name,
        "embedding_model": model_name,
        "debug_s3_key": debug_key,
    }


@dg.op(
    description=(
        "Capture figures from one manually-uploaded PDF (opt-in via "
        "KB_CAPTURE_FIGURES): detect + crop + transcribe + enrich, upsert to "
        "report_figures, then merge the transcriptions into the knowledgebase "
        "(figures_only) so the upload's infographics are searchable in the same "
        "run. No-op when capture is disabled."
    ),
)
def manual_document_capture_figures(
    context: OpExecutionContext,
    kb_result: dict,
) -> dict:
    """Figure capture for the manual path — the weekly ``reliefweb_weekly_figures``
    asset's work for a single uploaded PDF, plus the inline KB merge.

    Runs only when ``KB_CAPTURE_FIGURES`` is set (same gate as the weekly asset),
    because rendering graphics-dense pages is memory-heavy. Best-effort: a capture
    or merge failure is logged and the op still returns, so the KB text ingest
    (already committed by the previous op) is never rolled back by a figure
    problem."""
    report_id = kb_result["report_id"]
    if not _CAPTURE_FIGURES:
        context.log.info(
            "KB_CAPTURE_FIGURES not set — skipping figure capture for %s", report_id,
        )
        return {**kb_result, "figures_captured": False, "figure_chunks_merged": 0}

    bucket = os.environ["S3_BUCKET"]
    s3 = _s3_client()

    try:
        obj = s3.get_object(Bucket=bucket, Key=kb_result["s3_key"])
        pdf_bytes = obj["Body"].read()
    except Exception as exc:  # noqa: BLE001 — don't fail the run over a figure fetch
        context.log.warning(
            "figure capture: S3 fetch failed for %s (%s) — skipping figures",
            kb_result["s3_key"], exc,
        )
        return {**kb_result, "figures_captured": False, "figure_chunks_merged": 0}

    # Detect + crop + transcribe. `iso3="manual"` just picks the S3 prefix
    # (reliefweb/kb/figures/manual/…); manual uploads aren't country-partitioned.
    raw_figures, _ = _capture_pdf(
        context, pdf_bytes,
        report_id=report_id, iso3="manual", page_offset=0,
        seen_hashes=set(), remaining=_MAX_FIGURES_PER_REPORT,
    )
    if not raw_figures:
        context.log.info("figure capture: no figures detected in %s", report_id)
        return {**kb_result, "figures_captured": True, "figures_written": 0, "figure_chunks_merged": 0}

    llm_extract = make_llm_provider("extraction")
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
                context.log.warning(
                    "figure enrich failed for %s p%s: %s", report_id, fig["page_num"], exc,
                )
            # No ReliefWeb publisher metadata on a manual upload — the figure's own
            # printed source (if any) is the only attribution hint.
            source_id = _resolve_source_id(t, {})

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

    # Write to report_figures (the asset store). upsert_report_figures deletes-
    # then-inserts per report, so figures_input is non-empty here by construction.
    try:
        result = clear_api.upsert_report_figures(
            report_id=report_id,
            report_title=kb_result["report_title"],
            source_url=kb_result["source_url"],
            extracted_by_model=_VISION_MODEL_TAG,
            figures=figures_input,
        )
        written = int(result.get("count", len(figures_input)))
    except Exception as exc:  # noqa: BLE001 — don't roll back the text ingest
        context.log.warning("upsert_report_figures failed for %s: %s", report_id, exc)
        return {**kb_result, "figures_captured": False, "figure_chunks_merged": 0}

    # Inline KB merge so the upload's infographics are searchable now. Normalize
    # the report_figures input shape to the merge helper's shape.
    normalized = [{
        "s3_key": f["s3Key"],
        "kind": f["kind"],
        "page": f["pageNumber"],
        "title": f["title"],
        "transcription": f["transcription"],
        "location_ids": f["locationIds"],
        "location_pcodes": f["locationPcodes"],
        "event_types": f["eventTypes"],
        "need_sectors": f["needSectors"],
        "time_range_start": f["timeRangeStart"],
        "time_range_end": f["timeRangeEnd"],
    } for f in figures_input]

    merged = 0
    try:
        merged = merge_report_figures_to_kb(
            report_id=report_id,
            report_title=kb_result["report_title"],
            source_url=kb_result["source_url"],
            report_s3_key=kb_result["s3_key"],
            published_at=kb_result["published_at"],
            figures=normalized,
            embedder=make_embedding_provider(),
            team_id=kb_result.get("team_id") or None,
            uploaded_by_user_id=kb_result.get("uploaded_by_user_id") or None,
        )
    except Exception as exc:  # noqa: BLE001 — figures are in report_figures regardless;
        # the weekly figure→KB sync will merge them on its next run.
        context.log.warning(
            "figure→KB merge failed for %s (figures still in report_figures; "
            "weekly sync will retry): %s", report_id, exc,
        )

    context.log.info(
        "manual figure capture for %s: figures_written=%d figure_chunks_merged=%d",
        report_id, written, merged,
    )
    return {
        **kb_result,
        "figures_captured": True,
        "figures_written": written,
        "figure_chunks_merged": merged,
    }


# ────────────────────────────────────────────────────────────────────
# Downstream ops — parity with the weekly job for manual uploads.
#
# When a document lands via `uploadKnowledgebaseDocument` in clear-api,
# the operator's intent is "this contains crucial info we want on the
# dashboard now." The knowledgebase upsert alone makes it searchable,
# but the deterministic Datapoints tile + the situation-analysis
# narrative both derive from `report_datapoints` and
# `aggregated_datapoints` — so those need to run too, else the manual
# upload only lands in vector RAG and stays invisible to the dashboard
# until the next weekly cron picks it up.
# ────────────────────────────────────────────────────────────────────


@dg.op(
    description=(
        "Domain-partitioned datapoint extraction for one manually "
        "uploaded document. Reuses the same helper the weekly asset "
        "runs — six sub-schemas, per-domain failure isolation, hot "
        "totals hoisted, upsert into `report_datapoints`."
    ),
)
def manual_document_extract_datapoints(
    context: OpExecutionContext,
    kb_result: dict,
) -> dict:
    """Reads the report's doc_text off the previous op's output and
    runs the same extraction pipeline the weekly asset does."""
    from clear_pipeline.defs.knowledgebase.datapoints_extract import (
        _NothingExtracted,
        extract_datapoints_for_one_report,
    )

    guardrails = load_guardrails()
    if guardrails.skip_contextualization:
        # Same kill-switch guarding the weekly extraction path — when
        # LLM budget is exhausted, skip cleanly and let the operator
        # re-run later.
        context.log.warning(
            "KB_SKIP_CONTEXTUALIZATION set — skipping datapoint extraction for manual upload",
        )
        return {**kb_result, "datapoints_extracted": False}

    llm = make_llm_provider("datapoints")
    s3 = _s3_client()
    bucket = os.environ["S3_BUCKET"]

    try:
        summary = extract_datapoints_for_one_report(
            report_id=kb_result["report_id"],
            report_title=kb_result["report_title"],
            source_url=kb_result["source_url"],
            published_at=kb_result["published_at"],
            doc_text=kb_result["doc_text"],
            llm=llm,
            s3=s3,
            s3_bucket=bucket,
            log_context=context.log,
        )
    except _NothingExtracted as exc:
        # Every domain failed — probably a scanned PDF with no useful
        # text, or a provider-side outage. Fail the op loud rather
        # than silently upserting nothing; operator sees a red asset
        # in Dagster UI and can re-run.
        raise dg.Failure(
            description=f"Datapoint extraction produced nothing for {kb_result['report_id']}: {exc}",
        ) from exc

    context.log.info(
        "manual datapoint extraction: report_id=%s domains_ok=%d/%d locations_resolved=%d",
        summary["report_id"], len(summary["domains_ok"]), 6,
        summary["resolved_locations"],
    )
    return {
        **kb_result,
        "datapoints_extracted": True,
        "reporting_period_start": summary["reporting_period_start"],
        "reporting_period_end": summary["reporting_period_end"],
        "datapoints_summary": {
            "domains_ok": summary["domains_ok"],
            "domains_failed": summary["domains_failed"],
            "resolved_locations": summary["resolved_locations"],
            "unresolved_pcodes": summary["unresolved_pcodes"],
        },
    }


@dg.op(
    description=(
        "Refresh aggregated datapoints for the window covering this "
        "manually-uploaded report. Cascades to invalidate the "
        "corresponding yearly-country situation-analysis snapshot (via "
        "the mutation's built-in cascade) so the regen op picks it up."
    ),
)
def manual_document_refresh_aggregations(
    context: OpExecutionContext,
    dp_result: dict,
) -> dict:
    """Narrow-window aggregation refresh keyed off the manual report's
    reporting period. The refresh walks all four tiers internally, so
    weekly-A2 / monthly-A1 / yearly-country / all-time-country all get
    the new report's contribution folded in.

    Skips gracefully when the datapoint extraction step didn't
    produce a new row — nothing to aggregate."""
    if not dp_result.get("datapoints_extracted"):
        context.log.info("no datapoints extracted — skipping aggregation refresh")
        return {**dp_result, "aggregation_refreshed": False}

    from clear_pipeline.defs.knowledgebase.datapoints_schemas import (
        SCHEMA_VERSION as DP_SCHEMA_VERSION,
    )

    # Window derivation. `reportingPeriodEnd` is the aggregation-side
    # convention for placing a report in its bucket; fall back to
    # `published_at` when the LLM didn't infer a period.
    period_end = dp_result.get("reporting_period_end") or dp_result["published_at"]
    period_start = dp_result.get("reporting_period_start") or dp_result["published_at"]
    # Refresh over a small window around the report — a 2-day pad
    # either side handles any timezone drift between the extractor's
    # ISO date and the aggregator's TIMESTAMPTZ bucketing.
    from_iso, to_iso = _pad_window(period_start, period_end, pad_days=2)

    try:
        result = clear_api.refresh_aggregated_datapoints(
            from_iso=from_iso, to_iso=to_iso, schema_version=DP_SCHEMA_VERSION,
        )
    except clear_api.ClearApiError as exc:
        raise dg.Failure(
            description=f"clear-api rejected aggregation refresh: {exc}",
        ) from exc

    context.log.info(
        "manual aggregation refresh: computed=%d superseded=%d situation_analyses_invalidated=%d",
        result["computedBuckets"], result["supersededBuckets"],
        result.get("situationAnalysesInvalidated", 0),
    )
    return {
        **dp_result,
        "aggregation_refreshed": True,
        "aggregation_result": result,
    }


@dg.op(
    description=(
        "Regenerate the situation-analysis snapshot for the country "
        "and calendar year covering this manually-uploaded report. "
        "POC scope: hardcoded to Sudan; future generalisation would "
        "resolve the country from the report's resolved locations."
    ),
)
def manual_document_situation_analysis(
    context: OpExecutionContext,
    agg_result: dict,
) -> dict:
    """Runs the same generator the weekly asset uses (single-country
    helper). The `aggregatedDatapointId` linkage in the resulting
    situation_analysis row points at the fresh yearly-country bucket
    the previous op just materialised."""
    if not agg_result.get("aggregation_refreshed"):
        context.log.info(
            "no aggregation refresh happened — skipping situation-analysis regen",
        )
        return {**agg_result, "situation_regenerated": False}

    from clear_pipeline.defs.situation.generate import (
        generate_and_upsert_for_country_year,
    )
    from datetime import datetime, timezone

    # POC: Sudan only. When we generalise to multi-country, this op's
    # config takes a `country_name` field and we call
    # generate_and_upsert_for_country_year for each affected country.
    country_name = "Sudan"
    year = datetime.now(timezone.utc).year

    summary = generate_and_upsert_for_country_year(
        country_name=country_name,
        year=year,
        log_context=context.log,
    )
    if summary is None:
        # generator returned None → country location not resolved or
        # upsert rejected. Downgrade to a warning; the manual upload
        # still landed in KB + report_datapoints, just no fresh
        # situation snapshot.
        context.log.warning(
            "situation-analysis regen returned no summary for %s/%d — skipping",
            country_name, year,
        )
        return {**agg_result, "situation_regenerated": False}

    context.log.info(
        "manual situation-analysis regen complete: analysis_id=%s superseded=%s",
        summary["analysis_id"], summary["superseded_previous"],
    )
    return {
        **agg_result,
        "situation_regenerated": True,
        "situation_analysis": summary,
    }


def _pad_window(start_iso: str, end_iso: str, *, pad_days: int) -> tuple[str, str]:
    """Widen an ISO-8601 window by `pad_days` on each side. Isolated
    here so both the aggregation op and any future backfill can reuse
    the same pad convention."""
    from datetime import datetime, timedelta

    def _parse(iso: str) -> datetime:
        return datetime.fromisoformat(iso.replace("Z", "+00:00"))

    start = _parse(start_iso) - timedelta(days=pad_days)
    end = _parse(end_iso) + timedelta(days=pad_days)
    return start.isoformat(), end.isoformat()


@dg.job(
    description=(
        "One-off manual document ingest. Full chain per upload: "
        "extract → chunk → embed → knowledgebase upsert → figure capture "
        "(opt-in) → datapoint extraction → aggregation refresh → "
        "situation-analysis regen. Matches the weekly ReliefWeb flow, just "
        "single-report."
    ),
)
def process_manual_document_job():
    kb = process_manual_document()
    # Figure capture hangs off the KB result and runs independently of the
    # datapoint/aggregation chain — a no-op unless KB_CAPTURE_FIGURES is set, and
    # best-effort either way so it never blocks the dashboard pipeline below.
    manual_document_capture_figures(kb)
    dp = manual_document_extract_datapoints(kb)
    agg = manual_document_refresh_aggregations(dp)
    manual_document_situation_analysis(agg)
