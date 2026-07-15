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

import boto3
import dagster as dg
from dagster import OpExecutionContext
from dotenv import load_dotenv

from clear_context_pipeline.defs.knowledgebase.chunks import (
    CHUNK_OVERLAP_TOKENS,
    CHUNK_TOKENS,
    _slice_into_chunks,
)
from clear_context_pipeline.defs.knowledgebase.enrich import (
    _resolve_location,
    _run_context,
    _run_extraction,
)
from clear_context_pipeline.defs.knowledgebase.pdf_text import _extract_pages
from clear_context_pipeline.providers import (
    clear_api,
    load_guardrails,
    make_embedding_provider,
    make_llm_provider,
)

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


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        region_name=os.environ["S3_REGION"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    )


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
        pages = _extract_pages(pdf_bytes)
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
    from clear_context_pipeline.defs.knowledgebase.datapoints_extract import (
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

    llm = make_llm_provider("extraction")
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

    from clear_context_pipeline.defs.knowledgebase.datapoints_schemas import (
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

    from clear_context_pipeline.defs.situation.generate import (
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
        summary["situation_analysis_id"], summary["superseded_previous"],
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
        "extract → chunk → embed → knowledgebase upsert → datapoint "
        "extraction → aggregation refresh → situation-analysis regen. "
        "Matches the weekly ReliefWeb flow, just single-report."
    ),
)
def process_manual_document_job():
    kb = process_manual_document()
    dp = manual_document_extract_datapoints(kb)
    agg = manual_document_refresh_aggregations(dp)
    manual_document_situation_analysis(agg)
