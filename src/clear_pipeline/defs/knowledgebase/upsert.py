"""Embed enriched chunks and upsert into ``clear-api.knowledgebase``.

All DB access routes through clear-api's ``upsertKnowledgebaseChunks``
mutation — matches clear-pipeline's pattern (no direct psycopg writes
from Dagster). clear-api owns:
  - the transaction (delete + N inserts as one unit),
  - the pgvector cast (``$14::vector(1024)``),
  - dimension validation,
  - auth (Bearer API key with admin/pipeline role).

Per-report loop:
  1. Read enriched chunk JSONL from S3.
  2. Batch-embed ``embedded_text`` through the configured provider.
  3. POST one ``upsertKnowledgebaseChunks`` mutation with the whole
     report's payload. Server does delete-then-insert atomically.

Chunk grouping keeps the mutation payload bounded — one report at a
time avoids a single 20 MB GraphQL body when a run has many reports.
"""

import json
import os
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dotenv import load_dotenv

from clear_pipeline.defs.reliefweb_partitions import country_partitions
from clear_pipeline.providers import clear_api, make_embedding_provider
from clear_pipeline.providers.vision import (
    FigureTranscription,
    flatten_transcription,
)

load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

# Voyage's public batch limit; Together AI and TEI accept larger batches
# but there's no upside pushing past this — the LLM contextualization
# step upstream is the throughput bottleneck, not embeddings.
EMBED_BATCH_SIZE = 128

# Figure-transcription chunks (infographic capture) are merged into the same
# per-report KB batch as text, but their chunk_index must not collide with the
# text chunks' 0..N indices in the delete-then-insert. Base them high.
FIGURE_CHUNK_INDEX_BASE = 1_000_000


def _s3_client():
    from clear_pipeline.providers.s3 import s3_client

    return s3_client()


def _read_enriched(s3, bucket: str, key: str) -> list[dict]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line]


def _figure_kb_rows(s3, bucket: str, record_key: str | None) -> list[dict]:
    """Turn a report's captured-figure record (written by the figures asset) into
    knowledge-base rows so figures are retrievable by their own numbers/labels
    alongside body text (the RAG merge). Same row shape as an enriched text chunk
    plus ``figure_s3_key`` / ``figure_kind``, so the embed + upsert path treats
    them uniformly.

    Skips figures whose vision transcription failed or is empty (nothing to
    embed) — the image is still stored in ``report_figures`` regardless. Returns
    ``[]`` when capture is disabled or the record is absent."""
    if not record_key:
        return []
    try:
        body = s3.get_object(Bucket=bucket, Key=record_key)["Body"].read().decode("utf-8")
    except Exception:  # noqa: BLE001 — no record (capture off / not run) → no figure rows
        return []

    rows: list[dict] = []
    for i, line in enumerate(ln for ln in body.splitlines() if ln.strip()):
        fig = json.loads(line)
        tdict = fig.get("transcription")
        if not tdict:
            continue
        try:
            text = flatten_transcription(FigureTranscription(**tdict))
        except Exception:  # noqa: BLE001 — malformed transcription → skip this figure
            continue
        if not text:
            continue
        kind = fig.get("kind") or "infographic"
        page = fig.get("pageNumber")
        # A short label prefixes the embedded text so lexical search matches on
        # "chart"/"map"/the figure title, and so a rendered hit reads sensibly.
        label = f"[{kind} figure] {fig.get('title') or ''}".strip()
        rows.append({
            "chunk_index": FIGURE_CHUNK_INDEX_BASE + i,
            "page_start": page,
            "page_end": page,
            "chunk_text": text,
            "context_prefix": label,
            "embedded_text": f"{label}\n{text}" if label else text,
            "location_ids": fig.get("locationIds") or [],
            "location_pcodes": fig.get("locationPcodes") or [],
            "time_range_start": fig.get("timeRangeStart"),
            "time_range_end": fig.get("timeRangeEnd"),
            "event_types": fig.get("eventTypes") or [],
            "need_sectors": fig.get("needSectors") or [],
            "figure_s3_key": fig.get("s3Key"),
            "figure_kind": kind,
        })
    return rows


def _embedding_batches(enriched: list[dict], embedder):
    """Slice enriched chunks into embedding batches that stay under the
    provider's per-call input-count limit AND, where the provider enforces
    one, its per-batch TOKEN limit.

    Voyage rejects any single call whose inputs sum to more than
    ``MAX_TOKENS_PER_BATCH`` (120k) regardless of the 128-input count cap, so
    a fixed 128-slice fails on large backfills where chunks are big. Providers
    that don't expose a token limit (e.g. Together/TEI) fall back to
    count-only batching. A small margin is left under the hard limit for the
    provider's own per-text truncation accounting.
    """
    max_tokens = getattr(embedder, "MAX_TOKENS_PER_BATCH", None)
    count_tokens = getattr(embedder, "count_tokens", None)
    token_budget = int(max_tokens * 0.95) if (max_tokens and count_tokens) else None

    batch: list[dict] = []
    batch_tokens = 0
    for e in enriched:
        n = count_tokens([e["embedded_text"]]) if token_budget is not None else 0
        over_count = len(batch) >= EMBED_BATCH_SIZE
        over_tokens = token_budget is not None and batch_tokens + n > token_budget
        if batch and (over_count or over_tokens):
            yield batch
            batch, batch_tokens = [], 0
        batch.append(e)
        batch_tokens += n
    if batch:
        yield batch


@dg.asset(group_name="reliefweb_kb", partitions_def=country_partitions)
def reliefweb_weekly_knowledgebase_upsert(
    context: AssetExecutionContext,
    reliefweb_weekly_enriched_chunks: list[dict],
    reliefweb_weekly_figures: list[dict],
) -> dg.MaterializeResult:
    """Embed enriched chunks — and, when figure capture is enabled, the report's
    figure transcriptions merged in as extra KB rows (the RAG merge) — and hand
    the combined per-report batch to clear-api for upsert.

    Figures ride the SAME ``upsertKnowledgebaseChunks`` call (not a second one):
    the mutation deletes-then-inserts per ``report_id``, so a separate figure
    upsert would wipe the text chunks. Merging keeps both in one atomic batch and
    makes figures rank alongside text in hybrid search. Empty (capture off) → a
    plain text-only upsert, unchanged."""
    bucket = os.environ["S3_BUCKET"]
    s3 = _s3_client()
    embedder = make_embedding_provider()

    # report_id → its captured-figure record key (written by the figures asset).
    # Empty when KB_CAPTURE_FIGURES is off (that asset returns no summaries).
    figure_record_by_report = {
        r["report_id"]: r.get("s3_record_key")
        for r in reliefweb_weekly_figures
        if r.get("report_id")
    }

    # DIAGNOSTIC (figure-merge handoff): figure_chunks_written has been 0 in dev
    # despite report_figures being populated. Log where the handoff drops so one
    # re-materialize pinpoints it. The overlap is the tell: 0 → the figures-asset
    # and enriched-chunks report_id sets don't line up; >0 → the per-report record
    # read/parse is yielding nothing (caught below).
    _enriched_ids = [r.get("report_id") for r in reliefweb_weekly_enriched_chunks]
    _keys_present = [rid for rid, k in figure_record_by_report.items() if k]
    _overlap = set(_enriched_ids) & set(figure_record_by_report)
    context.log.info(
        "[FIGURE-MERGE] figure_summaries=%d with_record_key=%d enriched_reports=%d "
        "overlap=%d | sample figure_ids=%s | sample enriched_ids=%s",
        len(reliefweb_weekly_figures), len(_keys_present), len(_enriched_ids),
        len(_overlap), list(figure_record_by_report)[:3], _enriched_ids[:3],
    )

    total_upserted = 0
    total_figure_chunks = 0
    total_reports = 0
    for report in reliefweb_weekly_enriched_chunks:
        report_id = report["report_id"]
        enriched = _read_enriched(s3, bucket, report["s3_enriched_key"])

        # Merge in this report's transcribed figures as extra KB rows (same shape
        # as a text chunk + figure_s3_key/kind). No-op when capture is disabled.
        figure_rows = _figure_kb_rows(s3, bucket, figure_record_by_report.get(report_id))
        if figure_record_by_report.get(report_id) and not figure_rows:
            context.log.warning(
                "[FIGURE-MERGE] report %s: record_key present but 0 figure rows — "
                "S3 record missing/unreadable or all transcriptions empty/unparseable",
                report_id,
            )
        rows_to_embed = enriched + figure_rows
        if not rows_to_embed:
            continue

        # Batched embedding, bounded by BOTH the provider's input-count cap
        # and its per-batch token limit (Voyage: 120k tokens/call, which a
        # naive 128-slice exceeds on large backfills).
        embeddings: list[list[float]] = []
        provider_name = embedder.provider_name
        model_name = embedder.model
        for batch in _embedding_batches(rows_to_embed, embedder):
            results = embedder.embed(
                [e["embedded_text"] for e in batch],
                input_type="document",
            )
            embeddings.extend(r.embedding for r in results)

        if len(embeddings) != len(rows_to_embed):
            context.log.error(
                "embedding count mismatch for %s: got %d for %d chunks — skipping",
                report_id, len(embeddings), len(rows_to_embed),
            )
            continue

        # Build the GraphQL payload. Field names are camelCase to match
        # the KnowledgebaseChunkInput schema in clear-api. Text rows leave
        # figureS3Key/figureKind null; figure rows carry both.
        chunk_inputs: list[dict] = []
        for row, vec in zip(rows_to_embed, embeddings, strict=True):
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
                "figureS3Key": row.get("figure_s3_key"),
                "figureKind": row.get("figure_kind"),
            })

        try:
            result = clear_api.upsert_knowledgebase_chunks(
                report_id=report_id,
                report_title=report["report_title"],
                source_url=report["source_url"],
                s3_key=report["s3_key"],
                published_at=report["published_at"],
                chunks=chunk_inputs,
            )
        except clear_api.ClearApiError as exc:
            # 4xx from clear-api — the payload shape is wrong. Log and
            # skip so the run can finish; the caller can fix the
            # mutation and re-materialize.
            context.log.error(
                "clear-api rejected upsert for %s (non-retryable): %s",
                report_id, exc,
            )
            continue
        except Exception as exc:  # noqa: BLE001
            context.log.error(
                "clear-api upsert failed for %s after retries: %s",
                report_id, exc,
            )
            continue

        total_reports += 1
        total_upserted += result["chunksInserted"]
        total_figure_chunks += len(figure_rows)
        context.log.info(
            "upserted report %s: deleted=%d inserted=%d (%d figure) (%s/%s)",
            report_id, result["chunksDeleted"], result["chunksInserted"],
            len(figure_rows), provider_name, model_name,
        )

    return dg.MaterializeResult(metadata={
        "reports_written": dg.MetadataValue.int(total_reports),
        "chunks_written": dg.MetadataValue.int(total_upserted),
        "figure_chunks_written": dg.MetadataValue.int(total_figure_chunks),
        "embedding_provider": dg.MetadataValue.text(embedder.provider_name),
        "embedding_model": dg.MetadataValue.text(embedder.model),
        "embedding_dimensions": dg.MetadataValue.int(embedder.dimensions),
    })


# ── Figure → KB sync (decoupled merge) ──────────────────────────────────────
#
# The weekly text upsert above only merges a report's figures when the SAME run
# also (re)ingested that report's text — figures captured in a reused or earlier
# run never get merged (`figure_chunks_written` stayed 0 in dev despite
# `report_figures` filling up, because the per-country reports were reused, not
# re-enriched). This asset closes that gap independently of the text run:
#
#   1. Ask clear-api which captured figures belong in the KB but aren't merged
#      yet — reports already in the KB that have `report_figures` rows whose crop
#      isn't present as a `figure_s3_key` in the KB.
#   2. Embed each report's figure transcriptions and upsert them with
#      `figures_only=True`, so attaching figures deletes only the report's
#      existing figure rows, never its text chunks.
#
# Unpartitioned and global on purpose: the pending set spans all countries, so
# one pass drains it regardless of which country partitions ran this week. That
# also makes a one-off materialize the BACKFILL for every figure captured before
# this asset existed.
_FIGURE_SYNC_TAGS = {"clear/weight": "heavy"}
# Bounded by REPORTS (the resolver clamps to 500). A single fetch avoids an
# offset-less paging loop: reports that merge successfully drop out of the
# pending set, but a report whose figures are all empty/unparseable stays
# pending forever — paging would spin on it. At current scale (low tens of
# figure-reports) one page covers everything; if a run ever hits the cap it logs
# a warning and the next run picks up the remainder.
_FIGURE_SYNC_MAX_REPORTS = 500


# A "normalized figure" dict is the shared input shape for the figure→KB merge,
# so the merge logic serves BOTH the weekly sync (figures from clear-api's
# pending query) and the manual-upload path (figures captured in-process). Keys:
#   s3_key, kind, page, title, transcription (dict|None),
#   location_ids, location_pcodes, event_types, need_sectors,
#   time_range_start, time_range_end


def _figure_chunk_rows(figures: list[dict]) -> list[dict]:
    """Turn normalized figure dicts into embeddable chunk rows — flattened
    transcription text plus a ``[<kind> figure] <title>`` label that prefixes the
    embedded text (so lexical search matches on kind/title and a rendered hit
    reads sensibly). Skips figures whose transcription is empty or unparseable:
    the image stays in ``report_figures`` regardless, there's just nothing to
    embed."""
    rows: list[dict] = []
    for fig in figures:
        tdict = fig.get("transcription")
        if not tdict:
            continue
        try:
            text = flatten_transcription(FigureTranscription(**tdict))
        except Exception:  # noqa: BLE001 — malformed transcription → skip this figure
            continue
        if not text:
            continue
        kind = fig.get("kind") or "infographic"
        label = f"[{kind} figure] {fig.get('title') or ''}".strip()
        rows.append({
            "page": fig.get("page"),
            "kind": kind,
            "label": label,
            "chunk_text": text,
            "embedded_text": f"{label}\n{text}" if label else text,
            "fig": fig,
        })
    return rows


def merge_report_figures_to_kb(
    *,
    report_id: str,
    report_title: str,
    source_url: str,
    report_s3_key: str,
    published_at: str,
    figures: list[dict],
    embedder,
    team_id: str | None = None,
    uploaded_by_user_id: str | None = None,
) -> int:
    """Embed a report's figure transcriptions and upsert them into the KB with
    ``figures_only=True`` — so attaching figures deletes only the report's
    existing figure rows, never its text chunks.

    ``figures`` are normalized figure dicts (see the key list above).
    ``team_id`` / ``uploaded_by_user_id`` attribute a team-scoped manual upload
    (Cap 1) onto the figure chunks too, so they filter alongside the text; both
    None for the weekly sync. Returns the number of figure chunks inserted (0
    when nothing was embeddable, i.e. no upsert ran). Propagates
    ``clear_api.ClearApiError`` and embedding errors to the caller, which decides
    how to log/skip."""
    chunk_rows = _figure_chunk_rows(figures)
    if not chunk_rows:
        return 0

    embeddings: list[list[float]] = []
    for batch in _embedding_batches(chunk_rows, embedder):
        results = embedder.embed(
            [e["embedded_text"] for e in batch],
            input_type="document",
        )
        embeddings.extend(r.embedding for r in results)
    if len(embeddings) != len(chunk_rows):
        raise RuntimeError(
            f"figure embedding count mismatch for {report_id}: "
            f"{len(embeddings)} for {len(chunk_rows)}",
        )

    chunk_inputs: list[dict] = []
    for i, (row, vec) in enumerate(zip(chunk_rows, embeddings, strict=True)):
        fig = row["fig"]
        chunk_inputs.append({
            "chunkIndex": FIGURE_CHUNK_INDEX_BASE + i,
            "pageStart": row["page"],
            "pageEnd": row["page"],
            "chunkText": row["chunk_text"],
            "contextPrefix": row["label"],
            "embeddedText": row["embedded_text"],
            "embeddingProvider": embedder.provider_name,
            "embeddingModel": embedder.model,
            "embedding": vec,
            "locationIds": fig.get("location_ids") or [],
            "locationPcodes": fig.get("location_pcodes") or [],
            "timeRangeStart": fig.get("time_range_start"),
            "timeRangeEnd": fig.get("time_range_end"),
            "eventTypes": fig.get("event_types") or [],
            "needSectors": fig.get("need_sectors") or [],
            "figureS3Key": fig.get("s3_key"),
            "figureKind": row["kind"],
        })

    result = clear_api.upsert_knowledgebase_chunks(
        report_id=report_id,
        report_title=report_title,
        source_url=source_url,
        s3_key=report_s3_key,
        published_at=published_at,
        chunks=chunk_inputs,
        figures_only=True,
        team_id=team_id,
        uploaded_by_user_id=uploaded_by_user_id,
    )
    return result["chunksInserted"]


def _normalize_pending_figure(fig: dict) -> dict:
    """clear-api ``ReportFigureForMerge`` row → the normalized figure dict
    ``merge_report_figures_to_kb`` consumes."""
    return {
        "s3_key": fig.get("figureS3Key"),
        "kind": fig.get("figureKind") or "infographic",
        "page": fig.get("pageNumber"),
        "title": fig.get("title"),
        "transcription": fig.get("transcription"),
        "location_ids": fig.get("locationIds") or [],
        "location_pcodes": fig.get("locationPcodes") or [],
        "event_types": fig.get("eventTypes") or [],
        "need_sectors": fig.get("needSectors") or [],
        "time_range_start": fig.get("timeRangeStart"),
        "time_range_end": fig.get("timeRangeEnd"),
    }


@dg.asset(group_name="reliefweb_kb_figures")
def reliefweb_weekly_figure_kb_sync(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Merge captured figures into the KB independently of the weekly text run.

    Drains clear-api's ``reportFiguresPendingKbMerge`` queue: for each pending
    report, embed its figure transcriptions and upsert them ``figures_only=True``
    (deletes only that report's figure rows, never its text). Idempotent — a
    merged figure drops out of the queue, so re-running only advances. Doubles as
    the one-shot backfill for figures captured before this asset existed."""
    embedder = make_embedding_provider()

    pending = clear_api.report_figures_pending_kb_merge(limit=_FIGURE_SYNC_MAX_REPORTS)

    # Group the flat figure rows by report, preserving first-seen order. Keep the
    # first raw row per report for its report-level fields (title/url/pdf/date).
    by_report: dict[str, list[dict]] = {}
    head_by_report: dict[str, dict] = {}
    for fig in pending:
        rid = fig["reportId"]
        by_report.setdefault(rid, []).append(_normalize_pending_figure(fig))
        head_by_report.setdefault(rid, fig)

    if len(by_report) >= _FIGURE_SYNC_MAX_REPORTS:
        context.log.warning(
            "[FIGURE-SYNC] hit the %d-report cap — more may be pending; re-run to "
            "drain the remainder",
            _FIGURE_SYNC_MAX_REPORTS,
        )

    total_reports = 0
    total_figure_chunks = 0
    total_skipped_reports = 0
    for report_id, figures in by_report.items():
        head = head_by_report[report_id]
        try:
            inserted = merge_report_figures_to_kb(
                report_id=report_id,
                report_title=head["reportTitle"],
                source_url=head["sourceUrl"],
                report_s3_key=head["reportS3Key"],
                published_at=head["publishedAt"],
                figures=figures,
                embedder=embedder,
                # Carry the report's team attribution onto the merged figures so
                # they stay visible under the same team filter as the text.
                team_id=head.get("teamId"),
                uploaded_by_user_id=head.get("uploadedByUserId"),
            )
        except clear_api.ClearApiError as exc:
            context.log.error(
                "[FIGURE-SYNC] clear-api rejected figure upsert for %s (non-retryable): %s",
                report_id, exc,
            )
            total_skipped_reports += 1
            continue
        except Exception as exc:  # noqa: BLE001 — isolate one report, finish the rest
            context.log.error(
                "[FIGURE-SYNC] figure merge failed for %s: %s", report_id, exc,
            )
            total_skipped_reports += 1
            continue

        if inserted == 0:
            total_skipped_reports += 1
            context.log.info(
                "[FIGURE-SYNC] report %s: no embeddable figure transcriptions "
                "(%d figure(s), all empty/unparseable) — skipping",
                report_id, len(figures),
            )
            continue

        total_reports += 1
        total_figure_chunks += inserted
        context.log.info(
            "[FIGURE-SYNC] report %s: merged %d figure chunk(s) (%s/%s)",
            report_id, inserted, embedder.provider_name, embedder.model,
        )

    return dg.MaterializeResult(metadata={
        "reports_pending": dg.MetadataValue.int(len(by_report)),
        "reports_merged": dg.MetadataValue.int(total_reports),
        "reports_skipped": dg.MetadataValue.int(total_skipped_reports),
        "figure_chunks_written": dg.MetadataValue.int(total_figure_chunks),
        "embedding_provider": dg.MetadataValue.text(embedder.provider_name),
        "embedding_model": dg.MetadataValue.text(embedder.model),
    })


reliefweb_weekly_figure_kb_sync_job = dg.define_asset_job(
    # Distinct from the asset key — a job sharing an asset's name collides with
    # that asset's implicit __ASSET_JOB graph ("Op/Graph definition names must be
    # unique within a repository").
    name="reliefweb_weekly_figure_kb_sync_job",
    selection=[reliefweb_weekly_figure_kb_sync],
    tags=_FIGURE_SYNC_TAGS,
)

# Tuesday 12:00 UTC — six hours after the per-country KB run (06:00) so this
# week's text is in the KB before the sync looks for unmerged figures. It is
# idempotent and only ever ADDS figure rows to reports already in the KB, so the
# exact offset isn't load-bearing: anything it misses this week it catches next.
reliefweb_weekly_figure_kb_sync_schedule = dg.ScheduleDefinition(
    name="reliefweb_weekly_figure_kb_sync_schedule",
    job=reliefweb_weekly_figure_kb_sync_job,
    cron_schedule="0 12 * * TUE",
    execution_timezone="UTC",
)
