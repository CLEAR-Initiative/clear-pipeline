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

import boto3
import dagster as dg
from dagster import AssetExecutionContext
from dotenv import load_dotenv

from clear_context_pipeline.providers import clear_api, make_embedding_provider

load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

# Voyage's public batch limit; Together AI and TEI accept larger batches
# but there's no upside pushing past this — the LLM contextualization
# step upstream is the throughput bottleneck, not embeddings.
EMBED_BATCH_SIZE = 128


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        region_name=os.environ["S3_REGION"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    )


def _read_enriched(s3, bucket: str, key: str) -> list[dict]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line]


@dg.asset(group_name="reliefweb_kb")
def reliefweb_weekly_knowledgebase_upsert(
    context: AssetExecutionContext,
    reliefweb_weekly_enriched_chunks: list[dict],
) -> dg.MaterializeResult:
    """Embed enriched chunks and hand them to clear-api for upsert."""
    bucket = os.environ["S3_BUCKET"]
    s3 = _s3_client()
    embedder = make_embedding_provider()

    total_upserted = 0
    total_reports = 0
    for report in reliefweb_weekly_enriched_chunks:
        report_id = report["report_id"]
        enriched = _read_enriched(s3, bucket, report["s3_enriched_key"])
        if not enriched:
            continue

        # Batched embedding. Voyage takes up to 128 at a time; larger
        # batches don't reduce latency once you're inside the SDK's
        # HTTP round-trip.
        embeddings: list[list[float]] = []
        provider_name = embedder.provider_name
        model_name = embedder.model
        for i in range(0, len(enriched), EMBED_BATCH_SIZE):
            batch = enriched[i : i + EMBED_BATCH_SIZE]
            results = embedder.embed(
                [e["embedded_text"] for e in batch],
                input_type="document",
            )
            embeddings.extend(r.embedding for r in results)

        if len(embeddings) != len(enriched):
            context.log.error(
                "embedding count mismatch for %s: got %d for %d chunks — skipping",
                report_id, len(embeddings), len(enriched),
            )
            continue

        # Build the GraphQL payload. Field names are camelCase to match
        # the KnowledgebaseChunkInput schema in clear-api.
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
        context.log.info(
            "upserted report %s: deleted=%d inserted=%d (%s/%s)",
            report_id, result["chunksDeleted"], result["chunksInserted"],
            provider_name, model_name,
        )

    return dg.MaterializeResult(metadata={
        "reports_written": dg.MetadataValue.int(total_reports),
        "chunks_written": dg.MetadataValue.int(total_upserted),
        "embedding_provider": dg.MetadataValue.text(embedder.provider_name),
        "embedding_model": dg.MetadataValue.text(embedder.model),
        "embedding_dimensions": dg.MetadataValue.int(embedder.dimensions),
    })
