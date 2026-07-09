"""LLM-driven chunk enrichment: contextualize + extract + resolve.

Per chunk, this asset does three things:

  1. **Contextualize** — call the ``context`` LLM with the full doc as
     a cached prefix and the chunk as the per-request tail. Get back
     50–100 tokens of context that situate the chunk within its
     report (Anthropic's Contextual Retrieval technique). Skipped
     entirely when ``KB_SKIP_CONTEXTUALIZATION=1``.
  2. **Extract parameters** — call the ``extraction`` LLM with the
     contextualized chunk and a Pydantic schema for ``{locations,
     time_range, event_types, need_sectors}``. Provider handles
     JSON-schema mode + repair.
  3. **Resolve locations** — for each ``{pcode?, name?}`` the LLM
     emits, look up ``locations.id`` in clear-api's Postgres by
     pcode-first, name-fallback. Unresolved refs are kept as raw
     pcode strings on the row so a future backfill can retry them.

Truncation guardrail: reports with more than
``KB_MAX_CHUNKS_PER_REPORT`` chunks are truncated (head N) — protects
the run from a malformed PDF that tokenises to tens of thousands of
chunks.

S3 layout:
    reliefweb/kb/enriched/<iso3>/<format-slug>/<report_id>.jsonl
"""

import json
import os
from pathlib import Path
from typing import Literal

import boto3
import dagster as dg
from dagster import AssetExecutionContext
from dotenv import load_dotenv
from pydantic import BaseModel, Field

from clear_context_pipeline.providers import (
    clear_api,
    load_guardrails,
    make_llm_provider,
)

load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

COUNTRY_ISO3 = "sdn"
FORMAT_SLUG = "situation-report"

S3_ENRICHED_PREFIX = f"reliefweb/kb/enriched/{COUNTRY_ISO3}/{FORMAT_SLUG}"

# SAF sectors — mirrored from the Norwegian Refugee Council's operational
# taxonomy. Constraining the LLM output with a Literal enum forces JSON-
# schema mode to reject off-taxonomy values, so downstream filter
# queries don't have to normalise variants.
SafSector = Literal[
    "Shelter", "WASH", "Protection", "Health", "Food Security", "Education",
]


class LocationRef(BaseModel):
    """A location mentioned in the chunk.

    The extractor must emit at least one of ``pcode`` or ``name``.
    ``pcode`` is preferred when the chunk cites an explicit OCHA code
    (e.g. ``SD01`` for Khartoum) since it's unambiguous; ``name`` is a
    fallback. ``admin_level`` narrows the resolver's SQL — 0 (country),
    1 (state), 2 (locality), 3 (sub-locality) — omitted when the chunk
    is ambiguous.
    """
    pcode: str | None = Field(default=None, description="OCHA pcode when known")
    name: str | None = Field(default=None, description="Plain place name")
    admin_level: Literal[0, 1, 2, 3] | None = Field(
        default=None, description="0 country … 3 sub-locality",
    )


class ExtractedParameters(BaseModel):
    """Structured parameters extracted from one knowledge-base chunk."""
    locations: list[LocationRef] = Field(
        default_factory=list,
        description="Every location the chunk mentions; empty if none",
    )
    time_range_start: str | None = Field(
        default=None,
        description="ISO date (YYYY-MM-DD) — earliest event the chunk covers",
    )
    time_range_end: str | None = Field(
        default=None,
        description="ISO date (YYYY-MM-DD) — latest event the chunk covers",
    )
    event_types: list[str] = Field(
        default_factory=list,
        description=(
            "Disaster / event categories — free-text tags like 'conflict', "
            "'flood', 'displacement'; GLIDE codes when known"
        ),
    )
    need_sectors: list[SafSector] = Field(
        default_factory=list,
        description="NRC SAF sectors the chunk discusses",
    )


class ChunkContext(BaseModel):
    """50-100 token contextual prefix positioning a chunk within its doc."""
    context: str = Field(
        description=(
            "50–100 tokens describing what part of the report this chunk "
            "comes from and what topic it covers. Used as a retrieval-time "
            "hint prepended to the chunk before embedding."
        ),
    )


CONTEXT_SYSTEM = (
    "You are a humanitarian analyst preparing chunks of Norwegian Refugee "
    "Council (NRC) reports for a retrieval system. For each chunk, you'll "
    "receive the FULL report as context and the specific chunk to describe. "
    "Return 50–100 tokens that situate the chunk within its report — what "
    "section it belongs to, what topic it addresses, and what geographic / "
    "temporal scope applies. Be concrete: name the section, the country / "
    "district if identifiable, and the crisis phase. Do not summarise the "
    "chunk itself; the retriever already has that text."
)


EXTRACTION_SYSTEM = (
    "You are an information extractor for humanitarian sitreps produced by "
    "the Norwegian Refugee Council (NRC). Given one chunk of a report, emit "
    "the structured parameters that describe what the chunk is about.\n\n"
    "Rules:\n"
    "- `locations` — every place the chunk explicitly refers to. Prefer OCHA "
    "  pCodes (SD###) when the chunk contains them; otherwise use the plain "
    "  place name. Set `admin_level` when clear (state = 1, locality = 2).\n"
    "- `time_range_start` / `time_range_end` — dates the chunk describes "
    "  events for, in ISO YYYY-MM-DD form. Leave null when the chunk is "
    "  reference material without a specific window.\n"
    "- `event_types` — categorical tags for the events discussed: 'conflict', "
    "  'flood', 'displacement', 'disease outbreak', etc. Include GLIDE codes "
    "  only when the chunk cites one explicitly.\n"
    "- `need_sectors` — must be drawn from the NRC SAF taxonomy: Shelter, "
    "  WASH, Protection, Health, Food Security, Education. Emit only sectors "
    "  the chunk directly discusses (not the whole report's default set)."
)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        region_name=os.environ["S3_REGION"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    )


def _enriched_key(report_id: str) -> str:
    return f"{S3_ENRICHED_PREFIX}/{report_id}.jsonl"


def _read_chunks(s3, bucket: str, key: str) -> list[dict]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line]


def _read_pages_concat(s3, bucket: str, key: str) -> str:
    """Full report text used as the cached prefix in contextualization."""
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    lines = [json.loads(line) for line in body.splitlines() if line]
    return "\n\n".join(f"[page {p['page_num']}]\n{p['text']}" for p in lines)


def _resolve_location(ref: LocationRef) -> str | None:
    """Resolve one LLM-emitted location ref via clear-api's
    ``resolveKnowledgebaseLocation`` query.

    Kept as a thin wrapper so per-chunk error handling stays in the
    caller — a single failed lookup should not abort the batch, so we
    catch the transport error, log, and return None. That falls the
    ref through to the ``locationPcodes`` array where a future backfill
    can retry.
    """
    try:
        return clear_api.resolve_location(
            pcode=ref.pcode, name=ref.name, admin_level=ref.admin_level,
        )
    except clear_api.ClearApiError:
        # 4xx from clear-api means the query shape is wrong — propagate.
        raise
    except Exception as exc:  # noqa: BLE001
        # 5xx / transport hiccup — treat as an unresolvable location.
        # A later backfill can retry from the stored pcode.
        _log_hiccup(ref, exc)
        return None


def _log_hiccup(ref: LocationRef, exc: BaseException) -> None:
    # Isolated helper so per-chunk error handling stays terse.
    import logging
    logging.getLogger(__name__).warning(
        "clear-api resolve_location hiccup for pcode=%s name=%s: %s",
        ref.pcode, ref.name, exc,
    )


@dg.asset(group_name="reliefweb_kb")
def reliefweb_weekly_enriched_chunks(
    context: AssetExecutionContext,
    reliefweb_weekly_chunks: list[dict],
    reliefweb_weekly_pdf_text: list[dict],
) -> list[dict]:
    """Chunks with LLM-generated context + extracted parameters +
    resolved location IDs. Persists to S3 for replay; returns a summary
    list the upsert asset consumes."""
    bucket = os.environ["S3_BUCKET"]
    guardrails = load_guardrails()
    s3 = _s3_client()

    llm_context = None if guardrails.skip_contextualization else make_llm_provider("context")
    llm_extract = make_llm_provider("extraction")

    if guardrails.skip_contextualization:
        context.log.warning(
            "KB_SKIP_CONTEXTUALIZATION is set — embedding raw chunk text "
            "without doc-level context. Retrieval quality will be degraded.",
        )

    # Build a report_id → text_key map so we can pull the full doc for
    # each chunk's contextualization step without shuffling it through
    # the Dagster IO manager.
    text_key_by_report = {r["report_id"]: r["s3_text_key"] for r in reliefweb_weekly_pdf_text}

    summaries: list[dict] = []
    total_enriched = 0
    total_skipped = 0
    for report in reliefweb_weekly_chunks:
        report_id = report["report_id"]
        chunks = _read_chunks(s3, bucket, report["s3_chunks_key"])

        if len(chunks) > guardrails.max_chunks_per_report:
            context.log.warning(
                "report %s has %d chunks — truncating to %d "
                "(KB_MAX_CHUNKS_PER_REPORT)",
                report_id, len(chunks), guardrails.max_chunks_per_report,
            )
            chunks = chunks[: guardrails.max_chunks_per_report]

        doc_text = (
            _read_pages_concat(s3, bucket, text_key_by_report[report_id])
            if not guardrails.skip_contextualization
            else ""
        )

        enriched: list[dict] = []
        for chunk in chunks:
            try:
                ctx_prefix = _run_context(
                    llm_context, doc_text, chunk["text"], cache_key=report_id,
                ) if llm_context else ""

                embedded_text = (
                    f"{ctx_prefix}\n\n{chunk['text']}" if ctx_prefix
                    else chunk["text"]
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
                    "report_id": report_id,
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
            except Exception as exc:  # noqa: BLE001 — isolate per-chunk failures
                total_skipped += 1
                context.log.warning(
                    "enrich failed for report %s chunk %d: %s",
                    report_id, chunk["chunk_index"], exc,
                )

        if not enriched:
            context.log.warning("no enriched chunks for report %s", report_id)
            continue

        enriched_key = _enriched_key(report_id)
        body = b"\n".join(
            json.dumps(e, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            for e in enriched
        ) + b"\n"
        s3.put_object(
            Bucket=bucket, Key=enriched_key, Body=body,
            ContentType="application/x-ndjson",
        )
        total_enriched += len(enriched)
        summaries.append({
            **report,
            "s3_enriched_key": enriched_key,
            "num_enriched": len(enriched),
        })
        context.log.info(
            "enriched %d/%d chunks for %s → s3://%s/%s",
            len(enriched), len(chunks), report_id, bucket, enriched_key,
        )

    context.add_output_metadata({
        "reports_enriched": dg.MetadataValue.int(len(summaries)),
        "chunks_enriched": dg.MetadataValue.int(total_enriched),
        "chunks_skipped": dg.MetadataValue.int(total_skipped),
        "contextualization_skipped": dg.MetadataValue.bool(guardrails.skip_contextualization),
        "s3_prefix": dg.MetadataValue.text(f"s3://{bucket}/{S3_ENRICHED_PREFIX}/"),
    })
    return summaries


def _run_context(llm, doc_text: str, chunk_text: str, *, cache_key: str) -> str:
    """One contextualization call. Doc goes in `system` so prompt
    caching keys off it; the chunk-specific tail lives in `user`."""
    system = (
        f"{CONTEXT_SYSTEM}\n\n"
        f"---\nFULL REPORT (cached; do not repeat):\n{doc_text}\n---"
    )
    user = f"Chunk to contextualize:\n\n{chunk_text}"
    result = llm.complete_structured(
        system=system,
        user=user,
        schema=ChunkContext,
        max_tokens=200,
        cache_key=cache_key,
    )
    return result.context.strip()


def _run_extraction(llm, embedded_text: str) -> ExtractedParameters:
    """One structured-extraction call. Chunk with its context prefix
    goes in `user`; the constrained schema does the rest."""
    return llm.complete_structured(
        system=EXTRACTION_SYSTEM,
        user=f"Chunk:\n\n{embedded_text}",
        schema=ExtractedParameters,
        max_tokens=1500,
    )
