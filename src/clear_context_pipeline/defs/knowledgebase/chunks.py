"""Split extracted PDF text into overlapping token windows.

Chunk parameters:
  size   = 800 tokens   — large enough to preserve local context
                          (a single sitrep bullet + its rationale),
                          small enough that a chunk still fits inside
                          the LLM contextualization prompt without
                          eating the doc-level cache budget.
  overlap = 100 tokens  — enough to keep sentences that straddle a
                          boundary retrievable from either side. The
                          same paragraph may appear in two chunks;
                          that's a feature for retrieval, not a bug.

We tokenise with ``cl100k_base`` (tiktoken) because it's a stable,
provider-agnostic proxy. Actual per-model token counts differ (Voyage
uses a SentencePiece variant, Claude uses its own) but the *relative*
chunk lengths are what matters for the retrieval quality curve; using
one tokenizer everywhere keeps chunk boundaries reproducible even when
we swap providers.

Page-range preservation: every chunk carries the min/max page number
of the source text it covers, so a retrieval hit can cite "report X,
pages 4–5" back to the user.

S3 layout:
    reliefweb/kb/chunks/<iso3>/<format-slug>/<report_id>.jsonl

Each line is::

    {"report_id": str, "chunk_index": int, "page_start": int,
     "page_end": int, "text": str}
"""

import json
import os
from pathlib import Path

import dagster as dg
import tiktoken
from dagster import AssetExecutionContext
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

COUNTRY_ISO3 = "sdn"
FORMAT_SLUG = "situation-report"

CHUNK_TOKENS = 800
CHUNK_OVERLAP_TOKENS = 100

S3_CHUNKS_PREFIX = f"reliefweb/kb/chunks/{COUNTRY_ISO3}/{FORMAT_SLUG}"

# Lazily-loaded tiktoken encoding — deliberately NOT loaded at import.
# `get_encoding` downloads the BPE vocab over the network on a cold cache,
# and this module is imported by every Dagster step worker when the code
# location reloads. Loading it at import would turn a cold cache or blocked
# egress into a hang at *step startup* — for every step, not just chunking.
# Load on first use (in the op body); it's then amortised across the run.
_ENCODING = None


def _encoding():
    global _ENCODING
    if _ENCODING is None:
        _ENCODING = tiktoken.get_encoding("cl100k_base")
    return _ENCODING


def _s3_client():
    from clear_context_pipeline.providers.s3 import s3_client

    return s3_client()


def _chunks_key(report_id: str) -> str:
    return f"{S3_CHUNKS_PREFIX}/{report_id}.jsonl"


def _read_pages(s3, bucket: str, key: str) -> list[dict]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line]


def _slice_into_chunks(
    pages: list[dict], *, chunk_tokens: int, overlap_tokens: int,
) -> list[dict]:
    """Concatenate pages, encode once, window over token IDs.

    The alternative — chunking per-page — leaves short pages under-
    represented and long pages sliced arbitrarily. Whole-doc windowing
    is simpler and produces uniformly-sized chunks; the per-token page
    map lets us still recover the source pages a chunk spans.
    """
    # Build a token stream with an index → page_num side map. `+ 1` on
    # the encoded page appends a small delimiter's worth so successive
    # pages don't get glued into a single word by the tokenizer's
    # boundary handling — using "\n\n" between pages is enough.
    enc = _encoding()
    token_ids: list[int] = []
    token_to_page: list[int] = []
    for page in pages:
        page_tokens = enc.encode(page["text"])
        token_ids.extend(page_tokens)
        token_to_page.extend([page["page_num"]] * len(page_tokens))
        # Separator tokens; count against the page they follow so they
        # don't distort chunk boundaries.
        sep_tokens = enc.encode("\n\n")
        token_ids.extend(sep_tokens)
        token_to_page.extend([page["page_num"]] * len(sep_tokens))

    if not token_ids:
        return []

    chunks: list[dict] = []
    step = chunk_tokens - overlap_tokens
    if step <= 0:
        raise ValueError(
            f"Overlap {overlap_tokens} must be less than chunk size "
            f"{chunk_tokens}",
        )

    for start in range(0, len(token_ids), step):
        end = min(start + chunk_tokens, len(token_ids))
        window = token_ids[start:end]
        if not window:
            break
        text = enc.decode(window).strip()
        if not text:
            continue
        page_start = token_to_page[start]
        page_end = token_to_page[end - 1]
        chunks.append({
            "chunk_index": len(chunks),
            "page_start": page_start,
            "page_end": page_end,
            "text": text,
        })
        if end == len(token_ids):
            break

    return chunks


@dg.asset(group_name="reliefweb_kb")
def reliefweb_weekly_chunks(
    context: AssetExecutionContext,
    reliefweb_weekly_pdf_text: list[dict],
) -> list[dict]:
    """Per-report chunk stream as JSONL in S3.

    Downstream (``reliefweb_weekly_enriched_chunks``) reads back from
    S3 so chunk boundaries can be re-tweaked and the enrichment
    replayed without re-extracting PDFs.
    """
    bucket = os.environ["S3_BUCKET"]
    s3 = _s3_client()

    summaries: list[dict] = []
    total_chunks = 0
    for report in reliefweb_weekly_pdf_text:
        pages = _read_pages(s3, bucket, report["s3_text_key"])
        chunks = _slice_into_chunks(
            pages,
            chunk_tokens=CHUNK_TOKENS,
            overlap_tokens=CHUNK_OVERLAP_TOKENS,
        )
        if not chunks:
            context.log.warning(
                "no chunks produced for report %s", report["report_id"],
            )
            continue

        chunks_key = _chunks_key(report["report_id"])
        body = b"\n".join(
            json.dumps(
                {"report_id": report["report_id"], **c},
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
            for c in chunks
        ) + b"\n"
        s3.put_object(
            Bucket=bucket, Key=chunks_key, Body=body,
            ContentType="application/x-ndjson",
        )

        total_chunks += len(chunks)
        summaries.append({**report, "s3_chunks_key": chunks_key, "num_chunks": len(chunks)})
        context.log.info(
            "chunked report %s into %d chunks → s3://%s/%s",
            report["report_id"], len(chunks), bucket, chunks_key,
        )

    context.add_output_metadata({
        "reports_chunked": dg.MetadataValue.int(len(summaries)),
        "total_chunks": dg.MetadataValue.int(total_chunks),
        "chunk_size_tokens": dg.MetadataValue.int(CHUNK_TOKENS),
        "chunk_overlap_tokens": dg.MetadataValue.int(CHUNK_OVERLAP_TOKENS),
        "s3_prefix": dg.MetadataValue.text(f"s3://{bucket}/{S3_CHUNKS_PREFIX}/"),
    })
    return summaries
