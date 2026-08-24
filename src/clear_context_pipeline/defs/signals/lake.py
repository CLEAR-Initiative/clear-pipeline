"""S3 data-lake helpers (bronze layer = raw source blobs only).

The lake stores ONLY raw payloads, partitioned by ``source/date`` so a backfill
job can replay a date range independently of the live event-driven path. All
processed state (signals/events/alerts/crises) stays in clear-api. Reuses
clear-context-pipeline's shared ``providers.s3.s3_client`` (same S3-compatible
config the KB pipeline uses). See ``docs/DAGSTER_MIGRATION_PROPOSAL.md`` §D1.
"""

from clear_context_pipeline.providers.s3 import s3_client  # re-exported for the assets

__all__ = ["s3_client", "raw_key", "write_raw"]


def raw_key(source: str, published_at: str, external_id: str) -> str:
    """Bronze raw-blob key: ``raw/<source>/<YYYY-MM-DD>/<external_id>.json``.

    Partitioned by publication date (the ISO-8601 prefix of ``published_at``) so
    a date-range backfill reads a bounded set of keys. ``external_id`` is the
    source's stable id (e.g. Dataminr ``alertId``); slashes are escaped."""
    day = (published_at or "")[:10] or "unknown"
    safe = external_id.replace("/", "_")
    return f"raw/{source}/{day}/{safe}.json"


def write_raw(s3, bucket: str, key: str, body: bytes) -> None:
    """Write a raw blob to the lake (idempotent — same key overwrites)."""
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
