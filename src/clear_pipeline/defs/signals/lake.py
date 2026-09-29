"""S3 data-lake helpers (bronze layer = raw source blobs only).

The lake stores ONLY raw payloads, partitioned by ``source/date`` so a backfill
job can replay a date range independently of the live event-driven path. All
processed state (signals/events/alerts/crises) stays in clear-api. Reuses
clear-pipeline's shared ``providers.s3.s3_client`` (same S3-compatible
config the KB pipeline uses). See ``docs/DAGSTER_MIGRATION_PROPOSAL.md`` §D1.
"""

import json

from botocore.exceptions import ClientError

from clear_pipeline.providers.s3 import s3_client  # re-exported for the assets

__all__ = ["s3_client", "raw_key", "write_raw", "write_json", "read_json", "list_keys"]


def raw_key(source: str, published_at: str, external_id: str, *, layer: str = "raw") -> str:
    """Medallion blob key: ``<layer>/<source>/<YYYY-MM-DD>/<external_id>.json``.
    Default ``layer="raw"`` is bronze, unchanged from before. Medallion silver
    assets pass ``layer="silver"`` to reuse the same day-partitioned key shape.

    Partitioned by publication date (the ISO-8601 prefix of ``published_at``) so
    a date-range backfill reads a bounded set of keys. ``external_id`` is the
    source's stable id (e.g. Dataminr ``alertId``); slashes are escaped."""
    day = (published_at or "")[:10] or "unknown"
    safe = external_id.replace("/", "_")
    return f"{layer}/{source}/{day}/{safe}.json"


def write_raw(s3, bucket: str, key: str, body: bytes) -> None:
    """Write a raw blob to the lake (idempotent — same key overwrites)."""
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def write_json(s3, bucket: str, key: str, payload: object) -> None:
    """Write a JSON-serializable object to the lake (idempotent — overwrites).
    Used by silver/gold medallion assets; not partitioned by convention like
    ``raw_key`` — callers pass whatever key shape their layer needs (e.g.
    gold's per-signal/per-event tables, keyed by id, not by date)."""
    s3.put_object(
        Bucket=bucket, Key=key, Body=json.dumps(payload, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def read_json(s3, bucket: str, key: str) -> dict | None:
    """Read one JSON object back, or None if the key doesn't exist."""
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(body)


def list_keys(s3, bucket: str, prefix: str) -> list[str]:
    """List every key under a prefix (paginated). Fine at medallion volumes;
    ponytail: a full prefix scan, add a manifest/index if this gets expensive."""
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys
