"""Gold events table: SCD2 over Apache Iceberg, one per source
(`gold.<source>_events`). Rationale (Iceberg vs. Delta, the SCD2 columns)
is in docs/data-quality-medallion-implementation.md §6 — this module is
just the mechanics.

Writes: `append` for a new key, `overwrite` to atomically close the
current row and insert the next version. Not `Table.upsert` — that's
Type-1 (updates in place) and would destroy the history this table exists
to keep.

`clearApiEventId` lives outside this table, in a side JSON file
`<source>_push` owns — it changes on every push regardless of content, so
keeping it here would version-spam on every push. This table is pure
business history; `push` only ever reads it.

Timestamps are ISO-8601 strings, not Iceberg's native timestamp type —
sorts fine as a string, consistent with the rest of the codebase.
"""

import hashlib
import json
from datetime import datetime

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, LongType, NestedField, StringType

from clear_pipeline.signals.config import settings

NAMESPACE = "gold"

# Business fields — a change here spawns a new SCD2 version. Everything
# else (eventId/version/effectiveFrom/effectiveTo/isCurrent/contentHash) is
# bookkeeping. Column names: `signalIds` (a list everywhere else in the
# pipeline) is `signalIdsJson` here — converted at the module boundary only.
_CONTENT_FIELDS = [
    "districtKey", "eventType", "glideCode", "title", "description",
    "severity", "casualties", "signalIdsJson", "startedAt",
    "firstSignalCreatedAt", "lastSignalCreatedAt",
]

_SCHEMA = Schema(
    NestedField(1, "eventId", StringType(), required=True),
    NestedField(2, "version", LongType(), required=True),
    NestedField(3, "effectiveFrom", StringType(), required=True),
    NestedField(4, "effectiveTo", StringType(), required=False),
    NestedField(5, "isCurrent", BooleanType(), required=True),
    NestedField(6, "contentHash", StringType(), required=True),
    NestedField(7, "districtKey", StringType(), required=False),
    NestedField(8, "eventType", StringType(), required=False),
    NestedField(9, "glideCode", StringType(), required=False),
    NestedField(10, "title", StringType(), required=False),
    NestedField(11, "description", StringType(), required=False),
    NestedField(12, "severity", LongType(), required=False),
    NestedField(13, "casualties", LongType(), required=False),
    NestedField(14, "signalIdsJson", StringType(), required=False),
    NestedField(15, "startedAt", StringType(), required=False),
    NestedField(16, "firstSignalCreatedAt", StringType(), required=False),
    NestedField(17, "lastSignalCreatedAt", StringType(), required=False),
)


def _catalog() -> Catalog:
    warehouse = settings.iceberg_warehouse or f"s3://{settings.s3_bucket}/gold-iceberg"
    properties: dict[str, str] = {
        "type": "sql",
        "uri": settings.iceberg_catalog_uri,
        "warehouse": warehouse,
    }
    if warehouse.startswith("s3://"):
        properties |= {
            "s3.endpoint": settings.s3_endpoint,
            "s3.region": settings.s3_region,
            "s3.access-key-id": settings.s3_access_key_id,
            "s3.secret-access-key": settings.s3_secret_access_key,
            # This pipeline always talks to a custom S3-compatible endpoint
            # (providers/s3.py), never bare AWS S3 — path-style addressing
            # is what those backends (e.g. MinIO) expect.
            "s3.path-style-access": "true",
        }
    return load_catalog("gx_pipeline", **properties)


def get_events_table(source: str):
    """The `gold.<source>_events` table, created on first use."""
    catalog = _catalog()
    if NAMESPACE not in [ns[0] for ns in catalog.list_namespaces()]:
        catalog.create_namespace(NAMESPACE)
    identifier = f"{NAMESPACE}.{source}_events"
    if not catalog.table_exists(identifier):
        return catalog.create_table(identifier, schema=_SCHEMA)
    return catalog.load_table(identifier)


def _to_column_dict(event: dict) -> dict:
    """Caller-facing event dict (`signalIds` as a list) -> Iceberg column
    dict (`signalIdsJson` as a JSON string)."""
    out = {k: event.get(k) for k in _CONTENT_FIELDS if k != "signalIdsJson"}
    out["signalIdsJson"] = json.dumps(sorted(event.get("signalIds") or []))
    return out


def _from_column_dict(row: dict) -> dict:
    """Inverse of `_to_column_dict`, for rows read back from the table.
    pandas represents a missing optional column as NaN, not None — normalize
    so callers can keep writing `event.get("description")`-style checks."""
    out = {k: (None if pd.isna(v) else v) for k, v in row.items()}
    out["signalIds"] = json.loads(out.pop("signalIdsJson") or "[]")
    return out


def _content_hash(event: dict) -> str:
    payload = json.dumps(_to_column_dict(event), sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _row(event: dict, *, version: int, effective_from: str, effective_to: str | None, is_current: bool, content_hash: str) -> dict:
    row = {
        "eventId": event["eventId"], "version": version,
        "effectiveFrom": effective_from, "effectiveTo": effective_to,
        "isCurrent": is_current, "contentHash": content_hash,
    }
    row.update(_to_column_dict(event))
    return row


def current_event(table, event_id: str) -> dict | None:
    """The current version of one event, or None if it has never been written.
    Shaped like every other event dict in the pipeline (`signalIds` a list)."""
    df = table.scan(row_filter=f"eventId == '{event_id}' AND isCurrent == true").to_pandas()
    if df.empty:
        return None
    return _from_column_dict(df.iloc[0].to_dict())


def current_events_df(table) -> pd.DataFrame:
    """Every event's current version — what `<source>_temporal` clusters
    against. Kept as raw columns (including `signalIdsJson`); callers here
    only need `districtKey`/`eventType`/`lastSignalCreatedAt`/`eventId`."""
    return table.scan(row_filter="isCurrent == true").to_pandas()


def merge_event(table, event: dict) -> dict:
    """Write `event` as the new current version, UNLESS its content is
    identical to what's already current (contentHash match — a no-op, so an
    idempotent re-run doesn't spawn version noise). Returns the resulting
    current row (the existing one, if this was a no-op)."""
    now_iso = datetime.now().astimezone().isoformat()
    content_hash = _content_hash(event)
    existing = current_event(table, event["eventId"])

    if existing is not None and existing["contentHash"] == content_hash:
        return existing

    new_version = (int(existing["version"]) + 1) if existing else 1
    new_arrow_row = _row(  # column shape, for the write
        event, version=new_version, effective_from=now_iso, effective_to=None,
        is_current=True, content_hash=content_hash,
    )
    new_row = {  # caller-facing shape, for the return value
        **event, "version": new_version, "effectiveFrom": now_iso,
        "effectiveTo": None, "isCurrent": True, "contentHash": content_hash,
    }
    arrow_schema = table.schema().as_arrow()

    if existing is None:
        table.append(pa.Table.from_pylist([new_arrow_row], schema=arrow_schema))
        return new_row

    # Via `_row`, not `dict(existing)` — existing is caller-facing shape.
    closed_row = _row(
        existing, version=int(existing["version"]), effective_from=existing["effectiveFrom"],
        effective_to=now_iso, is_current=False, content_hash=existing["contentHash"],
    )
    table.overwrite(
        pa.Table.from_pylist([closed_row, new_arrow_row], schema=arrow_schema),
        overwrite_filter=f"eventId == '{event['eventId']}'",
    )
    return new_row
