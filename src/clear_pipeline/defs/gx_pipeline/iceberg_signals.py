"""Gold signals table: Type-1 (update-in-place) over Apache Iceberg, one
per source (`gold.<source>_signals`). A signal is a write-once fact with a
single mutable field (`pushedAt`) — no history to keep, so `Table.upsert`
keyed on `externalId` is the whole write path (unlike `iceberg_events.py`,
which needs real SCD2). See
docs/data-quality-medallion-implementation.md §6.

`signalInput` (the clear-api CreateSignalInput payload, shape varies per
source) is stored as a JSON string column — same pattern as
`iceberg_events.py`'s `signalIdsJson`.
"""

import json

import pandas as pd
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from clear_pipeline.defs.gx_pipeline.iceberg_catalog import NAMESPACE, catalog, ensure_namespace

_COLUMNS = [
    "externalId", "eventId", "relevanceScore", "eventType", "districtKey",
    "matchOutcome", "severity", "populationAffectedContribution",
    "casualtiesContribution", "createdAt", "pushedAt", "signalInputJson",
]

_SCHEMA = Schema(
    NestedField(1, "externalId", StringType(), required=True),
    NestedField(2, "eventId", StringType(), required=False),
    NestedField(3, "relevanceScore", DoubleType(), required=False),
    NestedField(4, "eventType", StringType(), required=False),
    NestedField(5, "districtKey", StringType(), required=False),
    NestedField(6, "matchOutcome", StringType(), required=False),
    NestedField(7, "severity", LongType(), required=False),
    NestedField(8, "populationAffectedContribution", LongType(), required=False),
    NestedField(9, "casualtiesContribution", LongType(), required=False),
    NestedField(10, "createdAt", StringType(), required=False),
    NestedField(11, "pushedAt", StringType(), required=False),
    NestedField(12, "signalInputJson", StringType(), required=False),
    identifier_field_ids=[1],
)


def get_signals_table(source: str):
    """The `gold.<source>_signals` table, created on first use."""
    cat = catalog()
    ensure_namespace(cat)
    identifier = f"{NAMESPACE}.{source}_signals"
    if not cat.table_exists(identifier):
        return cat.create_table(identifier, schema=_SCHEMA)
    return cat.load_table(identifier)


def _to_column_dict(row: dict) -> dict:
    out = {k: row.get(k) for k in _COLUMNS if k != "signalInputJson"}
    out["signalInputJson"] = json.dumps(row.get("signalInput"), default=str)
    return out


def _from_column_dict(row: dict) -> dict:
    out = {k: (None if pd.isna(v) else v) for k, v in row.items()}
    out["signalInput"] = json.loads(out.pop("signalInputJson") or "null")
    return out


def upsert_signals(table, rows: list[dict]) -> None:
    """Write-or-update by `externalId` — Type-1, no history kept."""
    if not rows:
        return
    arrow_rows = [_to_column_dict(r) for r in rows]
    arrow_table = pa.Table.from_pylist(arrow_rows, schema=table.schema().as_arrow())
    table.upsert(arrow_table, join_cols=["externalId"])


def unpushed_signals(table) -> list[dict]:
    """Every signal row still awaiting push (`pushedAt IS NULL`)."""
    df = table.scan(row_filter="pushedAt IS NULL").to_pandas()
    return [_from_column_dict(row) for row in df.to_dict("records")]
