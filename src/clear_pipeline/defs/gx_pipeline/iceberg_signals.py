"""Gold signals: one Type-1 Iceberg table per source (`gold.<source>_signals`),
upserted on `externalId`, no history (docs/data-quality-medallion-implementation.md §6).
`signalInput` is a JSON string column. `groupKey`/`retracted` keep cross-poll
supersession readable from gold (`retracted` is reversible). `pushedState` is
the `sync_state` Postgres last received; a mismatch means the row needs an update."""

import json

import pandas as pd
import pyarrow as pa
from pyiceberg.expressions import In
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DoubleType, LongType, NestedField, StringType

from clear_pipeline.defs.gx_pipeline.iceberg_catalog import (
    NAMESPACE,
    catalog,
    ensure_namespace,
)

_COLUMNS = [
    "externalId", "eventId", "relevanceScore", "eventType", "districtKey",
    "matchOutcome", "severity", "populationAffectedContribution",
    "casualtiesContribution", "createdAt", "pushedAt", "signalInputJson",
    "groupKey", "retracted", "pushedState",
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
    NestedField(13, "groupKey", StringType(), required=False),
    NestedField(14, "retracted", BooleanType(), required=False),
    NestedField(15, "pushedState", StringType(), required=False),
    identifier_field_ids=[1],
)


def _ensure_columns(cat, identifier: str, table):
    """Add any `_SCHEMA` column the table lacks (`_SCHEMA` applies only at
    create), then rewrite existing rows once. `Table.upsert` scans via
    `use_ref("main")`, which PyIceberg 0.12.0 projects through that snapshot's
    schema, so a pre-evolution snapshot breaks the next upsert ("Target schema's
    field names are not matching"). Atomic, but loads the table into memory."""
    missing = [f for f in _SCHEMA.fields if f.name not in table.schema().column_names]
    if not missing:
        return table
    with table.update_schema() as update:
        for field in missing:
            update.add_column(field.name, field.field_type)
    table = cat.load_table(identifier)

    existing = table.scan().to_arrow()  # projects the new columns as NULL
    if existing.num_rows:
        table.overwrite(existing)
        table = cat.load_table(identifier)
    return table


def get_signals_table(source: str):
    """The `gold.<source>_signals` table, created on first use and migrated
    up to `_SCHEMA` if it predates a column."""
    cat = catalog()
    ensure_namespace(cat)
    identifier = f"{NAMESPACE}.{source}_signals"
    if not cat.table_exists(identifier):
        return cat.create_table(identifier, schema=_SCHEMA)
    return _ensure_columns(cat, identifier, cat.load_table(identifier))


def _na_to_none(value):
    """pandas NaN -> None. A partly-None string column (`["ev-1", None]`) holds
    NaN, which Arrow rejects ("Expected bytes, got a 'float' object")."""
    try:
        return None if pd.isna(value) else value
    except (TypeError, ValueError):
        # Non-scalar (dict/list): pd.isna returns an array; not missing.
        return value


def _to_column_dict(row: dict) -> dict:
    out = {k: _na_to_none(row.get(k)) for k in _COLUMNS if k != "signalInputJson"}
    out["signalInputJson"] = json.dumps(row.get("signalInput"), default=str)
    # Never write NULL: readers treat `retracted` as two-valued. NaN is
    # normalized above because `bool(float("nan"))` is True (would retract).
    out["retracted"] = bool(out["retracted"])
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


def sync_state(row: dict) -> str:
    """What Postgres should hold for this row: ``"<contentHash>|<0/1>"``.
    Gold-only bookkeeping, never sent as ``contentHash``."""
    content_hash = (row.get("signalInput") or {}).get("contentHash")
    return f"{content_hash}|{int(bool(row.get('retracted')))}"


def signals_to_sync(table, can_update: bool) -> dict[str, list[dict]]:
    """Rows `_push` must send: ``create`` (unpushed, live), ``probe`` (unpushed,
    retracted, never probed: a create may have landed unrecorded), ``update``
    (Postgres holds an older ``sync_state``). Never create-then-retract: the row
    would sit as `status=NEW`, which the drain selects. Filtered in pandas so a
    NULL `retracted` (migrated row) counts as live; an Iceberg filter would skip it."""
    rows = [_from_column_dict(r) for r in table.scan().to_pandas().to_dict("records")]
    out: dict[str, list[dict]] = {"create": [], "probe": [], "update": []}
    for row in rows:
        if row.get("pushedAt") is None:
            if not row.get("retracted"):
                out["create"].append(row)
            elif can_update and row.get("pushedState") is None:
                out["probe"].append(row)
        elif can_update and sync_state(row) != row.get("pushedState"):
            out["update"].append(row)
    return out


def sync_hashes(table, external_ids: list[str]) -> dict[str, tuple[str | None, str | None]]:
    """``{externalId: (contentHash, pushedState)}`` for these rows — what the
    bronze skip compares a freshly polled hash against."""
    if not external_ids:
        return {}
    df = table.scan(
        row_filter=In("externalId", external_ids),
        selected_fields=("externalId", "signalInputJson", "pushedState"),
    ).to_pandas()
    out = {}
    for row in df.to_dict("records"):
        signal_input = json.loads(row["signalInputJson"] or "null") or {}
        pushed_state = None if pd.isna(row["pushedState"]) else row["pushedState"]
        out[row["externalId"]] = (signal_input.get("contentHash"), pushed_state)
    return out


def signals_in_groups(table, group_keys: list[str]) -> list[dict]:
    """Every gold row in these `groupKey`s, retracted ones included: a later
    poll can reverse a retraction, so hiding them would make it one-way."""
    if not group_keys:
        return []
    df = table.scan(row_filter=In("groupKey", group_keys)).to_pandas()
    return [_from_column_dict(row) for row in df.to_dict("records")]


def existing_push_state(table, external_ids: list[str]) -> dict[str, dict]:
    """Current ``{pushedAt, pushedState}`` per `externalId`. `upsert_signals`
    overwrites every column, so a signal re-entering gold must carry these
    forward or it reverts to unpushed and gets re-pushed."""
    if not external_ids:
        return {}
    df = table.scan(
        row_filter=In("externalId", external_ids),
        selected_fields=("externalId", "pushedAt", "pushedState"),
    ).to_pandas()
    return {
        row["externalId"]: {
            "pushedAt": None if pd.isna(row["pushedAt"]) else row["pushedAt"],
            "pushedState": None if pd.isna(row["pushedState"]) else row["pushedState"],
        }
        for row in df.to_dict("records")
    }
