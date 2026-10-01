"""Gold signals table: Type-1 (update-in-place) over Apache Iceberg, one
per source (`gold.<source>_signals`). A signal is a write-once fact with a
small number of mutable delivery/verdict fields (`pushedAt`, `retracted`) —
no history to keep, so `Table.upsert` keyed on `externalId` is the whole
write path (unlike `iceberg_events.py`, which needs real SCD2). See
docs/data-quality-medallion-implementation.md §6.

`signalInput` (the clear-api CreateSignalInput payload, shape varies per
source) is stored as a JSON string column — same pattern as
`iceberg_events.py`'s `signalIdsJson`.

`groupKey`/`retracted` support cross-poll supersession (`<source>_reconcile`
in factory.py): some sources deliver several rows that compete to describe
the same real-world fact, and which one survives can only be decided against
the whole group — which spans polls, so it has to be *readable back out of
gold*, not just held in a batch. `retracted` is the verdict, and it is
reversible: a later poll can bring a row back to life.
"""

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
    "groupKey", "retracted",
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
    identifier_field_ids=[1],
)


def _ensure_columns(cat, identifier: str, table):
    """Additively add any `_SCHEMA` column the table doesn't have yet.

    `_SCHEMA` is only applied at CREATE time, so a table created by an
    earlier version of this module never gains columns added later — it
    would keep loading with the old schema and `upsert_signals` would fail
    building an Arrow table against it. Adding a column is metadata-only and
    non-destructive: existing rows read back NULL for it.

    Adding the columns is NOT sufficient on its own, and the reason is
    subtle enough to be worth writing down. `Table.upsert` scans the rows it
    might update through `use_ref("main")`, which pins the scan to the
    current snapshot — and PyIceberg projects a snapshot-pinned scan through
    *that snapshot's* `schema_id`, not through `current_schema_id`. A
    snapshot written before the evolution therefore hands back
    pre-evolution columns, and the upsert dies casting the new column set
    onto the old one ("Target schema's field names are not matching").
    Every row already in the table is in such a snapshot, so the FIRST
    upsert after migrating would fail — not a test artifact, the next real
    poll. Verified against pyiceberg 0.12.0.

    So the existing rows are rewritten once, which lands them in a fresh
    snapshot stamped with the new schema. `overwrite` is atomic (it commits
    one new snapshot), and this runs only on the single poll that migrates
    the table. It reads the table into memory, which is fine at gold signal
    volumes and would want revisiting if that stops being true."""
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
    """Normalize pandas' missing-value spellings to None.

    Rows reach here straight off a DataFrame, and pandas represents a
    missing value in an inferred-`str` column as float NaN rather than
    None — so a column that is partly populated (`["ev-1", None]` infers
    `str`) hands Arrow a float for a string field and the write dies with
    "Expected bytes, got a 'float' object". A column that is *entirely*
    None stays object dtype and keeps its Nones, which is why this only
    bites on the mixed case: some rows grouped, some not."""
    try:
        return None if pd.isna(value) else value
    except (TypeError, ValueError):
        # Non-scalar (dict/list) — pd.isna returns an array, and it isn't
        # a missing value anyway.
        return value


def _to_column_dict(row: dict) -> dict:
    out = {k: _na_to_none(row.get(k)) for k in _COLUMNS if k != "signalInputJson"}
    out["signalInputJson"] = json.dumps(row.get("signalInput"), default=str)
    # Never write NULL here: a three-valued `retracted` would make every
    # reader repeat the "NULL means live" special case, and a row migrated
    # from the pre-`retracted` schema reads back NULL already. NaN is
    # normalized first — `bool(float("nan"))` is True, which would retract
    # a live signal.
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


def unpushed_signals(table) -> list[dict]:
    """Every signal row still awaiting push: `pushedAt IS NULL` and not
    retracted.

    The `retracted` half is filtered in pandas rather than in the Iceberg
    row filter because rows written before that column existed read back
    NULL, and `retracted = false` would not match them — silently stranding
    every pre-migration row as unpushable. `None` is falsy, so this treats
    NULL as live, which is what it meant.

    Excluding retracted rows here is also what keeps a superseded row that
    was never pushed from being created in clear-api at all. Creating it and
    retracting it afterwards is NOT equivalent: it would be visible as
    `status=NEW` in between, which is exactly what the drain selects on."""
    df = table.scan(row_filter="pushedAt IS NULL").to_pandas()
    rows = [_from_column_dict(row) for row in df.to_dict("records")]
    return [row for row in rows if not row.get("retracted")]


def signals_in_groups(table, group_keys: list[str]) -> list[dict]:
    """Every gold row belonging to one of these `groupKey`s — **including
    already-retracted ones**.

    Retracted rows are deliberately in scope: a retraction is a verdict
    about a group, and a later poll can reverse it (e.g. the Recommended
    figure that superseded a row stops being one). Hiding retracted rows
    from the reconciler would make retraction a one-way door and leave a
    row that should come back to life permanently dead."""
    if not group_keys:
        return []
    df = table.scan(row_filter=In("groupKey", group_keys)).to_pandas()
    return [_from_column_dict(row) for row in df.to_dict("records")]


def existing_pushed_at(table, external_ids: list[str]) -> dict[str, str | None]:
    """Current `pushedAt` for these `externalId`s. `upsert_signals` is a
    Type-1 MERGE that overwrites every column — a caller re-upserting a
    signal that re-enters gold (e.g. merged into another event) must read
    this first and carry the value forward, or it clobbers an already-
    pushed row's `pushedAt` back to NULL and the signal gets re-pushed."""
    if not external_ids:
        return {}
    df = table.scan(
        row_filter=In("externalId", external_ids),
        selected_fields=("externalId", "pushedAt"),
    ).to_pandas()
    return {
        row["externalId"]: (None if pd.isna(row["pushedAt"]) else row["pushedAt"])
        for row in df.to_dict("records")
    }
