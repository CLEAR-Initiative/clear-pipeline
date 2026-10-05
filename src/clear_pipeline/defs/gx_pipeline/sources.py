"""Per-source adapters — the ONLY per-source code in this package.

**Add a data source = add an adapter here** and register it in
``GX_SOURCES``. Mirrors ``defs/signals/connectors.py``'s
``SignalSource``/``CONNECTORS`` shape, but calls ``providers/<source>.py``
directly instead of wrapping a connector — those connectors are thin
one-line passthroughs to ``providers/`` anyway, so this is the same
behavior with zero import dependency on ``defs/signals``. That's
deliberate: ``defs/signals/`` runs the production poll -> drain pipeline
and its connectors carry drain-only methods (``project``,
``to_content_update_input``) this package doesn't need — once this
package replaces that pipeline, `defs/signals/` is deletable with no
change here.

``to_silver_input`` is the one method with no production equivalent: a
pure transform, no clear-api write, built on
``providers/signal.py``'s ``build_signal_input(..., promote=False)``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from clear_pipeline.providers import acled, darfur24, dataminr, idmc
from clear_pipeline.providers.clear_api import (
    get_locations_by_level,
    get_source_id_by_name,
)
from clear_pipeline.providers.signal import build_signal_input
from clear_pipeline.signals.config import settings


@runtime_checkable
class GXSource(Protocol):
    """What ``factory.py`` needs from a source. Every method except
    ``to_silver_input`` already exists as a `providers/<source>.py`
    function or attribute — adapters below call it directly, no
    `defs/signals` import (see module docstring)."""

    @property
    def source(self) -> str:
        """DataSource name in clear-api. Doubles as the S3 prefix
        (``raw|silver|gold/<source>/…``) and every Dagster asset's name
        prefix (``<source>_bronze``, ``<source>_silver``, …)."""
        ...

    def poll(self, since: datetime | None) -> list[Any]:
        """Fetch source records published since ``since`` (None = initial lookback)."""
        ...

    def external_id(self, record: Any) -> str:
        """Stable upstream id — the S3 key + gold row id."""
        ...

    def published_at(self, record: Any) -> str:
        """ISO-8601 publication timestamp — drives the S3 date partition."""
        ...

    def raw_bytes(self, record: Any) -> bytes:
        """The record serialised for the bronze S3 blob."""
        ...

    def parse(self, raw: bytes) -> Any:
        """Inverse of ``raw_bytes``: rebuild the record from an S3 blob."""
        ...

    def api_source_id(self) -> str:
        """Resolve this source's clear-api ``DataSource`` id (a read, not a write)."""
        ...

    def last_synced(self) -> datetime | None:
        """Read the poll watermark (None = resume / initial lookback)."""
        ...

    def set_watermark(self, ts: datetime) -> None:
        """Advance the poll watermark. Called only after a clean bronze batch."""
        ...

    def to_silver_input(self, record: Any, source_id: str) -> dict:
        """Normalize a record into a clear-api ``CreateSignalInput``-shaped
        dict, with NO clear-api write as a side effect (unlike the
        production connector's ``to_signal_input``, which may promote a
        geoparser candidate to a real L4 location row). The only method
        without a direct production equivalent."""
        ...

    def mark_seen(self, external_id: str) -> None:
        """Mark this record ingested in the source's own seen-set, called
        only after ``_push`` confirms the create. No-op for sources with no
        seen-set (matches production's connectors — Dataminr dedups on
        watermark alone). Sources with one (ACLED, Darfur24) must not rely
        on `defs/signals`' drain populating it — this pipeline runs
        independently and needs its own mark."""
        ...


@dataclass(frozen=True)
class DataminrGXSource:
    """Calls `providers/dataminr.py` directly — identical fetch, keys, and
    watermark behavior to today's `raw_dataminr` ingest asset, with no
    import from `defs/signals` (see module docstring)."""

    @property
    def source(self) -> str:
        return settings.dataminr_source_name

    def poll(self, since: datetime | None) -> list[Any]:
        return dataminr.fetch_signals(since=since)

    def external_id(self, record: Any) -> str:
        return record.alertId

    def published_at(self, record: Any) -> str:
        return record.alertTimestamp

    def raw_bytes(self, record: Any) -> bytes:
        return record.model_dump_json().encode("utf-8")

    def parse(self, raw: bytes) -> Any:
        return dataminr.DataminrSignal.model_validate_json(raw)

    def api_source_id(self) -> str:
        return get_source_id_by_name(settings.dataminr_source_name)

    def last_synced(self) -> datetime | None:
        return dataminr.get_last_synced()

    def set_watermark(self, ts: datetime) -> None:
        dataminr.set_last_synced(ts)

    def to_silver_input(self, record: Any, source_id: str) -> dict:
        # promote=False: geoparser enrichment stays a pure transform here —
        # no opportunistic L4 landmark write. See build_signal_input's
        # docstring for what `promote` controls.
        return build_signal_input(record, source_id, promote=False)

    def mark_seen(self, external_id: str) -> None:
        pass  # no seen-set — production's DataminrConnector doesn't mark_seen either


@dataclass(frozen=True)
class ACLEDGXSource:
    """Calls `providers/acled.py` directly — same recipe as `DataminrGXSource`."""

    @property
    def source(self) -> str:
        return settings.acled_source_name

    def poll(self, since: datetime | None) -> list[Any]:
        return acled.fetch_acled_events(since=since)

    def external_id(self, record: Any) -> str:
        return record["acled_id"]

    def published_at(self, record: Any) -> str:
        return record.get("event_date") or ""

    def raw_bytes(self, record: Any) -> bytes:
        return json.dumps(record).encode("utf-8")

    def parse(self, raw: bytes) -> Any:
        return json.loads(raw)

    def api_source_id(self) -> str:
        return get_source_id_by_name(settings.acled_source_name)

    def last_synced(self) -> datetime | None:
        return acled.get_last_synced()

    def set_watermark(self, ts: datetime) -> None:
        acled.set_last_synced(ts)

    def to_silver_input(self, record: Any, source_id: str) -> dict:
        return acled.build_acled_signal_input(record, source_id, promote=False)

    def mark_seen(self, external_id: str) -> None:
        acled.mark_seen(external_id)


@dataclass
class Darfur24GXSource:
    """Calls `providers/darfur24.py` directly. Not frozen, unlike the other
    adapters — caches the resolved country L0 location id on first use
    (same reasoning as the production connector's `_resolve_location_id`:
    it's a clear-api read, worth not repeating per article)."""

    _location_id: str | None = None

    @property
    def source(self) -> str:
        return settings.darfur24_source_name

    def poll(self, since: datetime | None) -> list[Any]:
        return darfur24.fetch_darfur24_articles()  # RSS has no time window

    def external_id(self, record: Any) -> str:
        return record["darfur24_id"]

    def published_at(self, record: Any) -> str:
        return record.get("published_at") or ""

    def raw_bytes(self, record: Any) -> bytes:
        return json.dumps(record).encode("utf-8")

    def parse(self, raw: bytes) -> Any:
        return json.loads(raw)

    def api_source_id(self) -> str:
        return get_source_id_by_name(settings.darfur24_source_name)

    def last_synced(self) -> datetime | None:
        return darfur24.get_last_synced()

    def set_watermark(self, ts: datetime) -> None:
        darfur24.set_last_synced(ts)

    def _resolve_location_id(self) -> str | None:
        if self._location_id is None:
            for loc in get_locations_by_level(0):
                if loc["name"] == settings.darfur24_default_country:
                    self._location_id = loc["id"]
                    break
        return self._location_id

    def to_silver_input(self, record: Any, source_id: str) -> dict:
        # No promote param — darfur24 never calls the geoparser at all
        # (news articles carry no coordinates to enrich from).
        return darfur24.build_darfur24_signal_input(record, source_id, self._resolve_location_id())

    def mark_seen(self, external_id: str) -> None:
        darfur24.mark_seen(external_id)


@dataclass(frozen=True)
class IDMCGXSource:
    """Calls `providers/idmc.py` directly — same recipe as `ACLEDGXSource`.
    IDMC's only ingestion path: production's `raw_idmc` no longer exists.
    IDU rows revise in place (same `idu_id`, new content): the
    `content_hash`/`content_update_input` hooks let bronze skip unchanged
    rows and `_push` send revisions and retractions."""

    @property
    def source(self) -> str:
        return settings.idmc_source_name

    def poll(self, since: datetime | None) -> list[Any]:
        return idmc.fetch_idu_records(since=since)

    def external_id(self, record: Any) -> str:
        return record["idu_id"]

    def published_at(self, record: Any) -> str:
        return record.get("created_at") or ""

    def raw_bytes(self, record: Any) -> bytes:
        return json.dumps(record).encode("utf-8")

    def parse(self, raw: bytes) -> Any:
        return json.loads(raw)

    def api_source_id(self) -> str:
        return get_source_id_by_name(settings.idmc_source_name)

    def last_synced(self) -> datetime | None:
        return idmc.get_last_synced()

    def set_watermark(self, ts: datetime) -> None:
        idmc.set_last_synced(ts)

    def to_silver_input(self, record: Any, source_id: str) -> dict:
        return idmc.build_idmc_signal_input(record, source_id, promote=False)

    def mark_seen(self, external_id: str) -> None:
        # No seen-set: the bronze skip (gold hash + pushedState) dedups.
        pass

    # ── Optional sync hooks ────────────────────────────────────────────────
    # Only IDMC revises rows in place. Probed via `getattr` in factory.py,
    # like the group hooks below.

    def content_hash(self, record: Any) -> str:
        return record["content_hash"]

    def content_update_input(self, signal_input: dict, *, retracted: bool) -> dict:
        return idmc.build_signal_content_update(signal_input, retracted=retracted)

    # ── Optional group-supersession hooks ─────────────────────────────────
    # IDMC-specific: one IDU `event_id` can have several role-tagged rows
    # (Recommended figure vs Triangulation) competing to describe the same
    # displacement, and only some survive — no other source has this shape,
    # so no other adapter defines these. See providers/idmc.py for the rules.
    #
    # Deliberately NOT part of the `GXSource` Protocol — `factory.py`'s
    # `_reconcile` probes for both via `getattr` and passes the batch
    # straight through unless both are present, so Dataminr/ACLED/Darfur24
    # need no change at all to stay unaffected.

    def group_member(self, external_id: str, raw_data: dict | None) -> dict | None:
        """Place a row in its supersession group, or None if it's in none.
        `raw_data` is whatever `to_silver_input` stored as `rawData`, so
        this works identically on a freshly polled row and on one read back
        out of gold — which is what lets `_reconcile` compare the two."""
        return idmc.group_member(external_id, raw_data)

    def resolve_group(self, members: list[dict]) -> dict[str, str]:
        """Verdict per member of ONE group: `"retract"` kills the row,
        anything else keeps it. Must be total over `members` and must be
        reversible — a member that retracted on an earlier poll has to be
        able to come back if the group's composition changes."""
        return idmc.resolve_group(members)


# IDMC is registered for the classify/geo/QA/filtering slice of this
# pipeline: IDU figures flow through classify -> geo -> temporal -> match ->
# gold -> push, giving QA visibility and letting low-quality/irrelevant
# signals be filtered before clear-api sees them — same as ACLED/Darfur24.
# Two things this deliberately does NOT mean:
#   1. IDMC-native event-grouping is designed. `_temporal`/`_match`'s
#      district+type heuristic runs for IDMC exactly as it does for every
#      other source; whether that's semantically right for IDU figures is
#      still open. The drain groups pushed IDMC rows with the same
#      district+type heuristic (IDMCConnector, drained=True). This pipeline's
#      gold *events* table is a QA-only Iceberg sandbox that never writes to
#      clear-api (only `_push` does, and only Signal rows), so nothing here
#      creates real clear-api Events either way.
#   2. That other sources' revisions reach clear-api. Only IDMCGXSource
#      defines the sync hooks; Dataminr/ACLED/Darfur24 stay create-only.
GX_SOURCES: list[GXSource] = [
    DataminrGXSource(),
    ACLEDGXSource(),
    Darfur24GXSource(),
    IDMCGXSource(),
]
