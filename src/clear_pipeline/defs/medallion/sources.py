"""Per-source medallion adapters — the ONLY per-source code in this package.

**Add a data source = add an adapter here** and register it in
``MEDALLION_SOURCES``. Mirrors ``defs/signals/connectors.py``'s
``SignalSource``/``CONNECTORS`` pattern deliberately: each adapter *wraps*
that module's existing production connector (composition, not
modification — ``defs/signals/`` stays untouched) for the bronze plumbing
every source already has (poll, id/timestamp extraction, raw-byte
serialization, parse, watermark), and adds exactly one new method,
``to_silver_input``, the medallion-specific pure transform with no
clear-api write.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from clear_pipeline.defs.signals.connectors import DataminrConnector
from clear_pipeline.providers import dataminr
from clear_pipeline.providers.signal import build_signal_input


@runtime_checkable
class MedallionSource(Protocol):
    """What ``factory.py`` needs from a source. Every method except
    ``to_silver_input`` already exists on the matching production connector
    in ``defs/signals/connectors.py`` — adapters below delegate to it."""

    @property
    def source(self) -> str:
        """DataSource name in clear-api. Doubles as the S3 prefix
        (``raw|silver|gold/<source>/…``) and every Dagster asset's name
        prefix (``<source>_bronze``, ``<source>_silver``, …). Read-only —
        adapters expose it as a `@property` delegating to their wrapped
        connector."""
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
        geoparser candidate to a real L4 location row). The only method a
        medallion adapter adds beyond its wrapped production connector."""
        ...


@dataclass(frozen=True)
class DataminrMedallionSource:
    """Wraps the production ``DataminrConnector`` for everything bronze
    needs — identical fetch, keys, and watermark behavior to today's
    ``raw_dataminr`` ingest asset."""

    _connector: DataminrConnector = field(default_factory=DataminrConnector)

    @property
    def source(self) -> str:
        return self._connector.source

    def poll(self, since: datetime | None) -> list[Any]:
        return self._connector.poll(since)

    def external_id(self, record: Any) -> str:
        return self._connector.external_id(record)

    def published_at(self, record: Any) -> str:
        return self._connector.published_at(record)

    def raw_bytes(self, record: Any) -> bytes:
        return self._connector.raw_bytes(record)

    def parse(self, raw: bytes) -> Any:
        return self._connector.parse(raw)

    def api_source_id(self) -> str:
        return self._connector.api_source_id()

    def last_synced(self) -> datetime | None:
        return dataminr.get_last_synced()

    def set_watermark(self, ts: datetime) -> None:
        dataminr.set_last_synced(ts)

    def to_silver_input(self, record: Any, source_id: str) -> dict:
        # promote=False: geoparser enrichment stays a pure transform here —
        # no opportunistic L4 landmark write. See build_signal_input's
        # docstring for what `promote` controls.
        return build_signal_input(record, source_id, promote=False)


# Add ACLEDMedallionSource / Darfur24MedallionSource / IDMCMedallionSource
# here, each wrapping its connectors.py counterpart, once their own
# to_silver_input is ready (ACLED/Darfur24 need the same promote=False
# passthrough added to build_acled_signal_input / build_darfur24_signal_input
# first — Darfur24 doesn't call the geoparser at all, so it may not need one).
MEDALLION_SOURCES: list[MedallionSource] = [
    DataminrMedallionSource(),
]
