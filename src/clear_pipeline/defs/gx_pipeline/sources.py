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

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from clear_pipeline.providers import dataminr
from clear_pipeline.providers.clear_api import get_source_id_by_name
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


# Add ACLEDGXSource / Darfur24GXSource / IDMCGXSource here, each calling its
# providers/<source>.py module directly (same recipe as DataminrGXSource
# above — no defs/signals import), once their own to_silver_input is ready
# (ACLED/Darfur24 need the same promote=False passthrough added to
# build_acled_signal_input / build_darfur24_signal_input first — Darfur24
# doesn't call the geoparser at all, so it may not need one).
GX_SOURCES: list[GXSource] = [
    DataminrGXSource(),
]
