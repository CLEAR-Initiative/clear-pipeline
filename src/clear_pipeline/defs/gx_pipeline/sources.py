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

import json

from clear_pipeline.providers import acled, darfur24, dataminr
from clear_pipeline.providers.clear_api import get_locations_by_level, get_source_id_by_name
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


# IDMC is NOT registered below, on purpose — not just "not yet written".
# Production's own IDMCConnector (defs/signals/connectors.py) sets
# drained=False for the same reason: grouping IDMC signals into events
# needs design work that hasn't happened ("needs new features that aren't
# built yet"). This factory has no equivalent of that flag — every
# registered source runs the full classify/geo/temporal/match chain, which
# IS event-grouping — so registering an IDMCGXSource today would build on
# the exact gap production explicitly deferred, not just reuse a pattern.
# Add it once IDMC event-grouping has a real design, not before.
GX_SOURCES: list[GXSource] = [
    DataminrGXSource(),
    ACLEDGXSource(),
    Darfur24GXSource(),
]
