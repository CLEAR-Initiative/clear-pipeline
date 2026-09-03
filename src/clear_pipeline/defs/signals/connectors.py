"""Source connectors — one per data source, the ONLY per-source code.

**Add a data source = add a connector here** and register it in ``CONNECTORS``.
The factory (``factory.py``) turns each connector into its Dagster defs; nothing
else is source-specific.

The pipeline is **stage-based**, not source-vertical: per-source **ingest** feeds
one shared set of drain stages (classify → group → alert → translate, see
``stages.py``) that process signals from every source. A connector declares two
capability flags:

  ┌────────────┬─────────┬─────────┬───────────────────────────────────────────┐
  │ source     │ polled  │ drained │ role                                        │
  ├────────────┼─────────┼─────────┼───────────────────────────────────────────┤
  │ dataminr   │  True   │  True   │ ingest asset + poll sensor; feeds stages    │
  │ acled      │  True   │  True   │ ingest asset + poll sensor; feeds stages    │
  │ gdacs      │  True   │  True   │ ingest asset + poll sensor; feeds stages    │
  │ darfur24   │  True   │  True   │ ingest asset + poll sensor; feeds stages    │
  │ idmc       │  True   │  False  │ ingest asset + poll sensor; NOT grouped     │
  │ manual     │  False  │  True   │ no ingest — analyst-created; feeds stages   │
  └────────────┴─────────┴─────────┴───────────────────────────────────────────┘

- **polled** — has an external API to poll. The factory builds an ingest asset +
  poll sensor: it writes raw blobs to the lake and ``createSignal(status=NEW,
  rawS3Key=…)``. The shared classify/group stage rehydrates the record from the
  blob (``parse`` → ``project``). Manual signals are analyst-created directly in
  clear-api (no poll, no lake blob) so ``project`` reads the signal row itself
  (``record=None``).
- **drained** — its NEW signals are processed by the classify/group stage.
  ``idmc`` is the one exception: its grouping logic is different and needs new
  features that aren't built yet, so its signals are ingested but not grouped
  into events for now (see ``DRAINED_SOURCES``).

Connectors reuse the consolidated ``clear_pipeline.providers`` modules
(dataminr, acled, gdacs, darfur24, signal, …) so every source shares one
implementation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from clear_pipeline.providers import acled, darfur24, dataminr, gdacs, idmc
from clear_pipeline.providers.signal import build_signal_input
from clear_pipeline.signals.config import settings


@dataclass(frozen=True)
class SignalView:
    """Canonical, source-agnostic projection of one signal.

    The generic drain (classify → group → escalate) reads ONLY this view plus
    the clear-api ``created`` signal row — never a source-specific model. Each
    connector's ``project(record, created)`` is the single place that knows how
    to pull these fields out of its own payload (Dataminr ``headline`` /
    ``estimatedEventLocation``, ACLED ``location`` / ``lat``, or — for manual —
    the clear-api row itself, with ``record=None``).
    """

    external_id: str                      # dedup key + per-signal Redis lock
    title: str                            # raw headline — classify/group title
    timestamp: str                        # ISO-8601 publication time
    description: str | None = None        # short summary
    location_name: str | None = None
    url: str | None = None
    lat: float | None = None
    lng: float | None = None
    probability_radius_km: float | None = None
    # Extra free-text the v1 (Claude) classifier gets as context. Empty for
    # sources whose payload has no richer body than title/description.
    raw_context: str = "(no additional context)"


@runtime_checkable
class SignalSource(Protocol):
    """Base contract every connector satisfies (drain side)."""

    #: DataSource name in clear-api. Doubles as the S3 lake prefix
    #: (``raw/<source>/…``) and the ``pendingSignals(source=…)`` drain filter.
    source: str
    #: Has an external API to poll (→ ingest asset + poll sensor). False = manual.
    polled: bool
    #: Its NEW signals are processed by the classify/group stage. All current sources drained.
    drained: bool
    #: For a polled source: poll cadence (min). For a drain-only source: how often
    #: the drain sensor checks clear-api for pending signals. Editable live via the
    #: sensor cursor in the Dagster UI.
    poll_interval_minutes: int

    def project(self, record: Any, created: dict) -> SignalView:
        """Project a record (+ its clear-api row) into the canonical
        :class:`SignalView` the generic drain consumes. ``record`` is the parsed
        lake blob for a polled source, or ``None`` for a manual source (project
        from ``created`` alone). The ONLY per-source field-extraction code."""
        ...


@runtime_checkable
class PollSource(SignalSource, Protocol):
    """A connector that polls an external API and lands raw blobs in the lake."""

    def poll(self, since: datetime | None) -> list[Any]:
        """Fetch source records published since ``since`` (None = initial lookback)."""
        ...

    def external_id(self, record: Any) -> str:
        """Stable upstream id — the raw-blob filename + dedup key."""
        ...

    def published_at(self, record: Any) -> str:
        """ISO-8601 publication timestamp — drives the lake's date partition."""
        ...

    def raw_bytes(self, record: Any) -> bytes:
        """The record serialised for the bronze lake blob."""
        ...

    def api_source_id(self) -> str:
        """Resolve this source's clear-api ``DataSource`` id (cached upstream)."""
        ...

    def to_signal_input(self, record: Any, api_source_id: str) -> dict:
        """Normalise the record into a clear-api ``createSignal`` input dict.
        ``rawS3Key`` is added by the ingest asset after the blob lands."""
        ...

    def last_synced(self) -> datetime | None:
        """Read the poll watermark (None = resume / initial lookback)."""
        ...

    def set_watermark(self, ts: datetime) -> None:
        """Advance the poll watermark. Called by the ingest asset AFTER a clean
        batch (all records persisted) — never inside ``poll`` — so a partial
        failure can't strand un-created records behind an advanced watermark."""
        ...

    def post_create(self, record: Any) -> None:
        """Per-record hook after ``createSignal`` succeeds — marks the record seen
        in the source's dedup set so a failed persistence leaves it re-pollable.
        No-op for sources with no seen-set (Dataminr uses the watermark alone)."""
        ...

    def to_content_update_input(self, input_data: dict, created: dict) -> dict | None:
        """Adapt an already-built ``to_signal_input`` payload into an
        ``updateSignalContent`` input dict for a per-record content revision, or
        ``None`` if this source never revises records in place. Takes
        ``input_data`` as-is (not rebuilt from ``record``) so create and update
        always agree and no enrichment (geoparser/L4 promotion) runs twice.
        ``created`` is ``createSignal``'s result, providing the target id.
        No-op for every source except IDMC."""
        ...

    def parse(self, raw: bytes) -> Any:
        """Inverse of ``raw_bytes``: rebuild the record from a lake blob."""
        ...


def _first_location_name(created: dict) -> str | None:
    """Best resolved location name off a clear-api signal row (origin > general
    > destination) — the same priority the grouping layer uses."""
    for key in ("originLocation", "generalLocation", "destinationLocation"):
        loc = created.get(key)
        if loc and loc.get("name"):
            return loc["name"]
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Dataminr — real-time alert feed (polled + drained)
# ──────────────────────────────────────────────────────────────────────────────
class DataminrConnector:
    source = settings.dataminr_source_name  # consistent with the drain's dispatch key
    polled = True
    drained = True
    poll_interval_minutes = settings.dataminr_poll_interval_minutes

    def poll(self, since: datetime | None) -> list[Any]:
        return dataminr.fetch_signals(since=since)

    def external_id(self, record: Any) -> str:
        return record.alertId

    def published_at(self, record: Any) -> str:
        return record.alertTimestamp

    def raw_bytes(self, record: Any) -> bytes:
        return record.model_dump_json().encode("utf-8")

    def api_source_id(self) -> str:
        from clear_pipeline.providers.clear_api import get_source_id_by_name

        return get_source_id_by_name(settings.dataminr_source_name)

    def to_signal_input(self, record: Any, api_source_id: str) -> dict:
        return build_signal_input(record, api_source_id)

    def last_synced(self) -> datetime | None:
        return dataminr.get_last_synced()

    def set_watermark(self, ts: datetime) -> None:
        dataminr.set_last_synced(ts)

    def post_create(self, record: Any) -> None:
        return None  # Dataminr dedups via the watermark — no seen-set to mark

    def to_content_update_input(self, input_data: dict, created: dict) -> dict | None:
        return None  # Dataminr signals are never revised in place

    def parse(self, raw: bytes) -> dataminr.DataminrSignal:
        return dataminr.DataminrSignal.model_validate_json(raw)

    def project(self, record: dataminr.DataminrSignal, created: dict) -> SignalView:
        loc = record.estimatedEventLocation
        lat = lng = radius = None
        if loc and loc.coordinates and len(loc.coordinates) >= 2:
            lat, lng = loc.coordinates[0], loc.coordinates[1]
        if loc and loc.probabilityRadius is not None:
            radius = loc.probabilityRadius
        return SignalView(
            external_id=record.alertId,
            title=record.headline,
            timestamp=record.alertTimestamp,
            # The prose description (subHeadline text), not the headline.
            description=created.get("description") or created.get("title"),
            location_name=loc.name if loc else None,
            url=record.publicPost.href if record.publicPost else None,
            lat=lat,
            lng=lng,
            probability_radius_km=radius,
            raw_context=_dataminr_raw_context(record),
        )


def _dataminr_raw_context(signal: dataminr.DataminrSignal) -> str:
    """Extra free-text context for the v1 (Claude) classifier. Verbatim from the
    the classifier"""
    parts: list[str] = []
    if signal.publicPost and signal.publicPost.text:
        parts.append(f"Post text: {signal.publicPost.text}")
    if signal.publicPost and signal.publicPost.translatedText:
        parts.append(f"Translated: {signal.publicPost.translatedText}")
    if signal.eventCorroboration and signal.eventCorroboration.summary:
        for s in signal.eventCorroboration.summary:
            if s.content:
                parts.append(f"Corroboration: {s.content}")
    return "\n".join(parts) if parts else "(no additional context)"


# ──────────────────────────────────────────────────────────────────────────────
# ACLED — armed-conflict events (polled + drained)
# ──────────────────────────────────────────────────────────────────────────────
class ACLEDConnector:
    source = settings.acled_source_name
    polled = True
    drained = True
    poll_interval_minutes = settings.acled_poll_interval_minutes

    def poll(self, since: datetime | None) -> list[Any]:
        return acled.fetch_acled_events(since=since)

    def external_id(self, record: Any) -> str:
        return record["acled_id"]

    def published_at(self, record: Any) -> str:
        return record.get("event_date") or ""

    def raw_bytes(self, record: Any) -> bytes:
        return json.dumps(record).encode("utf-8")

    def api_source_id(self) -> str:
        from clear_pipeline.providers.clear_api import get_source_id_by_name

        return get_source_id_by_name(settings.acled_source_name)

    def to_signal_input(self, record: Any, api_source_id: str) -> dict:
        return acled.build_acled_signal_input(record, api_source_id)

    def last_synced(self) -> datetime | None:
        return acled.get_last_synced()

    def set_watermark(self, ts: datetime) -> None:
        acled.set_last_synced(ts)

    def post_create(self, record: Any) -> None:
        acled.mark_seen(record["acled_id"])  # only after createSignal confirmed

    def to_content_update_input(self, input_data: dict, created: dict) -> dict | None:
        return None  # ACLED signals are never revised in place

    def parse(self, raw: bytes) -> dict:
        return json.loads(raw)

    def project(self, record: dict, created: dict) -> SignalView:
        return SignalView(
            external_id=record["acled_id"],
            title=record["title"],
            timestamp=created.get("publishedAt") or record.get("event_date") or "",
            description=record.get("description"),
            location_name=record.get("location") or _first_location_name(created),
            url=record.get("source_url"),
            lat=record.get("lat"),
            lng=record.get("lng"),
        )


# ──────────────────────────────────────────────────────────────────────────────
# GDACS — global disaster alerts (polled + drained)
# ──────────────────────────────────────────────────────────────────────────────
class GDACSConnector:
    source = settings.gdacs_source_name
    polled = True
    drained = True
    poll_interval_minutes = settings.gdacs_poll_interval_minutes

    def poll(self, since: datetime | None) -> list[Any]:
        return gdacs.fetch_gdacs_events(since=since)

    def external_id(self, record: Any) -> str:
        return record["gdacs_id"]

    def published_at(self, record: Any) -> str:
        return record.get("from_date") or ""

    def raw_bytes(self, record: Any) -> bytes:
        return json.dumps(record).encode("utf-8")

    def api_source_id(self) -> str:
        from clear_pipeline.providers.clear_api import get_source_id_by_name

        return get_source_id_by_name(settings.gdacs_source_name)

    def to_signal_input(self, record: Any, api_source_id: str) -> dict:
        return gdacs.build_gdacs_signal_input(record, api_source_id)

    def last_synced(self) -> datetime | None:
        return gdacs.get_last_synced()

    def set_watermark(self, ts: datetime) -> None:
        gdacs.set_last_synced(ts)

    def post_create(self, record: Any) -> None:
        gdacs.mark_seen(record["gdacs_id"])  # only after createSignal confirmed

    def to_content_update_input(self, input_data: dict, created: dict) -> dict | None:
        return None  # GDACS signals are never revised in place

    def parse(self, raw: bytes) -> dict:
        return json.loads(raw)

    def project(self, record: dict, created: dict) -> SignalView:
        return SignalView(
            external_id=record["gdacs_id"],
            title=record["title"],
            timestamp=created.get("publishedAt") or record.get("from_date") or "",
            description=record.get("description"),
            location_name=record.get("country") or _first_location_name(created),
            url=record.get("url"),
            lat=record.get("lat"),
            lng=record.get("lng"),
        )


# ──────────────────────────────────────────────────────────────────────────────
# darfur24 — Sudanese news RSS (polled, NOT drained — Phase-0 tracer)
# ──────────────────────────────────────────────────────────────────────────────
class Darfur24Connector:
    source = settings.darfur24_source_name
    polled = True
    drained = True  # news signals flow through the same classify → group → alert stages
    poll_interval_minutes = settings.darfur24_poll_interval_minutes

    def __init__(self) -> None:
        self._location_id: str | None = None

    def _resolve_location_id(self) -> str | None:
        """L0 location id for ``darfur24_default_country`` (cached, best-effort —
        None on failure so a signal is never dropped over a location lookup)."""
        if self._location_id is not None:
            return self._location_id
        from clear_pipeline.providers.clear_api import get_locations_by_level

        try:
            for loc in get_locations_by_level(0):
                if loc["name"] == settings.darfur24_default_country:
                    self._location_id = loc["id"]
                    return self._location_id
        except Exception:  # noqa: BLE001 — location is best-effort (expo-385)
            return None
        return None

    def poll(self, since: datetime | None) -> list[Any]:
        return darfur24.fetch_darfur24_articles()  # RSS has no time window

    def external_id(self, record: Any) -> str:
        return record["darfur24_id"]

    def published_at(self, record: Any) -> str:
        return record.get("published_at") or ""

    def raw_bytes(self, record: Any) -> bytes:
        return json.dumps(record).encode("utf-8")

    def api_source_id(self) -> str:
        from clear_pipeline.providers.clear_api import get_source_id_by_name

        return get_source_id_by_name(settings.darfur24_source_name)

    def to_signal_input(self, record: Any, api_source_id: str) -> dict:
        return darfur24.build_darfur24_signal_input(
            record, api_source_id, self._resolve_location_id()
        )

    def last_synced(self) -> datetime | None:
        return darfur24.get_last_synced()

    def set_watermark(self, ts: datetime) -> None:
        darfur24.set_last_synced(ts)  # informational only — RSS has no time window

    def post_create(self, record: Any) -> None:
        # Mark seen ONLY after createSignal confirmed the signal (expo-383).
        darfur24.mark_seen(record["darfur24_id"])

    def to_content_update_input(self, input_data: dict, created: dict) -> dict | None:
        return None  # darfur24 signals are never revised in place

    def parse(self, raw: bytes) -> dict:
        return json.loads(raw)

    def project(self, record: dict, created: dict) -> SignalView:
        return SignalView(
            external_id=record["darfur24_id"],
            title=record["title"],
            timestamp=created.get("publishedAt") or record.get("published_at") or "",
            description=record.get("description"),
            location_name=_first_location_name(created),
            url=record.get("url"),
        )


# ──────────────────────────────────────────────────────────────────────────────
# IDMC IDU — internal displacement updates (polled, NOT drained)
# ──────────────────────────────────────────────────────────────────────────────
class IDMCConnector:
    """IDU has no server-side filter or pagination — one poll fetches the
    entire global dataset and filters to configured countries/displacement
    types client-side, deduplicating on (id, content hash) rather than id
    alone so a revised figure (same id, changed role/figure/dates) is
    detected and re-submitted instead of silently skipped. See
    ``providers/idmc.py`` for the fetch/dedup mechanics.

    ``drained = False``: grouping signals into events works differently for
    IDMC and needs new features that aren't built yet, so grouping is
    deliberately deferred to a follow-up PR."""

    source = settings.idmc_source_name
    polled = True
    drained = False
    poll_interval_minutes = settings.idmc_poll_interval_minutes

    def poll(self, since: datetime | None) -> list[Any]:
        return idmc.fetch_idu_records(since=since)

    def external_id(self, record: Any) -> str:
        return record["idu_id"]

    def published_at(self, record: Any) -> str:
        return record.get("created_at") or ""

    def raw_bytes(self, record: Any) -> bytes:
        return json.dumps(record).encode("utf-8")

    def api_source_id(self) -> str:
        from clear_pipeline.providers.clear_api import get_source_id_by_name

        return get_source_id_by_name(settings.idmc_source_name)

    def to_signal_input(self, record: Any, api_source_id: str) -> dict:
        return idmc.build_idmc_signal_input(record, api_source_id)

    def last_synced(self) -> datetime | None:
        return idmc.get_last_synced()

    def set_watermark(self, ts: datetime) -> None:
        idmc.set_last_synced(ts)

    def post_create(self, record: Any) -> None:
        idmc.mark_seen(record["idu_id"], record["content_hash"])  # only after createSignal confirmed

    def to_content_update_input(self, input_data: dict, created: dict) -> dict | None:
        return idmc.build_signal_content_update(input_data, created["id"])

    def parse(self, raw: bytes) -> dict:
        return json.loads(raw)

    def project(self, record: dict, created: dict) -> SignalView:
        return SignalView(
            external_id=record["idu_id"],
            title=record["title"],
            timestamp=created.get("publishedAt") or record.get("created_at") or "",
            description=record.get("description"),
            location_name=record.get("locations_name") or _first_location_name(created),
            url=record.get("source_url"),
            lat=record.get("lat"),
            lng=record.get("lng"),
        )


# ──────────────────────────────────────────────────────────────────────────────
# Manual — analyst-created signals (NOT polled, drained)
# ──────────────────────────────────────────────────────────────────────────────
class ManualConnector:
    """No external API: analysts create ``source=manual`` signals directly in
    clear-api. There is no ingest asset and no lake blob — the drain reads NEW
    manual signals and ``project`` builds the view from the signal row itself."""

    source = settings.manual_source_name
    polled = False
    drained = True
    poll_interval_minutes = settings.manual_poll_interval_minutes

    def project(self, record: Any, created: dict) -> SignalView:
        # record is None — everything comes from the clear-api signal row.
        return SignalView(
            external_id=created["id"],
            title=created.get("title") or "",
            timestamp=created.get("publishedAt") or "",
            description=created.get("description"),
            location_name=_first_location_name(created),
        )


#: The connector registry — the factory builds every source's ingest defs from
#: this, and the shared drain stages dispatch per-signal projection through
#: CONNECTORS_BY_SOURCE.
CONNECTORS: list[SignalSource] = [
    DataminrConnector(),
    ACLEDConnector(),
    GDACSConnector(),
    Darfur24Connector(),
    IDMCConnector(),
    ManualConnector(),
]

#: source name → connector, so the shared classify/group stage can rehydrate +
#: project a signal from ANY source (dispatch on ``created["source"]["name"]``).
CONNECTORS_BY_SOURCE: dict[str, SignalSource] = {c.source: c for c in CONNECTORS}

#: Sources whose NEW signals the classify/group stage should process. All current
#: sources are drained; an ingest-only source (drained=False) would be excluded.
DRAINED_SOURCES: frozenset[str] = frozenset(c.source for c in CONNECTORS if c.drained)
