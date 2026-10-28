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

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from clear_pipeline.providers import acled, darfur24, dataminr, idmc, inso, newsletter_mailbox, sudan_witness
from clear_pipeline.providers.clear_api import (
    get_locations_by_level,
    get_source_id_by_name,
)
from clear_pipeline.providers.classify import SignalClassification
from clear_pipeline.providers.signal import build_signal_input
from clear_pipeline.signals.config import settings


class Quarantined(Exception):
    """A record the source can't trust or parse. Raised from ``parse`` or
    ``to_silver_inputs`` by a source with the quarantine hook (see
    ``InsoGXSource``): `_silver` moves the raw blob to ``quarantine/`` with
    this message as the reason and goes on with the batch."""


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
    """Calls `providers/idmc.py` directly, like `ACLEDGXSource`; IDMC's only
    ingestion path. IDU rows revise in place (same `idu_id`), so the sync hooks
    let bronze skip unchanged rows and `_push` send revisions and retractions."""

    # Optional, probed via `getattr` in factory.py: gives the job a poll sensor.
    # Only gx-only sources (IDMC, Sudan Witness) set it — the other sources'
    # watermarks and seen-sets are shared with the production poll in
    # `defs/signals`, which a second poller would race.
    poll_interval_minutes: int = settings.idmc_poll_interval_minutes

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
    # IDMC revises rows in place (so does Sudan Witness). Probed via `getattr`
    # in factory.py, like the group hooks below.

    def content_hash(self, record: Any) -> str:
        return record["content_hash"]

    def content_update_input(self, signal_input: dict, *, retracted: bool) -> dict:
        return idmc.build_signal_content_update(signal_input, retracted=retracted)

    # ── Optional group-supersession hooks ─────────────────────────────────
    # One IDU `event_id` can have several role-tagged rows (Recommended figure
    # vs Triangulation) competing for the same displacement; rules in
    # providers/idmc.py. Outside the Protocol: `_reconcile` probes via `getattr`.

    def group_member(self, external_id: str, raw_data: dict | None) -> dict | None:
        """Place a row in its group, or None. Reads only stored `rawData`, so a
        polled row and a gold row compare alike in `_reconcile`."""
        return idmc.group_member(external_id, raw_data)

    def resolve_group(self, members: list[dict]) -> dict[str, str]:
        """Verdict per member of ONE group: `"retract"` kills, anything else
        keeps. Total over `members`, and reversible when the group changes."""
        return idmc.resolve_group(members)



@dataclass(frozen=True)
class SudanWitnessGXSource:
    """Calls `providers/sudan_witness.py` directly; its only ingestion path,
    like IDMC. CIR republishes the whole layer, so every poll fetches all rows
    since the floor: the sync hooks skip unchanged rows at bronze and push revisions. No seen-set
    and no group hooks (one row = one piece of evidence, nothing supersedes it)."""

    poll_interval_minutes: int = settings.sudan_witness_poll_interval_minutes

    @property
    def source(self) -> str:
        return settings.sudan_witness_source_name

    def poll(self, since: datetime | None) -> list[Any]:
        return sudan_witness.fetch_sudan_witness_records(since=since)

    def external_id(self, record: Any) -> str:
        return record["suw_id"]

    def published_at(self, record: Any) -> str:
        return record["published_at"]

    def raw_bytes(self, record: Any) -> bytes:
        return json.dumps(record, ensure_ascii=False).encode("utf-8")

    def parse(self, raw: bytes) -> Any:
        return json.loads(raw)

    def api_source_id(self) -> str:
        return get_source_id_by_name(settings.sudan_witness_source_name)

    def last_synced(self) -> datetime | None:
        return sudan_witness.get_last_synced()

    def set_watermark(self, ts: datetime) -> None:
        sudan_witness.set_last_synced(ts)

    def to_silver_input(self, record: Any, source_id: str) -> dict:
        # Already a pure transform: no geoparser, so no `promote` to pass.
        return sudan_witness.build_sudan_witness_signal_input(record, source_id)

    def mark_seen(self, external_id: str) -> None:
        pass  # No seen-set: the bronze skip (gold hash + pushedState) dedups.

    # ── Optional sync hooks (probed via `getattr` in factory.py) ──────────
    def content_hash(self, record: Any) -> str:
        return record["content_hash"]

    def content_update_input(self, signal_input: dict, *, retracted: bool) -> dict:
        return sudan_witness.build_signal_content_update(signal_input, retracted=retracted)


def _normalise_country(name: str) -> str:
    """Casefolded country name without its long form: `Republic of Sudan`,
    `Sudan (Republic of the)` and `the Sudan` all give `sudan`, while
    `Republic of South Sudan` gives `south sudan`, not `sudan`."""
    name = re.sub(r"\([^)]*\)", "", name).strip().casefold()
    name = re.sub(r"^.*\brepublic of\s+(the\s+)?", "", name)
    return re.sub(r"^the\s+", "", name).strip()


@dataclass
class InsoGXSource:
    """INSO security newsletters, read from the shared newsletter mailbox
    (`providers/newsletter_mailbox.py`) and parsed by `providers/inso.py`;
    gx is its only ingestion path. One email lands in bronze verbatim (`.eml`
    + provenance sidecar) and fans out into one signal per incident at silver.

    Quarantine (the hook): an untrusted sender at bronze; a parse failure, an
    unknown country or an unusable extraction at extract. The IMAP cursor
    (`UIDVALIDITY:UID`) replaces the date watermark: `set_watermark` (a clean
    bronze batch) stages it, `commit_watermark` (`_push` reached) advances it,
    so an email whose silver/classify step fails is fetched again next poll.
    Not frozen: holds the polled cursor and the L0 locations, like
    `Darfur24GXSource`."""

    poll_interval_minutes: int = settings.inso_poll_interval_minutes
    raw_format: tuple[str, str] = ("eml", "message/rfc822")
    # A new prompt version re-extracts; any other rerun reads the stored answer.
    extraction_version: str = inso.EXTRACTION_PROMPT_VERSION
    _pending_cursor: tuple[int, int] | None = None
    _countries: list[dict] | None = field(default=None, repr=False)

    @property
    def source(self) -> str:
        return settings.inso_source_name

    def poll(self, since: datetime | None) -> list[Any]:
        """Every message after the stored cursor, as ``{uid, uidvalidity,
        folder, fetched_at, raw}``. ``since`` is unused: the cursor is IMAP's.
        Drops a cursor an earlier run staged but never committed: a bronze that
        fails here won't restage, and `_push` would commit that stale one past
        an email that never reached gold."""
        self._pending_cursor = None
        newsletter_mailbox.stage_cursor(self.source, None)
        validity, last_uid = newsletter_mailbox.get_cursor(self.source)
        folder = settings.inso_imap_folder
        batch = newsletter_mailbox.fetch_after(folder, validity, last_uid)
        fetched_at = datetime.now(UTC).isoformat()
        if batch.messages:
            self._pending_cursor = (batch.uidvalidity, max(uid for uid, _ in batch.messages))
        elif batch.uidvalidity != validity:
            self._pending_cursor = (batch.uidvalidity, 0)
        return [
            {"uid": uid, "uidvalidity": batch.uidvalidity, "folder": folder, "fetched_at": fetched_at, "raw": raw}
            for uid, raw in batch.messages
        ]

    def external_id(self, record: Any) -> str:
        """The email's content hash: bronze is keyed by it, not by the serial
        (a parse failure must still land somewhere)."""
        return hashlib.sha256(record["raw"]).hexdigest()

    def published_at(self, record: Any) -> str:
        """The Date header for the bronze partition; "" (an `unknown` day) when
        it can't be read — `parse` quarantines that email at silver."""
        try:
            return inso.published_at(record["raw"])
        except inso.InsoParseError:
            return ""

    def raw_bytes(self, record: Any) -> bytes:
        return record["raw"]

    def sidecar(self, record: Any) -> dict:
        return {
            "imapUid": record["uid"],
            "uidValidity": record["uidvalidity"],
            "folder": record["folder"],
            "fetchedAt": record["fetched_at"],
            "sha256": self.external_id(record),
            "authenticationResults": newsletter_mailbox.authentication_results(record["raw"]),
            "requireDkim": settings.inso_require_dkim,
        }

    def quarantine_reason(self, record: Any) -> str | None:
        allowed = {s.strip().lower() for s in settings.inso_allowed_senders.split(",") if s.strip()}
        return newsletter_mailbox.sender_rejection(record["raw"], allowed, require_auth=settings.inso_require_dkim)

    def parse(self, raw: bytes) -> Any:
        try:
            return inso.parse(raw)
        except inso.InsoParseError as e:
            raise Quarantined(f"parse: {e}") from e

    def api_source_id(self) -> str:
        return get_source_id_by_name(settings.inso_source_name)

    def last_synced(self) -> datetime | None:
        return None  # the cursor is IMAP's, read in `poll`

    def set_watermark(self, ts: datetime) -> None:
        """Stage the cursor `poll` reached; `commit_watermark` advances it."""
        newsletter_mailbox.stage_cursor(self.source, self._pending_cursor)

    def commit_watermark(self) -> None:
        """Called by `_push`: every email of the batch went through or was
        quarantined. Quarantined emails advance it too — they're replayed from
        quarantine, not by rewinding."""
        newsletter_mailbox.commit_cursor(self.source)

    def _country(self, report: inso.InsoReport) -> inso.Country:
        if self._countries is None:
            self._countries = get_locations_by_level(0)
        wanted = _normalise_country(report.country_raw)
        for loc in self._countries:
            if loc.get("name") and _normalise_country(loc["name"]) == wanted:
                if not loc.get("pCode"):
                    raise Quarantined(f"country {loc['name']} has no pCode to scope the geoparser")
                return inso.Country(location_id=loc["id"], name=loc["name"], iso2=loc["pCode"][:2].lower())
        raise Quarantined(f"country {report.country_raw!r} matches no country location")

    def to_silver_input(self, record: Any, source_id: str) -> dict:
        raise NotImplementedError("INSO fans out: the factory calls to_silver_inputs")

    def extract(self, record: Any) -> dict:
        """INFORMATION → incidents (LLM split + fields), checked verbatim. The
        country is resolved first, so an unknown one never costs a call."""
        self._country(record)
        try:
            incidents = inso.extract_incidents(record)
            uncovered = inso.check_extraction(record, incidents)
        except inso.InsoParseError as e:
            raise Quarantined(f"extraction: {e}") from e
        return {"incidents": [i.model_dump() for i in incidents], "uncovered": uncovered}

    def to_silver_inputs(self, record: Any, source_id: str, extraction: dict) -> list[dict]:
        """One signal input per stored incident: pure code, no LLM call."""
        incidents = [inso.Incident.model_validate(i) for i in extraction["incidents"]]
        try:
            return inso.build_inso_signal_inputs(record, incidents, source_id, self._country(record))
        except inso.InsoParseError as e:
            raise Quarantined(f"extraction: {e}") from e

    def classify(self, row: dict) -> SignalClassification:
        """The shared event-classification prompt, one incident per call."""
        return inso.classify_incident(row["signalInput"])

    def mark_seen(self, external_id: str) -> None:
        pass  # No seen-set: gold dedups on externalId.


# IDMC runs the full chain for QA and pre-push filtering, like ACLED/Darfur24.
# IDMC-native event grouping is NOT designed: the district+type heuristic runs
# as-is (as in the drain, IDMCConnector drained=True), and gold events are a
# QA-only sandbox that never reaches clear-api. Only IDMCGXSource and
# SudanWitnessGXSource define the sync hooks; other sources stay create-only.
GX_SOURCES: list[GXSource] = [
    DataminrGXSource(),
    ACLEDGXSource(),
    Darfur24GXSource(),
    IDMCGXSource(),
    SudanWitnessGXSource(),
    InsoGXSource(),
]
