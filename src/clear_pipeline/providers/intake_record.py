"""The **intake record** — the source-agnostic, per-item artifact the ingestion
router forks on (C-1445; design: docs/ingestion-router-design.md §3).

Today each source's medallion emits a clear-api ``CreateSignalInput`` the
whole way (``signal_input`` at silver, a ``<source>_signals`` gold table,
``createSignal`` at push). That fuses two things the router needs separate:

  * **absorb + extract** — normalize a raw item and run ``extract_event``
    on it. Every property that belongs to the *single item* is pulled here;
    this is type-agnostic and useful to *both* the event and context
    pipelines. (Grouping items together is a later, branch-specific stage.)
  * **Signal materialization** — the clear-api framing (``sourceId``, the
    ``<source>:``-prefixed ``externalId``, ``rawS3Key``) and everything the
    event branch does with it.

``IntakeRecord`` names the first half: the canonical form every source is
reduced to **at intake**, before anything forks. It is the router's input
surface and the single parse/classify artifact both branches reuse.

**V0 scope (slice 1 of C-1445 — no behavior change).** This record *retains*
the source ``signal_input`` verbatim and ``to_signal_input()`` returns it
unchanged, so the event branch is byte-for-byte identical to today. The typed
fields below are a **read-only projection** of that dict — the bounded routing
surface the Jev/LLM router (slice 2) reads without touching the full document.
Inverting ownership (building the clear-api input *from* the intake record
instead of retaining it) is deliberately left to a later slice; the per-source
``to_silver_input`` code is untouched here.

``event_type`` / ``relevance`` / ``glide_code`` are populated by the
``classify`` stage, which still runs after silver in V0; they are ``None`` on a
freshly absorbed record until classification folds onto the record (slice 3).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class IntakeRecord:
    """A normalized, extracted item, independent of whether it becomes a
    clear-api Signal (event branch) or context (knowledgebase branch).

    Holds only properties of the *single item* — grouping across items is a
    downstream, branch-specific stage. Construct via :meth:`from_signal_input`;
    read the typed fields for routing; call :meth:`to_signal_input` to get the
    clear-api input the event branch materializes (V0: the retained source
    dict, unchanged)."""

    # ── identity / provenance ────────────────────────────────────────────
    external_id: str          # the row key — unprefixed `source.external_id(record)`
    source: str               # DataSource name (dataminr, acled, …)
    published_at: str | None   # ISO-8601 issue time — feeds ALERTING, not routing

    # ── normalized surface (title · lead · metadata) ─────────────────────
    title: str | None = None
    lead: str | None = None            # == clear-api `description`
    url: str | None = None
    raw_data: Any = None               # the verbatim source payload (metadata)

    # ── location anchors (any one lets a branch place the item) ──────────
    lat: float | None = None
    lng: float | None = None
    geoparsed_data: dict | None = None
    location_id: str | None = None
    country_ids: list[str] = field(default_factory=list)

    # ── extraction (extract_event output; already run inside silver) ─────
    severity: int | None = None        # 1-5, or None when unknown
    start_date: str | None = None
    start_date_accuracy: str | None = None
    end_date: str | None = None
    end_date_accuracy: str | None = None
    magnitude: float | None = None
    magnitude_unit: str | None = None
    actors: list[str] | None = None
    casualties: int | None = None

    # ── classification (filled by the classify stage; None until then) ───
    event_type: str | None = None      # clear-api `eventType` / type_level_2
    relevance: float | None = None     # 0-1 relevance gate
    glide_code: str | None = None      # PRIMARY disaster type

    # ── retained clear-api input (V0: the event branch's materialization) ─
    # Kept verbatim so `to_signal_input()` is byte-identical to pre-router
    # silver. Not part of the routing surface — the router reads the typed
    # fields above, never this.
    signal_input: dict = field(default_factory=dict)

    # Keys the typed fields above are projected from — everything else in a
    # `CreateSignalInput` (sourceId, the prefixed externalId, rawS3Key, …) is
    # clear-api framing the event branch owns and stays only in `signal_input`.
    @classmethod
    def from_signal_input(
        cls,
        signal_input: dict,
        *,
        external_id: str,
        source: str,
        published_at: str | None = None,
    ) -> "IntakeRecord":
        """Project a source's ``CreateSignalInput``-shaped dict onto the typed
        intake surface, retaining the dict verbatim for ``to_signal_input``.

        ``external_id`` is the row key (``source.external_id(record)``), which
        is intentionally *not* the ``<source>:``-prefixed ``externalId`` inside
        the dict — the prefixed id is clear-api framing and stays in
        ``signal_input``."""
        si = signal_input
        return cls(
            external_id=external_id,
            source=source,
            published_at=published_at if published_at is not None else si.get("publishedAt"),
            title=si.get("title"),
            lead=si.get("description"),
            url=si.get("url"),
            raw_data=si.get("rawData"),
            lat=si.get("lat"),
            lng=si.get("lng"),
            geoparsed_data=si.get("geoparsedData"),
            location_id=si.get("locationId"),
            country_ids=si.get("countryIds") or [],
            severity=si.get("severity"),
            start_date=si.get("startDate"),
            start_date_accuracy=si.get("startDateAccuracy"),
            end_date=si.get("endDate"),
            end_date_accuracy=si.get("endDateAccuracy"),
            magnitude=si.get("magnitude"),
            magnitude_unit=si.get("magnitudeUnit"),
            actors=si.get("actors"),
            casualties=si.get("casualties"),
            signal_input=si,
        )

    def to_signal_input(self) -> dict:
        """The clear-api ``CreateSignalInput`` the event branch materializes.

        V0: the retained source dict, unchanged — so the event path is
        byte-for-byte identical to pre-router silver."""
        return self.signal_input

    @property
    def event_span(self) -> tuple[str | None, str | None]:
        """The (start, end) the item covers. Its *shape* — bounded vs
        open-ended — is the router's occurrence/analysis cue (§4.2); its
        recency is not a routing input."""
        return (self.start_date, self.end_date)

    def routing_surface(self) -> dict:
        """The bounded surface the router classifies on (§3/§4) — title, lead,
        metadata and the extraction — never the full document. Deliberately
        excludes the retained ``signal_input``."""
        return {
            "external_id": self.external_id,
            "source": self.source,
            "title": self.title,
            "lead": self.lead,
            "published_at": self.published_at,
            "event_start": self.start_date,
            "event_end": self.end_date,
            "event_type": self.event_type,
            "severity": self.severity,
            "relevance": self.relevance,
            "glide_code": self.glide_code,
        }


def to_signal_input(record: IntakeRecord) -> dict:
    """Module-level adapter seam the event branch calls to turn a routed
    intake record into the clear-api input it materializes. Mirrors
    :meth:`IntakeRecord.to_signal_input` so call sites can import a function
    rather than reach for a method."""
    return record.to_signal_input()
