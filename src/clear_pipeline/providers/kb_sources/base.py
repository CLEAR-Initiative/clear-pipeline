"""The shared knowledge-base source contract.

A `KbConnector` fetches a source's PDF reports, stages each PDF into S3, and
returns a `KbRawDocument` per PDF. Those fields are exactly what the existing
`reliefweb_weekly_pdf_text` asset already emits per report, so the whole
downstream chain (text → chunks → enrich → upsert → datapoints → figures) can
consume documents from ANY connector without change.

Pull sources (ReliefWeb, future APIs) implement `fetch(...)`. A push source
(manual upload) stages a single `KbRawDocument` directly and does not poll, so
`fetch` is optional — presence is checked with `has_fetch()`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class KbRawDocument:
    """One source PDF staged for the shared KB pipeline.

    ``source`` is the connector key and the S3 namespace the PDF lives under, so
    documents never collide across connectors. ``report_id`` must be stable for
    the same logical report (content-addressed — ``<source>:<sha>`` — for uploads
    and ad-hoc sources; the source's native id where it has a durable one) so the
    (sourceId, externalId)-style dedup downstream is idempotent across re-fetches.
    """

    source: str
    report_id: str
    pdf_s3_key: str
    title: str
    published_at: str  # ISO-8601
    source_url: str | None = None
    # Publisher → resolved to the report-level data_sources row downstream
    # (resolveDataSource); carries the trusted-source grading for uploads.
    publisher_name: str | None = None
    publisher_homepage: str | None = None
    # Partition / scoping key for the downstream (country subtree). None = unscoped.
    country_iso3: str | None = None
    filename: str | None = None
    # Escape hatch for source-specific extras a connector wants to carry through
    # (never read by the source-agnostic downstream).
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("source", "report_id", "pdf_s3_key", "title", "published_at"):
            if not (getattr(self, name) or "").strip():
                raise ValueError(f"KbRawDocument.{name} must be non-empty")


@runtime_checkable
class KbConnector(Protocol):
    """A knowledge-base source.

    ``key`` is the stable source id recorded on every `KbRawDocument.source` and
    used as its S3 namespace. Pull connectors implement ``fetch``; push
    connectors (manual upload) omit it and stage documents directly.
    """

    key: str

    def fetch(
        self, *, country_iso3: str, window_start: str, window_end: str
    ) -> list[KbRawDocument]:
        """Fetch the source's reports for the window, stage each PDF into S3 under
        this connector's namespace, and return one `KbRawDocument` per staged PDF.
        May return an empty list (nothing new)."""
        ...


def has_fetch(connector: KbConnector) -> bool:
    """True when a connector polls (implements a real ``fetch``), vs a push-only
    source like manual upload."""
    return callable(getattr(connector, "fetch", None))


# ── Registry ─────────────────────────────────────────────────────────────────
# Connectors register themselves at import so the drain can enumerate every
# source without hard-coding them — the KB analogue of the signals providers.
_REGISTRY: dict[str, KbConnector] = {}


def register_connector(connector: KbConnector) -> None:
    key = connector.key
    if not key or not key.strip():
        raise ValueError("connector.key must be non-empty")
    if key in _REGISTRY and _REGISTRY[key] is not connector:
        raise ValueError(f"duplicate KB connector key: {key!r}")
    _REGISTRY[key] = connector


def get_connector(key: str) -> KbConnector | None:
    return _REGISTRY.get(key)


def all_connectors() -> list[KbConnector]:
    """Every registered connector, in a stable (key-sorted) order."""
    return [_REGISTRY[k] for k in sorted(_REGISTRY)]
