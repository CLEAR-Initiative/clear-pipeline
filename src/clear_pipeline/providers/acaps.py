"""ACAPS Seasonal Events Calendar client + blob builder.

The calendar is a paginated list of *recurring* events per country (rainy/lean
seasons, harvests, disease-outbreak seasons, elections) — the seasonal backdrop
against which new signals are read. It is background context, not observed
incidents.

Pure fetch + transform (no Dagster, no clear-api). The Dagster asset in
``defs/location_metadata/`` fetches once, filters to each pipeline country, and
upserts one ``locationMetadata`` (type ``acaps_seasonal_calendar``) row per
location.

Auth: the ACAPS API key CLEAR already uses for INFORM Severity / Protection Risks
Monitor, sent as ``Authorization: Token <key>`` (ACAPS API v1 / DRF convention).
Configure with ``ACAPS_API_KEY``.

Keying (per the spec):
  - ``country_wide: true``  → the event scopes to the whole country → admin0 blob.
  - ``country_wide: false`` → the event scopes to the admin1s in ``adm1_eng_name``
    → one entry copied into each named admin1's blob. ``adm1`` holds GADM codes
    (comma-joined) which don't map to clear-api pcodes without a GADM→COD table,
    so we match admin1s by name; entries with no ``adm1_eng_name`` are skipped.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from clear_pipeline.providers import http_retry

logger = logging.getLogger(__name__)

SEASONAL_CALENDAR_URL = (
    "https://api.acaps.org/api/v1/seasonal-events-calendar/seasonal-calendar/"
)

_MONTH_ORDER = {
    m: i for i, m in enumerate(
        [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ],
        start=1,
    )
}


def _headers() -> dict[str, str]:
    key = os.environ.get("ACAPS_API_KEY", "")
    if not key:
        raise RuntimeError(
            "ACAPS_API_KEY is not set — cannot call the ACAPS API."
        )
    return {"Authorization": f"Token {key}", "Accept": "application/json"}


def fetch_seasonal_calendar(*, timeout: float = 60.0, max_pages: int = 200) -> list[dict]:
    """Fetch every seasonal-calendar entry, following the ``next`` links.

    The endpoint isn't reliably filterable by country, so we page the whole
    calendar once (a few hundred entries) and let the caller filter by ISO3.
    ``max_pages`` is a runaway guard, not an expected limit.
    """
    entries: list[dict] = []
    url: str | None = SEASONAL_CALENDAR_URL
    pages = 0
    while url and pages < max_pages:
        resp = http_retry.get(url, headers=_headers(), timeout=timeout)
        body = resp.json()
        entries.extend(r for r in (body.get("results") or []) if isinstance(r, dict))
        url = body.get("next")
        pages += 1
    logger.info("[ACAPS] fetched %d seasonal-calendar entries over %d pages", len(entries), pages)
    return entries


def _sorted_months(months: Any) -> list[str]:
    """Sort month names to calendar order (source order isn't guaranteed);
    unknown values sort last, preserving their input order."""
    if not isinstance(months, list):
        return []
    return sorted(months, key=lambda m: _MONTH_ORDER.get(m, 99))


def _map_entry(entry: dict) -> dict:
    """One calendar entry → the per-event record stored in a location's blob."""
    return {
        "id": entry.get("id"),
        "event": entry.get("event"),
        "event_type": entry.get("event_type"),
        "label": entry.get("label"),
        "months": _sorted_months(entry.get("months")),
        "comment": entry.get("comment"),
        "source": entry.get("source"),
        "source_date": entry.get("source_date"),
        "source_link": entry.get("source_link"),
        "country_wide": entry.get("country_wide"),
        "adm1": entry.get("adm1"),
        "adm1_eng_name": entry.get("adm1_eng_name"),
    }


def build_blobs(entries: list[dict], iso3: str) -> dict[str, Any]:
    """Filter ``entries`` to ``iso3`` and split into country-wide vs per-admin1.

    Returns ``{"country": [event, ...], "admin1": {admin1_name: [event, ...]},
    "skipped_no_adm1_name": int}``. ``country`` is empty when the country has no
    country-wide entries; ``admin1`` maps each named admin1 to its events.
    """
    country: list[dict] = []
    admin1: dict[str, list[dict]] = {}
    skipped = 0

    for entry in entries:
        if iso3 not in (entry.get("iso") or []):
            continue
        record = _map_entry(entry)
        if entry.get("country_wide"):
            country.append(record)
            continue
        names = entry.get("adm1_eng_name")
        if not isinstance(names, list) or not names:
            # Subnational entry with no resolvable admin1 name (GADM-only) — we
            # can't place it without a GADM→COD table, so skip (logged upstream).
            skipped += 1
            continue
        for name in names:
            if name:
                admin1.setdefault(name, []).append(record)

    return {"country": country, "admin1": admin1, "skipped_no_adm1_name": skipped}
