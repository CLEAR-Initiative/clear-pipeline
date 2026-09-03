"""HAPI (HDX Humanitarian API) v2 client + per-endpoint blob builders.

HAPI is OCHA's standardized API layer over HDX data grids — quantitative
humanitarian context down to admin 2, p-coded and joinable to the locations
tree. The endpoint catalogue and target blob shapes are captured in
``HAPI_ENDPOINTS`` below.

This module is pure fetch + transform (no Dagster, no clear-api). The Dagster
assets in ``defs/location_metadata/`` fetch rows here, resolve admin pcodes to
clear-api location ids, and upsert one ``locationMetadata`` row per location.

Auth: an HDX HAPI *app identifier* (a base64 ``email:app`` token) sent via the
``X-HDX-HAPI-APP-IDENTIFIER`` header. Configure with ``HAPI_APP_IDENTIFIER``;
generate one at https://hapi.humdata.org/docs#/ (Encode Identifier).

Design of the generic blob:
    locationMetadata is one row per (locationId, type), but HAPI returns many
    rows per location (e.g. humanitarian-needs has one row per sector). So the
    generic builder groups a page by the keying admin pcode and stores every
    raw row for that location under ``records``, plus the derived admin identity
    and the widest reference window. Nothing is aggregated away — downstream
    consumers read the records they need. Operational presence (OCHA 3W) is the
    one exception: it keeps its bespoke sector/org roll-up (ported from
    clear-api's ``scripts/ingest-sudan-3w.ts``).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

from clear_pipeline.providers import http_retry

logger = logging.getLogger(__name__)

HAPI_BASE = "https://hapi.humdata.org/api/v2"

# Location-identity columns lifted to the blob top-level and dropped from each
# stored record. Time-series endpoints (``series_key`` set) drop only these and
# KEEP the reference-period columns per record so each collapsed observation
# stays self-describing ("this price, as of this month"). The keep-all path also
# drops the period columns, since it stamps one window on the blob.
_LOCATION_COLUMNS = frozenset({
    "location_code", "location_name",
    "admin1_code", "admin1_name",
    "admin2_code", "admin2_name",
})
_ADMIN_COLUMNS = _LOCATION_COLUMNS | {"reference_period_start", "reference_period_end"}

OPERATIONAL_PRESENCE_PATH = "coordination-context/operational-presence"
OCHA_3W_TYPE = "ocha_3w"


def _app_identifier() -> str:
    # Required, like the DTM/ACAPS keys. `.env.example` ships an empty value, so
    # treat empty/whitespace as absent and fail clearly rather than sending an
    # empty auth header (which HAPI rejects with an opaque error).
    identifier = os.environ.get("HAPI_APP_IDENTIFIER", "").strip()
    if not identifier:
        raise RuntimeError(
            "HAPI_APP_IDENTIFIER is not set — cannot call HAPI. Generate one at "
            "https://hapi.humdata.org/docs (Encode Identifier) and set it in .env."
        )
    return identifier


def _headers() -> dict[str, str]:
    return {
        "X-HDX-HAPI-APP-IDENTIFIER": _app_identifier(),
        "Accept": "application/json",
    }


def fetch(
    path: str,
    *,
    location_code: str,
    location_filter: str = "location_code",
    extra_params: dict[str, Any] | None = None,
    page_limit: int = 1000,
    timeout: float = 60.0,
) -> list[dict]:
    """GET every row for ``path`` scoped to one ISO3, paging through HAPI's
    ``limit``/``offset`` until a short page ends the run.

    ``location_filter`` is the query param that scopes by country — usually
    ``location_code``, but ``origin_location_code`` for the refugees/returnees
    endpoints (which silently IGNORE ``location_code`` and would otherwise return
    the entire global dataset). HAPI caps ``limit`` at 10000; we page at 1000 to
    keep responses small and resumable. Returns the flat list of raw row dicts.
    """
    rows: list[dict] = []
    offset = 0
    url = f"{HAPI_BASE}/{path.strip('/')}"
    while True:
        params: dict[str, Any] = {
            location_filter: location_code,
            "output_format": "json",
            "limit": page_limit,
            "offset": offset,
        }
        if extra_params:
            params.update(extra_params)
        resp = http_retry.get(url, params=params, headers=_headers(), timeout=timeout)
        page = resp.json().get("data") or []
        rows.extend(r for r in page if isinstance(r, dict))
        logger.info(
            "[HAPI] GET %s location=%s offset=%d → %d rows (total %d)",
            path, location_code, offset, len(page), len(rows),
        )
        if len(page) < page_limit:
            break
        offset += page_limit
    return rows


@dataclass(frozen=True)
class EndpointSpec:
    """One HAPI endpoint we ingest generically.

    type_: locationMetadata.type written (e.g. "hapi_funding").
    path: HAPI endpoint path under the v2 base.
    source: the `source` tag stamped on each blob (e.g. "unhcr_hapi_v2").
    key_level: admin level whose pcode keys the location join — 0 (location_code),
        1 (admin1_code), or 2 (admin2_code). Rows missing that pcode are skipped.
    series_key: dimension fields that identify one parallel *series* at a
        location (e.g. a commodity at a market). When set, each location's rows
        are collapsed to the LATEST observation per series (max
        reference_period_end), so the blob holds a current snapshot instead of a
        full time series — keeping blobs small. None ⇒ keep every row.
    location_filter: the query param that scopes the fetch to one ISO3. Most
        endpoints use ``location_code``; the cross-border affected-people
        endpoints (refugees/returnees) ignore it and must be scoped by
        ``origin_location_code`` instead (else the whole global dataset returns).
    pcode_field / name_field: override the row field the blob is keyed/named on.
        Default None derives them from ``key_level`` (location_code/admin1_code/
        admin2_code). Refugees/returnees key on ``origin_location_code`` — the
        country of origin — not a generic admin pcode.
    """
    type_: str
    path: str
    source: str
    key_level: int
    series_key: tuple[str, ...] | None = None
    location_filter: str = "location_code"
    pcode_field: str | None = None
    name_field: str | None = None


# The 7 generic endpoints. Operational presence (OCHA 3W) is handled separately
# by build_operational_presence_blobs() because of its bespoke roll-up shape.
# ``series_key`` names the dimensions of one series; the endpoint's measure(s)
# and period are intentionally NOT in it, so the latest observation per series is
# what survives the collapse.
HAPI_ENDPOINTS: tuple[EndpointSpec, ...] = (
    # Refugees/returnees are keyed on country of ORIGIN and use
    # origin_/asylum_location_code, not the generic location_code — so scope,
    # key, and name all come from the origin fields; asylum stays in the record.
    EndpointSpec("hapi_refugees", "affected-people/refugees-persons-of-concern",
                 "unhcr_hapi_v2", 0,
                 series_key=("asylum_location_code", "population_group", "gender",
                             "age_range", "min_age", "max_age"),
                 location_filter="origin_location_code",
                 pcode_field="origin_location_code", name_field="origin_location_name"),
    EndpointSpec("hapi_returnees", "affected-people/returnees",
                 "unhcr_hapi_v2", 0,
                 series_key=("asylum_location_code", "population_group", "gender",
                             "age_range", "min_age", "max_age"),
                 location_filter="origin_location_code",
                 pcode_field="origin_location_code", name_field="origin_location_name"),
    EndpointSpec("hapi_humanitarian_needs", "affected-people/humanitarian-needs",
                 "ocha_hpc_hapi_v2", 2,
                 series_key=("sector_code", "category", "population_status",
                             "gender", "age_range", "disabled_marker")),
    EndpointSpec("hapi_funding", "coordination-context/funding",
                 "ocha_fts_hapi_v2", 0,
                 series_key=("appeal_code",)),
    EndpointSpec("hapi_food_security", "food-security-nutrition-poverty/food-security",
                 "ipc_ch_hapi_v2", 2,
                 series_key=("ipc_type", "ipc_phase")),
    EndpointSpec("hapi_food_prices", "food-security-nutrition-poverty/food-prices-market-monitor",
                 "wfp_vam_hapi_v2", 2,
                 series_key=("market_code", "commodity_code", "price_type",
                             "unit", "currency_code")),
    EndpointSpec("hapi_poverty_rate", "food-security-nutrition-poverty/poverty-rate",
                 "ophi_mpi_hapi_v2", 1,
                 series_key=()),  # one MPI series per admin1 → keep the latest survey
)

_LEVEL_PCODE_FIELD = {0: "location_code", 1: "admin1_code", 2: "admin2_code"}
_LEVEL_NAME_FIELD = {0: "location_name", 1: "admin1_name", 2: "admin2_name"}


def _latest_per_series(rows: list[dict], series_key: tuple[str, ...]) -> list[dict]:
    """Collapse a time series to the latest observation per series.

    Group ``rows`` by the ``series_key`` field values and keep, per group, the
    row with the greatest ``reference_period_end`` (tie-break ``_start``). An
    empty ``series_key`` treats all rows as one series → keeps the single latest.
    """
    latest: dict[tuple, dict] = {}
    for row in rows:
        key = tuple(row.get(f) for f in series_key)
        end = row.get("reference_period_end") or ""
        start = row.get("reference_period_start") or ""
        cur = latest.get(key)
        if cur is None or (end, start) > (cur["_end"], cur["_start"]):
            latest[key] = {"_row": row, "_end": end, "_start": start}
    return [v["_row"] for v in latest.values()]


def _finest_admin_level(row: dict) -> int | None:
    """The finest admin level this row is scoped to. HAPI rows carry the full
    parent hierarchy (an admin2 row also has admin1_code + location_code), so the
    finest populated pcode is what identifies the row's actual granularity."""
    if row.get("admin2_code"):
        return 2
    if row.get("admin1_code"):
        return 1
    if row.get("location_code"):
        return 0
    return None


def build_blobs(rows: list[dict], spec: EndpointSpec, level: int | None = None) -> dict[str, dict]:
    """Group ``rows`` by the keying admin pcode and build one blob per location,
    at admin ``level`` (default ``spec.key_level``).

    A HAPI response for an admin2 endpoint mixes admin0/admin1/admin2 rows, and
    each carries its parent pcodes — so grouping strictly by a level's pcode field
    would fold admin2 rows into the national blob. We therefore only take rows
    whose FINEST level equals ``level``, letting the caller ingest each level to
    its own locations (so the national PIN figure isn't discarded). Custom-keyed
    endpoints (refugees/returnees on ``origin_location_code``) are single-level
    and skip the finest filter.

    Returns ``{key_pcode: blob}``. Time-series endpoints (``spec.series_key`` set)
    collapse each location to the latest observation per series.
    """
    effective_level = spec.key_level if level is None else level
    pcode_field = spec.pcode_field or _LEVEL_PCODE_FIELD[effective_level]
    name_field = spec.name_field or _LEVEL_NAME_FIELD[effective_level]
    finest_filter = spec.pcode_field is None

    grouped: dict[str, list[dict]] = {}
    dropped = 0
    for row in rows:
        if finest_filter and _finest_admin_level(row) != effective_level:
            continue
        pcode = row.get(pcode_field)
        if not pcode:
            dropped += 1
            continue
        grouped.setdefault(pcode, []).append(row)
    if dropped:
        logger.info(
            "[HAPI] %s admin%d: dropped %d rows with no %s",
            spec.type_, effective_level, dropped, pcode_field,
        )

    blobs: dict[str, dict] = {}
    for pcode, group in grouped.items():
        first = group[0]
        # Time-series endpoints: keep the latest observation per series and
        # retain each record's own period. Otherwise keep all rows and stamp one
        # window on the blob (period columns dropped from the records).
        #
        # Why latest-only rather than the full history in the blob: the HAPI job
        # runs DAILY, and clear-api's location_metadata is bitemporal — every run
        # that brings a new latest value writes a new version, while the
        # unchanged-guard skips a run whose data is identical. So the HAPI time
        # series is preserved across runs, in the row history, day by day.
        # Duplicating that history inside each blob would just bloat the payload
        # (the food-prices series is what blew the upsert timeout) and re-store
        # the same old months every single day. (This daily-cadence argument is
        # HAPI-specific — the monthly IOM DTM ingest instead carries per-round
        # history inside the blob via `recent_rounds`; see providers/iom_dtm.py.)
        # Always drop the keying pcode/name from the records — they're lifted to
        # the blob top-level (a no-op for standard endpoints whose key is already
        # in _LOCATION_COLUMNS; drops origin_location_code/name for refugees).
        if spec.series_key is not None:
            source_rows = _latest_per_series(group, spec.series_key)
            strip = _LOCATION_COLUMNS | {pcode_field, name_field}
        else:
            source_rows = group
            strip = _ADMIN_COLUMNS | {pcode_field, name_field}
        starts = [r.get("reference_period_start") for r in source_rows if r.get("reference_period_start")]
        ends = [r.get("reference_period_end") for r in source_rows if r.get("reference_period_end")]
        records = [
            {k: v for k, v in r.items() if k not in strip}
            for r in source_rows
        ]
        blobs[pcode] = {
            "source": spec.source,
            "endpoint": spec.path,
            "admin_name": first.get(name_field),
            "admin_level": effective_level,
            "admin_pcode": pcode,
            "admin1_pcode": first.get("admin1_code"),
            "reference_period_start": min(starts) if starts else None,
            "reference_period_end": max(ends) if ends else None,
            "record_count": len(records),
            "records": records,
        }
    return blobs


# ─── OCHA 3W (operational presence) — bespoke roll-up ──────────────────────
# Ported from clear-api's scripts/ingest-sudan-3w.ts: group operational-presence
# rows by admin2, then by sector, counting unique organisations (by acronym) and
# their org-type mix. Keeps the same `ocha_3w` blob shape already in the DB.

def build_operational_presence_blobs(rows: list[dict]) -> dict[str, dict]:
    """Group operational-presence rows by admin2 → one ``ocha_3w`` blob each,
    with per-sector unique-organisation roll-ups. Returns ``{admin2_code: blob}``."""
    by_admin2: dict[str, list[dict]] = {}
    for row in rows:
        code = row.get("admin2_code")
        if not code:
            continue
        by_admin2.setdefault(code, []).append(row)

    result: dict[str, dict] = {}
    for admin2_code, locality_rows in by_admin2.items():
        first = locality_rows[0]
        as_of = (first.get("reference_period_end") or "").split("T")[0]
        period_start = (first.get("reference_period_start") or "").split("T")[0]

        by_sector: dict[str, list[dict]] = {}
        for row in locality_rows:
            by_sector.setdefault(row.get("sector_code") or "", []).append(row)

        sectors: list[dict] = []
        for sector_code, sector_rows in by_sector.items():
            unique_orgs: dict[str, dict] = {}
            for row in sector_rows:
                acronym = row.get("org_acronym") or row.get("org_name") or ""
                unique_orgs.setdefault(acronym, row)

            by_type: dict[str, int] = {}
            for row in unique_orgs.values():
                t = row.get("org_type_description") or "Unknown"
                by_type[t] = by_type.get(t, 0) + 1

            sectors.append({
                "code": sector_code,
                "name": sector_rows[0].get("sector_name"),
                "org_count": len(unique_orgs),
                "by_type": by_type,
                "organizations": [
                    {
                        "acronym": r.get("org_acronym"),
                        "name": r.get("org_name"),
                        "type": r.get("org_type_description") or "Unknown",
                    }
                    for r in unique_orgs.values()
                ],
            })

        total_orgs = {r.get("org_acronym") for r in locality_rows}
        result[admin2_code] = {
            "source": "ocha_3w_hapi_v2",
            "endpoint": OPERATIONAL_PRESENCE_PATH,
            "as_of": as_of,
            "period_start": period_start,
            "admin_level": 2,
            "admin_pcode": admin2_code,
            "admin_name": first.get("admin2_name"),
            "active_sector_count": len(sectors),
            "total_org_count": len(total_orgs),
            "sectors": sectors,
        }
    return result
