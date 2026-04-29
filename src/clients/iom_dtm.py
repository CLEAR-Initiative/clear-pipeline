"""IOM DTM Services API v3 client.

Reference: https://dtmapi.iom.int/v3/ (OpenAPI spec in docs/IOM-DTMServiceAPI-V3.json).

We use the `/displacement/admin2` endpoint to fetch the latest displaced-
person counts per admin-2 district. Auth is an Azure APIM subscription
key sent via the `Ocp-Apim-Subscription-Key` header.

Response shape is not documented in the OpenAPI spec — only "200 Success".
From the public IOM DTM data we expect records like:

    {
      "operation": "...",
      "admin0Pcode": "SDN",
      "admin1Pcode": "SD01",
      "admin2Pcode": "SD0101",
      "admin2Name": "Jebel Aulia",
      "roundNumber": 7,
      "reportingDate": "2025-03-15",
      "numPresentIdpInd": 12345,   # primary: present IDPs (individuals)
      "numPresentIdpHH":  2500,    # households
      ...
    }

Real field names vary by operation and sometimes by round; we try a list
of common candidates and fall back gracefully when values are missing.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from src.config import settings

logger = logging.getLogger(__name__)

# Candidate field names for "displaced individuals" across DTM operations.
# First match wins. Order is deliberate — prefer individuals over households,
# present over returning.
DISPLACEMENT_FIELD_CANDIDATES: tuple[str, ...] = (
    "numPresentIdpInd",
    "numPresentIdp",
    "numPresentIdps",
    "numIdpInd",
    "numIdp",
    "totalIdpInd",
    "totalIdp",
    "idpInd",
    "idpIndividuals",
    "populationDisplaced",
)


def _headers() -> dict[str, str]:
    if not settings.iom_dtm_subscription_key:
        raise RuntimeError(
            "IOM_DTM_SUBSCRIPTION_KEY is not set — cannot call the IOM DTM API."
        )
    return {
        "Ocp-Apim-Subscription-Key": settings.iom_dtm_subscription_key,
        "Accept": "application/json",
    }


def _unwrap_records(payload: Any) -> list[dict]:
    """Different DTM endpoints return either a bare list or a wrapped object.
    Normalise to a list of record dicts."""
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        # Common wrapper keys
        for key in ("result", "results", "data", "records", "items"):
            val = payload.get(key)
            if isinstance(val, list):
                return [r for r in val if isinstance(r, dict)]
    logger.warning("[IOM DTM] Unexpected payload shape: %s", type(payload).__name__)
    return []


def _fetch_admin(
    admin_level: int,
    *,
    country_name: str | None = None,
    admin0_pcode: str | None = None,
    admin1_pcode: str | None = None,
    admin2_pcode: str | None = None,
    operation: str | None = None,
    from_round: int | None = None,
    to_round: int | None = None,
    timeout: float = 60.0,
) -> list[dict]:
    """Generic GET /v3/displacement/admin{level} call.

    `operation` scopes to a single DTM "Operation" (their term for a data-
    gathering project). For Sudan, the active project is
    "Armed Clashes in Sudan (Overview)" — older operations exist but are
    stale and dominate when the filter is omitted.
    """
    if admin_level not in (0, 1, 2):
        raise ValueError(f"admin_level must be 0, 1, or 2 — got {admin_level}")

    params: dict[str, Any] = {}
    if country_name:
        params["CountryName"] = country_name
    if admin0_pcode:
        params["Admin0Pcode"] = admin0_pcode
    if admin1_pcode and admin_level >= 1:
        params["Admin1Pcode"] = admin1_pcode
    if admin2_pcode and admin_level >= 2:
        params["Admin2Pcode"] = admin2_pcode
    if operation:
        params["Operation"] = operation
    if from_round is not None:
        params["FromRoundNumber"] = from_round
    if to_round is not None:
        params["ToRoundNumber"] = to_round

    url = f"{settings.iom_dtm_base_url.rstrip('/')}/displacement/admin{admin_level}"
    logger.info("[IOM DTM] GET %s params=%s", url, params)

    resp = httpx.get(url, params=params, headers=_headers(), timeout=timeout)
    resp.raise_for_status()

    records = _unwrap_records(resp.json())
    logger.info("[IOM DTM] Received %d admin%d records", len(records), admin_level)
    return records


def fetch_admin0_displacement(**kwargs) -> list[dict]:
    """GET /v3/displacement/admin0 — country-level displacement."""
    return _fetch_admin(0, **kwargs)


def fetch_admin1_displacement(**kwargs) -> list[dict]:
    """GET /v3/displacement/admin1 — state/province-level displacement."""
    return _fetch_admin(1, **kwargs)


def fetch_admin2_displacement(**kwargs) -> list[dict]:
    """GET /v3/displacement/admin2 — district-level displacement."""
    return _fetch_admin(2, **kwargs)


def extract_displacement_value(record: dict) -> int | None:
    """Pick the displaced-individuals value from a DTM record using the
    candidate-field list. Returns None if none of the candidates have a
    non-null numeric value."""
    for key in DISPLACEMENT_FIELD_CANDIDATES:
        val = record.get(key)
        if val is None:
            continue
        try:
            n = int(float(val))
            if n >= 0:
                return n
        except (TypeError, ValueError):
            continue
    return None


def latest_round_per_pcode(records: list[dict], admin_level: int) -> dict[str, dict]:
    """Group records by `admin{level}Pcode` and keep the record with the
    highest `roundNumber` (tie-break by `reportingDate`).

    Returns `{pcode: record}` — pcodes with no value are dropped.
    """
    key_camel = f"admin{admin_level}Pcode"
    key_pascal = f"Admin{admin_level}Pcode"

    latest: dict[str, dict] = {}
    for rec in records:
        pcode = rec.get(key_camel) or rec.get(key_pascal)
        if not pcode:
            continue
        rn = rec.get("roundNumber") or rec.get("RoundNumber") or 0
        rd = rec.get("reportingDate") or rec.get("ReportingDate") or ""
        current = latest.get(pcode)
        if not current:
            latest[pcode] = rec
            continue
        cur_rn = current.get("roundNumber") or current.get("RoundNumber") or 0
        cur_rd = current.get("reportingDate") or current.get("ReportingDate") or ""
        if (rn, rd) > (cur_rn, cur_rd):
            latest[pcode] = rec
    return latest


def latest_round_per_admin2(records: list[dict]) -> dict[str, dict]:
    """Backwards-compatible shim — level-2 specific wrapper."""
    return latest_round_per_pcode(records, admin_level=2)


def record_pcode(record: dict, admin_level: int) -> str | None:
    """Return the admin{N}Pcode from a DTM record, handling camel/pascal case."""
    return record.get(f"admin{admin_level}Pcode") or record.get(f"Admin{admin_level}Pcode")


def record_name(record: dict, admin_level: int) -> str | None:
    """Return the admin{N}Name from a DTM record, handling camel/pascal case."""
    return record.get(f"admin{admin_level}Name") or record.get(f"Admin{admin_level}Name")
