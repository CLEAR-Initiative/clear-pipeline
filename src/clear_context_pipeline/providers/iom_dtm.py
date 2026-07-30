"""IOM DTM Services API v3 client.

Reference: https://dtmapi.iom.int/v3/.

We use the ``/displacement/admin{0,1,2}`` endpoints to fetch the latest
displaced-person counts per admin district. Auth is an Azure APIM subscription
key sent via the ``Ocp-Apim-Subscription-Key`` header.

Ported verbatim (logic-wise) from clear-pipeline's ``src/clients/iom_dtm.py``
so the two codebases stay recognisable; the only change is config — this reads
``IOM_DTM_*`` from the environment (clear-context-pipeline convention) instead
of ``pydantic-settings``.

Response shape is not documented in the OpenAPI spec — only "200 Success".
From the public IOM DTM data we expect records like::

    {
      "operation": "...",
      "admin0Pcode": "SDN",
      "admin1Pcode": "SD01",
      "admin2Pcode": "SD0101",
      "admin2Name": "Jebel Aulia",
      "roundNumber": 7,
      "reportingDate": "2025-03-15",
      "numPresentIdpInd": 12345,   # primary: present IDPs (individuals)
      ...
    }

Real field names vary by operation and sometimes by round; we try a list of
common candidates and fall back gracefully when values are missing.
"""

from __future__ import annotations

import logging
import os
from typing import Any, TypedDict

from clear_context_pipeline.providers import http_retry

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://dtmapi.iom.int/v3"

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


def _base_url() -> str:
    return os.environ.get("IOM_DTM_BASE_URL", _DEFAULT_BASE_URL)


def _headers() -> dict[str, str]:
    key = os.environ.get("IOM_DTM_SUBSCRIPTION_KEY", "")
    if not key:
        raise RuntimeError(
            "IOM_DTM_SUBSCRIPTION_KEY is not set — cannot call the IOM DTM API."
        )
    return {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
    }


def _unwrap_records(payload: Any) -> list[dict]:
    """Different DTM endpoints return either a bare list or a wrapped object.
    Normalise to a list of record dicts."""
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
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

    ``operation`` scopes to a single DTM "Operation" (their term for a
    data-gathering project). For Sudan the active project is
    "Armed Clashes in Sudan (Overview)" — older operations exist but are stale
    and dominate when the filter is omitted. Pass an empty string / None to
    fetch across all operations (needed for countries with no canonical one).
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

    url = f"{_base_url().rstrip('/')}/displacement/admin{admin_level}"
    logger.info("[IOM DTM] GET %s params=%s", url, params)

    resp = http_retry.get(url, params=params, headers=_headers(), timeout=timeout)

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


# ─── Aggregator (the correct way to summarise DTM data) ────────────────────
class AggregatedDisplacement(TypedDict):
    """Per-destination summary returned by aggregate_displacement_by_destination."""
    admin_pcode: str
    admin_name: str | None
    round_number: int
    reporting_date: str | None
    operation: str | None
    # Which assessmentType produced this row (e.g. "BA", "FM"). None when the
    # caller passed assessment_type_filter=None (no filter applied).
    assessment_type: str | None
    population_displaced: int  # total IDPs at the destination, summed across all origins
    origin_breakdown: list[dict[str, Any]]  # [{origin_admin1_pcode, origin_admin1_name, count}, ...]


def _aggregate_for_type(
    records: list[dict],
    admin_level: int,
    assessment_type: str | None,
) -> dict[str, AggregatedDisplacement]:
    """Single-pass aggregation for one assessmentType (or all if None)."""
    key_camel = f"admin{admin_level}Pcode"
    key_pascal = f"Admin{admin_level}Pcode"
    name_camel = f"admin{admin_level}Name"
    name_pascal = f"Admin{admin_level}Name"

    # Stage 1 — group by (pcode, round). Each bucket accumulates a total +
    # an origin-keyed sub-tally + the metadata we need for the payload.
    buckets: dict[tuple[str, int], dict[str, Any]] = {}

    for rec in records:
        if assessment_type is not None:
            rec_type = rec.get("assessmentType") or rec.get("AssessmentType")
            if rec_type != assessment_type:
                continue

        pcode = rec.get(key_camel) or rec.get(key_pascal)
        if not pcode:
            continue

        rn_raw = rec.get("roundNumber") or rec.get("RoundNumber") or 0
        try:
            rn = int(rn_raw)
        except (TypeError, ValueError):
            continue

        ind = extract_displacement_value(rec)
        if ind is None or ind <= 0:
            continue

        key = (pcode, rn)
        bucket = buckets.get(key)
        if bucket is None:
            bucket = {
                "admin_pcode": pcode,
                "admin_name": rec.get(name_camel) or rec.get(name_pascal),
                "round_number": rn,
                "reporting_date": rec.get("reportingDate") or rec.get("ReportingDate"),
                "operation": rec.get("operation") or rec.get("Operation"),
                "assessment_type": assessment_type,
                "population_displaced": 0,
                # origin admin1 pcode → {name, count}
                "_origins": {},
            }
            buckets[key] = bucket

        bucket["population_displaced"] += ind

        origin_pcode = rec.get("idpOriginAdmin1Pcode") or rec.get("IdpOriginAdmin1Pcode")
        if origin_pcode:
            origin_name = (
                rec.get("idpOriginAdmin1Name")
                or rec.get("IdpOriginAdmin1Name")
            )
            existing = bucket["_origins"].get(origin_pcode)
            if existing is None:
                bucket["_origins"][origin_pcode] = {"name": origin_name, "count": ind}
            else:
                existing["count"] += ind
                # Prefer the first non-null name we saw — names should be
                # consistent across rows but be defensive.
                if existing["name"] is None and origin_name is not None:
                    existing["name"] = origin_name

    # Stage 2 — collapse to one bucket per pcode (latest round wins).
    latest: dict[str, AggregatedDisplacement] = {}
    for (pcode, rn), bucket in buckets.items():
        cur = latest.get(pcode)
        if cur is None or rn > cur["round_number"] or (
            rn == cur["round_number"]
            and (bucket["reporting_date"] or "") > (cur["reporting_date"] or "")
        ):
            # Materialise the origin breakdown, sorted descending by count.
            origins = bucket.pop("_origins")
            origin_list = sorted(
                (
                    {
                        "origin_admin1_pcode": op,
                        "origin_admin1_name": v["name"],
                        "count": v["count"],
                    }
                    for op, v in origins.items()
                ),
                key=lambda x: x["count"],
                reverse=True,
            )
            bucket["origin_breakdown"] = origin_list
            latest[pcode] = bucket  # type: ignore[assignment]

    return latest


def aggregate_displacement_by_destination(
    records: list[dict],
    admin_level: int,
    *,
    assessment_type_filter: str | list[str] | None = "BA",
) -> dict[str, AggregatedDisplacement]:
    """Sum IDP counts per destination admin pcode + build per-origin breakdown.

    DTM admin-level endpoints return one row per
    (destination_admin{N}, origin_admin1, displacementReason, assessmentType).
    To get the total IDP count at a destination, we sum across origins (and
    reasons) for the same destination + round.

    ``assessment_type_filter`` accepts:
      - "BA"          — single type.
      - ["BA", "FM"]  — priority list. Aggregate the first type; for pcodes
                        with no rows there, fall back to the next type. Avoids
                        double-counting (BA + FM measure different things) while
                        filling gaps for districts where only FM data exists.
      - None          — no filter; pool all assessment types together.

    Returns ``{admin_pcode: AggregatedDisplacement}``. Destinations with zero
    IDPs (after filtering) are omitted.
    """
    if assessment_type_filter is None:
        return _aggregate_for_type(records, admin_level, None)
    if isinstance(assessment_type_filter, str):
        return _aggregate_for_type(records, admin_level, assessment_type_filter)

    # Priority list: first type wins per pcode; subsequent types fill gaps only.
    merged: dict[str, AggregatedDisplacement] = {}
    for atype in assessment_type_filter:
        pass_result = _aggregate_for_type(records, admin_level, atype)
        added = 0
        for pcode, bucket in pass_result.items():
            if pcode not in merged:
                merged[pcode] = bucket
                added += 1
        logger.info(
            "[IOM DTM] admin%d assessmentType=%r filled %d pcodes (%d already covered)",
            admin_level, atype, added, len(pass_result) - added,
        )
    return merged


def record_pcode(record: dict, admin_level: int) -> str | None:
    """Return the admin{N}Pcode from a DTM record, handling camel/pascal case."""
    return record.get(f"admin{admin_level}Pcode") or record.get(f"Admin{admin_level}Pcode")


def record_name(record: dict, admin_level: int) -> str | None:
    """Return the admin{N}Name from a DTM record, handling camel/pascal case."""
    return record.get(f"admin{admin_level}Name") or record.get(f"Admin{admin_level}Name")
