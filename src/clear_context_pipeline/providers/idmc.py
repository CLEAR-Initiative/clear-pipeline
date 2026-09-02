"""IDMC IDU (Internal Displacement Updates) API client: event-level records of
internal displacement flows (conflict and disaster triggered), via the Helix
Tools API.

Requires a registered `client_id` (query param), request one via IDMC; the
endpoint 403s without one. Read from `IDMC_CLIENT_ID`, matching the
production connector's convention on `dev` (`feat/idmc-integration`, PR #50),
which this POC's provider module predates and duplicates. See the module
docstring caveat below the imports for the resulting file collision.

Endpoint: GET https://helix-tools-api.idmcdb.org/external-api/idus/last-180-days/
(302 -> S3 dump). No server-side country/type filtering or pagination via
query params: every fetch returns IDMC's whole last-180-days feed and
callers filter client-side.

One row = one *figure*, not one event: an IDU `event_id` can have many rows
across locations, dates, and revisions. This module is a pure fetch+parse
provider (no Dagster, no clear-api, no dedup/watermark state). The
bronze/silver/gold POC pipeline (`defs/dq_poc_idmc/`) owns orchestration.

Docs: https://helix-tools-api.idmcdb.org/external-api/#/IDU/idus_last_180_days_retrieve

NOTE: `dev` now has its own, more advanced `providers/idmc.py` from the
merged `feat/idmc-integration` PR (content-hash revision detection,
origin/destination location pairing, clear-api geoparser integration). This
POC branch was cut from `dev` before that merge and independently created
this same file, deliberately simplified (no clear-api/Redis coupling) for
standalone use. The two will conflict on rebase/merge; not resolved here
since this file's job is just to unblock the demo, not to reconcile with
production. See the two providers side by side before deciding which wins.
"""

import logging
import os

import httpx

logger = logging.getLogger(__name__)

IDU_URL = "https://helix-tools-api.idmcdb.org/external-api/idus/last-180-days/"

# IDMC's own top-level displacement_type taxonomy: conflict-induced vs
# disaster-induced. Not every value CLEAR is scoped to ingest; callers filter.
DISPLACEMENT_TYPES = frozenset({"Conflict", "Disaster"})


def fetch_idu_records(*, timeout: float = 120.0) -> list[dict]:
    """Fetch IDMC's last-180-days IDU feed. No country/type filter: the
    endpoint ignores those query params server-side; the 302 lands on an S3
    dump of every row IDMC currently serves for this window."""
    client_id = os.environ.get("IDMC_CLIENT_ID", "")
    if not client_id:
        logger.error(
            "[IDMC] IDMC_CLIENT_ID is not set: the endpoint 403s without one. "
            "Request one via IDMC and add it to .env."
        )
        return []

    logger.info("[IDMC] fetching last-180-days IDU feed from %s", IDU_URL)
    try:
        resp = httpx.get(
            IDU_URL,
            params={"client_id": client_id},
            follow_redirects=True,
            timeout=timeout,
        )
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.error("[IDMC] API request failed: %s", e)
        return []

    try:
        data = resp.json()
    except Exception as e:
        logger.error("[IDMC] JSON parse failed: %s, body=%s", e, resp.text[:200])
        return []

    if not isinstance(data, list):
        logger.error("[IDMC] unexpected response type: %s", type(data).__name__)
        return []

    logger.info("[IDMC] fetched %d raw rows", len(data))
    return data


def parse_idu_record(raw: dict) -> dict | None:
    """Normalize one raw IDU row. Returns None if the row lacks the fields a
    signal needs (id, figure, coordinates)."""
    idu_id = raw.get("id")
    figure = raw.get("figure")
    if idu_id is None or figure is None:
        return None

    lat = lng = None
    try:
        if raw.get("latitude") is not None:
            lat = float(raw["latitude"])
        if raw.get("longitude") is not None:
            lng = float(raw["longitude"])
    except (ValueError, TypeError):
        pass

    try:
        figure = int(figure)
    except (ValueError, TypeError):
        return None

    # Severity from displacement magnitude: an exact flow count is a better
    # severity signal than anything a text classifier would infer.
    if figure >= 50_000:
        severity = 5
    elif figure >= 10_000:
        severity = 4
    elif figure >= 1_000:
        severity = 3
    elif figure >= 100:
        severity = 2
    else:
        severity = 1

    return {
        "idu_id": str(idu_id),
        "iso3": (raw.get("iso3") or "").upper(),
        "displacement_type": raw.get("displacement_type") or "",
        "figure": figure,
        "severity": severity,
        "role": raw.get("role") or "",
        "title": raw.get("event_name") or "",
        "description": raw.get("standard_popup_text") or raw.get("standard_info_text"),
        "lat": lat,
        "lng": lng,
        "locations_name": raw.get("locations_name"),
        "locations_type": raw.get("locations_type"),
        "displacement_start_date": raw.get("displacement_start_date"),
        "displacement_end_date": raw.get("displacement_end_date"),
        "event_id": raw.get("event_id"),
        "source_url": raw.get("source_url"),
        "created_at": raw.get("created_at"),
        "raw": raw,
    }
