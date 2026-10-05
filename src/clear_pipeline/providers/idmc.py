"""IDMC IDU (Internal Displacement Updates) API client — event-level records of
internal displacement flows (conflict and disaster triggered), via the Helix
Tools API.

Requires a registered `client_id` (query param) — request one via IDMC.
Endpoint: GET https://helix-tools-api.idmcdb.org/external-api/idus/last-180-days/
(302 → S3 dump). Unlike ACLED/GDACS, this endpoint has NO server-side
country/type filtering or pagination via query params — every poll fetches
IDMC's whole last-180-days feed and filters client-side. IDMC's own docs don't
specify which date field scopes the 180-day window (displacement date? event
date? created_at?) — see docs/idmc-signal-revision-propagation.md for the risk
this poses to catching revisions on records that age out of the window.

One row = one *figure*, not one event: an IDU `event_id` can have many rows
across locations, dates, and revisions. Filtering + dedup are keyed on the
row-level `id`. A single row can itself describe multiple flows of
displacement (e.g. one origin, several destinations).

Docs: https://helix-tools-api.idmcdb.org/external-api/#/IDU/idus_last_180_days_retrieve
"""

import hashlib
import logging
from datetime import UTC, datetime

import httpx
import redis

from clear_pipeline.providers.signal import enrich_with_geoparser
from clear_pipeline.providers.translation_hash import _stable_stringify
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

IDU_URL = "https://helix-tools-api.idmcdb.org/external-api/idus/last-180-days/"


def _parse_event(raw: dict) -> dict | None:
    """Normalize a raw IDU row into our signal-like dict. Returns None if the
    row lacks the fields a signal needs (id, figure, coordinates)."""
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
        figure = int(float(figure))
    except (ValueError, TypeError):
        return None

    # Severity from displacement-magnitude, mirroring ACLED's fatality ladder
    # (acled.py:_parse_event) — IDU gives an exact flow count, a better signal
    # of severity than title/description text.
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
        "event_id": raw.get("event_id"),
        "iso3": raw.get("iso3") or "",
        "displacement_type": raw.get("displacement_type") or "",
        "figure": figure,
        "role": raw.get("role") or "",
        "title": raw.get("event_name") or "",
        "description": raw.get("standard_popup_text") or raw.get("standard_info_text"),
        "severity": severity,
        "lat": lat,
        "lng": lng,
        "locations_name": raw.get("locations_name"),
        "locations_type": raw.get("locations_type"),
        "displacement_start_date": raw.get("displacement_start_date"),
        "displacement_end_date": raw.get("displacement_end_date"),
        "source_url": raw.get("source_url"),
        "created_at": raw.get("created_at"),
        "raw": raw,
    }


_ROLE_RECOMMENDED = "Recommended figure"
_ROLE_TRIANGULATION = "Triangulation"


# ── Role-based supersession within an IDU `event_id` group ────────────────
# One `event_id` can carry several role-tagged rows (reviewed "Recommended
# figure" vs corroborating "Triangulation"). A verdict depends on the whole
# group, including earlier polls' rows in gold, so these are group primitives,
# not a batch filter; the caller (`<source>_reconcile`) assembles the group.

KEEP = "keep"
RETRACT = "retract"


def group_key(raw_data: dict | None) -> str | None:
    """`idmc:eventId:<event_id>`, or None without an `event_id` (group of one).
    Namespaced because `groupKey` is one column shared by every source's gold table."""
    if not raw_data:
        return None
    event_id = raw_data.get("event_id")
    return f"idmc:eventId:{event_id}" if event_id else None


def group_member(external_id: str, raw_data: dict | None) -> dict | None:
    """Normalize a raw IDU row for `resolve_group`, or None if ungrouped.
    `raw_data` is the verbatim row stored as `rawData`, so polled and gold rows
    read alike. `external_id` is the bare `idu_id`, not the `idmc:`-prefixed one."""
    key = group_key(raw_data)
    if key is None or raw_data is None:
        return None
    return {
        "externalId": external_id,
        "groupKey": key,
        # Same `or ""` normalization `_parse_event` applies — IDU can send a
        # null role, and the rules below compare against exact strings.
        "role": (raw_data.get("role") or ""),
        "createdAt": raw_data.get("created_at") or "",
    }


def resolve_group(members: list[dict]) -> dict[str, str]:
    """`KEEP`/`RETRACT` for every member of ONE `event_id` group: a Recommended
    figure retracts all Triangulation rows; an all-Triangulation group keeps only
    the latest `created_at`; any other mix keeps everything rather than guess.
    Total, so a reversed verdict is detectable: callers must pass retracted rows too."""
    roles = [m["role"] for m in members]
    if _ROLE_RECOMMENDED in roles:
        return {
            m["externalId"]: (RETRACT if m["role"] == _ROLE_TRIANGULATION else KEEP)
            for m in members
        }
    if roles and all(role == _ROLE_TRIANGULATION for role in roles):
        most_recent = max(members, key=lambda m: m["createdAt"])
        return {
            m["externalId"]: (KEEP if m["externalId"] == most_recent["externalId"] else RETRACT)
            for m in members
        }
    return {m["externalId"]: KEEP for m in members}


# IDMC's backend recomputes this row's centroid independently on every poll,
# with float noise around 1e-11 to 1e-14 degrees (sub-nanometer on the
# ground) — enough to flip latitude/longitude/centroid alone with nothing
# actually revised. Rounded before hashing so the fingerprint tracks real
# content changes, not that noise. 6 decimals (~11cm) is far finer than
# IDU's own admin/settlement-level location accuracy.
_HASH_COORD_DECIMALS = 6


def _round_centroid(value: str | None) -> str | None:
    """Round a `"[lat, lng]"` centroid string to `_HASH_COORD_DECIMALS`.
    Returns the value unchanged if it isn't in the expected shape."""
    if not value:
        return value
    coord = _parse_coordinate(value.strip().strip("[]"))
    if coord is None:
        return value
    lat, lng = (round(c, _HASH_COORD_DECIMALS) for c in coord)
    return f"[{lat}, {lng}]"


def _content_hash(raw_data: dict) -> str:
    """Fingerprint of a raw IDU row, used to detect revisions. IDU has no
    `updated_at` — entries are revised in place (same `id`), so a plain
    id-based seen-set would silently miss revisions. Hashes the full raw
    payload via stable (sorted-key) JSON serialization, so any change
    anywhere in the row is caught and produces a new hash — except
    latitude/longitude/centroid, rounded first (see `_HASH_COORD_DECIMALS`)."""
    normalized = dict(raw_data)
    for field in ("latitude", "longitude"):
        value = normalized.get(field)
        if isinstance(value, (int, float)):
            # float() first: IDMC sometimes serializes an exact-integer
            # coordinate as a JSON int (9) instead of a JSON float (9.0)
            # between polls. round() preserves the input type, so without
            # this cast an int/float split on an otherwise-identical value
            # still changes the JSON text ("9" vs "9.0") and flips the hash.
            normalized[field] = round(float(value), _HASH_COORD_DECIMALS)
    if "centroid" in normalized:
        normalized["centroid"] = _round_centroid(normalized.get("centroid"))
    stringified_data = _stable_stringify(normalized)
    return hashlib.sha256(stringified_data.encode("utf-8")).hexdigest()[:16]


def _fetch_all() -> list[dict]:
    """Fetch IDMC's last-180-days IDU feed. No country/type filter — the
    endpoint ignores those query params server-side; the 302 lands on an S3
    dump of every row IDMC currently serves for this window."""
    logger.info("[IDMC] fetching last-180-days IDU feed from %s", IDU_URL)
    try:
        resp = httpx.get(
            IDU_URL,
            params={"client_id": settings.idmc_client_id},
            follow_redirects=True,
            timeout=120,
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


def _parse_coordinate(pair: str) -> tuple[float, float] | None:
    """Parse one `"lat, lng"` entry from `locations_coordinates`. Returns
    None on a malformed or empty pair."""
    parts = [p.strip() for p in pair.split(",")]
    if len(parts) != 2:
        return None
    try:
        return float(parts[0]), float(parts[1])
    except (TypeError, ValueError):
        return None


def fetch_idu_records(since: datetime | None = None) -> list[dict]:
    """Fetch + filter IDU records for the configured countries and displacement
    types. `since` is ignored (`PollSource` parity): the API has no date filter,
    so every poll re-scans the last 180 days. No cross-poll dedup: gx compares
    `content_hash` against gold, and a seen-set here would hide revisions."""
    countries = {c.strip().upper() for c in settings.idmc_countries.split(",") if c.strip()}
    allowed_types = {t.strip() for t in settings.idmc_allowed_types.split(",") if t.strip()}

    raw_rows = _fetch_all()

    events: list[dict] = []
    parse_failed = filtered_out = deduped = 0
    batch_keys: set[str] = set()
    for raw in raw_rows:
        parsed = _parse_event(raw)
        if not parsed:
            parse_failed += 1
            continue

        if parsed["iso3"].upper() not in countries:
            filtered_out += 1
            continue
        if parsed["displacement_type"] not in allowed_types:
            filtered_out += 1
            continue

        parsed["content_hash"] = _content_hash(parsed["raw"])
        batch_key = f"{parsed['idu_id']}:{parsed['content_hash']}"
        if batch_key in batch_keys:
            deduped += 1
            continue
        batch_keys.add(batch_key)
        events.append(parsed)

    logger.info(
        "[IDMC] Result: %d events (parse_failed=%d, filtered_out=%d, "
        "duplicate_in_batch=%d) out of %d raw",
        len(events), parse_failed, filtered_out, deduped, len(raw_rows),
    )
    return events


def get_last_synced() -> datetime | None:
    val = _redis.get("idmc:last_synced")
    if val:
        return datetime.fromisoformat(val)
    return None


def set_last_synced(ts: datetime) -> None:
    _redis.set("idmc:last_synced", ts.isoformat())


def build_idmc_signal_input(event: dict, source_id: str, *, promote: bool = True) -> dict:
    """Convert a parsed IDU row into a CLEAR CreateSignalInput dict. `promote`
    threads through to `enrich_with_geoparser`, as in `acled.py::build_acled_signal_input`."""
    published_at = event.get("created_at") or datetime.now(UTC).isoformat()

    input_data: dict = {
        "sourceId": source_id,
        # Dedup key — the row-level `id`, per the requirements doc's schema
        # mapping (externalId ← id). One IDU row = one CLEAR signal; CLEAR's
        # own classify/group stage clusters related rows (shared event_id)
        # into one internal event, same as it does for ACLED.
        "externalId": f"idmc:{event['idu_id']}",
        "rawData": event["raw"],
        "publishedAt": published_at,
        "title": event["title"],
        "description": event.get("description"),
        "severity": event.get("severity"),
        "url": event.get("source_url"),
        "contentHash": event["content_hash"],
    }

    # Pass lat/lng for server-side PostGIS geo-resolution into a general
    # locationId — same as ACLED/GDACS.
    has_lat = event.get("lat") is not None
    has_lng = event.get("lng") is not None
    if has_lat != has_lng:
        logger.warning(
            "[IDMC] idu_id=%s: partial coordinate (lat=%s, lng=%s) — skipping "
            "centroid resolution",
            event.get("idu_id"), event.get("lat"), event.get("lng"),
        )
    if has_lat and has_lng:
        input_data["lat"] = event["lat"]
        input_data["lng"] = event["lng"]

    enrich_with_geoparser(
        input_data,
        title=event["title"],
        description=event.get("description"),
        promote=promote,
        log_tag=f"idmc:{event.get('idu_id')}",
    )

    return input_data


def build_signal_content_update(input_data: dict, *, retracted: bool | None = None) -> dict:
    """Adapt a build_idmc_signal_input dict into an updateSignalContent input keyed
    by ``(sourceId, externalId)`` (gold never has the clear-api id), reusing its
    values so create and update agree. lat/lng/geoparsedData/rawS3Key are omitted
    when absent: an absent key leaves the field alone, None would erase a resolved
    value on a transient gap. ``retracted=None`` leaves the flag unchanged."""
    update = {
        "sourceId": input_data["sourceId"],
        "externalId": input_data["externalId"],
        "contentHash": input_data["contentHash"],
        "rawData": input_data["rawData"],
        "title": input_data.get("title"),
        "description": input_data.get("description"),
        "severity": input_data.get("severity"),
        "url": input_data.get("url"),
        **{
            k: input_data[k]
            for k in ("lat", "lng", "geoparsedData")
            if k in input_data
        },
    }
    if input_data.get("rawS3Key"):
        update["rawS3Key"] = input_data["rawS3Key"]
    if retracted is not None:
        update["retracted"] = retracted
    return update
