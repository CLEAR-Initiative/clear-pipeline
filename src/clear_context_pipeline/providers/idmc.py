"""IDMC IDU (Internal Displacement Updates) API client — event-level records of
internal displacement flows (conflict and disaster triggered), via the Helix
Tools API.

Requires a registered `client_id` (query param) — request one via IDMC.
Endpoint: GET https://helix-tools-api.idmcdb.org/external-api/idus/all/
(302 → S3 dump). Unlike ACLED/GDACS, this endpoint has NO server-side
filtering or pagination — every poll fetches the entire global dataset and
filters client-side.

One row = one *figure*, not one event: an IDU `event_id` can have many rows
across locations, dates, and revisions. Filtering + dedup are keyed on the
row-level `id`.

Docs: https://helix-tools-api.idmcdb.org/external-api/#/IDU/idus_all_retrieve
"""

import hashlib
import logging
from datetime import UTC, datetime

import httpx
import redis

from clear_context_pipeline.providers.signal import enrich_with_geoparser
from clear_context_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

IDU_URL = "https://helix-tools-api.idmcdb.org/external-api/idus/all/"

# Displacement-type values IDU carries that CLEAR is scoped to ingest (per the
# requirements doc's "Event types covered" — Conflict, Displacement, Natural
# Hazards). Anything else (e.g. a future IDU category CLEAR hasn't scoped in)
# is dropped rather than silently ingested.
_ALLOWED_DISPLACEMENT_TYPES = frozenset({"Conflict", "Disaster"})


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
        figure = int(figure)
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


def _content_hash(event: dict) -> str:
    """Fingerprint of the fields that define a "revision" of a figure. IDU has
    no `updated_at` — entries are revised in place (role upgraded, figure
    corrected) while keeping the same `id`, so a plain id-based seen-set would
    silently ignore revisions. Hashing these fields lets a changed revision be
    detected and re-submitted while an unchanged row is skipped."""
    parts = "|".join([
        event["role"],
        str(event["figure"]),
        event.get("displacement_start_date") or "",
        event.get("displacement_end_date") or "",
        event.get("locations_name") or "",
    ])
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()[:16]


def _fetch_all() -> list[dict]:
    """Fetch the entire IDU dataset. No date/country filter — the endpoint
    ignores query params server-side; the 302 lands on an S3 dump of every row."""
    logger.info("[IDMC] fetching full IDU dataset from %s", IDU_URL)
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


def fetch_idu_records(since: datetime | None = None) -> list[dict]:
    """Fetch + filter IDU records for the configured countries and displacement
    types, deduplicated against the Redis seen-set (id + content hash — see
    `_content_hash`). `since` is accepted for `PollSource` protocol parity but
    ignored: the API has no date filter, so every poll re-scans the full dump
    and the content-hash dedup does the "what's new/changed" work instead.
    """
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

        parsed["content_hash"] = _content_hash(parsed)
        seen_key = f"idmc:seen:{parsed['idu_id']}:{parsed['content_hash']}"
        if seen_key in batch_keys or _redis.exists(seen_key):
            deduped += 1
            continue
        batch_keys.add(seen_key)
        events.append(parsed)

    logger.info(
        "[IDMC] Result: %d new/changed events (parse_failed=%d, filtered_out=%d, "
        "already_seen=%d) out of %d raw",
        len(events), parse_failed, filtered_out, deduped, len(raw_rows),
    )
    return events


def mark_seen(idu_id: str, content_hash: str) -> None:
    """Mark a (id, content_hash) revision ingested — called only after
    createSignal is confirmed, so a failed persistence leaves the row eligible
    for retry on the next poll."""
    _redis.setex(f"idmc:seen:{idu_id}:{content_hash}", settings.dedup_ttl_hours * 3600, "1")


def get_last_synced() -> datetime | None:
    val = _redis.get("idmc:last_synced")
    if val:
        return datetime.fromisoformat(val)
    return None


def set_last_synced(ts: datetime) -> None:
    _redis.set("idmc:last_synced", ts.isoformat())


def build_idmc_signal_input(event: dict, source_id: str) -> dict:
    """Convert a parsed IDU row into a CLEAR CreateSignalInput dict."""
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
    }

    if event.get("source_url"):
        input_data["url"] = event["source_url"]

    # Pass lat/lng for server-side PostGIS geo-resolution — same as ACLED/GDACS.
    # NOTE: IDU rows carry a `locations_type` (Origin/Destination/Both), but no
    # existing connector sends directional coordinates (Dataminr's
    # origin/destination path only ever uses pre-resolved location IDs from
    # text, never raw coords), so that distinction is not represented here —
    # every row's point lands on the signal's general location for now.
    if event.get("lat") is not None and event.get("lng") is not None:
        input_data["lat"] = event["lat"]
        input_data["lng"] = event["lng"]

    enrich_with_geoparser(
        input_data,
        title=event["title"],
        description=event.get("description"),
        log_tag=f"idmc:{event.get('idu_id')}",
    )

    return input_data
