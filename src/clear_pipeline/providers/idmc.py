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
# IDMC-specific: one IDU `event_id` can carry several role-tagged rows
# (analyst-reviewed "Recommended figure" vs. corroborating "Triangulation")
# — no other source has this shape. Used only by the gx_pipeline medallion
# (`IDMCGXSource`'s group hooks -> `<source>_reconcile`), not by
# production's `fetch_idu_records`/`IDMCConnector`.
#
# These are deliberately *group primitives*, not a batch filter. The verdict
# for a row depends on every other row sharing its `event_id` — including
# ones ingested by an earlier poll and already sitting in gold. A function
# that only ever sees the current batch cannot compute it: a Triangulation
# row polled alone on Monday looks unopposed, and stays live forever once
# Tuesday's poll delivers the Recommended figure that supersedes it. So the
# rule is split into "what group is this row in" (`group_key` /
# `group_member`) and "given the whole group, what survives"
# (`resolve_group`), and the caller is responsible for assembling the whole
# group from both sources.

KEEP = "keep"
RETRACT = "retract"


def group_key(raw_data: dict | None) -> str | None:
    """The supersession group a raw IDU row belongs to — its `event_id`,
    namespaced as `idmc:eventId:<event_id>`. `groupKey` is a single column
    shared by every source's gold table (`iceberg_signals.py`); the
    namespace keeps IDMC's numeric `event_id`s from colliding with another
    source's group key if one is ever added, and makes the column
    self-describing when read directly out of Iceberg.

    None when the row carries no `event_id`: it's a group of one and no
    supersession rule can apply to it."""
    if not raw_data:
        return None
    event_id = raw_data.get("event_id")
    return f"idmc:eventId:{event_id}" if event_id else None


def group_member(external_id: str, raw_data: dict | None) -> dict | None:
    """Normalize a raw IDU row into the shape `resolve_group` reads, or None
    if it isn't in any group.

    `raw_data` is the verbatim IDU row — `build_idmc_signal_input` stores it
    on the signal input as `rawData`, so a freshly polled record and a row
    read back out of gold both reach this through the same field, with
    `event_id`/`role`/`created_at` under those same names. `externalId` is
    passed separately because the pipeline's row id is the bare `idu_id`,
    while the signal input's own `externalId` is the `idmc:`-prefixed form."""
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
    """Given every member of ONE `event_id` group, return each member's
    verdict: `KEEP` or `RETRACT`.

    Three rules, unchanged from the batch filter this replaces:
      1. A Recommended figure present -> every Triangulation row is
         superseded and retracts; everything else keeps.
      2. An all-Triangulation group -> only the most recent row (by
         `created_at`) keeps; the rest retract.
      3. Anything else (no Recommended figure, not all Triangulation) ->
         everything keeps, rather than guessing at an unknown role mix.

    Total over `members`: every member gets a verdict, so a caller can
    compare it against what it previously recorded and detect a group whose
    verdict has *reversed* — a retracted row becoming live again. That
    reversibility is why the caller must feed in already-retracted rows too.
    """
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
    types, deduplicated against the Redis seen-set (id + content hash — see
    `_content_hash`). `since` is accepted for `PollSource` protocol parity but
    ignored: the API takes no client-controllable date filter, so every poll
    re-scans IDMC's whole last-180-days window and the content-hash dedup does
    the "what's new/changed" work instead.
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

        parsed["content_hash"] = _content_hash(parsed["raw"])
        seen_key = f"idmc:seen:{parsed['idu_id']}:{parsed['content_hash']}"
        if seen_key in batch_keys:
            deduped += 1
            continue
        # Renew, don't just check — unlike ACLED/GDACS, IDMC re-checks the same
        # idu_id forever, so a fixed TTL would eventually expire on an unchanged
        # row and misfire it as "new". EXPIRE renews and reports existence in one call
        if _redis.expire(seen_key, settings.dedup_ttl_hours * 3600):
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


def build_idmc_signal_input(event: dict, source_id: str, *, promote: bool = True) -> dict:
    """Convert a parsed IDU row into a CLEAR CreateSignalInput dict.

    `promote` threads through to `enrich_with_geoparser` (default True,
    today's behavior). See `acled.py::build_acled_signal_input`'s docstring —
    same parameter, same reason."""
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


def build_signal_content_update(input_data: dict, signal_id: str) -> dict:
    """Adapt a create_signal input dict (already built by
    build_idmc_signal_input) into an updateSignalContent input dict targeting
    an existing signal — reuses the same values rather than recomputing them,
    so a revision's create and update calls always agree.

    lat/lng/geoparsedData are spread in only when build_idmc_signal_input
    actually set them, never defaulted via `.get()`. An ABSENT key tells
    clear-api's Prisma update "leave this field alone"; sending an explicit
    None instead would NULL OUT a previously-resolved value just because
    this poll's data happened to be missing it transiently.
    """
    return {
        "id": signal_id,
        "contentHash": input_data["contentHash"],
        "rawData": input_data["rawData"],
        "title": input_data.get("title"),
        "description": input_data.get("description"),
        "severity": input_data.get("severity"),
        "url": input_data.get("url"),
        # lat/lng/geoparsedData can be transiently missing (bad coordinate
        # data, or Nominatim being down) — omit, don't null a resolved value.
        **{
            k: input_data[k]
            for k in ("lat", "lng", "geoparsedData")
            if k in input_data
        },
    }
