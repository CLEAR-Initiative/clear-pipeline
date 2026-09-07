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
row-level `id`.

Docs: https://helix-tools-api.idmcdb.org/external-api/#/IDU/idus_last_180_days_retrieve
"""

import hashlib
import logging
import re
from datetime import UTC, datetime

import httpx
import redis

from clear_pipeline.providers.signal import enrich_with_geoparser
from clear_pipeline.providers.translation_hash import _stable_stringify
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

IDU_URL = "https://helix-tools-api.idmcdb.org/external-api/idus/last-180-days/"

# Separator between entries in IDU's compound locations_* fields
# (locations_name, locations_type, locations_coordinates, locations_accuracy)
# — semicolon, with variable surrounding whitespace observed in live data.
_LOCATION_SEP_RE = re.compile(r"\s*;\s*")


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


def _classify_role(location_type: str) -> str:
    """Classify one `locations_type` entry: "origin", "destination", "both"
    (ambiguous — "Origin and destination"), or "neither"."""
    t = (location_type or "").strip().lower()
    is_origin = "origin" in t
    is_destination = "destination" in t
    if is_origin and is_destination:
        return "both"
    if is_origin:
        return "origin"
    if is_destination:
        return "destination"
    return "neither"


def _build_split(parsed: dict, raw: dict, figure: int, name: str, type_: str,
                  coords: str, accuracy: str, idu_id: str | None) -> dict:
    """One output dict shared by both split strategies below — a fresh
    `raw` (never a shared reference) plus the same fields surfaced
    top-level, per index/pair."""
    split_raw = {
        **raw,
        "figure": figure,
        "locations_name": name,
        "locations_type": type_,
        "locations_coordinates": coords,
        "locations_accuracy": accuracy,
    }
    split = {
        **parsed,
        "figure": figure,
        "locations_name": name,
        "locations_type": type_,
        "locations_coordinates": coords,
        "locations_accuracy": accuracy,
        "raw": split_raw,
    }
    if idu_id is not None:
        split["idu_id"] = idu_id
    return split


def _split_independent(parsed: dict, raw: dict, names: list[str], types: list[str],
                        coords: list[str], accuracies: list[str]) -> list[dict]:
    """Fallback: one signal per raw location, figure divided equally across
    all N (not pair-aware) — used when the row's origin/destination
    composition doesn't resolve to a clean pairing (see TODO.md)."""
    n = len(names)
    figure = parsed.get("figure") or 0
    base, remainder = divmod(figure, n)
    return [
        _build_split(
            parsed, raw, base + (1 if i < remainder else 0),
            names[i], types[i], coords[i], accuracies[i],
            f"{parsed['idu_id']}:{i}" if n > 1 else None,
        )
        for i in range(n)
    ]


def _split_pairs(parsed: dict, raw: dict, names: list[str], types: list[str],
                  coords: list[str], accuracies: list[str],
                  origins: list[int], destinations: list[int]) -> list[dict]:
    """One signal per (origin, destination) pair — 1:1 merges into a single
    signal carrying the full, undivided figure and the row's original
    `idu_id`; 1:N/N:1 fans out into N signals, figure divided by N (the
    pair count, not the raw location count)."""
    pairs = [(o, d) for o in origins for d in destinations]
    n_pairs = len(pairs)
    figure = parsed.get("figure") or 0
    base, remainder = divmod(figure, n_pairs)
    return [
        _build_split(
            parsed, raw, base + (1 if i < remainder else 0),
            f"{names[o]}; {names[d]}", f"{types[o]}; {types[d]}",
            f"{coords[o]}; {coords[d]}", f"{accuracies[o]}; {accuracies[d]}",
            f"{parsed['idu_id']}:{i}" if n_pairs > 1 else None,
        )
        for i, (o, d) in enumerate(pairs)
    ]


def _split_by_location(parsed: dict) -> list[dict]:
    """Split a multi-location IDU row into flow signals, pairing origins
    with destinations rather than treating every named location as an
    independent occurrence.

    Example — idu_id=174447, figure=1000, 1 origin (Al Jazirah) + 1
    destination (Al Fao): merges into ONE signal, `idu_id` unchanged,
    full undivided figure — it's one flow, not two. With 1 origin + 2
    destinations instead, it fans out into 2 signals (`idu_id:0`/`:1`),
    figure divided by 2. "Origin and destination" fills whichever role
    has zero plain matches elsewhere in the row (it's not a role of its
    own — see the classify step below).

    Falls back to `_split_independent` (equal division per raw location,
    ignoring role) when the composition doesn't resolve to a clean
    pairing — multiple origins AND multiple destinations at once, or any
    location with neither role. See TODO.md — rare (only seen in old
    2018 Triangulation-role data so far), logged at INFO when it fires.

    A single-location row keeps its `idu_id` unchanged (no suffix) — an
    already-ingested row's dedup identity must not shift. A count mismatch
    across the four locations_* fields returns `[]` (dropped, not guessed
    at), logged as an error.
    """
    raw = parsed.get("raw") or {}
    names = _LOCATION_SEP_RE.split((raw.get("locations_name") or "").strip())
    types = _LOCATION_SEP_RE.split((raw.get("locations_type") or "").strip())
    coords = _LOCATION_SEP_RE.split((raw.get("locations_coordinates") or "").strip())
    accuracies = _LOCATION_SEP_RE.split((raw.get("locations_accuracy") or "").strip())

    if not (len(names) == len(types) == len(coords) == len(accuracies)):
        logger.error(
            "[IDMC] idu_id=%s: locations_* field count mismatch "
            "(names=%d types=%d coords=%d accuracy=%d) — dropping row",
            parsed.get("idu_id"), len(names), len(types), len(coords), len(accuracies),
        )
        return []

    # No explicit n==1 shortcut needed: with a single location, `origins`
    # and `destinations` can never both be non-empty, so the checks below
    # always fall back to `_split_independent` — which handles n==1
    # correctly on its own (single output, idu_id unchanged).
    roles = [_classify_role(t) for t in types]
    origins = [i for i, r in enumerate(roles) if r == "origin"]
    destinations = [i for i, r in enumerate(roles) if r == "destination"]
    ambiguous = [i for i, r in enumerate(roles) if r == "both"]
    neither = [i for i, r in enumerate(roles) if r == "neither"]

    unresolved_ambiguous = False
    if ambiguous:
        if destinations and not origins:
            origins = origins + ambiguous
        elif origins and not destinations:
            destinations = destinations + ambiguous
        else:
            # Neither role is otherwise singular — can't tell which one
            # each ambiguous entry fills. TODO.md.
            unresolved_ambiguous = True

    if (
        neither or unresolved_ambiguous or not origins or not destinations
        or (len(origins) > 1 and len(destinations) > 1)
    ):
        logger.info(
            "[IDMC] idu_id=%s: locations_type composition isn't a clean "
            "1:1/1:N/N:1 pairing (origins=%d destinations=%d neither=%d) — "
            "falling back to independent per-location split (TODO.md)",
            parsed.get("idu_id"), len(origins), len(destinations), len(neither),
        )
        return _split_independent(parsed, raw, names, types, coords, accuracies)

    return _split_pairs(parsed, raw, names, types, coords, accuracies, origins, destinations)


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
    parse_failed = filtered_out = deduped = mismatched = 0
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

        splits = _split_by_location(parsed)
        if not splits:
            mismatched += 1
            continue

        for split in splits:
            split["content_hash"] = _content_hash(split["raw"])
            seen_key = f"idmc:seen:{split['idu_id']}:{split['content_hash']}"
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
            events.append(split)

    logger.info(
        "[IDMC] Result: %d new/changed events (parse_failed=%d, filtered_out=%d, "
        "already_seen=%d, mismatched=%d) out of %d raw",
        len(events), parse_failed, filtered_out, deduped, mismatched, len(raw_rows),
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
        "contentHash": event["content_hash"],
    }

    if event.get("source_url"):
        input_data["url"] = event["source_url"]

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
        log_tag=f"idmc:{event.get('idu_id')}",
    )

    return input_data


def build_signal_content_update(input_data: dict, signal_id: str) -> dict:
    """Adapt a create_signal input dict (already built by
    build_idmc_signal_input) into an updateSignalContent input dict targeting
    an existing signal — reuses the same values rather than recomputing them,
    so a revision's create and update calls always agree.

    url/lat/lng/geoparsedData are spread in only when build_idmc_signal_input
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
        # lat/lng/geoparsedData can be transiently missing (bad coordinate
        # data, or Nominatim being down) — omit, don't null a resolved value.
        **{
            k: input_data[k]
            for k in ("url", "lat", "lng", "geoparsedData")
            if k in input_data
        },
    }
