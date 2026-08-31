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
import re
from datetime import UTC, datetime

import httpx
import redis

from clear_context_pipeline.providers.clear_api import find_or_create_landmark_l4
from clear_context_pipeline.providers.signal import enrich_with_geoparser
from clear_context_pipeline.providers.translation_hash import _stable_stringify
from clear_context_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

IDU_URL = "https://helix-tools-api.idmcdb.org/external-api/idus/all/"

# Displacement-type values IDU carries that CLEAR is scoped to ingest (per the
# requirements doc's "Event types covered" — Conflict, Displacement, Natural
# Hazards). Anything else (e.g. a future IDU category CLEAR hasn't scoped in)
# is dropped rather than silently ingested.
_ALLOWED_DISPLACEMENT_TYPES = frozenset({"Conflict", "Disaster"})

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


def _content_hash(raw_data: dict) -> str:
    """Fingerprint of a raw IDU row, used to detect revisions. IDU has no
    `updated_at` — entries are revised in place (same `id`), so a plain
    id-based seen-set would silently miss revisions. Hashes the full raw
    payload via stable (sorted-key) JSON serialization, so any change
    anywhere in the row is caught and produces a new hash."""
    stringified_data = _stable_stringify(raw_data)
    return hashlib.sha256(stringified_data.encode("utf-8")).hexdigest()[:16]


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
    across the four locations_* fields returns `[parsed]` fully unchanged,
    logged as a warning.
    """
    raw = parsed.get("raw") or {}
    names = _LOCATION_SEP_RE.split((raw.get("locations_name") or "").strip())
    types = _LOCATION_SEP_RE.split((raw.get("locations_type") or "").strip())
    coords = _LOCATION_SEP_RE.split((raw.get("locations_coordinates") or "").strip())
    accuracies = _LOCATION_SEP_RE.split((raw.get("locations_accuracy") or "").strip())

    if not (len(names) == len(types) == len(coords) == len(accuracies)):
        logger.warning(
            "[IDMC] locations_* field count mismatch for idu_id=%s "
            "(names=%d types=%d coords=%d accuracy=%d) — skipping split",
            parsed.get("idu_id"), len(names), len(types), len(coords), len(accuracies),
        )
        return [parsed]

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


def _kind_from_locations_accuracy(accuracy: str | None) -> str:
    """Map IDU's `locations_accuracy` free text to clear-api's
    `findOrCreateLandmarkL4` `kind` argument — a hard-validated binary,
    only "landmark" or "admin" (clear-api rejects anything else).

    Every value seen in live IDU data ("Locality", "District/Zone/
    Department (ADM2)", "County/City/town/Village/Woreda (ADM3)")
    describes an administrative/settlement precision level, never a
    POI-style landmark — IDU's `locations_name` entries are towns,
    villages, and districts, not landmarks. So this currently always
    resolves to "admin"; kept as an explicit mapping (not a bare
    constant) so a future accuracy value that genuinely denotes a
    landmark has somewhere to plug in.
    """
    return "admin"


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

        for split in _split_by_location(parsed):
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


def _promote_location(
    *, name: str, coord_str: str, accuracy: str,
    source_lat: float | None, source_lng: float | None, idu_id: str | None,
) -> str | None:
    """Resolve one location's name + coordinate into a real L4 landmark id
    via `find_or_create_landmark_l4`. Best-effort, same convention
    `signal.py`'s own use of this function follows: a transport hiccup
    shouldn't drop the whole signal (IDMC polls once every 24h), so
    failures are logged and swallowed, not raised."""
    first_segment = (name or "").split(",")[0].strip()
    coord = _parse_coordinate(coord_str or "")
    if not (first_segment and coord):
        return None
    try:
        promo = find_or_create_landmark_l4(
            name=first_segment,
            lat=coord[0],
            lng=coord[1],
            kind=_kind_from_locations_accuracy(accuracy),
            # Anchor against the row's own centroid so a wildly mismatched
            # candidate aborts instead of mis-attributing the split.
            source_lat=source_lat,
            source_lng=source_lng,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[IDMC] L4 promotion failed for idu_id=%s: %s", idu_id, exc)
        return None
    if promo.get("abortedReason"):
        logger.info(
            "[IDMC] L4 promotion aborted (%s) for idu_id=%s", promo["abortedReason"], idu_id,
        )
        return None
    return promo.get("locationId")


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

    # Best-effort origin/destination resolution: promote each location's
    # own name + coordinate into a real L4 landmark, then route the result
    # to originId/destinationId. `locations_name`/`locations_type`/etc. are
    # 1 entry for a plain location, 2 for a paired flow (_split_pairs) —
    # this loop covers both without caring which.
    names = _LOCATION_SEP_RE.split((event.get("locations_name") or "").strip())
    types = _LOCATION_SEP_RE.split((event.get("locations_type") or "").strip())
    coords_list = _LOCATION_SEP_RE.split((event.get("locations_coordinates") or "").strip())
    accuracies = _LOCATION_SEP_RE.split((event.get("locations_accuracy") or "").strip())
    origin_coord: tuple[float, float] | None = None
    for name_i, type_i, coord_str_i, accuracy_i in zip(
        names, types, coords_list, accuracies, strict=True
    ):
        location_id = _promote_location(
            name=name_i, coord_str=coord_str_i, accuracy=accuracy_i,
            source_lat=event.get("lat"), source_lng=event.get("lng"),
            idu_id=event.get("idu_id"),
        )
        loc_type = type_i.strip().lower()
        if "origin" in loc_type and origin_coord is None:
            origin_coord = _parse_coordinate(coord_str_i)
        if not location_id:
            continue
        # Both fields set when "Origin and destination" — the same place
        # served both roles for this movement.
        if "origin" in loc_type:
            input_data["originId"] = location_id
        if "destination" in loc_type:
            input_data["destinationId"] = location_id
        # Blank/unrecognized: leave both unset — general locationId (via
        # lat/lng + enrich_with_geoparser below) is the fallback.

    # Pass lat/lng for server-side PostGIS geo-resolution — same as
    # ACLED/GDACS. Prefer the origin's own precise coordinate when one was
    # provided (more specific than the row's shared centroid); fall back
    # to the centroid otherwise (destination-only rows, or independent
    # splits with no role at all).
    lat, lng = origin_coord if origin_coord else (event.get("lat"), event.get("lng"))
    if lat is not None and lng is not None:
        input_data["lat"] = lat
        input_data["lng"] = lng

    enrich_with_geoparser(
        input_data,
        title=event["title"],
        description=event.get("description"),
        log_tag=f"idmc:{event.get('idu_id')}",
    )

    return input_data
