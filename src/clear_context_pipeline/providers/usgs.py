"""USGS FDSN seismic client + slim blob builder (Expo #465 backend).

Pure fetch + transform — no Dagster, no clear-api. The Dagster asset in
``defs/location_metadata/`` loops the pipeline countries, calls
``build_seismic_collection`` with each country's padded bbox, and upserts one
``locationMetadata`` (type ``usgs_earthquakes``) row per admin0 country. The
clear-api ``GET /api/usgs/earthquakes`` route serves the stored blob (adding the
request-time ``age_days`` / ``stale`` fields).

Contract: ``clear-context-pipeline/docs/data-source-specs/USGS-earthquake.md``
and the ``SeismicMapCollection`` shape the front end paints.

Design notes:
  - Identity is the **top-level** Feature ``id`` (e.g. ``us6000tjl2``), NOT
    ``properties.id`` (often missing).
  - We slim here (drop detail/nst/rms/gap/… — see ``_SLIM_DROP``) so the stored
    blob is ~⅓ the size of the fat FDSN payload; ``bytes_in``/``bytes_out`` record
    the reduction. ShakeMap contours are USGS isoseismals copied verbatim — never
    re-gridded or turned into polygons.
  - ``age_days`` / ``stale`` are deliberately NOT stored: they are relative to
    request time, so the serve route computes them (a stored value would rot).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from clear_context_pipeline.providers import http_retry

logger = logging.getLogger(__name__)

FDSN_QUERY_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
DEFAULT_MIN_MAGNITUDE = 5.5
DEFAULT_WINDOW_DAYS = 30
STALE_AFTER_DAYS = 30
# USGS hard cap per query; we paginate by offset past it (rare at M5.5+ / bbox).
_FDSN_MAX_LIMIT = 20000
# Cont­our payloads are the scale risk; a single ShakeMap is small but we bound
# how many events we chase per run so a pathological batch can't stall the asset.
_MAX_SHAKEMAP_FETCH = 200

BBox = tuple[float, float, float, float]  # (minLng, minLat, maxLng, maxLat)


def _fdsn_params(bbox: BBox, min_magnitude: float, start_time: str, *, offset: int) -> dict[str, Any]:
    min_lng, min_lat, max_lng, max_lat = bbox
    return {
        "format": "geojson",
        "eventtype": "earthquake",
        "minmagnitude": min_magnitude,
        "starttime": start_time,
        "minlongitude": min_lng,
        "minlatitude": min_lat,
        "maxlongitude": max_lng,
        "maxlatitude": max_lat,
        "orderby": "time",
        "limit": _FDSN_MAX_LIMIT,
        "offset": offset,  # FDSN offset is 1-based
    }


def fetch_earthquakes(
    *, bbox: BBox, min_magnitude: float, start_time: str, timeout: float = 60.0,
) -> list[dict[str, Any]]:
    """Fetch raw FDSN earthquake Features for ``bbox`` since ``start_time`` (ISO).

    Paginates by ``offset`` if a single query would exceed the 20k USGS cap
    (essentially never at M5.5+ over a country bbox, but correct if it does).
    Returns the raw (fat) Feature dicts — slimming happens in
    ``build_seismic_collection``."""
    features: list[dict[str, Any]] = []
    offset = 1
    while True:
        resp = http_retry.get(
            FDSN_QUERY_URL,
            params=_fdsn_params(bbox, min_magnitude, start_time, offset=offset),
            timeout=timeout,
        )
        body = resp.json()
        page = [f for f in (body.get("features") or []) if isinstance(f, dict)]
        features.extend(page)
        # Stop when this page didn't fill the cap (no more), or we've collected
        # the metadata count. USGS returns metadata.count for the whole matching
        # set (not just the page).
        count = ((body.get("metadata") or {}).get("count")) or len(features)
        if len(page) < _FDSN_MAX_LIMIT or len(features) >= count:
            break
        offset += _FDSN_MAX_LIMIT
    return features


def has_shakemap(feature: dict[str, Any]) -> bool:
    """True when the event carries a ShakeMap product (gate for contour fetch).
    USGS lists product types in ``properties.types`` as a comma-wrapped string
    like ``,origin,shakemap,losspager,``."""
    types = ((feature.get("properties") or {}).get("types") or "")
    return "shakemap" in str(types).lower()


def slim_feature(feature: dict[str, Any]) -> dict[str, Any] | None:
    """Reduce a fat FDSN Feature to the map contract. Returns None for a feature
    we reject (non-Point / null geometry / missing id). Drops the heavy fields;
    keeps only what paint + popup need. Does NOT set ``age_days`` / ``stale``."""
    event_id = feature.get("id")  # TOP-LEVEL id, not properties.id
    geom = feature.get("geometry")
    if not event_id or not isinstance(geom, dict) or geom.get("type") != "Point":
        return None
    coords = geom.get("coordinates")
    if not (isinstance(coords, list) and len(coords) >= 2):
        return None
    lng, lat = coords[0], coords[1]
    depth_km = coords[2] if len(coords) >= 3 else None
    p = feature.get("properties") or {}
    return {
        "type": "Feature",
        "id": event_id,
        "geometry": {"type": "Point", "coordinates": [lng, lat, depth_km]},
        "properties": {
            "id": event_id,
            "mag": p.get("mag"),
            "mag_type": p.get("magType"),
            "place": p.get("place"),
            "title": p.get("title"),
            "time": p.get("time"),       # ms epoch
            "updated": p.get("updated"),  # ms epoch
            "depth_km": depth_km,
            "alert": p.get("alert"),
            "mmi": p.get("mmi"),
            "url": p.get("url"),
            "has_shakemap": has_shakemap(feature),
            "status": p.get("status"),
            # age_days / stale intentionally omitted (serve-time, see docstring).
        },
    }


def fetch_shakemap_contours(detail_url: str, *, timeout: float = 60.0) -> list[dict[str, Any]] | None:
    """Fetch a single event's MMI isoseismal contours via its ``properties.detail``
    URL → ``products.shakemap[0]`` → ``contents["download/cont_mmi.json"]``.

    Returns the contour Features (``MultiLineString`` / ``LineString`` with
    ``properties.value`` = MMI) verbatim, or None when the event has no usable
    ShakeMap contour product. Best-effort: any error yields None (the epicenter
    still ships without bands) rather than failing the run. We never fetch
    ``grid.xml`` (~28 MB); only the small ``cont_mmi.json``."""
    try:
        detail = http_retry.get(detail_url, timeout=timeout).json()
        products = ((detail.get("properties") or {}).get("products") or {})
        shakemaps = products.get("shakemap") or []
        if not shakemaps:
            return None
        contents = (shakemaps[0].get("contents") or {})
        cont = contents.get("download/cont_mmi.json") or {}
        cont_url = cont.get("url")
        if not cont_url:
            return None
        fc = http_retry.get(cont_url, timeout=timeout).json()
        feats = [f for f in (fc.get("features") or []) if isinstance(f, dict)]
        return feats or None
    except Exception as exc:  # noqa: BLE001 — contours are best-effort
        logger.warning("[USGS] ShakeMap contour fetch failed (%s): %s", detail_url, exc)
        return None


def build_seismic_collection(
    *,
    bbox: BBox,
    min_magnitude: float = DEFAULT_MIN_MAGNITUDE,
    window_days: int = DEFAULT_WINDOW_DAYS,
    start_time: str | None = None,
    fetch_contours: bool = True,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Fetch + slim one country's earthquakes into the stored blob shape.

    Re-fetches ``NOW - window_days`` each run (small volume; self-healing;
    satisfies "don't drop stale, age < window"). Attaches ShakeMap contours for
    ``has_shakemap`` events. ``bytes_in`` / ``bytes_out`` record the reduction.
    The blob is what lands in ``locationMetadata.data``; the serve route injects
    ``age_days`` / ``stale`` / ``feature_count`` at request time."""
    pulled_at = datetime.now(UTC).isoformat()
    start = start_time or (datetime.now(UTC) - timedelta(days=window_days)).isoformat()

    raw = fetch_earthquakes(bbox=bbox, min_magnitude=min_magnitude, start_time=start, timeout=timeout)
    bytes_in = len(json.dumps(raw, separators=(",", ":")).encode("utf-8"))

    features: list[dict[str, Any]] = []
    shakemaps: list[dict[str, Any]] = []
    fetched = 0
    for feat in raw:
        slim = slim_feature(feat)
        if slim is None:
            continue
        features.append(slim)
        if fetch_contours and slim["properties"]["has_shakemap"] and fetched < _MAX_SHAKEMAP_FETCH:
            detail_url = (feat.get("properties") or {}).get("detail")
            if detail_url:
                fetched += 1
                contours = fetch_shakemap_contours(detail_url, timeout=timeout)
                if contours:
                    shakemaps.append({
                        "eventId": slim["id"],
                        "type": "FeatureCollection",
                        "features": contours,
                    })

    blob: dict[str, Any] = {
        "source": "usgs-ingest",
        "pulled_at": pulled_at,
        "min_magnitude": min_magnitude,
        "window_days": window_days,
        "bbox": list(bbox),
        "features": features,
        "shakemaps": shakemaps,
    }
    bytes_out = len(json.dumps(blob, separators=(",", ":")).encode("utf-8"))
    blob["bytes_in"] = bytes_in
    blob["bytes_out"] = bytes_out
    blob["reduction_ratio"] = round(1 - (bytes_out / bytes_in), 3) if bytes_in else 0.0
    return blob


def pad_bbox(bbox: BBox, degrees: float = 2.5) -> BBox:
    """Expand a country bbox by ``degrees`` on each side so adjacent-plate events
    still paint (spec: ~2.5° past borders). Clamped to valid lat/lng ranges."""
    min_lng, min_lat, max_lng, max_lat = bbox
    return (
        max(-180.0, min_lng - degrees), max(-90.0, min_lat - degrees),
        min(180.0, max_lng + degrees), min(90.0, max_lat + degrees),
    )
