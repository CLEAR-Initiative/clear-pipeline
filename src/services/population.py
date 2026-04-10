"""Population estimation from WorldPop GeoTIFF raster data.

Uses a country-clipped population GeoTIFF (100m resolution) to estimate
the number of people within a circular area around a point.

The GeoTIFF is downloaded from S3 on first use and cached locally.
For Sudan: WorldPop 2026 constrained 100m grid.
"""

import logging
import math
import os
import tempfile
from pathlib import Path

import numpy as np
import rasterio
from rasterio.mask import mask as raster_mask
from shapely.geometry import Point, mapping

from src.config import settings

logger = logging.getLogger(__name__)

# Local cache directory for GeoTIFF files
_CACHE_DIR = Path(tempfile.gettempdir()) / "clear_population_data"

# Country ISO3 → S3 key for population GeoTIFF
POPULATION_TIFF_S3_KEYS: dict[str, str] = {
    "SDN": "population/sdn_pop_2026_CN_100m_R2025A_v1.tif",
}

# Default radius in km when signal doesn't provide one
DEFAULT_RADIUS_KM = 1.0


def _get_s3_client():
    """Get a boto3 S3 client configured for the pipeline's S3 storage."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
    )


def _ensure_geotiff(iso3: str) -> Path:
    """Ensure the population GeoTIFF for a country is available locally.

    Downloads from S3 if not already cached.
    """
    iso3 = iso3.upper()
    s3_key = POPULATION_TIFF_S3_KEYS.get(iso3)
    if not s3_key:
        raise FileNotFoundError(f"No population GeoTIFF configured for country {iso3}")

    local_path = _CACHE_DIR / iso3 / "population.tif"

    if local_path.is_file():
        logger.debug("[POPULATION] GeoTIFF already cached at %s", local_path)
        return local_path

    # Download from S3
    os.makedirs(local_path.parent, exist_ok=True)
    logger.info("[POPULATION] Downloading GeoTIFF from S3: %s → %s", s3_key, local_path)

    s3 = _get_s3_client()
    s3.download_file(settings.s3_bucket, s3_key, str(local_path))

    logger.info("[POPULATION] GeoTIFF downloaded: %s (%.1f MB)", local_path, local_path.stat().st_size / 1e6)
    return local_path


def _circle_polygon(lat: float, lng: float, radius_km: float, n_points: int = 64):
    """Create a circle polygon in WGS84 around a point.

    Uses a simple equirectangular approximation which is accurate enough
    for small radii (< 50 km) near the equator and mid-latitudes.
    """
    # Convert radius from km to degrees (approximate)
    # 1 degree latitude ≈ 111.32 km
    lat_offset = radius_km / 111.32
    # 1 degree longitude ≈ 111.32 * cos(lat) km
    lng_offset = radius_km / (111.32 * math.cos(math.radians(lat)))

    coords = []
    for i in range(n_points):
        angle = 2 * math.pi * i / n_points
        coords.append((
            lng + lng_offset * math.cos(angle),
            lat + lat_offset * math.sin(angle),
        ))
    coords.append(coords[0])  # close the ring

    from shapely.geometry import Polygon
    return Polygon(coords)


def _sum_valid_pixels(arr: np.ndarray, nodata: float | int | None) -> float:
    """Sum finite pixels, excluding nodata values."""
    m = np.isfinite(arr)
    if nodata is not None:
        if isinstance(nodata, float) and np.isnan(nodata):
            m &= ~np.isnan(arr)
        else:
            m &= arr != nodata
    return float(np.sum(arr[m]))


def estimate_population_from_raster(
    lat: float,
    lng: float,
    radius_km: float,
    iso3: str = "SDN",
) -> int | None:
    """Estimate population within a circular area using GeoTIFF raster data.

    Args:
        lat: Latitude of the center point.
        lng: Longitude of the center point.
        radius_km: Radius in kilometers.
        iso3: Country ISO3 code (default: SDN for Sudan).

    Returns:
        Estimated population as integer, or None if estimation fails.
    """
    try:
        tiff_path = _ensure_geotiff(iso3)
    except FileNotFoundError as e:
        logger.warning("[POPULATION] %s", e)
        return None
    except Exception as e:
        logger.error("[POPULATION] Failed to get GeoTIFF: %s", e)
        return None

    try:
        circle = _circle_polygon(lat, lng, radius_km)
        circle_geojson = mapping(circle)

        with rasterio.open(tiff_path) as src:
            nodata = src.nodata

            # Mask the raster with the circle polygon
            out, _ = raster_mask(
                src,
                [circle_geojson],
                crop=True,
                indexes=1,
                filled=True,
            )
            region_arr = out[0] if out.ndim == 3 else out
            pop = _sum_valid_pixels(region_arr, nodata)

        estimate = max(int(round(pop)), 0)
        logger.info(
            "[POPULATION] Estimated %d people within %.1f km of (%.4f, %.4f)",
            estimate, radius_km, lat, lng,
        )
        return estimate

    except Exception as e:
        logger.error("[POPULATION] Raster estimation failed: %s", e, exc_info=True)
        return None


def estimate_population_for_signal(
    lat: float | None,
    lng: float | None,
    probability_radius_km: float | None = None,
    iso3: str = "SDN",
) -> int | None:
    """Estimate population affected by a signal.

    Strategy:
    1. If lat/lng not available, return None.
    2. If probabilityRadius is provided (e.g. from Dataminr), use it.
    3. Otherwise, use DEFAULT_RADIUS_KM (1 km).
    4. Estimate population within that circle from GeoTIFF data.

    Args:
        lat: Signal latitude.
        lng: Signal longitude.
        probability_radius_km: Radius from the data source (e.g. Dataminr), in km.
        iso3: Country ISO3 code.

    Returns:
        Estimated population or None.
    """
    if lat is None or lng is None:
        return None

    radius = probability_radius_km if probability_radius_km else DEFAULT_RADIUS_KM

    return estimate_population_from_raster(lat, lng, radius, iso3=iso3)


def estimate_population_for_event(
    signals: list[dict],
    iso3: str = "SDN",
) -> int | None:
    """Estimate population affected by an event based on its signals.

    Strategy:
    1. If any signals have populationAffected data, sum them up.
    2. Otherwise, estimate from GeoTIFF for each signal's location and take the max
       (signals in the same event may overlap geographically).

    Args:
        signals: List of signal dicts with lat, lng, probabilityRadius, populationAffected.
        iso3: Country ISO3 code.

    Returns:
        Estimated population or None.
    """
    if not signals:
        return None

    # Strategy 1: Sum up known population data from signals
    known_pop = 0
    has_known = False
    for s in signals:
        pop = s.get("populationAffected") or s.get("population_affected")
        if pop is not None:
            known_pop += int(pop)
            has_known = True

    if has_known and known_pop > 0:
        logger.info("[POPULATION] Event population from signal data: %d", known_pop)
        return known_pop

    # Strategy 2: Estimate from GeoTIFF for each signal location
    estimates = []
    for s in signals:
        lat = s.get("lat")
        lng = s.get("lng")
        if lat is None or lng is None:
            continue

        radius = s.get("probabilityRadius") or s.get("probability_radius_km") or DEFAULT_RADIUS_KM
        est = estimate_population_from_raster(float(lat), float(lng), float(radius), iso3=iso3)
        if est is not None:
            estimates.append(est)

    if estimates:
        # Take the max rather than sum — signals in the same event often refer to
        # overlapping or the same affected area
        result = max(estimates)
        logger.info("[POPULATION] Event population from raster (max of %d signals): %d", len(estimates), result)
        return result

    return None
