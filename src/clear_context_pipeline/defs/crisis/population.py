"""populationInArea computation for the crisis drain.

Ported from clear-pipeline's ``tasks/crisis.py``. Sums cached
``locations.population``; falls back to WorldPop raster masking for areal
locations that have no cached figure, and walks up to a parent location when a
district is a point (level-4) or has no usable geometry. De-duplicates by
resolved location id so a shared parent isn't summed twice.

Best-effort: any failure returns ``None`` (the crisis's ``populationAffected``
stays as-is) — population must never block enrichment.
"""

import logging

from clear_context_pipeline.providers import clear_api
from clear_context_pipeline.providers.population import (
    estimate_population_for_districts,
)

logger = logging.getLogger(__name__)


def _geometry_is_areal(geometry: dict | None) -> bool:
    """Only Polygon/MultiPolygon geometries can be raster-masked meaningfully.
    Point locations (level 4) produce near-zero population and should fall back."""
    if not geometry:
        return False
    return geometry.get("type") in ("Polygon", "MultiPolygon")


def _resolve_location_for_population(loc: dict) -> dict | None:
    """Return a location dict that has either a cached population OR an areal
    geometry. If the given location is a point (or has no geometry and no cached
    population), walk up to its parent. Returns None if no usable ancestor is
    found."""
    current = loc
    while current is not None:
        has_cached = current.get("population") is not None
        has_areal = _geometry_is_areal(current.get("geometry"))
        if has_cached or has_areal:
            return current

        parent_stub = current.get("parent")
        if not parent_stub:
            return None
        logger.info(
            "[crisis:population] Location %s (%s, level=%s) has no cached "
            "population or areal geometry — falling back to parent %s",
            current.get("name"), current.get("id"), current.get("level"),
            parent_stub.get("name"),
        )
        current = clear_api.get_location_with_geometry(parent_stub["id"])
    return None


def compute_population_in_area(district_ids: list[str]) -> int | None:
    """Sum cached location.population; fall back to raster for missing areals,
    and fall back to parent location when a district is a point or has no usable
    geometry. De-duplicates by resolved location id so shared parents aren't
    summed twice. Returns None when nothing usable resolves."""
    if not district_ids:
        return None

    resolved_by_id: dict[str, dict] = {}
    for did in district_ids:
        loc = clear_api.get_location_with_geometry(did)
        if not loc:
            logger.warning("[crisis:population] District %s not found", did)
            continue

        resolved = _resolve_location_for_population(loc)
        if not resolved:
            logger.warning(
                "[crisis:population] No usable ancestor for district %s (%s)",
                loc.get("name"), did,
            )
            continue

        # De-duplicate: if two districts resolved to the same state, count once.
        resolved_by_id[resolved["id"]] = resolved

    if not resolved_by_id:
        logger.warning("[crisis:population] No usable locations resolved")
        return None

    cached_total = 0
    missing_geometries: list[dict] = []
    for loc in resolved_by_id.values():
        pop_str = loc.get("population")
        if pop_str is not None:
            cached_total += int(pop_str)
        elif _geometry_is_areal(loc.get("geometry")):
            missing_geometries.append(loc["geometry"])

    if not missing_geometries:
        logger.info(
            "[crisis:population] All %d resolved locations cached: populationInArea=%d",
            len(resolved_by_id), cached_total,
        )
        return cached_total

    raster_pop = estimate_population_for_districts(missing_geometries) or 0
    total = cached_total + raster_pop
    logger.info(
        "[crisis:population] Mixed (%d resolved): cached=%d raster=%d → populationInArea=%d",
        len(resolved_by_id), cached_total, raster_pop, total,
    )
    return total
