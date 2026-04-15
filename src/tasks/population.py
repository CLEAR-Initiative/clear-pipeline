"""
Population computation Celery tasks.

Tasks:
  - compute_situation_population: compute populationInArea for a situation
    by summing the population of the given districts (via cached location
    population, falling back to raster estimation).
  - backfill_admin_geometries: fetch polygons from geoBoundaries CGAZ for
    admin levels 0/1/2 and upload via updateLocationGeometry.
  - backfill_location_population: run the raster against each location's
    geometry and cache the result on locations.population.
"""

import logging

from src.celery_app import app
from src.clients import graphql
from src.services.population import (
    estimate_population_for_districts,
    estimate_population_for_polygon,
)

logger = logging.getLogger(__name__)


@app.task(
    name="src.tasks.population.compute_situation_population",
    bind=True,
    max_retries=2,
    acks_late=True,
)
def compute_situation_population(
    self,
    situation_id: str,
    district_ids: list[str],
) -> dict:
    """Compute `populationInArea` for a situation from its district locations.

    Strategy:
      1. Fetch each district's cached `population` via GraphQL.
      2. If any are missing, fall back to raster-masking the union of district
         geometries.
      3. Write the result via updateSituationPopulation mutation.
    """
    logger.info(
        "[POPULATION] compute_situation_population: situation=%s districts=%d",
        situation_id, len(district_ids),
    )
    if not district_ids:
        logger.info("[POPULATION] No districts provided — skipping")
        return {"situation_id": situation_id, "population_in_area": None}

    try:
        # Try cached populations first
        cached_total = 0
        missing_geometries: list[dict] = []
        all_cached = True

        for did in district_ids:
            loc = graphql.get_location_with_geometry(did)
            if not loc:
                logger.warning("[POPULATION] District %s not found", did)
                continue

            pop_str = loc.get("population")
            if pop_str is not None:
                cached_total += int(pop_str)
            else:
                all_cached = False
                geometry = loc.get("geometry")
                if geometry:
                    missing_geometries.append(geometry)
                else:
                    logger.warning(
                        "[POPULATION] District %s has no geometry and no cached population",
                        did,
                    )

        if all_cached:
            population_in_area = cached_total
            logger.info(
                "[POPULATION] All %d districts cached: populationInArea=%d",
                len(district_ids), population_in_area,
            )
        else:
            # Raster-mask the union of missing geometries, then add cached total
            raster_pop = estimate_population_for_districts(missing_geometries)
            population_in_area = cached_total + (raster_pop or 0)
            logger.info(
                "[POPULATION] Mixed: cached=%d raster=%s → populationInArea=%d",
                cached_total, raster_pop, population_in_area,
            )

        graphql.update_situation_population(
            situation_id,
            population_in_area=population_in_area,
        )

        return {
            "situation_id": situation_id,
            "population_in_area": population_in_area,
        }

    except Exception as exc:
        logger.error(
            "[POPULATION] compute_situation_population failed for %s: %s",
            situation_id, exc, exc_info=True,
        )
        raise self.retry(exc=exc, countdown=30)


@app.task(
    name="src.tasks.population.backfill_location_population",
    bind=True,
    max_retries=1,
    acks_late=True,
)
def backfill_location_population(self, levels: list[int] | None = None) -> dict:
    """Backfill locations.population for all locations at the given admin levels.

    For each location, fetch its geometry, run the raster mask, and write the
    result via updateLocationPopulation.
    """
    target_levels = levels or [0, 1, 2]
    logger.info("[POPULATION] backfill_location_population: levels=%s", target_levels)

    processed = 0
    updated = 0
    skipped = 0

    try:
        for level in target_levels:
            locations = graphql.get_locations_by_level(level)
            logger.info("[POPULATION] Level %d: %d locations", level, len(locations))

            for loc in locations:
                processed += 1
                loc_id = loc["id"]
                loc_name = loc.get("name", "?")

                detail = graphql.get_location_with_geometry(loc_id)
                if not detail or not detail.get("geometry"):
                    logger.warning(
                        "[POPULATION] Skipping %s (%s): no geometry",
                        loc_name, loc_id,
                    )
                    skipped += 1
                    continue

                pop = estimate_population_for_polygon(detail["geometry"])
                if pop is None:
                    logger.warning(
                        "[POPULATION] Skipping %s (%s): raster estimate failed",
                        loc_name, loc_id,
                    )
                    skipped += 1
                    continue

                graphql.update_location_population(loc_id, pop)
                updated += 1
                logger.info(
                    "[POPULATION] Updated %s (level=%d): population=%d",
                    loc_name, level, pop,
                )

        logger.info(
            "[POPULATION] backfill_location_population done: processed=%d updated=%d skipped=%d",
            processed, updated, skipped,
        )
        return {"processed": processed, "updated": updated, "skipped": skipped}

    except Exception as exc:
        logger.error(
            "[POPULATION] backfill_location_population failed: %s",
            exc, exc_info=True,
        )
        raise self.retry(exc=exc, countdown=60)
