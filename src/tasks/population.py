"""
Population backfill Celery tasks.

Tasks:
  - backfill_location_population: run the raster against each location's
    geometry and cache the result on locations.population.

Note: situation population is computed inside `src.tasks.situation.enrich_situation`,
which also handles title/summary narrative generation.
"""

import logging

from src.celery_app import app
from src.clients import graphql
from src.services.population import estimate_population_for_polygon

logger = logging.getLogger(__name__)


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
