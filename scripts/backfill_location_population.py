"""
Standalone runner for locations.population backfill.

Runs synchronously in-process — no Celery worker needed. Fetches every
location at the target admin levels, runs the WorldPop raster mask against
its geometry, and writes the result back via updateLocationPopulation.

Usage:
    python scripts/backfill_location_population.py                # levels 0,1,2
    python scripts/backfill_location_population.py --levels 2     # level 2 only
    python scripts/backfill_location_population.py --levels 0,1   # two levels
    python scripts/backfill_location_population.py --force        # recompute even if populated
"""

import argparse
import logging
import sys
from pathlib import Path

# Ensure we can import from src/ when run from repo root or scripts/
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.clients import graphql  # noqa: E402
from src.services.population import estimate_population_for_polygon  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_levels(arg: str | None) -> list[int]:
    if not arg:
        return [0, 1, 2]
    try:
        return [int(x.strip()) for x in arg.split(",") if x.strip()]
    except ValueError:
        raise SystemExit(f"Invalid --levels value: {arg!r}")


def run(levels: list[int], force: bool) -> dict:
    """Backfill locations.population. Returns stats dict."""
    stats = {"processed": 0, "updated": 0, "skipped_cached": 0, "skipped_no_geom": 0, "failed": 0}

    for level in levels:
        locations = graphql.get_locations_by_level(level)
        logger.info("Level %d: %d locations", level, len(locations))

        for loc in locations:
            stats["processed"] += 1
            loc_id = loc["id"]
            loc_name = loc.get("name", "?")

            # Skip already-populated rows unless --force
            if not force and loc.get("population") is not None:
                stats["skipped_cached"] += 1
                logger.debug("[SKIP cached] %s (level=%d, pop=%s)", loc_name, level, loc["population"])
                continue

            detail = graphql.get_location_with_geometry(loc_id)
            if not detail or not detail.get("geometry"):
                stats["skipped_no_geom"] += 1
                logger.warning("[SKIP no geometry] %s (%s)", loc_name, loc_id)
                continue

            try:
                pop = estimate_population_for_polygon(detail["geometry"])
            except Exception as exc:
                stats["failed"] += 1
                logger.error("[FAILED raster] %s (%s): %s", loc_name, loc_id, exc)
                continue

            if pop is None:
                stats["failed"] += 1
                logger.warning("[FAILED estimate] %s (%s): raster returned None", loc_name, loc_id)
                continue

            try:
                graphql.update_location_population(loc_id, pop)
                stats["updated"] += 1
                logger.info("[OK] %s (level=%d): population=%d", loc_name, level, pop)
            except Exception as exc:
                stats["failed"] += 1
                logger.error("[FAILED update] %s (%s): %s", loc_name, loc_id, exc)

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill locations.population from WorldPop raster.")
    parser.add_argument(
        "--levels",
        default=None,
        help="Comma-separated admin levels to process (default: 0,1,2).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompute and overwrite even if population is already set.",
    )
    args = parser.parse_args()

    levels = parse_levels(args.levels)
    logger.info("Starting population backfill: levels=%s force=%s", levels, args.force)

    stats = run(levels, args.force)

    logger.info(
        "Done: processed=%(processed)d updated=%(updated)d "
        "skipped_cached=%(skipped_cached)d skipped_no_geom=%(skipped_no_geom)d "
        "failed=%(failed)d",
        stats,
    )


if __name__ == "__main__":
    main()
