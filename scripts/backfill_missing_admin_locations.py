"""
Create CLEAR location rows for OCHA admin boundaries whose pCode is not yet
present in the locations table.

For each OCHA feature at the target admin levels:
  - If a CLEAR location with the same pCode already exists → skip.
  - Otherwise:
      1. Resolve the parent (level N-1) in CLEAR by the OCHA parent pCode
         (adm{N-1}_pcode). If no parent, skip with a warning.
      2. Create the new CLEAR location at level N with pCode + name.
      3. Set its geometry to the OCHA polygon.
      4. Optionally (with --with-centroid-points) also create a level-4 point
         location at the polygon's representative point, child of the new row.

Usage:
    python scripts/backfill_missing_admin_locations.py                 # levels 0,1,2
    python scripts/backfill_missing_admin_locations.py --levels 2      # districts only
    python scripts/backfill_missing_admin_locations.py --with-centroid-points
    python scripts/backfill_missing_admin_locations.py --dry-run       # print plan only
"""

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.clients import graphql  # noqa: E402
from src.tasks.geometries import _download_and_extract, _feature_properties  # noqa: E402

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


def _parent_pcode_from_feature(feat: dict, level: int) -> str | None:
    """Read the parent admin's pCode from OCHA feature properties."""
    if level == 0:
        return None
    props = feat.get("properties", {}) or {}
    return props.get(f"adm{level - 1}_pcode")


def run(
    levels: list[int],
    with_centroid_points: bool,
    dry_run: bool,
    iso3: str = "SDN",
) -> dict:
    from shapely.geometry import shape

    stats = {
        "features_seen": 0,
        "skipped_existing": 0,
        "skipped_no_parent": 0,
        "skipped_bad_geom": 0,
        "created_polygon": 0,
        "created_centroid": 0,
        "failed": 0,
    }

    logger.info("Downloading OCHA admin boundaries for %s…", iso3)
    level_to_features = _download_and_extract(iso3)

    # Build pCode → CLEAR location_id lookup once for ALL levels (so we can
    # resolve parents for deeper levels quickly).
    pcode_to_clear_id: dict[str, str] = {}
    for lvl in (0, 1, 2, 3):
        for loc in graphql.get_locations_by_level(lvl):
            if loc.get("pCode"):
                pcode_to_clear_id[loc["pCode"]] = loc["id"]
    logger.info("Loaded %d existing CLEAR locations with pCode", len(pcode_to_clear_id))

    # Process in increasing order of level so parents are created/known first.
    for level in sorted(levels):
        features = level_to_features.get(level, [])
        if not features:
            logger.warning("No OCHA features for level %d — skipping", level)
            continue
        logger.info("Level %d: %d OCHA features", level, len(features))

        for feat in features:
            stats["features_seen"] += 1
            p_code, name = _feature_properties(feat, level)
            geometry = feat.get("geometry")

            if not p_code or not name or not geometry:
                stats["skipped_bad_geom"] += 1
                logger.warning("[SKIP] incomplete feature: pcode=%s name=%s", p_code, name)
                continue

            if p_code in pcode_to_clear_id:
                stats["skipped_existing"] += 1
                continue

            # Resolve parent via OCHA adm{N-1}_pcode → CLEAR location_id
            parent_id: str | None = None
            if level > 0:
                parent_pcode = _parent_pcode_from_feature(feat, level)
                if not parent_pcode:
                    stats["skipped_no_parent"] += 1
                    logger.warning(
                        "[SKIP no parent pcode] level=%d pcode=%s name=%s",
                        level, p_code, name,
                    )
                    continue
                parent_id = pcode_to_clear_id.get(parent_pcode)
                if not parent_id:
                    stats["skipped_no_parent"] += 1
                    logger.warning(
                        "[SKIP parent not in CLEAR] level=%d pcode=%s name=%s parent_pcode=%s "
                        "— consider running for level %d first",
                        level, p_code, name, parent_pcode, level - 1,
                    )
                    continue

            if dry_run:
                logger.info(
                    "[DRY-RUN] create level=%d pcode=%s name=%r parent_id=%s%s",
                    level, p_code, name, parent_id,
                    " + centroid point" if with_centroid_points else "",
                )
                continue

            # ── Create the boundary row ─────────────────────────────────
            try:
                created = graphql.create_location(
                    name=name,
                    level=level,
                    pCode=p_code,
                    parentId=parent_id,
                )
                new_id = created["id"]
                pcode_to_clear_id[p_code] = new_id  # so deeper levels can find it

                graphql.update_location_geometry(new_id, geometry)
                stats["created_polygon"] += 1
                logger.info(
                    "[OK polygon] level=%d pcode=%s name=%r → %s",
                    level, p_code, name, new_id,
                )
            except Exception as e:
                stats["failed"] += 1
                logger.error(
                    "[FAILED polygon] level=%d pcode=%s name=%r: %s",
                    level, p_code, name, e,
                )
                continue

            # ── Optional: create a centroid point child ─────────────────
            if with_centroid_points:
                try:
                    shapely_geom = shape(geometry)
                    if shapely_geom.is_empty:
                        continue
                    rep = shapely_geom.representative_point()
                    point_geojson = {
                        "type": "Point",
                        "coordinates": [rep.x, rep.y],
                    }
                    # Use pCode suffix to differentiate from the boundary row
                    centroid = graphql.create_location(
                        name=f"{name} (centroid)",
                        level=4,
                        pCode=f"{p_code}_PT",
                        parentId=new_id,
                    )
                    graphql.update_location_geometry(centroid["id"], point_geojson)
                    stats["created_centroid"] += 1
                    logger.info(
                        "[OK centroid] pcode=%s_PT → %s at (%.4f, %.4f)",
                        p_code, centroid["id"], rep.x, rep.y,
                    )
                except Exception as e:
                    stats["failed"] += 1
                    logger.error(
                        "[FAILED centroid] pcode=%s_PT: %s",
                        p_code, e,
                    )

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create CLEAR locations for OCHA admin boundaries not yet in the DB.",
    )
    parser.add_argument("--levels", default=None, help="Comma-separated admin levels (default: 0,1,2).")
    parser.add_argument("--iso3", default="SDN", help="Country ISO3 code (default: SDN).")
    parser.add_argument(
        "--with-centroid-points",
        action="store_true",
        help="Also create a level-4 point location (centroid) as a child of each new boundary.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without writing.")
    args = parser.parse_args()

    levels = parse_levels(args.levels)
    logger.info(
        "Starting: iso3=%s levels=%s with_centroid_points=%s dry_run=%s",
        args.iso3, levels, args.with_centroid_points, args.dry_run,
    )

    stats = run(levels, args.with_centroid_points, args.dry_run, iso3=args.iso3)

    logger.info("Done: %s", stats)


if __name__ == "__main__":
    main()
