"""
Synchronous runner for `backfill_admin_geometries` — same logic as the Celery
task but runs in-process, so you don't need a worker and can target specific
admin levels.

Matching order per OCHA feature:
  1. pCode (exact match vs locations.pCode)
  2. Normalised name (fallback)

Unmatched features are printed in an end-of-run summary with coords.

Usage:
    python scripts/backfill_admin_geometries_sync.py                # levels 0,1,2
    python scripts/backfill_admin_geometries_sync.py --levels 0     # country only
    python scripts/backfill_admin_geometries_sync.py --levels 2     # districts only
    python scripts/backfill_admin_geometries_sync.py --dry-run      # print, no writes
"""

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.clients import graphql  # noqa: E402
from src.tasks.geometries import (  # noqa: E402
    _download_and_extract,
    _feature_properties,
    _normalise_name,
)

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


def run(levels: list[int], dry_run: bool, iso3: str = "SDN") -> dict:
    stats = {
        "matched_by_pcode": 0,
        "matched_by_name": 0,
        "unmatched": 0,
        "skipped_no_geometry": 0,
        "failed": 0,
    }
    unmatched: list[dict] = []

    logger.info("Downloading OCHA admin boundaries for %s…", iso3)
    level_to_features = _download_and_extract(iso3)

    for level in levels:
        features = level_to_features.get(level, [])
        if not features:
            logger.warning("No OCHA features for level %d — skipping", level)
            continue

        clear_locations = graphql.get_locations_by_level(level)
        if not clear_locations:
            logger.warning("No CLEAR locations at level %d — skipping", level)
            continue

        # Lookups: pCode → id, normalised name → id
        pcode_to_id: dict[str, str] = {}
        name_to_id: dict[str, str] = {}
        for loc in clear_locations:
            if loc.get("pCode"):
                pcode_to_id[loc["pCode"]] = loc["id"]
            name_to_id[_normalise_name(loc["name"])] = loc["id"]

        logger.info(
            "Level %d: %d OCHA features vs %d CLEAR locations (%d with pCode)",
            level, len(features), len(clear_locations), len(pcode_to_id),
        )

        for feat in features:
            p_code, name = _feature_properties(feat, level)
            geometry = feat.get("geometry")

            if not geometry:
                stats["skipped_no_geometry"] += 1
                continue

            loc_id: str | None = None
            match_mode: str | None = None

            if p_code and p_code in pcode_to_id:
                loc_id = pcode_to_id[p_code]
                match_mode = "pcode"
            elif name and _normalise_name(name) in name_to_id:
                loc_id = name_to_id[_normalise_name(name)]
                match_mode = "name"

            if not loc_id:
                unmatched.append({"level": level, "pcode": p_code, "name": name})
                stats["unmatched"] += 1
                continue

            if dry_run:
                logger.info(
                    "[DRY-RUN via %s] level=%d %s (pcode=%s) → %s",
                    match_mode, level, name, p_code, loc_id,
                )
                continue

            try:
                graphql.update_location_geometry(loc_id, geometry)
                if match_mode == "pcode":
                    stats["matched_by_pcode"] += 1
                else:
                    stats["matched_by_name"] += 1
                logger.info(
                    "[OK via %s] level=%d %s (pcode=%s) → %s",
                    match_mode, level, name, p_code, loc_id,
                )
            except Exception as e:
                stats["failed"] += 1
                logger.error(
                    "[FAILED] level=%d name=%s (%s): %s",
                    level, name, loc_id, e,
                )

    if unmatched:
        print("\n" + "=" * 70)
        print(f"UNMATCHED OCHA FEATURES ({len(unmatched)}):")
        print("=" * 70)
        for u in unmatched:
            print(f"  level={u['level']}  pcode={u['pcode']}  name={u['name']!r}")
        print()

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync runner for OCHA admin geometry backfill.")
    parser.add_argument("--levels", default=None, help="Comma-separated admin levels (default: 0,1,2).")
    parser.add_argument("--iso3", default="SDN", help="Country ISO3 code (default: SDN).")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without writing.")
    args = parser.parse_args()

    levels = parse_levels(args.levels)
    logger.info(
        "Starting: iso3=%s levels=%s dry_run=%s",
        args.iso3, levels, args.dry_run,
    )

    stats = run(levels, args.dry_run, iso3=args.iso3)
    logger.info("Done: %s", stats)


if __name__ == "__main__":
    main()
