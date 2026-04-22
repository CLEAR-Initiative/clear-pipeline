"""
Backfill locations.pCode (and optionally canonical name) from OCHA HDX admin
boundaries, using spatial containment.

Why: CLEAR level-2 districts were seeded as Points with no pCode. This makes
pCode-based matching in `backfill_admin_geometries` impossible, and name
matching breaks on transliteration variants (e.g. "Jebel Aulia" vs "Jabal
Awliya"). Coordinates don't lie — a Point either lies inside an OCHA polygon
or it doesn't. This script finds the enclosing polygon for each location and
copies its pCode.

Strategy per CLEAR location missing a pCode:
  1. Get its GeoJSON geometry (Point for most level-2, Polygon for seed states).
  2. Compute a representative point (Point → itself; Polygon → centroid).
  3. Find the OCHA feature at the SAME admin level whose polygon contains
     that point.
  4. Copy the OCHA `adm{N}_pcode` onto the CLEAR location.

Run:
    python scripts/backfill_location_pcodes.py                       # all levels, spatial only
    python scripts/backfill_location_pcodes.py --levels 2            # districts only
    python scripts/backfill_location_pcodes.py --rename              # also adopt OCHA's name
    python scripts/backfill_location_pcodes.py --dry-run             # print plan, no writes
    python scripts/backfill_location_pcodes.py --name-fallback       # fuzzy-name for unmatched
    python scripts/backfill_location_pcodes.py --name-fallback --fuzzy-cutoff 0.6

A summary of UNMATCHED locations is printed at the end. Rerun with
--name-fallback to retry those via fuzzy name matching (automatically
adopts OCHA's canonical name when the fuzzy match wins).
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


def representative_point(geometry: dict):
    """Return a shapely Point representing a GeoJSON geometry.

    - Point → itself
    - Polygon/MultiPolygon → centroid
    """
    from shapely.geometry import shape

    geom = shape(geometry)
    if geom.is_empty:
        return None
    if geom.geom_type == "Point":
        return geom
    # For polygons, representative_point() is guaranteed to be inside; use it
    # instead of centroid which may fall outside concave shapes.
    return geom.representative_point()


def _normalise_name(name: str) -> str:
    """Lowercase + strip punctuation/common prefixes for fuzzy name matching."""
    import re

    s = name.strip().lower()
    # Common transliteration prefixes
    for prefix in ("el-", "el ", "al-", "al ", "as ", "ash "):
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s


def _fuzzy_name_match(
    clear_name: str,
    features: list[dict],
    cutoff: float = 0.7,
) -> dict | None:
    """Find the closest OCHA feature by name using difflib. Returns the best
    match dict (with "pcode" and "name") or None if no candidate is above cutoff."""
    from difflib import SequenceMatcher

    target = _normalise_name(clear_name)
    if not target:
        return None

    best = None
    best_score = 0.0
    for f in features:
        if not f.get("name") or not f.get("pcode"):
            continue
        score = SequenceMatcher(None, target, _normalise_name(f["name"])).ratio()
        if score > best_score:
            best_score = score
            best = f
    return best if best_score >= cutoff else None


def run(
    levels: list[int],
    rename: bool,
    dry_run: bool,
    iso3: str = "SDN",
    name_fallback: bool = False,
    fuzzy_cutoff: float = 0.7,
) -> dict:
    from shapely.geometry import shape
    from shapely.prepared import prep

    stats = {
        "processed": 0,
        "matched_spatial": 0,
        "matched_fuzzy_name": 0,
        "updated": 0,
        "skipped_has_pcode": 0,
        "skipped_no_geom": 0,
        "no_match": 0,
        "failed": 0,
    }
    unmatched: list[dict] = []  # collected across all levels, reported at end

    logger.info("Downloading OCHA admin boundaries for %s…", iso3)
    level_to_features = _download_and_extract(iso3)

    for level in levels:
        features = level_to_features.get(level, [])
        if not features:
            logger.warning("No OCHA features for level %d — skipping", level)
            continue

        # Pre-prepare polygons for fast contains() checks + keep feature metadata
        prepared = []
        feature_meta = []  # parallel list used for name-fallback lookup
        for feat in features:
            geom = feat.get("geometry")
            if not geom:
                continue
            try:
                shapely_geom = shape(geom)
                if shapely_geom.is_empty:
                    continue
                pcode, name = _feature_properties(feat, level)
                entry = {"pcode": pcode, "name": name}
                prepared.append({"prepared": prep(shapely_geom), **entry})
                feature_meta.append(entry)
            except Exception as e:
                logger.warning("Could not parse OCHA feature: %s", e)

        logger.info("Level %d: %d OCHA polygons ready", level, len(prepared))

        clear_locations = graphql.get_locations_by_level(level)
        logger.info("Level %d: %d CLEAR locations", level, len(clear_locations))

        for loc in clear_locations:
            stats["processed"] += 1
            loc_id = loc["id"]
            loc_name = loc.get("name", "?")

            if loc.get("pCode"):
                stats["skipped_has_pcode"] += 1
                continue

            detail = graphql.get_location_with_geometry(loc_id)
            if not detail or not detail.get("geometry"):
                stats["skipped_no_geom"] += 1
                logger.warning("[SKIP no geometry] %s (%s)", loc_name, loc_id)
                continue

            point = representative_point(detail["geometry"])
            if point is None:
                stats["skipped_no_geom"] += 1
                logger.warning("[SKIP empty geom] %s (%s)", loc_name, loc_id)
                continue

            # ── Pass 1: Spatial containment ─────────────────────────────
            match = None
            match_mode = None
            for p in prepared:
                if p["pcode"] and p["prepared"].contains(point):
                    match = {"pcode": p["pcode"], "name": p["name"]}
                    match_mode = "spatial"
                    break

            # ── Pass 2: Fuzzy name fallback (if enabled) ────────────────
            if not match and name_fallback:
                fuzzy = _fuzzy_name_match(loc_name, feature_meta, cutoff=fuzzy_cutoff)
                if fuzzy:
                    match = fuzzy
                    match_mode = "fuzzy-name"

            if not match:
                stats["no_match"] += 1
                unmatched.append({
                    "level": level,
                    "id": loc_id,
                    "name": loc_name,
                    "point": (point.x, point.y),
                })
                logger.warning(
                    "[NO MATCH] level=%d %s (%s) at (%.4f, %.4f)",
                    level, loc_name, loc_id, point.x, point.y,
                )
                continue

            if match_mode == "spatial":
                stats["matched_spatial"] += 1
            else:
                stats["matched_fuzzy_name"] += 1

            # For fuzzy-name matches, always adopt OCHA's name (that IS the match signal).
            # For spatial matches, only rename if --rename is set.
            should_rename = match_mode == "fuzzy-name" or rename
            new_name = match["name"] if should_rename and match["name"] else None

            if dry_run:
                logger.info(
                    "[DRY-RUN via %s] %s (%s) → pCode=%s%s",
                    match_mode, loc_name, loc_id, match["pcode"],
                    f" name='{new_name}'" if new_name else "",
                )
                continue

            try:
                update_fields = {"pCode": match["pcode"]}
                if new_name:
                    update_fields["name"] = new_name
                graphql.update_location(loc_id, **update_fields)
                stats["updated"] += 1
                logger.info(
                    "[OK via %s] %s (%s) → pCode=%s%s",
                    match_mode, loc_name, loc_id, match["pcode"],
                    f" → renamed to '{new_name}'" if new_name else "",
                )
            except Exception as e:
                stats["failed"] += 1
                logger.error("[FAILED] %s (%s): %s", loc_name, loc_id, e)

    # ── Unmatched summary ──────────────────────────────────────────────
    if unmatched:
        print("\n" + "=" * 70)
        print(f"UNMATCHED LOCATIONS ({len(unmatched)}) — no spatial or name match:")
        print("=" * 70)
        for u in unmatched:
            print(
                f"  level={u['level']}  name={u['name']!r:40s} "
                f"point=({u['point'][0]:.4f}, {u['point'][1]:.4f})  id={u['id']}"
            )
        print()
        if not name_fallback:
            print("Tip: rerun with --name-fallback to try fuzzy name matching for these.")
        else:
            print("These still failed after fuzzy matching. Likely causes:")
            print("  - Point coords outside the country's OCHA boundaries (wrong seed data)")
            print("  - Name too different from OCHA canonical name (tighten --fuzzy-cutoff)")
            print("Fix manually via the location(id) admin mutation.")
        print()

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill locations.pCode via spatial containment.")
    parser.add_argument("--levels", default=None, help="Comma-separated admin levels (default: 0,1,2).")
    parser.add_argument("--iso3", default="SDN", help="Country ISO3 code (default: SDN).")
    parser.add_argument(
        "--rename",
        action="store_true",
        help="Also overwrite the CLEAR name with OCHA's canonical name on spatial matches.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without writing.")
    parser.add_argument(
        "--name-fallback",
        action="store_true",
        help="For locations with no spatial match, try fuzzy name matching. "
             "When this wins, the OCHA name is always adopted (since the name "
             "itself is the match signal).",
    )
    parser.add_argument(
        "--fuzzy-cutoff",
        type=float,
        default=0.7,
        help="Minimum similarity ratio (0-1) to accept a fuzzy name match (default: 0.7).",
    )
    args = parser.parse_args()

    levels = parse_levels(args.levels)
    logger.info(
        "Starting pCode backfill: iso3=%s levels=%s rename=%s name_fallback=%s "
        "fuzzy_cutoff=%.2f dry_run=%s",
        args.iso3, levels, args.rename, args.name_fallback,
        args.fuzzy_cutoff, args.dry_run,
    )

    stats = run(
        levels,
        args.rename,
        args.dry_run,
        iso3=args.iso3,
        name_fallback=args.name_fallback,
        fuzzy_cutoff=args.fuzzy_cutoff,
    )

    logger.info("Done: %s", stats)


if __name__ == "__main__":
    main()
