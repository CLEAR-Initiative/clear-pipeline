"""
Identify duplicate / triplicate / N-plicate pCodes in the CLEAR locations table.

After running the various pCode backfills (spatial + fuzzy-name + missing
admin creation), some pCodes may end up on more than one location row — for
example if the seeded CLEAR row and a newly-created OCHA row both ended up
with the same pCode, or if fuzzy-matching assigned the same pCode to two
different seeded districts.

This script:
  1. Fetches every CLEAR location across all admin levels.
  2. Groups by pCode.
  3. Prints each group with count >= 2, showing id/level/name/parent so you
     can decide which row to keep.

Usage:
    python scripts/find_duplicate_pcodes.py                 # default
    python scripts/find_duplicate_pcodes.py --levels 1,2    # restrict to admin levels
    python scripts/find_duplicate_pcodes.py --min-count 3   # only triplicates+
    python scripts/find_duplicate_pcodes.py --json          # machine-readable
"""

import argparse
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.clients import graphql  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_levels(arg: str | None) -> list[int]:
    if not arg:
        return [0, 1, 2, 3, 4]
    try:
        return [int(x.strip()) for x in arg.split(",") if x.strip()]
    except ValueError:
        raise SystemExit(f"Invalid --levels value: {arg!r}")


def run(levels: list[int], min_count: int) -> dict:
    """Return { pCode: [rows] } for groups with len >= min_count."""
    pcode_groups: dict[str, list[dict]] = defaultdict(list)
    total = 0
    no_pcode = 0

    for level in levels:
        locations = graphql.get_locations_by_level(level)
        logger.info("Level %d: fetched %d locations", level, len(locations))
        for loc in locations:
            total += 1
            pcode = loc.get("pCode")
            if not pcode:
                no_pcode += 1
                continue
            pcode_groups[pcode].append(loc)

    logger.info(
        "Scanned %d locations (%d with pCode, %d without)",
        total, total - no_pcode, no_pcode,
    )

    dupes = {pc: rows for pc, rows in pcode_groups.items() if len(rows) >= min_count}
    return dupes


def print_text_report(dupes: dict[str, list[dict]], min_count: int) -> None:
    if not dupes:
        print(f"\nNo pCodes with count >= {min_count} found. All good.\n")
        return

    # Sort by count desc, then pCode
    sorted_groups = sorted(dupes.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    grand_total = sum(len(rows) for rows in dupes.values())

    print("\n" + "=" * 78)
    print(
        f"DUPLICATE pCODES — {len(dupes)} pCode(s) across "
        f"{grand_total} location rows (min_count={min_count})"
    )
    print("=" * 78)

    for pcode, rows in sorted_groups:
        label = {2: "DUPLICATE", 3: "TRIPLICATE"}.get(len(rows), f"{len(rows)}-PLICATE")
        print(f"\n  pCode={pcode}  [{label}, count={len(rows)}]")
        # Sort rows by level (shallower first), then name for readability
        for r in sorted(rows, key=lambda x: (x.get("level", 99), x.get("name", ""))):
            print(
                f"    • level={r.get('level')}  "
                f"name={r.get('name', '?'):30s}  "
                f"id={r.get('id')}"
            )
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find locations sharing the same pCode.",
    )
    parser.add_argument(
        "--levels",
        default=None,
        help="Comma-separated admin levels to scan (default: 0,1,2,3,4).",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=2,
        help="Minimum duplicate count to report (2 = duplicates, 3 = triplicates+).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of a formatted report.",
    )
    args = parser.parse_args()

    levels = parse_levels(args.levels)
    logger.info(
        "Scanning for duplicate pCodes: levels=%s min_count=%d",
        levels, args.min_count,
    )

    dupes = run(levels, args.min_count)

    if args.json:
        print(json.dumps(dupes, indent=2, default=str))
    else:
        print_text_report(dupes, args.min_count)


if __name__ == "__main__":
    main()
