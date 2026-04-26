"""
Standalone runner for IOM DTM displacement backfill.

Fetches the latest displaced-persons data from the IOM DTM API
(https://dtmapi.iom.int/v3/) at admin levels 0 (country), 1 (state), and 2
(district), then upserts into locationMetadata (type = "iom_dtm_displacement")
on clear-api. Writes are bulk — one network call per admin level.

Runs synchronously in-process — no Celery worker required.

Usage:
    python scripts/backfill_iom_dtm.py                      # all 3 levels
    python scripts/backfill_iom_dtm.py --levels 1,2         # skip country
    python scripts/backfill_iom_dtm.py --levels 2           # districts only
    python scripts/backfill_iom_dtm.py --dry-run            # fetch, match, print — no writes
    python scripts/backfill_iom_dtm.py --country "Sudan"
    python scripts/backfill_iom_dtm.py --admin0-pcode SDN
"""

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.clients import graphql, iom_dtm  # noqa: E402
from src.config import settings  # noqa: E402

METADATA_TYPE = "iom_dtm_displacement"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


_LEVEL_FETCH = {
    0: iom_dtm.fetch_admin0_displacement,
    1: iom_dtm.fetch_admin1_displacement,
    2: iom_dtm.fetch_admin2_displacement,
}


def _normalise_name(name: str) -> str:
    """Lowercase + strip punctuation for lenient name matching
    (e.g. "Sudan" vs "Republic of Sudan", "El Gezira" vs "Al Jazirah")."""
    import re

    s = (name or "").strip().lower()
    for prefix in ("republic of ", "the ", "el-", "el ", "al-", "al "):
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s


def parse_levels(arg: str | None) -> list[int]:
    if not arg:
        return [0, 1, 2]
    try:
        out = [int(x.strip()) for x in arg.split(",") if x.strip()]
    except ValueError:
        raise SystemExit(f"Invalid --levels value: {arg!r}")
    for lvl in out:
        if lvl not in (0, 1, 2):
            raise SystemExit(f"Unsupported admin level {lvl} — only 0, 1, 2 are valid.")
    return out


def run_level(
    admin_level: int,
    country_name: str,
    admin0_pcode: str,
    dry_run: bool,
) -> dict:
    stats = {
        "fetched": 0,
        "records_without_pcode": 0,
        "distinct": 0,
        "matched": 0,
        "upserted": 0,
        "skipped_no_value": 0,
        "unmatched_pcode": 0,
        "clear_total": 0,
        "clear_missing_pcode": 0,
        "clear_with_no_dtm_row": 0,
        "failed": 0,
    }

    fetch = _LEVEL_FETCH[admin_level]
    logger.info("=" * 60)
    logger.info("Level %d — fetching IOM DTM admin%d for %s…", admin_level, admin_level, country_name)
    records = fetch(country_name=country_name, admin0_pcode=admin0_pcode)
    stats["fetched"] = len(records)

    stats["records_without_pcode"] = sum(
        1 for r in records if not iom_dtm.record_pcode(r, admin_level)
    )

    latest = iom_dtm.latest_round_per_pcode(records, admin_level=admin_level)
    stats["distinct"] = len(latest)
    logger.info("Latest round per pCode: %d distinct", len(latest))

    clear_rows = graphql.get_locations_by_level(admin_level)
    stats["clear_total"] = len(clear_rows)
    pcode_to_id: dict[str, str] = {}
    name_to_id: dict[str, str] = {}
    clear_missing_pcode: list[str] = []
    for loc in clear_rows:
        if loc.get("pCode"):
            pcode_to_id[loc["pCode"]] = loc["id"]
        else:
            clear_missing_pcode.append(loc.get("name") or loc["id"])
        if loc.get("name"):
            name_to_id[_normalise_name(loc["name"])] = loc["id"]
    stats["clear_missing_pcode"] = len(clear_missing_pcode)
    logger.info(
        "CLEAR level-%d: %d total (%d with pCode, %d without)",
        admin_level, len(clear_rows), len(pcode_to_id), len(clear_missing_pcode),
    )
    if clear_missing_pcode:
        logger.warning(
            "CLEAR level-%d without pCode (first 10): %s",
            admin_level, clear_missing_pcode[:10],
        )

    dtm_pcodes = set(latest.keys())
    clear_no_dtm: list[str] = []
    for loc in clear_rows:
        pcode = loc.get("pCode")
        if pcode and pcode not in dtm_pcodes:
            clear_no_dtm.append(f"{loc.get('name')} ({pcode})")
    stats["clear_with_no_dtm_row"] = len(clear_no_dtm)
    if clear_no_dtm:
        logger.warning(
            "%d CLEAR level-%d locations have NO IOM DTM row (first 20): %s",
            len(clear_no_dtm), admin_level, clear_no_dtm[:20],
        )

    batch: list[dict] = []
    for pcode, rec in latest.items():
        clear_id = pcode_to_id.get(pcode)
        match_mode = "pcode" if clear_id else None

        if not clear_id:
            # Fallback: match by normalised name (handles pCode format drift,
            # e.g. IOM "SD" vs CLEAR "SDN" at admin0).
            rec_name = iom_dtm.record_name(rec, admin_level)
            if rec_name:
                candidate = name_to_id.get(_normalise_name(rec_name))
                if candidate:
                    clear_id = candidate
                    match_mode = "name"
                    logger.info(
                        "[NAME-MATCH L%d] pCode=%s name=%r → CLEAR id=%s",
                        admin_level, pcode, rec_name, clear_id,
                    )

        if not clear_id:
            stats["unmatched_pcode"] += 1
            logger.warning(
                "[UNMATCHED L%d] pCode=%s name=%s",
                admin_level, pcode, iom_dtm.record_name(rec, admin_level),
            )
            continue

        value = iom_dtm.extract_displacement_value(rec)
        if value is None:
            stats["skipped_no_value"] += 1
            logger.warning(
                "[NO VALUE L%d] pCode=%s round=%s",
                admin_level, pcode, rec.get("roundNumber") or rec.get("RoundNumber"),
            )
            continue

        payload = {
            "population_displaced": value,
            "round_number": rec.get("roundNumber") or rec.get("RoundNumber"),
            "reporting_date": rec.get("reportingDate") or rec.get("ReportingDate"),
            "operation": rec.get("operation") or rec.get("Operation"),
            "admin_level": admin_level,
            "admin_name": iom_dtm.record_name(rec, admin_level),
            "admin_pcode": pcode,
            "source": "iom_dtm_v3",
        }
        stats["matched"] += 1

        if dry_run:
            logger.info(
                "[DRY-RUN L%d] pCode=%s → %d displaced (round %s, %s)",
                admin_level, pcode, value,
                payload["round_number"], payload["reporting_date"],
            )
            continue

        batch.append({
            "locationId": clear_id,
            "type": METADATA_TYPE,
            "data": payload,
        })

    if batch and not dry_run:
        try:
            written = graphql.upsert_location_metadata_batch(batch)
            stats["upserted"] = len(written)
            logger.info("[OK L%d] Bulk-upserted %d rows", admin_level, len(written))
        except Exception as e:
            stats["failed"] = len(batch)
            logger.error("[FAILED L%d] Bulk upsert: %s", admin_level, e, exc_info=True)

    return stats


def run(levels: list[int], country_name: str, admin0_pcode: str, dry_run: bool) -> dict:
    if not settings.iom_dtm_subscription_key:
        raise SystemExit(
            "IOM_DTM_SUBSCRIPTION_KEY is not set. "
            "Get one from https://dtm-apim.developer.iom.int/ and add to .env."
        )

    all_stats: dict = {}
    for lvl in levels:
        all_stats[f"admin{lvl}"] = run_level(lvl, country_name, admin0_pcode, dry_run)

    all_stats["total_upserted"] = sum(
        s.get("upserted", 0) for s in all_stats.values() if isinstance(s, dict)
    )
    return all_stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill IOM DTM displacement into locationMetadata.")
    parser.add_argument(
        "--country",
        default=settings.iom_dtm_country_name,
        help=f"CountryName query filter (default: {settings.iom_dtm_country_name}).",
    )
    parser.add_argument(
        "--admin0-pcode",
        default=settings.iom_dtm_admin0_pcode,
        help=f"Admin0Pcode filter (default: {settings.iom_dtm_admin0_pcode}).",
    )
    parser.add_argument(
        "--levels",
        default=None,
        help="Comma-separated admin levels to process (default: 0,1,2).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print plan, no writes.")
    args = parser.parse_args()

    levels = parse_levels(args.levels)
    logger.info(
        "Starting IOM DTM backfill: country=%s admin0=%s levels=%s dry_run=%s",
        args.country, args.admin0_pcode, levels, args.dry_run,
    )

    stats = run(levels, args.country, args.admin0_pcode, args.dry_run)
    logger.info("Done: %s", json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
