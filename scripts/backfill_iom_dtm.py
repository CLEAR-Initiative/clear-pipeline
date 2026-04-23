"""
Standalone runner for IOM DTM displacement backfill.

Fetches the latest admin-2 displaced-persons data from the IOM DTM API
(https://dtmapi.iom.int/v3/) and upserts it into locationMetadata
(type = "iom_dtm_displacement") on clear-api.

Runs synchronously in-process — no Celery worker required.

Usage:
    python scripts/backfill_iom_dtm.py                         # Sudan (from .env)
    python scripts/backfill_iom_dtm.py --country "Sudan"
    python scripts/backfill_iom_dtm.py --admin0-pcode SDN
    python scripts/backfill_iom_dtm.py --dry-run               # fetch + print plan
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


def run(country_name: str, admin0_pcode: str, dry_run: bool) -> dict:
    stats = {
        "fetched": 0,
        "records_without_admin2_pcode": 0,
        "distinct_admin2": 0,
        "matched": 0,
        "upserted": 0,
        "skipped_no_value": 0,
        "unmatched_pcode": 0,
        "clear_level2_total": 0,
        "clear_level2_missing_pcode": 0,
        "clear_level2_with_no_dtm_row": 0,
        "failed": 0,
    }

    if not settings.iom_dtm_subscription_key:
        raise SystemExit(
            "IOM_DTM_SUBSCRIPTION_KEY is not set. "
            "Get one from https://dtm-apim.developer.iom.int/ and add to .env."
        )

    logger.info("Fetching IOM DTM admin-2 data for %s…", country_name)
    records = iom_dtm.fetch_admin2_displacement(
        country_name=country_name,
        admin0_pcode=admin0_pcode,
    )
    stats["fetched"] = len(records)

    # How many records did we drop for missing admin-2 pcode?
    stats["records_without_admin2_pcode"] = sum(
        1 for r in records
        if not (r.get("admin2Pcode") or r.get("Admin2Pcode"))
    )

    latest = iom_dtm.latest_round_per_admin2(records)
    stats["distinct_admin2"] = len(latest)
    logger.info("Latest round per admin-2: %d distinct districts", len(latest))

    clear_level2 = graphql.get_locations_by_level(2)
    stats["clear_level2_total"] = len(clear_level2)
    pcode_to_id: dict[str, str] = {}
    clear_missing_pcode: list[str] = []
    for loc in clear_level2:
        if loc.get("pCode"):
            pcode_to_id[loc["pCode"]] = loc["id"]
        else:
            clear_missing_pcode.append(loc.get("name") or loc["id"])
    stats["clear_level2_missing_pcode"] = len(clear_missing_pcode)
    logger.info(
        "CLEAR level-2: %d total (%d with pCode, %d without)",
        len(clear_level2), len(pcode_to_id), len(clear_missing_pcode),
    )
    if clear_missing_pcode:
        logger.warning(
            "CLEAR level-2 without pCode (first 10): %s",
            clear_missing_pcode[:10],
        )

    # Which CLEAR districts have NO matching IOM record?
    dtm_pcodes = set(latest.keys())
    clear_no_dtm: list[str] = []
    for loc in clear_level2:
        pcode = loc.get("pCode")
        if pcode and pcode not in dtm_pcodes:
            clear_no_dtm.append(f"{loc.get('name')} ({pcode})")
    stats["clear_level2_with_no_dtm_row"] = len(clear_no_dtm)
    if clear_no_dtm:
        logger.warning(
            "%d CLEAR districts have NO IOM DTM row (first 20): %s",
            len(clear_no_dtm), clear_no_dtm[:20],
        )

    for pcode, rec in latest.items():
        clear_id = pcode_to_id.get(pcode)
        if not clear_id:
            stats["unmatched_pcode"] += 1
            logger.warning(
                "[UNMATCHED] pCode=%s admin2Name=%s",
                pcode, rec.get("admin2Name") or rec.get("Admin2Name"),
            )
            continue

        value = iom_dtm.extract_displacement_value(rec)
        if value is None:
            stats["skipped_no_value"] += 1
            logger.warning(
                "[NO VALUE] pCode=%s round=%s — candidates tried but all empty",
                pcode, rec.get("roundNumber") or rec.get("RoundNumber"),
            )
            continue

        payload = {
            "population_displaced": value,
            "round_number": rec.get("roundNumber") or rec.get("RoundNumber"),
            "reporting_date": rec.get("reportingDate") or rec.get("ReportingDate"),
            "operation": rec.get("operation") or rec.get("Operation"),
            "admin2_name": rec.get("admin2Name") or rec.get("Admin2Name"),
            "admin2_pcode": pcode,
            "source": "iom_dtm_v3",
        }

        # Count the successful match regardless of dry-run so the stats are
        # informative.
        stats["matched"] += 1

        if dry_run:
            logger.info(
                "[DRY-RUN] pCode=%s → %d displaced (round %s, %s)",
                pcode, value, payload["round_number"], payload["reporting_date"],
            )
            continue

        try:
            graphql.upsert_location_metadata(clear_id, METADATA_TYPE, payload)
            stats["upserted"] += 1
            logger.info(
                "[OK] %s → population_displaced=%d (round %s)",
                pcode, value, payload["round_number"],
            )
        except Exception as e:
            stats["failed"] += 1
            logger.error("[FAILED] pCode=%s: %s", pcode, e)

    return stats


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
    parser.add_argument("--dry-run", action="store_true", help="Print plan, no writes.")
    args = parser.parse_args()

    logger.info(
        "Starting IOM DTM backfill: country=%s admin0=%s dry_run=%s",
        args.country, args.admin0_pcode, args.dry_run,
    )

    stats = run(args.country, args.admin0_pcode, args.dry_run)
    logger.info("Done: %s", json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
