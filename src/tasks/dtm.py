"""IOM DTM backfill Celery task.

Populates `locationMetadata(type="iom_dtm_displacement")` for every CLEAR
admin-2 location with a pCode, using the latest round of data from the
IOM DTM API.

Scheduled weekly via Celery Beat. Also callable manually (or from the
sync script in scripts/backfill_iom_dtm.py).
"""

from __future__ import annotations

import logging

from src.celery_app import app
from src.clients import graphql, iom_dtm
from src.config import settings

logger = logging.getLogger(__name__)

METADATA_TYPE = "iom_dtm_displacement"


@app.task(
    name="src.tasks.dtm.backfill_dtm_displacement",
    bind=True,
    max_retries=1,
    acks_late=True,
)
def backfill_dtm_displacement(
    self,
    country_name: str | None = None,
    admin0_pcode: str | None = None,
) -> dict:
    """Fetch IOM DTM admin-2 displacement data and upsert into
    locationMetadata for every matching CLEAR level-2 location.

    Returns a stats dict.
    """
    country = country_name or settings.iom_dtm_country_name
    admin0 = admin0_pcode or settings.iom_dtm_admin0_pcode

    stats = {
        "fetched": 0,
        "distinct_admin2": 0,
        "matched": 0,
        "upserted": 0,
        "skipped_no_pcode": 0,
        "skipped_no_value": 0,
        "unmatched_pcode": 0,
        "failed": 0,
    }

    try:
        if not settings.iom_dtm_subscription_key:
            logger.warning("[IOM DTM] Subscription key not configured — skipping")
            return stats

        records = iom_dtm.fetch_admin2_displacement(
            country_name=country,
            admin0_pcode=admin0,
        )
        stats["fetched"] = len(records)

        latest = iom_dtm.latest_round_per_admin2(records)
        stats["distinct_admin2"] = len(latest)
        logger.info("[IOM DTM] %d distinct admin-2 districts in latest round(s)", len(latest))

        # CLEAR locations at level 2 — build a pCode → id map
        clear_level2 = graphql.get_locations_by_level(2)
        pcode_to_id: dict[str, str] = {}
        for loc in clear_level2:
            if loc.get("pCode"):
                pcode_to_id[loc["pCode"]] = loc["id"]
        logger.info(
            "[IOM DTM] %d CLEAR admin-2 locations with pCodes to match against",
            len(pcode_to_id),
        )

        # Build a single batch of rows, then send in one mutation.
        batch: list[dict] = []
        for pcode, rec in latest.items():
            clear_id = pcode_to_id.get(pcode)
            if not clear_id:
                stats["unmatched_pcode"] += 1
                logger.debug(
                    "[IOM DTM] No CLEAR location for pCode=%s (admin2Name=%s)",
                    pcode, rec.get("admin2Name"),
                )
                continue

            value = iom_dtm.extract_displacement_value(rec)
            if value is None:
                stats["skipped_no_value"] += 1
                logger.debug(
                    "[IOM DTM] No displacement value for pCode=%s round=%s",
                    pcode, rec.get("roundNumber"),
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
            stats["matched"] += 1
            batch.append({
                "locationId": clear_id,
                "type": METADATA_TYPE,
                "data": payload,
            })

        if batch:
            try:
                written = graphql.upsert_location_metadata_batch(batch)
                stats["upserted"] = len(written)
                logger.info("[IOM DTM] Bulk-upserted %d rows", len(written))
            except Exception as e:
                stats["failed"] = len(batch)
                logger.error("[IOM DTM] Bulk upsert failed: %s", e, exc_info=True)

        logger.info("[IOM DTM] Done: %s", stats)
        return stats

    except Exception as exc:
        logger.error("[IOM DTM] backfill_dtm_displacement failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=300)
