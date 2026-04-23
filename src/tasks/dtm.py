"""IOM DTM backfill Celery task.

Populates `locationMetadata(type="iom_dtm_displacement")` for CLEAR locations
at admin levels 0 (country), 1 (state/province) and 2 (district) using the
latest round of data from the IOM DTM API.

Scheduled weekly via Celery Beat. Also callable manually (or from the sync
script in scripts/backfill_iom_dtm.py).
"""

from __future__ import annotations

import logging

from src.celery_app import app
from src.clients import graphql, iom_dtm
from src.config import settings

logger = logging.getLogger(__name__)

METADATA_TYPE = "iom_dtm_displacement"

# Per-admin-level config: which fetch function to call and the CLEAR level
# we upsert into.
_LEVEL_FETCH = {
    0: iom_dtm.fetch_admin0_displacement,
    1: iom_dtm.fetch_admin1_displacement,
    2: iom_dtm.fetch_admin2_displacement,
}


def _normalise_name(name: str | None) -> str:
    """Lowercase + strip punctuation for name-based match fallback."""
    import re

    s = (name or "").strip().lower()
    for prefix in ("republic of ", "the ", "el-", "el ", "al-", "al "):
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s


def _process_level(
    admin_level: int,
    country_name: str,
    admin0_pcode: str,
) -> dict:
    """Fetch the latest DTM round per pCode at `admin_level` and bulk-upsert
    into location_metadata. Returns per-level stats."""
    stats = {
        "fetched": 0,
        "distinct": 0,
        "matched": 0,
        "upserted": 0,
        "skipped_no_value": 0,
        "unmatched_pcode": 0,
        "failed": 0,
    }

    fetch = _LEVEL_FETCH[admin_level]
    records = fetch(country_name=country_name, admin0_pcode=admin0_pcode)
    stats["fetched"] = len(records)

    latest = iom_dtm.latest_round_per_pcode(records, admin_level=admin_level)
    stats["distinct"] = len(latest)
    logger.info(
        "[IOM DTM L%d] latest round per pCode: %d distinct",
        admin_level, len(latest),
    )

    # CLEAR locations at this level — build pCode and name lookup maps.
    # The name map is a fallback for cases where pCode format differs between
    # IOM and CLEAR (e.g. admin0: IOM returns "SD" but CLEAR stored "SDN").
    clear_rows = graphql.get_locations_by_level(admin_level)
    pcode_to_id: dict[str, str] = {}
    name_to_id: dict[str, str] = {}
    for loc in clear_rows:
        if loc.get("pCode"):
            pcode_to_id[loc["pCode"]] = loc["id"]
        if loc.get("name"):
            name_to_id[_normalise_name(loc["name"])] = loc["id"]
    logger.info(
        "[IOM DTM L%d] %d CLEAR locations with pCodes (%d with names)",
        admin_level, len(pcode_to_id), len(name_to_id),
    )

    batch: list[dict] = []
    for pcode, rec in latest.items():
        clear_id = pcode_to_id.get(pcode)
        if not clear_id:
            # Name-based fallback
            rec_name = iom_dtm.record_name(rec, admin_level)
            if rec_name:
                clear_id = name_to_id.get(_normalise_name(rec_name))
                if clear_id:
                    logger.info(
                        "[IOM DTM L%d NAME-MATCH] pCode=%s name=%r → %s",
                        admin_level, pcode, rec_name, clear_id,
                    )

        if not clear_id:
            stats["unmatched_pcode"] += 1
            logger.debug(
                "[IOM DTM L%d] No CLEAR location for pCode=%s name=%s",
                admin_level, pcode, iom_dtm.record_name(rec, admin_level),
            )
            continue

        value = iom_dtm.extract_displacement_value(rec)
        if value is None:
            stats["skipped_no_value"] += 1
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
        batch.append({
            "locationId": clear_id,
            "type": METADATA_TYPE,
            "data": payload,
        })

    if batch:
        try:
            written = graphql.upsert_location_metadata_batch(batch)
            stats["upserted"] = len(written)
            logger.info(
                "[IOM DTM L%d] Bulk-upserted %d rows",
                admin_level, len(written),
            )
        except Exception as e:
            stats["failed"] = len(batch)
            logger.error(
                "[IOM DTM L%d] Bulk upsert failed: %s",
                admin_level, e, exc_info=True,
            )

    return stats


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
    levels: list[int] | None = None,
) -> dict:
    """Fetch IOM DTM displacement data at admin levels 0, 1, and 2 and upsert
    into locationMetadata for every matching CLEAR location.

    `levels` lets callers scope to specific levels (default: all three).
    Returns per-level stats plus a top-level "total_upserted" convenience count.
    """
    country = country_name or settings.iom_dtm_country_name
    admin0 = admin0_pcode or settings.iom_dtm_admin0_pcode
    target_levels = levels or [0, 1, 2]

    if not settings.iom_dtm_subscription_key:
        logger.warning("[IOM DTM] Subscription key not configured — skipping")
        return {"skipped": "no_subscription_key"}

    all_stats: dict = {}
    try:
        for lvl in target_levels:
            if lvl not in _LEVEL_FETCH:
                logger.warning("[IOM DTM] Unsupported admin level %s — skipping", lvl)
                continue
            all_stats[f"admin{lvl}"] = _process_level(lvl, country, admin0)

        all_stats["total_upserted"] = sum(
            s.get("upserted", 0) for s in all_stats.values() if isinstance(s, dict)
        )
        logger.info("[IOM DTM] Done: %s", all_stats)
        return all_stats

    except Exception as exc:
        logger.error("[IOM DTM] backfill_dtm_displacement failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=300)
