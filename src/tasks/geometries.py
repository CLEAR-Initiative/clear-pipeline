"""
Admin geometry backfill task.

Source: OCHA / HDX admin boundaries (https://data.humdata.org/).
Each country's admin_boundaries zip contains admin0/1/2 GeoJSON files with
properties `adm{N}_pcode`, `adm{N}_name`, `adm{N}_name1` (local name).

We match features to our locations primarily by `pCode`, falling back to
(case-insensitive, punctuation-normalised) name.

Config:
  OCHA_ADMIN_BOUNDARIES_URLS maps ISO3 → zip URL. Extend this dict to add
  more countries.
"""

import io
import logging
import zipfile
from typing import Any

import httpx

from src.celery_app import app
from src.clients import graphql

logger = logging.getLogger(__name__)


# OCHA/HDX admin boundary zip per country (contains admin0/1/2 GeoJSON)
OCHA_ADMIN_BOUNDARIES_URLS: dict[str, str] = {
    "SDN": "https://data.humdata.org/dataset/a66a4b6c-92de-4507-9546-aa1900474180/resource/018af991-4aa7-4043-a0d5-e429a55851fb/download/sdn_admin_boundaries.geojson.zip",
    "NER": "https://data.humdata.org/dataset/c0e0998c-b45a-4aea-ac06-c1de1d94e596/resource/52a76bdf-7c22-43d4-9e2d-bad40351007c/download/ner_admin_boundaries.geojson.zip",
    "NGA": "https://data.humdata.org/dataset/81ac1d38-f603-4a98-804d-325c658599a3/resource/7e30ec96-7f29-4ee8-9f4c-77633b353cbb/download/nga_admin_boundaries.geojson.zip",
    "LBN": "https://data.humdata.org/dataset/569beba7-bad7-4951-a19d-468a035461cd/resource/81ca0135-b5c9-46e0-a546-f0bdd6d7fd42/download/lbn_admin_boundaries.geojson.zip",
    "AFG": "https://data.humdata.org/dataset/4c303d7b-8eae-4a5a-a3aa-b2331fa39d74/resource/330aad34-2254-4622-afac-e98ace1524ae/download/afg_admin_boundaries.geojson.zip",
    "IRN": "https://data.humdata.org/dataset/247b4026-79ff-4b16-95b9-0f366792d2cc/resource/f8314b60-dc85-4fd1-8936-3dff46a2283a/download/irn_admin_boundaries.geojson.zip",
}


def _download_and_extract(iso3: str) -> dict[int, list[dict[str, Any]]]:
    """Download the OCHA admin boundaries zip and return features keyed by admin level."""
    url = OCHA_ADMIN_BOUNDARIES_URLS.get(iso3.upper())
    if not url:
        raise ValueError(f"No OCHA admin boundaries URL configured for {iso3}")

    logger.info("[GEOMETRIES] Downloading OCHA admin boundaries: %s", url)
    resp = httpx.get(url, timeout=300, follow_redirects=True)
    resp.raise_for_status()

    import json

    level_to_features: dict[int, list[dict[str, Any]]] = {}
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        names = zf.namelist()
        logger.info("[GEOMETRIES] Zip contains: %s", names)
        for level in (0, 1, 2):
            # File name pattern: admin{level}.geojson (sometimes in a subdir)
            match = next(
                (n for n in names if n.endswith(f"admin{level}.geojson")),
                None,
            )
            if not match:
                logger.warning("[GEOMETRIES] admin%d.geojson not found in zip", level)
                level_to_features[level] = []
                continue

            with zf.open(match) as f:
                fc = json.load(f)
            features = fc.get("features", [])
            logger.info("[GEOMETRIES] Level %d: %d features", level, len(features))
            level_to_features[level] = features

    return level_to_features


def _normalise_name(name: str) -> str:
    return name.strip().lower().replace("-", " ").replace("_", " ")


def _feature_properties(feat: dict, level: int) -> tuple[str | None, str | None]:
    """Return (pCode, name) for an OCHA admin feature at the given level."""
    props = feat.get("properties", {}) or {}
    p_code = props.get(f"adm{level}_pcode") or props.get("pcode") or props.get("pCode")
    name = (
        props.get(f"adm{level}_name")
        or props.get(f"adm{level}_en")
        or props.get("name")
        or props.get("shapeName")
    )
    return p_code, name


@app.task(
    name="src.tasks.geometries.backfill_admin_geometries",
    bind=True,
    max_retries=1,
    acks_late=True,
)
def backfill_admin_geometries(
    self,
    iso3: str = "SDN",
    levels: list[int] | None = None,
) -> dict:
    """Fetch admin polygons from OCHA/HDX and update matching locations.

    Matching:
      1. Primary: by `pCode` (exact match).
      2. Fallback: by normalised name.
    Unmatched features are logged but do not fail the task.
    """
    target_levels = levels or [0, 1, 2]
    logger.info(
        "[GEOMETRIES] backfill_admin_geometries: iso3=%s levels=%s",
        iso3, target_levels,
    )

    stats = {
        "matched_by_pcode": 0,
        "matched_by_name": 0,
        "unmatched": 0,
        "errors": 0,
        "skipped_no_geometry": 0,
    }
    unmatched_features: list[str] = []

    try:
        level_to_features = _download_and_extract(iso3)

        for level in target_levels:
            features = level_to_features.get(level, [])
            if not features:
                continue

            locations = graphql.get_locations_by_level(level)
            if not locations:
                logger.warning("[GEOMETRIES] No locations at level %d — skipping", level)
                continue

            # Lookups by pCode and normalised name
            pcode_to_id: dict[str, str] = {}
            name_to_id: dict[str, str] = {}
            for loc in locations:
                if loc.get("pCode"):
                    pcode_to_id[loc["pCode"]] = loc["id"]
                name_to_id[_normalise_name(loc["name"])] = loc["id"]

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
                    unmatched_features.append(
                        f"level={level} pcode={p_code} name={name}"
                    )
                    stats["unmatched"] += 1
                    continue

                try:
                    graphql.update_location_geometry(loc_id, geometry)
                    if match_mode == "pcode":
                        stats["matched_by_pcode"] += 1
                    else:
                        stats["matched_by_name"] += 1
                    logger.info(
                        "[GEOMETRIES] Level %d: %s (pcode=%s, via=%s) → %s",
                        level, name, p_code, match_mode, loc_id,
                    )
                except Exception as e:
                    stats["errors"] += 1
                    logger.error(
                        "[GEOMETRIES] Failed to update %s (%s): %s",
                        name, loc_id, e,
                    )

        if unmatched_features:
            logger.warning(
                "[GEOMETRIES] %d unmatched features (first 20): %s",
                len(unmatched_features), unmatched_features[:20],
            )

        logger.info("[GEOMETRIES] Done: %s", stats)
        return stats

    except graphql.GraphQLClientError as exc:
        logger.error("[GEOMETRIES] permanently failed (non-retryable): %s", exc)
        raise
    except Exception as exc:
        logger.error(
            "[GEOMETRIES] backfill_admin_geometries failed: %s",
            exc, exc_info=True,
        )
        raise self.retry(exc=exc, countdown=120)
