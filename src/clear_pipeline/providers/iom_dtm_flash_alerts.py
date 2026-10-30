"""IOM DTM Flash Alerts client — narrative displacement bulletins, via a
static JSON export covering multiple countries and report types (e.g.
Sudan's Emergency Event Tracking Report / Displacement Report, Nigeria's
Flash Report, Mozambique's Situation Report). Currently configured to only
ingest Sudan via `iom_dtm_flash_alerts_countries`.

No authentication. Plain GET only — sending any extra header (Accept
included) makes the request fail. The export has no server-side filter or
pagination — every poll fetches every country in one response and filters
client-side.

Separate product from `providers/iom_dtm.py` (admin-level displacement
figures, feeding location_metadata, not signals) — same platform, different
feed.

Ingested only by gx (`IOMDTMFlashAlertsGXSource`): every poll returns every
country-matched bulletin, and gx's bronze skip (gold content hash) drops the
unchanged ones — no seen-set, like IDMC and Sudan Witness.

Endpoint: settings.iom_dtm_flash_alerts_export_url.
"""

import hashlib
import html
import json
import logging
from datetime import date, datetime
from typing import Any, cast
from urllib.parse import parse_qs, urlparse

import httpx
import redis

from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)


def fetch_iom_dtm_flash_alerts(since: datetime | None = None) -> list[dict]:
    """Fetch the full DTM Flash Alerts JSON export, filtered to the configured
    countries.

    `since` is accepted only for GXSource protocol parity and does not narrow
    the request.
    """
    try:
        resp = httpx.get(
            settings.iom_dtm_flash_alerts_export_url,
            timeout=120,
        )
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.error("[DTM_FLASH_ALERTS] request failed: %s", e)
        return []

    try:
        data = resp.json()
    except ValueError as e:
        logger.error(
            "[DTM_FLASH_ALERTS] JSON parse failed: %s, body=%s", e, resp.text[:200]
        )
        return []

    if not isinstance(data, list):
        logger.error(
            "[DTM_FLASH_ALERTS] unexpected response type: %s", type(data).__name__
        )
        return []

    logger.info("[DTM_FLASH_ALERTS] fetched %d raw records", len(data))

    countries = {
        c.strip()
        for c in settings.iom_dtm_flash_alerts_countries.split(",")
        if c.strip()
    }
    country_matched = [
        record for record in data if record["field_country1"] in countries
    ]

    new_records = []
    parse_failed = 0
    deduped = 0
    batch_ids: set[str] = set()
    for record in country_matched:
        parsed = parse_record(record)
        if not parsed:
            parse_failed += 1
            continue

        external_id = parsed["external_id"]
        if external_id in batch_ids:
            deduped += 1
            continue
        batch_ids.add(external_id)
        new_records.append(parsed)

    logger.info(
        "[DTM_FLASH_ALERTS] result: %d records (parse_failed=%d, duplicate=%d) "
        "out of %d country-matched",
        len(new_records),
        parse_failed,
        deduped,
        len(country_matched),
    )
    return new_records


def _extract_external_id_from_report_file(url: str | None) -> str | None:
    """Extract the stable `externalId` from a Flash Alert's `field_report_file`
    URL, e.g. ".../dtm_download_track/100646?file=1&type=node&id=65946" ->
    "iom_dtm_flash_alert:65946".

    Uses the `id` query param (the Drupal node id — the report's actual
    content identity)
    """
    if not url:
        logger.warning("[DTM_FLASH_ALERTS] no field_report_file to parse an id from")
        return None

    node_ids = parse_qs(urlparse(url).query).get("id")
    if not node_ids:
        logger.warning("[DTM_FLASH_ALERTS] no id param in field_report_file: %s", url)
        return None

    return f"iom_dtm_flash_alert:{node_ids[0]}"


def _decode_html_entities(value: Any) -> Any:
    """Recursively html.unescape every string in a JSON-shaped value.
    Empirically, 3 of the export's fields carry literal entities —
    field_report_file (546/547 records), field_summary (310/547), title
    (193/547) — but this runs on the whole record so rawData is clean too."""
    if isinstance(value, str):
        return html.unescape(value)
    if isinstance(value, list):
        return [_decode_html_entities(v) for v in value]
    if isinstance(value, dict):
        return {k: _decode_html_entities(v) for k, v in value.items()}
    return value


def _content_hash(raw: dict) -> str:
    """Fingerprint of the pristine record: a bulletin IOM edits (summary,
    period, file) gets a new hash, so gx pushes it as a revision."""
    serialized = json.dumps(raw, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


def parse_record(raw: dict) -> dict | None:
    """Decoded fields + `external_id` + `content_hash`, with the untouched
    original under "raw" (what `raw_bytes` writes to S3); None without an id."""
    decoded = _decode_html_entities(raw)
    external_id = _extract_external_id_from_report_file(
        decoded.get("field_report_file")
    )
    if not external_id:
        return None
    return {
        **decoded,
        "external_id": external_id,
        "content_hash": _content_hash(raw),
        "raw": raw,
    }


def get_last_synced() -> datetime | None:
    """Informational only — fetch_iom_dtm_flash_alerts ignores `since` (no
    server-side filter to narrow with), so this never gates what gets
    fetched. A debuggable "last clean poll" marker, nothing more."""
    # decode_responses=True guarantees str at runtime; the stub can't see that.
    val = cast("str | None", _redis.get("iom_dtm_flash_alerts:last_synced"))
    if val:
        return datetime.fromisoformat(val)
    return None


def set_last_synced(ts: datetime) -> None:
    _redis.set("iom_dtm_flash_alerts:last_synced", ts.isoformat())


def _day(value: Any) -> str | None:
    """The calendar day of a `field_from`/`field_to` value
    ("2025-10-25T12:00:00" → "2025-10-25"), or None when empty/unparseable.
    The export pins every date to a placeholder 12:00; a bare date makes
    clear-api store UTC midnight instead of a server-local noon."""
    if not isinstance(value, str) or len(value) < 10:
        return None
    try:
        return date.fromisoformat(value[:10]).isoformat()
    except ValueError:
        return None


def build_iom_dtm_flash_alert_signal_input(
    record: dict, source_id: str, location_id: str | None
) -> dict:
    """Convert a parsed Flash Alert record into a CLEAR CreateSignalInput dict.

    rawData is the decoded fields only — "external_id"/"content_hash" (derived,
    not from the source) and "raw" (the pristine copy, which `raw_bytes` sends
    to S3 instead) are excluded so Postgres stays clean and readable.

    No magnitude or actors: bulletins name places and IDP counts, not parties.
    """
    clean_fields = {
        k: v for k, v in record.items() if k not in ("external_id", "content_hash", "raw")
    }
    input_data = {
        "sourceId": source_id,
        "externalId": record["external_id"],
        "contentHash": record["content_hash"],
        "rawData": clean_fields,
        "publishedAt": record["field_published_date"],
        "url": record["field_report_file"],
        "title": record["title"],
        "description": record["field_summary"],
        # TODO: no "casualties" — Sudan's field_summary never states one. Revisit
        # if iom_dtm_flash_alerts_countries ever includes Nigeria or another
        # country whose reports carry real casualty figures (e.g. "three
        # fatalities").
    }
    start, end = _day(record.get("field_from")), _day(record.get("field_to"))
    if start:
        input_data["startDate"] = start
        input_data["startDateAccuracy"] = "DAY"
    if end:
        input_data["endDate"] = end
        input_data["endDateAccuracy"] = "DAY"
    # location_id: L0 id for record["field_country1"]; None when clear-api has
    # no such country.
    if location_id is not None:
        input_data["locationId"] = location_id
        input_data["countryIds"] = [location_id]
    return input_data


def build_signal_content_update(input_data: dict, *, retracted: bool | None = None) -> dict:
    """Adapt a `build_iom_dtm_flash_alert_signal_input` dict into an
    updateSignalContent input keyed by ``(sourceId, externalId)`` (gold never
    has the clear-api id), reusing its values so create and update agree.
    Fields the input lacks are left out, not sent as None: an absent key
    leaves the stored value alone. ``retracted=None`` leaves the flag unchanged."""
    update = {
        k: input_data[k]
        for k in (
            "sourceId", "externalId", "contentHash", "rawData", "url", "title", "description",
            "startDate", "startDateAccuracy", "endDate", "endDateAccuracy",
            "locationId", "countryIds", "rawS3Key",
        )
        if input_data.get(k) is not None
    }
    if retracted is not None:
        update["retracted"] = retracted
    return update
