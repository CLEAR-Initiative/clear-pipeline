"""IOM DTM Flash Alerts client: narrative displacement bulletins from a static multi-country JSON export.
Plain GET only (any extra header, Accept included, fails); no server-side filter, so countries filter client-side."""

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
    """Fetch the full export, filtered to the configured countries.
    Raises on a failed request or malformed export, so the run fails; [] means a valid export with no match.
    `since` exists only for GXSource protocol parity; it does not narrow the request."""
    resp = httpx.get(
        settings.iom_dtm_flash_alerts_export_url,
        timeout=120,
    )
    resp.raise_for_status()

    try:
        data = resp.json()
    except ValueError as e:
        raise ValueError(
            f"[DTM_FLASH_ALERTS] JSON parse failed: {e}, body={resp.text[:200]!r}"
        ) from e

    if not isinstance(data, list):
        raise TypeError(
            f"[DTM_FLASH_ALERTS] unexpected response type: {type(data).__name__}"
        )

    logger.info("[DTM_FLASH_ALERTS] fetched %d raw records", len(data))

    countries = {
        c.strip()
        for c in settings.iom_dtm_flash_alerts_countries.split(",")
        if c.strip()
    }
    # A record with no country can't be one of ours; skipping it keeps one malformed
    # bulletin (of any country) from failing every poll.
    no_country = sum(1 for record in data if not record.get("field_country1"))
    country_matched = [
        record for record in data if record.get("field_country1") in countries
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
        "out of %d country-matched (no_country=%d)",
        len(new_records),
        parse_failed,
        deduped,
        len(country_matched),
        no_country,
    )
    return new_records


def _extract_external_id_from_report_file(url: str | None) -> str | None:
    """Stable `externalId` from `field_report_file`'s `id` query param.
    That's the Drupal node id: the report's content identity."""
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
    The export embeds literal entities (`&amp;` in `field_report_file` breaks id parsing)."""
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
    """Informational "last clean poll" marker; never gates fetching (`since` is ignored)."""
    # decode_responses=True guarantees str at runtime; the stub can't see that.
    val = cast("str | None", _redis.get("iom_dtm_flash_alerts:last_synced"))
    if val:
        return datetime.fromisoformat(val)
    return None


def set_last_synced(ts: datetime) -> None:
    _redis.set("iom_dtm_flash_alerts:last_synced", ts.isoformat())


def _day(value: Any) -> str | None:
    """Calendar day of a `field_from`/`field_to` value, or None when empty/unparseable.
    Drops the export's placeholder 12:00, which clear-api would store as server-local noon."""
    if not isinstance(value, str) or len(value) < 10:
        return None
    try:
        return date.fromisoformat(value[:10]).isoformat()
    except ValueError:
        return None


def build_iom_dtm_flash_alert_signal_input(
    record: dict, source_id: str, country_id: str | None
) -> dict:
    """Convert a parsed Flash Alert record into a CLEAR CreateSignalInput dict.
    rawData excludes derived keys and "raw" (the pristine copy goes to S3); no actors: bulletins name none."""
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
        # TODO: no "casualties" — Sudan's summaries never state one; revisit if a country
        # whose reports carry casualty figures (e.g. Nigeria) is added.
    }
    start, end = _day(record.get("field_from")), _day(record.get("field_to"))
    if start:
        input_data["startDate"] = start
        input_data["startDateAccuracy"] = "DAY"
    if end:
        input_data["endDate"] = end
        input_data["endDateAccuracy"] = "DAY"
    # No locationId: the drain locates the signal from its text (`IOMDTMFlashAlertsConnector.locate`).
    if country_id is not None:
        input_data["countryIds"] = [country_id]
    return input_data


def build_signal_content_update(input_data: dict, *, retracted: bool | None = None) -> dict:
    """updateSignalContent input keyed by ``(sourceId, externalId)``, reusing the create input's values.
    Absent/None fields are omitted so the stored value stays; ``retracted=None`` leaves the flag alone."""
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
