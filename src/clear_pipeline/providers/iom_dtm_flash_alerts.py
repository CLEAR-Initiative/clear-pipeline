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

Endpoint: settings.iom_dtm_flash_alerts_export_url.
"""

import html
import logging
from datetime import datetime
from typing import Any, cast
from urllib.parse import parse_qs, urlparse

import httpx
import redis

from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)


def fetch_iom_dtm_flash_alerts(since: datetime | None = None) -> list[dict]:
    """Fetch the full DTM Flash Alerts JSON export.

    No server-side filter/pagination — `since` is accepted only for
    PollSource protocol parity and does not narrow the request.
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
        if external_id in batch_ids or _redis.exists(f"iomdtm:seen:{external_id}"):
            deduped += 1
            continue
        batch_ids.add(external_id)
        new_records.append(parsed)

    logger.info(
        "[DTM_FLASH_ALERTS] result: %d new (parse_failed=%d, already_seen=%d) "
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
    "iomdtm:65946".

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

    return f"iomdtm:{node_ids[0]}"


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


def parse_record(raw: dict) -> dict | None:
    """Normalize a raw Flash Alert record — decodes HTML entities throughout,
    attaches the parsed `external_id` (so the connector's `external_id()`
    lake key, this module's own dedup check, and
    `build_iom_dtm_flash_alert_signal_input`'s `externalId` all read the same
    precomputed value instead of parsing it independently), and keeps the
    untouched original under "raw" — the connector's `raw_bytes` serializes
    only that key, so the S3 lake blob stays a pristine, undecoded copy.
    Called again by the connector's `project()`, which only ever sees that
    pristine blob (read back from S3 during the drain stage) and needs the
    same decoding + external_id derivation redone from it."""
    decoded = _decode_html_entities(raw)
    external_id = _extract_external_id_from_report_file(
        decoded.get("field_report_file")
    )
    if not external_id:
        return None
    return {**decoded, "external_id": external_id, "raw": raw}


def mark_seen(external_id: str) -> None:
    """Mark a record ingested (Redis seen-set) — called only after createSignal
    is confirmed, so a failed persistence leaves the record eligible for re-poll."""
    _redis.setex(f"iomdtm:seen:{external_id}", settings.dedup_ttl_hours * 3600, "1")


def get_last_synced() -> datetime | None:
    """Informational only — fetch_iom_dtm_flash_alerts ignores `since` (no
    server-side filter to narrow with), so this never gates what gets
    fetched. A debuggable "last clean poll" marker, nothing more."""
    # decode_responses=True guarantees str at runtime; the stub can't see that.
    val = cast("str | None", _redis.get("iomdtm:last_synced"))
    if val:
        return datetime.fromisoformat(val)
    return None


def set_last_synced(ts: datetime) -> None:
    _redis.set("iomdtm:last_synced", ts.isoformat())


def build_iom_dtm_flash_alert_signal_input(
    record: dict, source_id: str, location_id: str | None
) -> dict:
    """Convert a parsed Flash Alert record into a CLEAR CreateSignalInput dict.

    rawData is the decoded fields only — "external_id" (derived, not from the
    source) and "raw" (the pristine copy, which the connector's raw_bytes
    sends to S3 instead) are excluded so Postgres stays clean and readable.
    """
    clean_fields = {k: v for k, v in record.items() if k not in ("external_id", "raw")}
    input_data = {
        "sourceId": source_id,
        "externalId": record["external_id"],
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
    # location_id is the connector's already-resolved lookup for
    # record["field_country1"]; None only when that lookup failed.
    if location_id is not None:
        input_data["locationId"] = location_id
    return input_data
