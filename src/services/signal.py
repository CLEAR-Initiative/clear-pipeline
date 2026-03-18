"""Signal creation: map Dataminr payload → CLEAR signal and persist via GraphQL."""

import logging

from src.clients.graphql import create_signal
from src.models.dataminr import DataminrSignal
from src.services.geo import resolve_location

logger = logging.getLogger(__name__)


def build_signal_input(signal: DataminrSignal, source_id: str) -> dict:
    """Map a Dataminr signal to a CLEAR CreateSignalInput dict."""
    # Build description from subHeadline fields
    description_parts = []
    if signal.subHeadline:
        if signal.subHeadline.title:
            description_parts.append(signal.subHeadline.title)
        if signal.subHeadline.subHeadlines:
            description_parts.append(signal.subHeadline.subHeadlines)
    description = " — ".join(description_parts) if description_parts else None

    # URL from publicPost
    url = None
    if signal.publicPost and signal.publicPost.link:
        url = signal.publicPost.link

    # Location resolution
    origin_id = None
    coords = signal.coordinates
    if coords:
        lat, lon = coords
        origin_id = resolve_location(lat, lon)

    # Full raw payload as JSON
    raw_data = signal.model_dump(mode="json")

    return {
        "sourceId": source_id,
        "rawData": raw_data,
        "publishedAt": signal.alertTimestamp,
        "url": url,
        "title": signal.headline,
        "description": description,
        "originId": origin_id,
    }


def ingest_signal(signal: DataminrSignal, source_id: str) -> dict:
    """Build and persist a CLEAR signal from a Dataminr payload. Returns the created signal."""
    input_data = build_signal_input(signal, source_id)
    result = create_signal(input_data)
    logger.info(
        "Created signal id=%s title=%s",
        result["id"],
        result.get("title", "")[:60],
    )
    return result
