"""Unified structured event extraction + severity/relevance (shared prompt).

One LLM call per signal does the whole job the pipeline used to split across
per-source severity heuristics and a separate alert-severity classifier
(superseding PR #131): it returns the full structured event field set (name,
start/end dates + accuracy, type, countries, geolocation, magnitude + unit,
conflict actors) together with relevance and a 1-5 severity.

Both the medallion classify stage and the drain call this. It is best-effort:
on any LLM/parse failure it returns ``None`` so ingest never dies on a model
blip — a null severity stays honest rather than inventing a floor.

1 signal -> 1 event: the general-case prompt returns a single event object (it
never splits — that is the shared classification prompt's job). Multi-event
splitting is left to a later change.
"""

from __future__ import annotations

import logging
from typing import Literal

from pydantic import BaseModel, Field

from clear_pipeline.providers.clear_api import resolve_country_location_id
from clear_pipeline.providers.llm import make_llm_provider
from clear_pipeline.providers.prompts.event_extraction import (
    EVENT_EXTRACTION_PROMPT,
    EVENT_EXTRACTION_PROMPT_VERSION,
)

logger = logging.getLogger(__name__)

_LLM_MAX_TOKENS = 1024

# The prompt uses title-case accuracy labels; clear-api's DateAccuracy enum is
# upper-case.
_ACCURACY_MAP = {"Day": "DAY", "Week": "WEEK", "Month": "MONTH", "Year": "YEAR"}

_PromptAccuracy = Literal["Day", "Week", "Month", "Year"]


class ExtractedEvent(BaseModel):
    """One event object from the shared event-classification prompt."""

    event_name: str
    date_created: str | None = None
    start_date: str | None = None
    start_date_accuracy: _PromptAccuracy | None = None
    end_date: str | None = None
    end_date_accuracy: _PromptAccuracy | None = None
    event_type: str
    countries: list[str] = Field(default_factory=list)
    event_geolocation: str | None = None
    event_magnitude: float | None = None
    magnitude_unit: str | None = None
    event_actors: list[str] | None = None
    relevance: Literal["RELEVANT", "NOT_RELEVANT"]
    severity: int | None = Field(None, ge=1, le=5)
    rationale: str = ""


def extract_event(
    *,
    title: str | None,
    description: str | None,
    country: str | None = None,
    log_tag: str = "extract",
) -> ExtractedEvent | None:
    """Run the general-case extraction prompt and return the single extracted
    event, or ``None`` on any failure (best-effort — ingest must not die on a
    model blip)."""
    # Send BOTH title and description, labeled — some sources (e.g. ACLED) put
    # facts in the title (event type, location, actors) that the description
    # doesn't repeat, so `description or title` would drop them.
    body = "\n\n".join(
        text
        for text in (
            f"Title: {title}" if title else "",
            f"Description: {description}" if description else "",
        )
        if text
    )
    user = f"Country: {country}\n\n{body}" if country else body
    try:
        event = make_llm_provider("signal").complete_structured(
            system=EVENT_EXTRACTION_PROMPT,
            user=user,
            schema=ExtractedEvent,
            max_tokens=_LLM_MAX_TOKENS,
            cache_key=EVENT_EXTRACTION_PROMPT_VERSION,
        )
    except Exception:  # noqa: BLE001 — ingest must not die on a model blip
        logger.warning("[%s] event extraction LLM failed", log_tag, exc_info=True)
        return None
    logger.info(
        "[%s] event extraction: type=%s relevance=%s severity=%s",
        log_tag,
        event.event_type,
        event.relevance,
        event.severity,
    )
    return event


def extracted_signal_fields(event: ExtractedEvent) -> dict:
    """Map an ``ExtractedEvent`` onto the clear-api signal/event input fields
    (``CreateSignalInput`` / ``CreateEventInput``). Resolves country names to
    admin-0 location ids and maps accuracy labels to the API enum. Only present
    fields are included so an absent value never clobbers a stored one."""
    out: dict = {}
    if event.severity is not None:
        out["severity"] = event.severity
    if event.start_date:
        out["startDate"] = event.start_date
    if event.start_date_accuracy:
        out["startDateAccuracy"] = _ACCURACY_MAP[event.start_date_accuracy]
    if event.end_date:
        out["endDate"] = event.end_date
    if event.end_date_accuracy:
        out["endDateAccuracy"] = _ACCURACY_MAP[event.end_date_accuracy]
    if event.event_magnitude is not None:
        out["magnitude"] = event.event_magnitude
    if event.magnitude_unit:
        out["magnitudeUnit"] = event.magnitude_unit
    if event.event_actors:
        out["actors"] = event.event_actors
    # Country resolution hits clear-api and can raise — keep it best-effort so
    # one failed lookup doesn't discard the other extracted fields.
    country_ids: list[str] = []
    for name in event.countries or []:
        try:
            loc_id = resolve_country_location_id(name)
        except Exception:  # noqa: BLE001 — country enrichment is best-effort
            logger.warning("country lookup failed for %r", name, exc_info=True)
            continue
        if loc_id is not None:
            country_ids.append(loc_id)
    if country_ids:
        out["countryIds"] = country_ids
    return out
