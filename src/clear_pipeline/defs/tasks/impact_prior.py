"""The ``event.impact_prior`` handler (clear-api ADR-0010): what has typically
happened before for this Event's hazard type in this country.

Tracer (this step): cases from CLEAR's own Events only — same GLIDE type,
same country, inside the horizon, excluding the input Event — labelled by
scope. The knowledge base, the model and usage follow.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from clear_pipeline.defs.tasks.worker import TaskOutcome, register_handler
from clear_pipeline.providers import clear_api

logger = logging.getLogger(__name__)

KIND = "event.impact_prior"
METHOD_VERSION = "clear-pipeline-impact-prior@0.1.0"
DEFAULT_HORIZON_YEARS = 10
_PAGE = 25
_MAX_PAGES = 8


def primary_location(event: dict[str, Any]) -> dict[str, Any] | None:
    """general → origin → destination, the order clear-api resolves the Event's country."""
    return event.get("generalLocation") or event.get("originLocation") or event.get("destinationLocation")


def resolve_country_id(event: dict[str, Any], country_ids: set[str]) -> str | None:
    """The level-0 ancestor of the Event's primary location (or the location itself at level 0)."""
    loc = primary_location(event)
    if not loc:
        return None
    if loc.get("level") == 0:
        return loc["id"]
    for ancestor in loc.get("ancestorIds") or []:
        if ancestor in country_ids:
            return ancestor
    return None


def district_id(event: dict[str, Any]) -> str | None:
    """The Event's level-2 location, when its primary location is one."""
    loc = primary_location(event)
    return loc["id"] if loc and loc.get("level") == 2 else None


def event_date(event: dict[str, Any]) -> str | None:
    return event.get("startedAt") or event.get("firstSignalCreatedAt")


def case_from_event(prior: dict[str, Any], *, input_district: str | None) -> dict[str, Any]:
    loc = primary_location(prior)
    scope = "district" if input_district and district_id(prior) == input_district else "country"
    title = (prior.get("title") or "").strip()
    description = (prior.get("description") or "").strip()
    quote = (title or description)[:300]
    return {
        "tier": "clear",
        "eventId": prior["id"],
        "occurredAt": event_date(prior),
        "locationLabel": loc.get("name") if loc else None,
        "scope": scope,
        "quote": quote,
    }


def clear_candidates(*, hazard: str, country_id: str, since: str, until: str) -> list[dict[str, Any]]:
    """CLEAR Events of the hazard under the country inside the horizon, oldest page first."""
    items: list[dict[str, Any]] = []
    for page in range(_MAX_PAGES):
        res = clear_api.worker_events_page({
            "eventTypes": [hazard],
            "locationId": country_id,
            "from": since,
            "to": until,
            "includeDummy": False,
            "limit": _PAGE,
            "offset": page * _PAGE,
            "orderBy": "CREATED_DESC",
        })
        items.extend(res.get("items") or [])
        if not res.get("hasMore"):
            break
    return items


@register_handler(KIND)
def handle_impact_prior(context, task: dict[str, Any]) -> TaskOutcome:
    event_id = task["subjectId"]
    horizon = int((task.get("payload") or {}).get("horizonYears") or DEFAULT_HORIZON_YEARS)

    event = clear_api.worker_get_event(event_id)
    if not event:
        raise RuntimeError(f"Event {event_id} not found")
    types = event.get("types") or []
    if not types:
        return TaskOutcome(result={"searched": [], "candidates": 0, "reason": "event has no hazard type"})
    hazard = types[0]

    country_ids = {loc["id"] for loc in clear_api.worker_locations_by_level(0)}
    country_id = resolve_country_id(event, country_ids)
    if not country_id:
        return TaskOutcome(result={"searched": [], "candidates": 0, "reason": "event has no resolvable country"})

    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=365 * horizon)).isoformat()
    until = event_date(event) or now.isoformat()
    candidates = clear_candidates(hazard=hazard, country_id=country_id, since=since, until=until)
    priors = [c for c in candidates if c["id"] != event_id]
    input_district = district_id(event)
    basis = [case_from_event(p, input_district=input_district) for p in priors]

    result = {
        "searched": [{"tool": "eventsPage", "eventTypes": [hazard], "locationId": country_id, "from": since, "to": until}],
        "candidates": len(candidates),
        "excluded": [{"id": event_id, "reason": "the input Event"}] if len(candidates) != len(priors) else [],
        "cases": len(basis),
        "method": METHOD_VERSION,
    }
    if not basis:
        return TaskOutcome(result=result)

    return TaskOutcome(
        result=result,
        impact_prior={
            "hazardType": hazard,
            "countryLocationId": country_id,
            "geographicScope": "district" if basis and all(c["scope"] == "district" for c in basis) else "country",
            "horizonYears": horizon,
            "numberOfCases": len(basis),
            "basis": basis,
            "methodVersion": METHOD_VERSION,
        },
    )
