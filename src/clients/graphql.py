"""CLEAR API GraphQL client with retry logic."""

import logging
import time

import httpx

from src.config import settings

logger = logging.getLogger(__name__)

# ─── Mutations ────────────────────────────────────────────────────────────────

CREATE_SIGNAL = """
mutation CreateSignal($input: CreateSignalInput!) {
  createSignal(input: $input) {
    id
    title
    severity
    publishedAt
    originLocation { id name level }
    destinationLocation { id name level }
    generalLocation { id name level }
  }
}
"""

UPDATE_SIGNAL_SEVERITY = """
mutation UpdateSignalSeverity($id: String!, $severity: Int!) {
  updateSignalSeverity(id: $id, severity: $severity) {
    id
    severity
  }
}
"""

CREATE_EVENT = """
mutation CreateEvent($input: CreateEventInput!) {
  createEvent(input: $input) {
    id
    title
    types
  }
}
"""

UPDATE_EVENT = """
mutation UpdateEvent($id: String!, $input: UpdateEventInput!) {
  updateEvent(id: $id, input: $input) {
    id
    title
  }
}
"""

CREATE_ALERT = """
mutation CreateAlert($input: CreateAlertInput!) {
  createAlert(input: $input) {
    id
    status
  }
}
"""

ESCALATE_EVENT = """
mutation EscalateEvent($eventId: String!, $userId: String!) {
  escalateEvent(eventId: $eventId, userId: $userId) {
    id
    isSituation
    validFrom
    validTo
  }
}
"""

NOTIFY_ALERT_SUBSCRIBERS = """
mutation NotifyAlertSubscribers($input: AlertNotifyInput!) {
  notifyAlertSubscribers(input: $input)
}
"""

NOTIFY_ALERT_DIGEST = """
mutation NotifyAlertDigest($input: AlertDigestInput!) {
  notifyAlertDigest(input: $input)
}
"""

UPDATE_LOCATION_GEOMETRY = """
mutation UpdateLocationGeometry($id: String!, $geometry: GeoJSON!) {
  updateLocationGeometry(id: $id, geometry: $geometry) { id }
}
"""

UPDATE_LOCATION_POPULATION = """
mutation UpdateLocationPopulation($id: String!, $population: String!) {
  updateLocationPopulation(id: $id, population: $population) { id population }
}
"""

UPDATE_SITUATION_POPULATION = """
mutation UpdateSituationPopulation($id: String!, $input: UpdateSituationPopulationInput!) {
  updateSituationPopulation(id: $id, input: $input) {
    id
    populationAffected
    populationInArea
  }
}
"""

GET_LOCATION_WITH_GEOMETRY = """
query LocationWithGeometry($id: String!) {
  location(id: $id) {
    id
    name
    level
    population
    geometry
    parent { id name level }
  }
}
"""

GET_LOCATIONS_BY_LEVEL = """
query LocationsByLevel($level: Int!) {
  locations(level: $level) {
    id
    name
    level
    pCode
    population
  }
}
"""

GET_EVENT_FOR_SITUATION = """
query EventForSituation($id: String!) {
  event(id: $id) {
    id
    title
    description
    types
    severity
    populationAffected
    originLocation { name }
    destinationLocation { name }
    generalLocation { name }
  }
}
"""

GET_RECENT_ALERTS = """
query RecentAlerts($since: DateTime!) {
  alerts(status: published) {
    id
    status
    event {
      id
      firstSignalCreatedAt
    }
  }
}
"""

# ─── Queries ──────────────────────────────────────────────────────────────────

GET_LATEST_SIGNAL = """
query LatestSignal {
  signals {
    id
    publishedAt
  }
}
"""

GET_EVENTS = """
query Events {
  events {
    id
    title
    description
    types
    severity
    rank
    validFrom
    validTo
    firstSignalCreatedAt
    lastSignalCreatedAt
    originLocation { id name }
    destinationLocation { id name }
    generalLocation { id name }
    alerts { id status }
  }
}
"""

GET_LOCATIONS = """
query Locations {
  locations {
    id
    name
    level
    parent { id name }
  }
}
"""

GET_DATA_SOURCES = """
query DataSources {
  dataSources {
    id
    name
  }
}
"""

GET_DISASTER_TYPES = """
query DisasterTypes {
  disasterTypes {
    id
    disasterType
    disasterClass
    glideNumber
    level1
    level2
    idType
  }
}
"""


def _execute(query: str, variables: dict | None = None, retries: int = 3) -> dict:
    """Execute a GraphQL query/mutation with retry logic."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {settings.clear_api_key}",
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    for attempt in range(1, retries + 1):
        try:
            resp = httpx.post(
                settings.clear_api_url,
                json=payload,
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            result = resp.json()

            if "errors" in result:
                logger.error("GraphQL errors: %s", result["errors"])
                raise RuntimeError(f"GraphQL errors: {result['errors']}")

            return result["data"]

        except (httpx.HTTPError, RuntimeError) as e:
            if attempt < retries:
                wait = 2**attempt
                logger.warning(
                    "GraphQL request failed (attempt %d/%d), retrying in %ds: %s",
                    attempt,
                    retries,
                    wait,
                    e,
                )
                time.sleep(wait)
            else:
                logger.error("GraphQL request failed after %d attempts: %s", retries, e)
                raise


# ─── Public API ───────────────────────────────────────────────────────────────


def create_signal(input_data: dict) -> dict:
    result = _execute(CREATE_SIGNAL, {"input": input_data})
    return result["createSignal"]


def update_signal_severity(signal_id: str, severity: int) -> dict:
    """Update a signal's severity score (1-5)."""
    result = _execute(UPDATE_SIGNAL_SEVERITY, {"id": signal_id, "severity": severity})
    return result["updateSignalSeverity"]


def create_event(input_data: dict) -> dict:
    result = _execute(CREATE_EVENT, {"input": input_data})
    return result["createEvent"]


def update_event(event_id: str, input_data: dict) -> dict:
    result = _execute(UPDATE_EVENT, {"id": event_id, "input": input_data})
    return result["updateEvent"]


def escalate_event(event_id: str, user_id: str) -> dict:
    """Escalate an event to an alert and record the user escalation."""
    result = _execute(ESCALATE_EVENT, {"eventId": event_id, "userId": user_id})
    return result["escalateEvent"]


def create_alert(input_data: dict) -> dict:
    result = _execute(CREATE_ALERT, {"input": input_data})
    return result["createAlert"]


def notify_alert_subscribers(alert_id: str) -> int:
    """Notify immediate subscribers of an alert. Returns notification count."""
    result = _execute(NOTIFY_ALERT_SUBSCRIBERS, {"input": {"alertId": alert_id}})
    return result["notifyAlertSubscribers"]


def notify_alert_digest(alert_ids: list[str], frequency: str) -> int:
    """Send digest notifications for alerts. Returns notification count."""
    result = _execute(NOTIFY_ALERT_DIGEST, {"input": {"alertIds": alert_ids, "frequency": frequency}})
    return result["notifyAlertDigest"]


def get_published_alerts() -> list[dict]:
    """Get all published alerts."""
    result = _execute(GET_RECENT_ALERTS)
    return result.get("alerts", [])


def get_latest_signal_timestamp() -> str | None:
    """Get the publishedAt of the most recent signal, or None if no signals exist."""
    result = _execute(GET_LATEST_SIGNAL)
    signals = result.get("signals", [])
    if not signals:
        return None
    # Find the most recent by publishedAt
    return max(signals, key=lambda s: s["publishedAt"])["publishedAt"]


def get_events() -> list[dict]:
    result = _execute(GET_EVENTS)
    return result.get("events", [])


def get_locations() -> list[dict]:
    result = _execute(GET_LOCATIONS)
    return result.get("locations", [])


def get_data_sources() -> list[dict]:
    result = _execute(GET_DATA_SOURCES)
    return result.get("dataSources", [])


def get_disaster_types() -> list[dict]:
    result = _execute(GET_DISASTER_TYPES)
    return result.get("disasterTypes", [])


def get_dataminr_source_id() -> str:
    """Find the dataminr data source ID from the CLEAR API."""
    sources = get_data_sources()
    for src in sources:
        if src["name"] == settings.dataminr_source_name:
            return src["id"]
    raise RuntimeError(
        f"Data source '{settings.dataminr_source_name}' not found in CLEAR API. "
        "Ensure it exists in the data_sources table."
    )


# ─── Population / Geometry helpers ────────────────────────────────────────────


def get_location_with_geometry(location_id: str) -> dict | None:
    result = _execute(GET_LOCATION_WITH_GEOMETRY, {"id": location_id})
    return result.get("location")


def get_locations_by_level(level: int) -> list[dict]:
    result = _execute(GET_LOCATIONS_BY_LEVEL, {"level": level})
    return result.get("locations", [])


def update_location_geometry(location_id: str, geometry: dict) -> dict:
    result = _execute(
        UPDATE_LOCATION_GEOMETRY,
        {"id": location_id, "geometry": geometry},
    )
    return result["updateLocationGeometry"]


def update_location_population(location_id: str, population: int) -> dict:
    result = _execute(
        UPDATE_LOCATION_POPULATION,
        {"id": location_id, "population": str(population)},
    )
    return result["updateLocationPopulation"]


def update_situation_population(
    situation_id: str,
    population_affected: int | None = None,
    population_in_area: int | None = None,
    title: str | None = None,
    summary: str | None = None,
) -> dict:
    input_data: dict = {}
    if population_affected is not None:
        input_data["populationAffected"] = str(population_affected)
    if population_in_area is not None:
        input_data["populationInArea"] = str(population_in_area)
    if title is not None:
        input_data["title"] = title
    if summary is not None:
        input_data["summary"] = summary
    result = _execute(
        UPDATE_SITUATION_POPULATION,
        {"id": situation_id, "input": input_data},
    )
    return result["updateSituationPopulation"]


def get_event_for_situation(event_id: str) -> dict | None:
    result = _execute(GET_EVENT_FOR_SITUATION, {"id": event_id})
    return result.get("event")
