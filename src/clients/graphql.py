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
    casualties
    externalId
    publishedAt
    originLocation { id name level ancestorIds }
    destinationLocation { id name level ancestorIds }
    generalLocation { id name level ancestorIds }
    # If the API returned an existing row (idempotent ingest), it may
    # already be linked to an event from a prior run. group_signal_v2 uses
    # this to short-circuit — a signal that already has an event must not
    # spawn another one.
    events { id title types severity casualties }
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
    isCrisis
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

UPDATE_LOCATION = """
mutation UpdateLocation($id: String!, $input: UpdateLocationInput!) {
  updateLocation(id: $id, input: $input) { id pCode name }
}
"""

CREATE_LOCATION = """
mutation CreateLocation($input: CreateLocationInput!) {
  createLocation(input: $input) { id name level pCode }
}
"""

ARCHIVE_STALE_ALERTS = """
mutation ArchiveStaleAlerts($olderThanDays: Int) {
  archiveStaleAlerts(olderThanDays: $olderThanDays) { alertsArchived }
}
"""

UPDATE_CRISIS_POPULATION = """
mutation UpdateCrisisPopulation($id: String!, $input: UpdateCrisisPopulationInput!) {
  updateCrisisPopulation(id: $id, input: $input) {
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

GET_EVENT_FOR_CRISIS = """
query EventForCrisis($id: String!) {
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

GET_EVENT_WITH_SIGNALS = """
query EventWithSignals($id: String!) {
  event(id: $id) {
    id
    title
    description
    types
    severity
    casualties
    signals {
      id
      title
      description
      severity
      casualties
      publishedAt
      source { id name type }
    }
  }
}
"""

GET_LOCATION_METADATA = """
query LocationMetadata($locationId: String!, $type: String) {
  locationMetadata(locationId: $locationId, type: $type) {
    id
    type
    data
    validFrom
    validTo
  }
}
"""

UPSERT_LOCATION_METADATA = """
mutation UpsertLocationMetadata($input: UpsertLocationMetadataInput!) {
  upsertLocationMetadata(input: $input) { id type data updatedAt }
}
"""

UPSERT_LOCATION_METADATA_BATCH = """
mutation UpsertLocationMetadataBatch($inputs: [UpsertLocationMetadataInput!]!) {
  upsertLocationMetadataBatch(inputs: $inputs) { id type }
}
"""

ALL_LOCATION_METADATA = """
query AllLocationMetadata($type: String!) {
  allLocationMetadata(type: $type) {
    id
    type
    data
    location { id name pCode }
  }
}
"""

GET_RECENT_ALERTS = """
query RecentAlerts {
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
    originLocation { id name level ancestorIds }
    destinationLocation { id name level ancestorIds }
    generalLocation { id name level ancestorIds }
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


class GraphQLClientError(Exception):
    """4xx / validation / schema errors from clear-api. NOT retryable —
    these indicate a bug in the request shape, missing field, bad auth,
    etc. Retrying them just amplifies damage (see the populationDisplaced
    incident: each retry created another duplicate event)."""


def _execute(query: str, variables: dict | None = None, retries: int = 3) -> dict:
    """Execute a GraphQL query/mutation with retry logic.

    4xx responses raise `GraphQLClientError` immediately with no retry.
    5xx / connection errors are retried with exponential backoff.
    """
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

            # 4xx → treat as non-retryable bug, raise and stop. The body
            # often has useful detail the default message hides.
            if 400 <= resp.status_code < 500:
                body_snippet = resp.text[:500] if resp.text else "(empty)"
                msg = (
                    f"GraphQL {resp.status_code} (non-retryable) for {settings.clear_api_url}: "
                    f"{body_snippet}"
                )
                logger.error(msg)
                raise GraphQLClientError(msg)

            resp.raise_for_status()
            result = resp.json()

            if "errors" in result:
                logger.error("GraphQL errors: %s", result["errors"])
                raise RuntimeError(f"GraphQL errors: {result['errors']}")

            return result["data"]

        except GraphQLClientError:
            # Never retry 4xx
            raise

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


def update_location(location_id: str, **fields) -> dict:
    """Update a location's scalar fields (pCode, name, geoId, osmId, level, parentId).
    Only fields passed are changed."""
    result = _execute(
        UPDATE_LOCATION,
        {"id": location_id, "input": fields},
    )
    return result["updateLocation"]


def create_location(name: str, level: int, **fields) -> dict:
    """Create a new location (geometry defaults to POINT(0 0); set via
    update_location_geometry afterwards)."""
    payload = {"name": name, "level": level, **fields}
    result = _execute(CREATE_LOCATION, {"input": payload})
    return result["createLocation"]


def archive_stale_alerts(older_than_days: int = 14) -> int:
    """Archive alerts whose event.lastSignalCreatedAt is older than N days.
    Returns the number of rows affected."""
    result = _execute(ARCHIVE_STALE_ALERTS, {"olderThanDays": older_than_days})
    return int(result["archiveStaleAlerts"]["alertsArchived"])


def update_crisis_population(
    crisis_id: str,
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
        UPDATE_CRISIS_POPULATION,
        {"id": crisis_id, "input": input_data},
    )
    return result["updateCrisisPopulation"]


def get_event_for_crisis(event_id: str) -> dict | None:
    result = _execute(GET_EVENT_FOR_CRISIS, {"id": event_id})
    return result.get("event")


def get_event_with_signals(event_id: str) -> dict | None:
    """Fetch an event plus all its linked signals. Used by the rewrite pass
    of the new grouping algorithm."""
    result = _execute(GET_EVENT_WITH_SIGNALS, {"id": event_id})
    return result.get("event")


def get_location_metadata(location_id: str, type_: str | None = None) -> list[dict]:
    """Return all locationMetadata rows for a location, optionally filtered by type."""
    variables: dict = {"locationId": location_id}
    if type_ is not None:
        variables["type"] = type_
    result = _execute(GET_LOCATION_METADATA, variables)
    return result.get("locationMetadata", []) or []


def upsert_location_metadata(location_id: str, type_: str, data: dict) -> dict:
    """Create or update a location's metadata entry for a given type."""
    result = _execute(
        UPSERT_LOCATION_METADATA,
        {"input": {"locationId": location_id, "type": type_, "data": data}},
    )
    return result["upsertLocationMetadata"]


def upsert_location_metadata_batch(
    rows: list[dict],
) -> list[dict]:
    """Bulk upsert. Each row must have {locationId, type, data}. Returns the
    resulting rows (order not guaranteed). Rows whose locationId doesn't exist
    are silently skipped by the server.
    """
    if not rows:
        return []
    result = _execute(UPSERT_LOCATION_METADATA_BATCH, {"inputs": rows})
    return result.get("upsertLocationMetadataBatch", []) or []


def get_all_location_metadata(type_: str) -> list[dict]:
    """Return every locationMetadata row of a given type across all locations."""
    result = _execute(ALL_LOCATION_METADATA, {"type": type_})
    return result.get("allLocationMetadata", []) or []
