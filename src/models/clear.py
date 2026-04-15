"""Pydantic models for CLEAR API GraphQL mutation inputs and responses."""

from pydantic import BaseModel


class CreateSignalInput(BaseModel):
    sourceId: str
    rawData: dict
    publishedAt: str  # ISO-8601
    collectedAt: str | None = None
    url: str | None = None
    title: str | None = None
    description: str | None = None
    originId: str | None = None
    destinationId: str | None = None
    locationId: str | None = None
    lat: float | None = None  # For server-side PostGIS geo-resolution
    lng: float | None = None


class CreateEventInput(BaseModel):
    signalIds: list[str]
    title: str | None = None
    description: str | None = None
    descriptionSignals: dict | None = None
    validFrom: str  # ISO-8601
    validTo: str  # ISO-8601
    firstSignalCreatedAt: str  # ISO-8601
    lastSignalCreatedAt: str  # ISO-8601
    originId: str | None = None
    destinationId: str | None = None
    locationId: str | None = None
    types: list[str]
    populationAffected: str | None = None
    rank: float
    lat: float | None = None  # For server-side PostGIS geo-resolution
    lng: float | None = None


class CreateAlertInput(BaseModel):
    eventId: str
    status: str | None = "published"


class SignalClassification(BaseModel):
    """Output from Claude signal classification."""

    disaster_types: list[str]  # glide numbers e.g. ["fl", "ff"]
    relevance: float  # 0.0-1.0
    severity: int  # 1-5
    summary: str


class EventGroupingResult(BaseModel):
    """Output from Claude event grouping."""

    action: str  # "create_new" or "add_to_existing"
    event_id: str | None = None  # if add_to_existing
    title: str | None = None  # for both actions (updated title when adding to existing)
    description: str | None = None  # for both actions (updated description when adding to existing)
    types: list[str] | None = None  # if create_new
    population_affected: int | None = None  # extracted from signal text


class AlertAssessment(BaseModel):
    """Output from Claude alert assessment."""

    should_alert: bool
    status: str = "published"  # "draft" or "published"


class SituationNarrative(BaseModel):
    """Output from Claude situation narrative generation."""

    title: str
    summary: str
