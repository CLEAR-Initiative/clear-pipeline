"""Pydantic models for the Dataminr First Alert API."""

from datetime import UTC, datetime

from pydantic import BaseModel, computed_field


class AlertType(BaseModel):
    name: str | None = None


class SubHeadline(BaseModel):
    title: str | None = None
    subHeadlines: str | None = None


class PublicPost(BaseModel):
    link: str | None = None
    text: str | None = None
    translatedText: str | None = None
    media: list[str] | None = None


class AlertList(BaseModel):
    name: str | None = None


class AlertTopic(BaseModel):
    name: str | None = None
    id: str | None = None


class LinkedAlert(BaseModel):
    parentAlertId: str | None = None
    count: int | None = None


class IntelSummary(BaseModel):
    title: str | None = None
    content: list[str] | None = None


class IntelAgent(BaseModel):
    summary: list[IntelSummary] | None = None


class CorroborationSummary(BaseModel):
    content: str | None = None


class EventCorroboration(BaseModel):
    summary: list[CorroborationSummary] | None = None


class LiveBrief(BaseModel):
    summary: str | None = None


class DataminrSignal(BaseModel):
    """A single Dataminr 'alert' — which maps to a CLEAR signal."""

    alertId: str
    eventTime: int  # Unix timestamp in milliseconds

    estimatedEventLocation: list[str] | None = None  # [name, lat, lon, radius, mgrs]
    alertType: AlertType | None = None
    headline: str | None = None
    subHeadline: SubHeadline | None = None
    publicPost: PublicPost | None = None
    firstAlertURL: str | None = None
    alertLists: list[AlertList] | None = None
    alertTopics: list[AlertTopic] | None = None
    linkedAlerts: list[LinkedAlert] | None = None
    termsOfUse: str | None = None
    intelAgents: list[IntelAgent] | None = None
    eventCorroboration: EventCorroboration | None = None
    liveBrief: list[LiveBrief] | None = None

    @computed_field
    @property
    def alertTimestamp(self) -> str:
        """ISO-8601 timestamp derived from eventTime for compatibility."""
        return datetime.fromtimestamp(self.eventTime / 1000, tz=UTC).isoformat()

    @property
    def locationName(self) -> str | None:
        """Extract location name from estimatedEventLocation array."""
        if self.estimatedEventLocation and len(self.estimatedEventLocation) > 0:
            return self.estimatedEventLocation[0]
        return None

    @property
    def coordinates(self) -> tuple[float, float] | None:
        """Extract (lat, lon) from estimatedEventLocation array."""
        if self.estimatedEventLocation and len(self.estimatedEventLocation) >= 3:
            try:
                return (float(self.estimatedEventLocation[1]), float(self.estimatedEventLocation[2]))
            except (ValueError, IndexError):
                return None
        return None


class DataminrAlertsResponse(BaseModel):
    """Top-level response from GET /alerts/1/alerts."""

    alerts: list[DataminrSignal] = []
    to: str | None = None  # Cursor for pagination
