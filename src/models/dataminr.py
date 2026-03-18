"""Pydantic models for the Dataminr First Alert API (new v1 format)."""

from pydantic import BaseModel


class EstimatedEventLocation(BaseModel):
    name: str | None = None
    coordinates: list[float] | None = None  # [lat, lon]
    probabilityRadius: float | None = None
    MGRS: str | None = None


class AlertType(BaseModel):
    name: str | None = None


class SubHeadline(BaseModel):
    title: str | None = None
    subHeadlines: str | None = None


class PublicPost(BaseModel):
    href: str | None = None
    text: str | None = None
    translatedText: str | None = None
    media: list[str] | None = None


class ListMatched(BaseModel):
    id: str | None = None
    name: str | None = None
    topicIds: list[str] | None = None


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
    alertTimestamp: str  # ISO-8601

    estimatedEventLocation: EstimatedEventLocation | None = None
    alertType: AlertType | None = None
    headline: str | None = None
    subHeadline: SubHeadline | None = None
    publicPost: PublicPost | None = None
    dataminrAlertUrl: str | None = None
    listsMatched: list[ListMatched] | None = None
    alertTopics: list[AlertTopic] | None = None
    linkedAlerts: list[LinkedAlert] | None = None
    termsOfUse: str | None = None
    intelAgents: list[IntelAgent] | None = None
    eventCorroboration: EventCorroboration | None = None
    liveBrief: list[LiveBrief] | None = None


class DataminrAlertsResponse(BaseModel):
    """Top-level response from GET /firstalert/v1/alerts."""

    alerts: list[DataminrSignal] = []
    nextPage: str | None = None
