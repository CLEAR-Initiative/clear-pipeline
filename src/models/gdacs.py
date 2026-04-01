"""Pydantic models for the GDACS API response format."""

from pydantic import BaseModel, Field


class GdacsEventGeo(BaseModel):
    """Geographic info from a GDACS event."""
    lat: float | None = Field(None, alias="lat")
    lng: float | None = Field(None, alias="lng")


class GdacsEvent(BaseModel):
    """A single GDACS event from the search/list API.

    Field names are based on the actual GDACS API JSON response
    (discovered empirically — the swagger doesn't document response schemas).
    """
    eventid: int | None = None
    eventtype: str | None = None  # EQ, TC, FL, VO, DR, WF, etc.
    name: str | None = None
    description: str | None = None
    htmldescription: str | None = None
    alertlevel: str | None = None  # Green, Orange, Red
    alertscore: float | None = None
    severity: float | None = None
    severitydata: dict | None = None
    country: str | None = None
    fromdate: str | None = None  # ISO datetime
    todate: str | None = None
    # Geographic
    geo_lat: float | None = Field(None, alias="lat")
    geo_lng: float | None = Field(None, alias="lng")
    # Links
    url: str | None = None
    # GLIDE number
    glide: str | None = None
    # ISO3 country code
    iso3: str | None = None

    model_config = {"populate_by_name": True}


class GdacsSearchResponse(BaseModel):
    """Response from /api/Events/geteventlist/search."""
    # The response structure varies, but typically contains a list of features/events
    features: list[dict] | None = None
    # Some endpoints return events directly
    events: list[GdacsEvent] | None = None
    # Pagination
    totalCount: int | None = None
    pageSize: int | None = None
    pageNumber: int | None = None
