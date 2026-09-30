"""Country-only Dataminr alerts attach to the country (L0), not a centroid point.

Dataminr labels an alert it can only place at country level with the bare
country name and puts the coordinates at the country centroid. Sudan's centroid
(12.8628, 30.2176) falls inside Sheikan locality, so forwarding those coords let
clear-api create an L4 "Point 12.8628, 30.2176" under Sheikan and national
stories (fuel prices, meat exports, UNHCR figures) clustered into El Obeid
events and alerts.
"""

from unittest.mock import patch

import pytest

from clear_pipeline.providers import location, signal
from clear_pipeline.providers.dataminr import DataminrSignal, EstimatedEventLocation
from clear_pipeline.providers.geoparser import GeoparseResult

SUDAN_L0 = "3c7edcfb-85f1-46e4-8a7b-29651f73d740"
VENEZUELA_L0 = "fd3e8bb7-70db-44e8-b1a8-2de13983d594"
L0_ROWS = [
    {"id": SUDAN_L0, "name": "Sudan", "level": 0},
    {"id": VENEZUELA_L0, "name": "Venezuela (Bolivarian Republic of)", "level": 0},
]
SUDAN_CENTROID = [12.8628, 30.2176]


@pytest.fixture(autouse=True)
def _l0_locations():
    location.invalidate_locations_cache()
    with patch.object(location, "get_locations_by_level", return_value=L0_ROWS) as m:
        yield m
    location.invalidate_locations_cache()


def _alert(name: str | None, coords: list[float] | None) -> DataminrSignal:
    return DataminrSignal(
        alertId="72906445594386907631790340684324-1790340684324-1",
        alertTimestamp="2026-09-25T12:52:14.999Z",
        estimatedEventLocation=EstimatedEventLocation(name=name, coordinates=coords),
        headline="UNHCR says more than 55,000 Sudanese refugees fled to eastern Chad in 2026",
    )


def _build(alert, *, geo_result=None, promo=None, candidate=None):
    with (
        patch.object(signal, "geoparse_signal", return_value=geo_result) as geoparse,
        patch.object(signal, "extract_top_candidate", return_value=candidate),
        patch.object(signal, "find_or_create_landmark_l4", return_value=promo or {}) as l4,
        patch.object(signal, "resolve_signal_location") as llm_resolve,
    ):
        out = signal.build_signal_input(alert, "src-dataminr")
    return out, geoparse, l4, llm_resolve


def test_country_only_alert_attaches_to_country_not_centroid_point():
    out, geoparse, _, llm_resolve = _build(_alert("Sudan", SUDAN_CENTROID))

    assert out["locationId"] == SUDAN_L0
    # No coords → clear-api never reaches createPointLocation.
    assert "lat" not in out and "lng" not in out
    # The geocode is still scoped to Sudan even though the coords were dropped.
    assert geoparse.call_args.kwargs["expected_country_codes"] == {"sd"}
    llm_resolve.assert_not_called()


def test_country_only_alert_drops_unresolved_point_name():
    out, *_ = _build(_alert("Sudan", SUDAN_CENTROID), candidate="Gezira")

    assert out["locationId"] == SUDAN_L0
    assert "pointName" not in out


def test_country_only_alert_still_promotes_a_place_named_in_the_text():
    geo = GeoparseResult(
        candidate="El Fasher", kind="admin", field="title", lat=13.63, lng=25.35,
        country_code="sd", osm_class=None, osm_type=None, importance=0.5,
        display_name="El Fasher", raw={},
    )
    out, _, l4, _ = _build(
        _alert("Sudan", SUDAN_CENTROID), geo_result=geo, promo={"locationId": "l4-el-fasher"}
    )

    assert out["locationId"] == "l4-el-fasher"
    # The centroid is not a real source location, so it must not feed the
    # same-A2 check (it would only ever allow places inside Sheikan).
    assert l4.call_args.kwargs["source_lat"] is None
    assert l4.call_args.kwargs["source_lng"] is None


def test_country_name_matches_long_form_l0_name():
    out, *_ = _build(_alert("Venezuela", [6.42, -66.58]))

    assert out["locationId"] == VENEZUELA_L0


def test_sub_national_alert_keeps_source_coords():
    out, geoparse, _, _ = _build(_alert("Khartoum, Sudan", [15.5974, 32.5356]))

    assert "locationId" not in out
    assert (out["lat"], out["lng"]) == (15.5974, 32.5356)
    assert geoparse.call_args.kwargs["expected_country_codes"] == {"sd"}
