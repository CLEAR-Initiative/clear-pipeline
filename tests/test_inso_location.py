"""INSO incidents get their location in the drain, from their own text.

gx sends only the report's country. The drain's ``InsoConnector.locate`` then
resolves the most specific state or district inside that country from the
incident's title and description (``locate_in_country``, as for darfur24 and
the DTM bulletins), falling back to the country. Until 2 Nov 2026 gx ran the
geoparser over the extracted place fields instead, and every incident it
couldn't resolve was pinned to the country L0, discarding the extracted admin1.
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from clear_pipeline.defs.signals import connectors, stages
from clear_pipeline.defs.signals.connectors import InsoConnector, LocatingSource
from clear_pipeline.providers import inso
from clear_pipeline.providers.clear_api import ClearApiError
from clear_pipeline.providers.llm import LLMUnavailableError
from clear_pipeline.providers.location import LocationsUnavailableError

GOLDEN = Path(__file__).parent / "golden" / "inso"
REPORT = inso.parse((GOLDEN / "01_multi_incident.eml").read_bytes())  # header: "Republic of Sudan, Darfur"
L0 = [
    {"id": "loc-ss", "name": "South Sudan", "level": 0, "pCode": "SS"},
    {"id": "loc-sd", "name": "Sudan", "level": 0, "pCode": "SD"},
]
ROW = {
    "id": "sig-inso-1",
    "externalId": "inso:SDN/Darfur/IR/20262010/R1:2",
    "title": "Armed men loot shops in Tawila",
    "description": "On 19 October, armed men looted shops in Tawila, North Darfur.",
    "publishedAt": "2026-10-20T09:30:00+00:00",
    "originLocation": None,
    "generalLocation": None,
    "destinationLocation": None,
}


def _view(report=REPORT):
    return InsoConnector().project(report, ROW)


def _locate(view, l0=L0, located="sd-nd-tawila"):
    connector = InsoConnector()
    with (
        patch.object(connectors, "get_locations_by_level", return_value=l0) as levels,
        patch.object(connectors, "locate_in_country", return_value=located) as locate,
    ):
        result = connector.locate(view)
    return result, locate, levels


def test_inso_is_a_locating_source():
    assert isinstance(InsoConnector(), LocatingSource)


def test_view_carries_the_header_country_as_written():
    assert _view().location_name == "Republic of Sudan"


def test_locate_reads_the_incident_text_inside_the_report_country():
    result, locate, _ = _locate(_view())
    assert result == "sd-nd-tawila"
    locate.assert_called_once_with(ROW["title"], ROW["description"], "Sudan", country_code="sd")


def test_a_long_form_header_country_is_not_mistaken_for_its_neighbour():
    # "Republic of Sudan" must resolve to Sudan, never South Sudan.
    assert inso.match_country("Republic of Sudan", L0).location_id == "loc-sd"
    assert inso.match_country("Republic of South Sudan", L0).location_id == "loc-ss"


def test_a_country_without_a_pcode_is_located_without_geocoding():
    _, locate, _ = _locate(_view(), l0=[{"id": "loc-sd", "name": "Sudan", "level": 0}])
    assert locate.call_args.kwargs["country_code"] is None


def test_an_unknown_country_is_left_unlocated_without_an_llm_call():
    result, locate, _ = _locate(_view(), l0=[L0[0]])
    assert result is None
    locate.assert_not_called()


def test_countries_are_fetched_once_per_connector():
    connector = InsoConnector()
    with (
        patch.object(connectors, "get_locations_by_level", return_value=L0) as levels,
        patch.object(connectors, "locate_in_country", return_value="sd"),
    ):
        connector.locate(_view())
        connector.locate(_view())
    levels.assert_called_once_with(0)


def test_a_countries_outage_defers_the_incident():
    with patch.object(connectors, "get_locations_by_level", side_effect=ClearApiError("down")), \
         pytest.raises(LocationsUnavailableError):
        InsoConnector().locate(_view())


@pytest.mark.parametrize("error", [LocationsUnavailableError("down"), LLMUnavailableError("billing")])
def test_locate_lets_an_outage_through(error):
    # Pinning to the country would lose the district for good; the drain defers the incident instead.
    with (
        patch.object(connectors, "get_locations_by_level", return_value=L0),
        patch.object(connectors, "locate_in_country", side_effect=error),
        pytest.raises(type(error)),
    ):
        InsoConnector().locate(_view())


def test_drain_locates_an_unplaced_incident_before_grouping():
    connector = InsoConnector()
    located = {"id": "sd-nd-tawila", "name": "Tawila", "level": 2, "ancestorIds": ["sd", "sd-nd"]}
    with (
        patch.object(connector, "locate", return_value="sd-nd-tawila"),
        patch.object(stages, "update_signal_location", return_value={"id": ROW["id"], "generalLocation": located}) as update,
    ):
        row = stages._locate_if_unplaced(connector, _view(), dict(ROW))
    update.assert_called_once_with("sig-inso-1", "sd-nd-tawila")
    assert row["generalLocation"] == located
