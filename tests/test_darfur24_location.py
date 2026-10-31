"""darfur24 articles get a Sudanese location from their text before grouping.

Until 31 Oct 2026 every darfur24 signal was pinned to the country at creation
(expo-385), so none reached a district and none could join an event (grouping
keys on district): 0% of 321 signals at district or finer on dev, 10 Aug to
31 Oct. The connector now creates the signal without a location and the drain
resolves one from the article text, scoped to Sudan, falling back to Sudan.
"""

from unittest.mock import patch

from clear_pipeline.defs.signals import connectors, stages
from clear_pipeline.defs.signals.connectors import Darfur24Connector, LocatingSource
from clear_pipeline.providers import darfur24
from clear_pipeline.signals.config import settings

ARTICLE = {
    "darfur24_id": "12345",
    "title": "Shelling kills 12 in El Fasher market",
    "description": "Shelling of the main market in El Fasher, North Darfur, killed at least 12 people on Monday.",
    "url": "https://darfur24.com/en/2026/10/27/shelling-el-fasher/",
    "published_at": "2026-10-27T09:00:00Z",
    "raw": {"guid": "12345"},
}
ROW = {
    "id": "sig-d24-1",
    "publishedAt": "2026-10-27T09:00:00Z",
    "originLocation": None,
    "generalLocation": None,
    "destinationLocation": None,
}


def _view():
    return Darfur24Connector().project(ARTICLE, ROW)


def test_darfur24_is_a_locating_source():
    assert isinstance(Darfur24Connector(), LocatingSource)


def test_signal_is_created_without_a_location():
    # The drain places it; pinning the country here would stop `locate` running.
    with patch.object(connectors.darfur24, "build_darfur24_signal_input", return_value={"sourceId": "ds"}) as build:
        Darfur24Connector().to_signal_input(ARTICLE, "ds")
    build.assert_called_once_with(ARTICLE, "ds", None)


def test_builder_omits_location_when_none_is_given():
    with patch.object(darfur24, "extract_event", return_value=None):
        assert "locationId" not in darfur24.build_darfur24_signal_input(ARTICLE, "ds", None)
        assert darfur24.build_darfur24_signal_input(ARTICLE, "ds", "sd")["locationId"] == "sd"


def test_locate_scopes_the_lookup_to_the_configured_country():
    with patch.object(connectors, "locate_in_country", return_value="sd-nd-elfasher") as locate:
        assert Darfur24Connector().locate(_view()) == "sd-nd-elfasher"
    locate.assert_called_once_with(
        ARTICLE["title"],
        ARTICLE["description"],
        settings.darfur24_default_country,
        country_code=settings.darfur24_country_code,
    )


def test_locate_falls_back_to_the_country_row():
    connector = Darfur24Connector()
    with (
        patch.object(connectors, "locate_in_country", return_value=None),
        patch.object(connector, "_resolve_location_id", return_value="sd"),
    ):
        assert connector.locate(_view()) == "sd"


def test_drain_locates_an_unplaced_article_before_grouping():
    connector = Darfur24Connector()
    located = {"id": "sd-nd-elfasher", "name": "El Fasher", "level": 2, "ancestorIds": ["sd", "sd-nd"]}
    with (
        patch.object(connector, "locate", return_value="sd-nd-elfasher"),
        patch.object(stages, "update_signal_location", return_value={"id": ROW["id"], "generalLocation": located}) as update,
    ):
        row = stages._locate_if_unplaced(connector, _view(), dict(ROW))
    update.assert_called_once_with("sig-d24-1", "sd-nd-elfasher")
    assert row["generalLocation"] == located


def test_drain_leaves_an_already_placed_article_alone():
    connector = Darfur24Connector()
    placed = {**ROW, "generalLocation": {"id": "sd", "name": "Sudan", "level": 0}}
    with (
        patch.object(connector, "locate") as locate,
        patch.object(stages, "update_signal_location") as update,
    ):
        assert stages._locate_if_unplaced(connector, _view(), placed) is placed
    locate.assert_not_called()
    update.assert_not_called()
