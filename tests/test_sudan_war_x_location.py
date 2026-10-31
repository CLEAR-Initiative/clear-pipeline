"""sudan-war-x signals get a Sudanese location before they are grouped.

X posts reach clear-api via ``POST /api/x/ingest`` with no location, so the
drain used to group every one into its own location-less event — invisible
under any country filter, and never merged with other posts about the same
strike (grouping keys on district). The drain now resolves a location from the
(mostly Arabic) text, scoped to Sudan, falling back to Sudan itself.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.defs.signals import stages
from clear_pipeline.defs.signals.connectors import (
    LocatingSource,
    ManualConnector,
    SudanWarXConnector,
)
from clear_pipeline.providers import location

SUDAN = {"id": "sd", "name": "Sudan", "level": 0, "parent": None}
CHAD = {"id": "td", "name": "Chad", "level": 0, "parent": None}
WEST_KORDOFAN = {"id": "sd-wk", "name": "West Kordofan", "level": 1, "parent": {"id": "sd"}}
DIBAB = {"id": "sd-wk-dibab", "name": "Al Dibab", "level": 2, "parent": {"id": "sd-wk"}}
KHARTOUM = {"id": "sd-kh", "name": "Khartoum", "level": 1, "parent": {"id": "sd"}}
SUDAN_POINT = {"id": "sd-pt", "name": "Point 12.89, 23.47", "level": 4, "parent": {"id": "sd-wk-dibab"}}
OUADDAI = {"id": "td-ou", "name": "Ouaddai", "level": 1, "parent": {"id": "td"}}
LOCATIONS = [SUDAN, CHAD, WEST_KORDOFAN, DIBAB, KHARTOUM, SUDAN_POINT, OUADDAI]

POST = "مقتل 84 شخصاً في هجومين بطائرات مسيرة في السودان، 80 في منطقة الدبيب بغرب كردفان"


@pytest.fixture(autouse=True)
def _locations():
    location.invalidate_locations_cache()
    with (
        patch.object(location, "get_locations", return_value=LOCATIONS),
        patch.object(location, "get_locations_by_level", return_value=[SUDAN, CHAD]),
    ):
        yield
    location.invalidate_locations_cache()


def _llm_returning(**fields):
    """Fake LLM: the location call answers `fields`; any other call (the
    Arabic spelling) answers its schema's defaults."""
    provider = MagicMock()
    provider.complete_structured.side_effect = lambda *, system, user, schema: (
        schema(**fields) if schema is location._LocationResolution else schema()
    )
    return patch.object(location, "make_llm_provider", return_value=provider), provider


# ── country-scoped resolution ────────────────────────────────────────────────

def test_scoped_locations_are_the_country_and_its_admin_areas_only():
    scoped = {loc["id"] for loc in location._country_scoped_locations("Sudan")}
    # Chad's rows and Sudan's coordinate-named L4 points are left out.
    assert scoped == {"sd", "sd-wk", "sd-wk-dibab", "sd-kh"}


def test_scoped_locations_for_unknown_country_is_empty():
    assert location._country_scoped_locations("Atlantis") == []


def test_scoped_resolution_only_offers_the_country_to_the_llm():
    patcher, provider = _llm_returning(location_type="general", location_id="sd-wk-dibab")
    with patcher:
        result = location.resolve_signal_location(POST, POST, None, country="Sudan")

    assert result["location_id"] == "sd-wk-dibab"
    prompt = provider.complete_structured.call_args.kwargs["user"]
    assert '"Al Dibab": "sd-wk-dibab"' in prompt
    assert "Ouaddai" not in prompt and "Point 12.89" not in prompt


def test_unscoped_resolution_still_offers_every_location():
    patcher, provider = _llm_returning(location_type="general", location_id="td-ou")
    with patcher:
        location.resolve_signal_location("Clashes in Ouaddai", None, None)
    assert "Ouaddai" in provider.complete_structured.call_args.kwargs["user"]


# ── locate_in_country ────────────────────────────────────────────────────────

def test_locate_in_country_returns_the_matched_district():
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk-dibab")
    with patcher:
        assert location.locate_in_country(POST, POST, "Sudan") == "sd-wk-dibab"


def test_locate_in_country_pins_displacement_to_its_origin():
    patcher, _ = _llm_returning(
        location_type="displacement", origin_id="sd-wk-dibab", destination_id="sd-kh",
    )
    with patcher:
        assert location.locate_in_country(POST, POST, "Sudan") == "sd-wk-dibab"


def test_locate_in_country_falls_back_to_the_country_when_nothing_matches():
    patcher, _ = _llm_returning(location_type="general")
    with patcher:
        assert location.locate_in_country("قصف مستمر", None, "Sudan") == "sd"


def test_locate_in_country_falls_back_when_the_llm_fails():
    provider = MagicMock()
    provider.complete_structured.side_effect = RuntimeError("model down")
    with patch.object(location, "make_llm_provider", return_value=provider):
        assert location.locate_in_country(POST, POST, "Sudan") == "sd"


def test_locate_in_country_drops_ids_outside_the_scope():
    # A hallucinated / out-of-scope id (Chad's) is rejected, not persisted.
    patcher, _ = _llm_returning(location_type="general", location_id="td-ou")
    with patcher:
        assert location.locate_in_country(POST, POST, "Sudan") == "sd"


# ── connectors ───────────────────────────────────────────────────────────────

def test_sudan_war_x_locates_and_manual_does_not():
    assert isinstance(SudanWarXConnector(), LocatingSource)
    # Analyst-created signals carry the analyst's chosen location; never guess.
    assert not isinstance(ManualConnector("manual"), LocatingSource)


# ── drain ────────────────────────────────────────────────────────────────────

def _x_row(**overrides) -> dict:
    row = {
        "id": "sig-x-1",
        "externalId": "x:2112241240762978429",
        "source": {"id": "ds-x", "name": "sudan-war-x"},
        "title": POST[:60] + "…",
        "description": POST,
        "severity": None,
        "casualties": None,
        "publishedAt": "2026-10-19T17:55:29Z",
        "status": "NEW",
        "revision": 0,
        "rawS3Key": None,
        "originLocation": None,
        "destinationLocation": None,
        "generalLocation": None,
        "events": [],
    }
    row.update(overrides)
    return row


@contextmanager
def _drain(rows, *, locate_to="sd-wk-dibab", update_side_effect=None):
    @contextmanager
    def fake_lock(key, **_kwargs):
        yield True

    batches = [rows, []]
    marked: list[tuple[str, list[str]]] = []

    def mark(items, status):
        marked.append((status, [i["id"] for i in items]))
        return len(items)

    def update(signal_id, location_id):
        return {
            "id": signal_id,
            "generalLocation": {"id": location_id, "name": "Al Dibab", "level": 2,
                                "ancestorIds": ["sd", "sd-wk"]},
        }

    with (
        patch.object(stages, "pending_signals", side_effect=lambda first: batches.pop(0) if batches else []),
        patch.object(stages, "pending_recomputes", return_value=[]),
        patch.object(stages, "mark_signals_processed", side_effect=mark),
        patch.object(stages, "redis_lock", side_effect=fake_lock),
        patch.object(stages, "classify_signal", return_value=MagicMock(relevance=0.9)),
        patch.object(stages, "group_signal", return_value={"id": "evt-1"}) as group,
        patch.object(stages, "enqueue_translation"),
        patch.object(stages, "sync_event_cards", return_value={"synced": 1, "skipped": 0}),
        patch.object(stages, "update_signal_location",
                     side_effect=update_side_effect or update) as update_loc,
        patch.object(SudanWarXConnector, "locate", return_value=locate_to) as locate,
        patch.object(stages, "_redis", MagicMock(**{"incr.return_value": 1})),
    ):
        yield {"group": group, "update": update_loc, "locate": locate, "marked": marked}


def test_drain_locates_an_unplaced_x_post_before_grouping():
    with _drain([_x_row()]) as m:
        stages._drain_signals_locked(MagicMock())

    m["update"].assert_called_once_with("sig-x-1", "sd-wk-dibab")
    grouped_row = m["group"].call_args.kwargs["created_signal"]
    assert grouped_row["generalLocation"]["id"] == "sd-wk-dibab"
    assert grouped_row["generalLocation"]["ancestorIds"] == ["sd", "sd-wk"]
    assert ("PROCESSED", ["sig-x-1"]) in m["marked"]


def test_drain_keeps_an_x_post_that_already_has_a_location():
    located = {"id": "sd-kh", "name": "Khartoum", "level": 1, "ancestorIds": ["sd"]}
    with _drain([_x_row(generalLocation=located)]) as m:
        stages._drain_signals_locked(MagicMock())

    m["locate"].assert_not_called()
    m["update"].assert_not_called()
    assert m["group"].call_args.kwargs["created_signal"]["generalLocation"] == located


def test_drain_groups_unlocated_when_nothing_resolves():
    with _drain([_x_row()], locate_to=None) as m:
        stages._drain_signals_locked(MagicMock())

    m["update"].assert_not_called()
    assert m["group"].call_args.kwargs["created_signal"]["generalLocation"] is None
    assert ("PROCESSED", ["sig-x-1"]) in m["marked"]


def test_drain_leaves_the_post_new_when_persisting_the_location_fails():
    # A clear-api blip must not let the post group location-less: it stays NEW
    # and the next drain retries it.
    with _drain([_x_row()], update_side_effect=RuntimeError("clear-api 502")) as m:
        stages._drain_signals_locked(MagicMock())

    m["group"].assert_not_called()
    assert all(not ids for _, ids in m["marked"])


# ── geocoding the named place (district level from a state-level pick) ──────

from shapely.geometry import Point, box  # noqa: E402

from clear_pipeline.providers import geoparser  # noqa: E402
from clear_pipeline.providers.geoparser import GeoparseResult  # noqa: E402

KADUGLI_POST = "🚨عاجل | جنوب كردفان \nالمليشيا تستهدف محطة كهرباء كادقلي عبر مسيرة استراتيجية."
# West Kordofan as a box; Al Dibab town sits inside it, Khartoum far outside.
WK_SHAPE = box(27.0, 10.0, 30.0, 13.0)


def _hit(lat: float, lng: float, *, kind: str = "admin") -> GeoparseResult:
    return GeoparseResult(
        candidate="Al Dibab", kind=kind, field="body", lat=lat, lng=lng,
        country_code="sd", osm_class=None, osm_type="geonames:PPL",
        importance=1.0, display_name="Al Dibab",
    )


@contextmanager
def _geocoding(*, hit=None, shape=WK_SHAPE, promo=None, geocode_error=None):
    # Like the real lookups: a hit the caller's area test rejects is a miss.
    def fake_geocode(name, *, expected_country_codes=None, accept=None, arabic_name=None,
                     within_location_id=None):
        if geocode_error:
            raise geocode_error
        return hit if hit and (accept is None or accept(hit.lat, hit.lng)) else None

    geocode = MagicMock(side_effect=fake_geocode)
    promote = MagicMock(return_value=promo or {"locationId": "l4-dibab", "reused": False,
                                               "abortedReason": None})
    geometry = {"type": "Polygon", "coordinates": [list(shape.exterior.coords)]} if shape else None
    with (
        patch.object(location, "geocode_place_name", geocode),
        patch.object(location, "find_or_create_landmark_l4", promote),
        patch.object(location, "get_location_with_geometry",
                     return_value={"id": "sd-wk", "geometry": geometry}),
    ):
        yield {"geocode": geocode, "promote": promote}


def test_prompt_asks_for_the_district_of_a_named_town_and_its_place_name():
    patcher, provider = _llm_returning(location_type="general", location_id="sd-wk")
    with patcher:
        location.resolve_signal_location(KADUGLI_POST, KADUGLI_POST, None, country="Sudan")
    system = provider.complete_structured.call_args.kwargs["system"]
    assert "even when a state name appears first" in system
    assert '"place_name"' in provider.complete_structured.call_args.kwargs["user"]


def test_state_pick_with_a_named_town_inside_it_becomes_a_point():
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Al Dibab")
    with patcher, _geocoding(hit=_hit(11.5, 28.5)) as m:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "l4-dibab"

    assert m["geocode"].call_args.args == ("Al Dibab",)
    assert m["geocode"].call_args.kwargs["expected_country_codes"] == {"sd"}
    accept = m["geocode"].call_args.kwargs["accept"]
    assert accept(11.5, 28.5) and not accept(15.6, 32.5)  # inside / outside West Kordofan
    m["promote"].assert_called_once_with(name="Al Dibab", lat=11.5, lng=28.5, kind="admin")


def test_a_same_named_place_outside_the_picked_area_is_rejected():
    # Village names repeat across Sudan: a hit in another state is the wrong one.
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Al Dibab")
    with patcher, _geocoding(hit=_hit(15.6, 32.5)) as m:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd-wk"
    m["promote"].assert_not_called()


def test_a_point_just_over_the_area_boundary_is_rejected():
    # GeoNames rounds some villages to the half degree; a point just outside
    # the area would be parented to the neighbouring state's district.
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Al Dibab")
    with patcher, _geocoding(hit=_hit(11.5, 30.01)) as m:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd-wk"
    m["promote"].assert_not_called()


@pytest.mark.parametrize("place", ["West Kordofan", "West Kordofan State", "Khartoum", "Sudan"])
def test_a_state_or_country_name_is_not_geocoded(place):
    # "Khartoum" names the state as well as its capital; geocoding it would pin
    # a state-wide post to the capital's district.
    patcher, _ = _llm_returning(location_type="general", location_id="sd-kh", place_name=place)
    with patcher, _geocoding(hit=_hit(11.5, 28.5)) as m:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd-kh"
    m["geocode"].assert_not_called()


def test_a_district_sharing_a_town_name_is_still_geocoded():
    # Only country/state names are skipped: "Al Dibab" is a district and a town.
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Al Dibab")
    with patcher, _geocoding(hit=_hit(11.5, 28.5)) as m:
        location.locate_in_country(POST, POST, "Sudan", country_code="sd")
    m["geocode"].assert_called_once()


@pytest.mark.parametrize("kwargs", [
    {"hit": None},                                              # no geocode
    {"hit": _hit(11.5, 28.5), "shape": None},                   # area has no geometry
    {"hit": _hit(11.5, 28.5), "promo": {"locationId": None, "abortedReason": "different_a2"}},
    {"geocode_error": RuntimeError("clear-api 502")},           # geocoder blew up
])
def test_geocoding_failures_keep_the_area(kwargs):
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Al Dibab")
    with patcher, _geocoding(**kwargs):
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd-wk"


def test_no_place_name_means_no_geocoding():
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk")
    with patcher, _geocoding(hit=_hit(11.5, 28.5)) as m:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd-wk"
    m["geocode"].assert_not_called()


def test_without_a_country_code_the_place_is_not_geocoded():
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Al Dibab")
    with patcher, _geocoding(hit=_hit(11.5, 28.5)) as m:
        assert location.locate_in_country(POST, POST, "Sudan") == "sd-wk"
    m["geocode"].assert_not_called()


def test_a_point_exactly_on_the_area_boundary_is_rejected():
    # A rounded coordinate on a shared border doesn't say which side owns it.
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Al Dibab")
    with patcher, _geocoding(hit=_hit(11.5, 30.0)) as m:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd-wk"
    m["promote"].assert_not_called()


def test_an_explicit_country_answer_is_not_geocoded():
    # Claude returned Sudan itself (places span several states): one town's
    # point would pull a multi-state report into that town's district.
    patcher, _ = _llm_returning(location_type="general", location_id="sd", place_name="Al Dibab")
    with patcher, _geocoding(hit=_hit(11.5, 28.5)) as m:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd"
    m["geocode"].assert_not_called()


def test_a_country_fallback_is_still_refined_by_a_geocoded_place():
    # Claude named a town but matched no admin area: check against Sudan itself.
    patcher, _ = _llm_returning(location_type="general", place_name="Al Dibab")
    with patcher, _geocoding(hit=_hit(11.5, 28.5)) as m:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "l4-dibab"
    assert m["promote"].called


def test_sudan_war_x_connector_geocodes_within_sudan():
    view = MagicMock(title="t", description="d")
    with patch("clear_pipeline.defs.signals.connectors.locate_in_country",
               return_value="l4") as locate:
        assert SudanWarXConnector().locate(view) == "l4"
    locate.assert_called_once_with("t", "d", "Sudan", country_code="sd")


# ── geoparser.geocode_place_name ─────────────────────────────────────────────

def test_geocode_place_name_tries_the_gazetteer_first():
    hit = {"name": "Ad Dibab", "latitude": "11.5", "longitude": "28.5",
           "countryCode": "SD", "featureClass": "P", "score": 0.9}
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", return_value=hit) as gaz,
        patch.object(geoparser.nominatim, "search") as search,
    ):
        result = geoparser.geocode_place_name(" Al Dibab ", expected_country_codes={"sd"})

    assert (result.lat, result.lng, result.candidate) == (11.5, 28.5, "Al Dibab")
    assert gaz.call_args.kwargs["country_code"] == "SD"
    search.assert_not_called()


def test_geocode_place_name_sends_landmarks_to_locationiq():
    row = {"lat": "11.0", "lon": "29.7", "class": "power", "importance": 0.1,
           "address": {"country_code": "sd"}, "display_name": "Kadugli power station"}
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location") as gaz,
        patch.object(geoparser.nominatim, "search", return_value=[row]),
    ):
        result = geoparser.geocode_place_name("Kadugli Airport", expected_country_codes={"sd"})

    gaz.assert_not_called()
    assert result.kind == "landmark" and (result.lat, result.lng) == (11.0, 29.7)


def test_geocode_place_name_ignores_blank_names():
    assert geoparser.geocode_place_name("  ") is None


def test_a_gazetteer_hit_outside_the_area_falls_through_to_locationiq():
    # "Tendalti" fuzzy-matches El Fasher in the gazetteer; LocationIQ has it.
    el_fasher = {"name": "El Fasher", "latitude": "13.63", "longitude": "25.35",
                 "countryCode": "SD", "featureClass": "P", "score": 0.5}
    tendalti = {"lat": "13.12", "lon": "32.06", "class": "place", "importance": 0.01,
                "address": {"country_code": "sd"}, "display_name": "Tendalti"}
    white_nile = box(31.0, 12.0, 33.5, 14.5)
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", return_value=el_fasher),
        patch.object(geoparser.nominatim, "search", return_value=[tendalti]) as search,
    ):
        result = geoparser.geocode_place_name(
            "Tendalti", expected_country_codes={"sd"},
            accept=lambda lat, lng: white_nile.covers(Point(lng, lat)),
        )
    assert search.called
    assert (result.lat, result.lng) == (13.12, 32.06)


def test_locationiq_prefers_a_result_inside_the_area_over_a_more_important_one():
    def row(lat, lng, importance):
        return {"lat": str(lat), "lon": str(lng), "class": "place", "importance": importance,
                "address": {"country_code": "sd"}, "display_name": "Al Abyad"}

    rows = [row(13.5, 27.5, 0.3), row(11.9, 28.2, 0.01)]  # North Darfur first, then West Kordofan
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", False),
        patch.object(geoparser.nominatim, "search", return_value=rows),
    ):
        inside = geoparser.geocode_place_name(
            "Al Abyad", expected_country_codes={"sd"},
            accept=lambda lat, lng: WK_SHAPE.covers(Point(lng, lat)),
        )
        anywhere = geoparser.geocode_place_name("Al Abyad", expected_country_codes={"sd"})
    assert (inside.lat, anywhere.lat) == (11.9, 13.5)


def test_no_point_inside_the_area_in_either_tier_is_a_miss():
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location",
                     return_value={"name": "X", "latitude": "15", "longitude": "32",
                                   "countryCode": "SD", "featureClass": "P", "score": 1}),
        patch.object(geoparser.nominatim, "search", return_value=None),
    ):
        assert geoparser.geocode_place_name("X", accept=lambda lat, lng: False) is None


# ── Arabic spelling first (GeoNames' Arabic alternate names) ────────────────

WHITE_NILE = box(31.0, 12.0, 33.5, 14.5)


def _gaz(name, lat, lng, *, cc="SD"):
    return {"name": name, "latitude": lat, "longitude": lng, "countryCode": cc,
            "featureClass": "P", "featureCode": "PPL", "score": 1, "exact": True}


def _arabic_lookup(table):
    """Fake resolveGazetteerLocation: Arabic names from `table`, Latin misses."""
    return lambda name, *, country_code=None, min_similarity=None, within_location_id=None: table.get(name)


def _spelling_llm(answer):
    provider = MagicMock()
    provider.complete_structured.return_value = location._ArabicSpelling(place_name_ar=answer)
    return patch.object(location, "make_llm_provider", return_value=provider), provider


def test_arabic_spelling_is_copied_from_the_post():
    patcher, provider = _spelling_llm("الدبيب")
    with patcher:
        assert location.arabic_spelling(POST, "Al Dibab") == "الدبيب"
    assert "Al Dibab" in provider.complete_structured.call_args.kwargs["user"]


def test_arabic_spelling_not_in_the_post_is_discarded():
    # A spelling the model made up must not reach the gazetteer.
    patcher, _ = _spelling_llm("الدبب")
    with patcher:
        assert location.arabic_spelling(POST, "Al Dibab") is None


def test_arabic_spelling_skips_posts_without_arabic():
    patcher, provider = _spelling_llm("نيالا")
    with patcher:
        assert location.arabic_spelling("Airstrikes on Nyala overnight", "Nyala") is None
    provider.complete_structured.assert_not_called()


def test_arabic_spelling_failure_is_none():
    provider = MagicMock()
    provider.complete_structured.side_effect = RuntimeError("model down")
    with patch.object(location, "make_llm_provider", return_value=provider):
        assert location.arabic_spelling(POST, "Al Dibab") is None


def test_locate_passes_the_posts_arabic_spelling_to_the_geocoder():
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Al Dibab")
    with (
        patcher,
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(location, "arabic_spelling", return_value="الدبيب") as spelling,
        _geocoding(hit=_hit(11.5, 28.5)) as m,
    ):
        location.locate_in_country(POST, POST, "Sudan", country_code="sd")
    spelling.assert_called_once_with(POST, "Al Dibab")
    assert m["geocode"].call_args.kwargs["arabic_name"] == "الدبيب"


@pytest.mark.parametrize("gazetteer, place, shape", [
    (False, "Al Dibab", WK_SHAPE),       # gazetteer off: the Arabic lookup never runs
    (True, "Al Dibab Airport", WK_SHAPE),  # landmarks skip the gazetteer
    (True, "Al Dibab", None),            # area without geometry: no geocoding at all
])
def test_the_arabic_spelling_is_not_bought_when_it_cannot_be_used(gazetteer, place, shape):
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name=place)
    with (
        patcher,
        patch.object(geoparser.settings, "geoparser_use_gazetteer", gazetteer),
        patch.object(location, "arabic_spelling") as spelling,
        _geocoding(hit=_hit(11.5, 28.5), shape=shape),
    ):
        location.locate_in_country(POST, POST, "Sudan", country_code="sd")
    spelling.assert_not_called()


def test_an_arabic_match_wins_over_a_wrong_latin_fuzzy_match():
    # "Tendalti" fuzzy-matches El Fasher in the gazetteer; تندلتي matches exactly.
    table = {"تندلتي": _gaz("Tendalti", 13.12, 32.06)}
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", side_effect=_arabic_lookup(table)),
        patch.object(geoparser.nominatim, "search") as search,
    ):
        result = geoparser.geocode_place_name(
            "Tendalti", expected_country_codes={"sd"}, arabic_name="تندلتي",
            accept=lambda lat, lng: WHITE_NILE.contains(Point(lng, lat)),
        )
    assert (result.lat, result.lng, result.candidate) == (13.12, 32.06, "Tendalti")
    search.assert_not_called()


def test_an_arabic_match_whose_latin_name_disagrees_is_skipped():
    # GeoNames gives Kosti Rabak's Arabic name (ربك).
    table = {"ربك": _gaz("Kosti", 13.16, 32.66)}
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", side_effect=_arabic_lookup(table)),
        patch.object(geoparser.nominatim, "search", return_value=None),
    ):
        assert geoparser.geocode_place_name(
            "Rabak", expected_country_codes={"sd"}, arabic_name="ربك",
        ) is None


def test_an_arabic_match_outside_the_area_is_skipped():
    table = {"الدبيب": _gaz("Ad Dibab", 15.6, 32.5)}
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", side_effect=_arabic_lookup(table)),
        patch.object(geoparser.nominatim, "search", return_value=None),
    ):
        assert geoparser.geocode_place_name(
            "Al Dibab", expected_country_codes={"sd"}, arabic_name="الدبيب",
            accept=lambda lat, lng: WK_SHAPE.contains(Point(lng, lat)),
        ) is None


def test_an_arabic_match_on_a_river_is_skipped():
    # النيل الأبيض is the White Nile river as well as the state.
    river = {**_gaz("White Nile", 13.0, 32.4), "featureClass": "H", "featureCode": "STM"}
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location",
                     side_effect=_arabic_lookup({"النيل الأبيض": river})),
        patch.object(geoparser.nominatim, "search", return_value=None),
    ):
        assert geoparser.geocode_place_name(
            "White Nile", expected_country_codes={"sd"}, arabic_name="النيل الأبيض",
        ) is None


def test_the_arabic_name_is_retried_without_its_article():
    table = {"فاشر": _gaz("El Fasher", 13.63, 25.35)}
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", side_effect=_arabic_lookup(table)) as gaz,
    ):
        result = geoparser.geocode_place_name(
            "Al Fasher", expected_country_codes={"sd"}, arabic_name="الفاشر",
        )
    assert result.display_name == "El Fasher"
    assert [c.args[0] for c in gaz.call_args_list][:2] == ["الفاشر", "فاشر"]


def test_spellings_agree_across_transliterations_only():
    agree = geoparser._spellings_agree
    assert agree("Ad-Damazin", "Ed Damazine") and agree("El Obeid", "Al Abiad")
    assert agree("Umm Ruwaba", "Um Rawaba") and agree("Tendalti", "Tandilti")
    assert not agree("Kosti", "Rabak") and not agree("El Fasher", "Tendalti")


# ── the gazetteer chooses by area (resolveGazetteerLocation withinLocationId) ─

def test_locate_asks_the_gazetteer_for_matches_inside_claudes_area():
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Al Dibab")
    with patcher, _geocoding(hit=_hit(11.5, 28.5)) as m:
        location.locate_in_country(POST, POST, "Sudan", country_code="sd")
    assert m["geocode"].call_args.kwargs["within_location_id"] == "sd-wk"


@pytest.mark.parametrize("arabic", [None, "الدبيب"])
def test_both_gazetteer_lookups_pass_the_area(arabic):
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", return_value=None) as gaz,
        patch.object(geoparser.nominatim, "search", return_value=None),
    ):
        geoparser.geocode_place_name("Al Dibab", expected_country_codes={"sd"},
                                     arabic_name=arabic, within_location_id="sd-wk")
    assert gaz.call_args_list and all(
        c.kwargs["within_location_id"] == "sd-wk" for c in gaz.call_args_list
    )


def test_the_gazetteer_client_sends_within_location_id():
    from clear_pipeline.providers import clear_api

    with (
        patch.object(clear_api, "_gazetteer_within_supported", True),
        patch.object(clear_api, "_execute", return_value={"resolveGazetteerLocation": None}) as ex,
    ):
        clear_api.resolve_gazetteer_location("Al Dibab", country_code="SD", within_location_id="sd-wk")
    query, variables = ex.call_args.args
    assert "withinLocationId: $withinLocationId" in query
    assert variables["withinLocationId"] == "sd-wk"


def test_the_gazetteer_client_drops_the_argument_on_an_older_clear_api():
    from clear_pipeline.providers import clear_api

    hit = {"name": "Ad Dibab"}
    calls = []

    def fake_execute(query, variables, retries=1):
        calls.append(variables)
        if "withinLocationId" in variables:
            raise clear_api.ClearApiError('Unknown argument "withinLocationId" on field "resolveGazetteerLocation"')
        return {"resolveGazetteerLocation": hit}

    with (
        patch.object(clear_api, "_gazetteer_within_supported", True),
        patch.object(clear_api, "_execute", side_effect=fake_execute),
    ):
        assert clear_api.resolve_gazetteer_location("Al Dibab", within_location_id="sd-wk") == hit
        assert clear_api.resolve_gazetteer_location("Al Dibab", within_location_id="sd-wk") == hit
    # One rejected attempt, then the argument isn't sent again in this process.
    assert ["withinLocationId" in v for v in calls] == [True, False, False]



# ── incident site over impact area (location-v3) ─────────────────────────────

def test_prompt_prefers_the_incident_site_over_the_impact_area():
    assert location.LOCATION_PROMPT_VERSION == "location-v3"
    assert "belongs to the site's district, not the country" in location.SYSTEM_PROMPT
    assert "or the places named span several states, use the country-level Sudan entry" in location.SYSTEM_PROMPT


def test_explicit_country_answer_with_a_facility_keeps_the_country():
    # Separate strikes on facilities in different states: the answer carries
    # one facility's name, so geocoding it would pin the whole report to that
    # state and hide its scope from location filters (Greptile on PR #152).
    patcher, _ = _llm_returning(location_type="general", location_id="sd", place_name="Merowe Dam")
    with patcher, patch.object(location, "_geocode_within") as geocode:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd"
    geocode.assert_not_called()


def test_explicit_country_answer_with_a_town_keeps_the_country():
    # Several incidents in several towns: one town's point must not pull the
    # whole report into its district.
    patcher, _ = _llm_returning(location_type="general", location_id="sd", place_name="Nyala")
    with patcher, patch.object(location, "_geocode_within") as geocode:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd"
    geocode.assert_not_called()


def test_state_answer_with_a_facility_is_geocoded_inside_the_state():
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Al Dibab power station")
    with patcher, patch.object(location, "_geocode_within", return_value="sd-pt") as geocode:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd-pt"
    assert geocode.call_args.args[:2] == ("Al Dibab power station", "sd-wk")
