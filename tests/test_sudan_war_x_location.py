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
from clear_pipeline.providers.llm import LLMUnavailableError

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
def _geocoding(*, hit=None, shape=WK_SHAPE, promo=None, geocode_error=None,
               tier_failed=False, known_elsewhere=False):
    # Like the real lookups: a hit the caller's area test rejects is a miss,
    # and the trace says why.
    def fake_geocode(name, *, expected_country_codes=None, accept=None, arabic_name=None,
                     within_location_id=None, trace=None):
        if geocode_error:
            raise geocode_error
        if tier_failed:
            trace.failed = True
            return None
        if hit and (accept is None or accept(hit.lat, hit.lng)):
            return hit
        if hit:
            trace.outside = True
        return None

    geocode = MagicMock(side_effect=fake_geocode)
    promote = MagicMock(return_value=promo or {"locationId": "l4-dibab", "reused": False,
                                               "abortedReason": None})
    geometry = {"type": "Polygon", "coordinates": [list(shape.exterior.coords)]} if shape else None
    with (
        patch.object(location, "geocode_place_name", geocode),
        patch.object(location, "gazetteer_knows", return_value=known_elsewhere),
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
    # GeoNames answers a landmark name with the town ("Kadugli"): that hit is
    # dropped. The gazetteer is still asked, because a place alias may hold
    # the facility itself.
    town = {"name": "Kadugli", "latitude": "11.01", "longitude": "29.72", "countryCode": "SD",
            "featureClass": "P", "score": 0.6, "source": "GEONAMES", "locationId": None}
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", return_value=town) as gaz,
        patch.object(geoparser.nominatim, "search", return_value=[row]),
    ):
        result = geoparser.geocode_place_name("Kadugli Airport", expected_country_codes={"sd"})

    gaz.assert_called_once()
    assert result.kind == "landmark" and (result.lat, result.lng) == (11.0, 29.7)
    assert result.location_id is None


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
    (False, "Al Dibab Airport", WK_SHAPE),
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



# ── incident site over impact area (location-v3), country-scoped (location-v4) ─

def test_prompt_prefers_the_incident_site_over_the_impact_area():
    assert location.LOCATION_PROMPT_VERSION == "location-v4"
    assert "belongs to the site's district, not the country" in location.SYSTEM_PROMPT
    assert "or the places named span several states, use the country-level Sudan entry" in location.SYSTEM_PROMPT


def test_a_non_sudan_bulletin_is_located_under_its_own_country_prompt():
    # Under the Sudan prompt, rule 5 would null every field of a Chad bulletin and
    # pin it to the country; the scoped prompt and candidates are Chad's.
    calls: list[dict] = []

    def answer(*, system, user, schema):
        calls.append({"system": system, "user": user})
        return schema(location_type="general", location_id="td-ou") if schema is location._LocationResolution else schema()

    provider = MagicMock()
    provider.complete_structured.side_effect = answer
    with patch.object(location, "make_llm_provider", return_value=provider):
        located = location.locate_in_country("Displacement in Ouaddai", "Families fled.", "Chad")

    assert located == "td-ou"
    system, user = calls[0]["system"], calls[0]["user"]
    assert "focused on Chad" in system and "country-level Chad entry" in system
    assert "NOT about Chad at all" in system and "Sudan" not in system
    assert "Ouaddai" in user and "West Kordofan" not in user


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


def _provider_error():
    import anthropic
    import httpx
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    return anthropic.BadRequestError(
        "credit balance is too low", response=httpx.Response(400, request=request), body=None
    )


def test_locate_in_country_fails_when_no_llm_is_usable():
    # A billing/auth/outage error is not "nothing matched": pinning to the country would hide it.
    provider = MagicMock()
    provider.complete_structured.side_effect = _provider_error()
    with patch.object(location, "make_llm_provider", return_value=provider), \
         pytest.raises(LLMUnavailableError):
        location.locate_in_country(POST, POST, "Sudan")


def test_locate_in_country_fails_when_the_llm_is_not_configured():
    with patch.object(location, "make_llm_provider", side_effect=RuntimeError("Missing env var LLM_SIGNAL_MODEL")), \
         pytest.raises(LLMUnavailableError):
        location.locate_in_country(POST, POST, "Sudan")


def test_locate_in_country_fails_when_the_locations_are_unreachable():
    # A clear-api outage is not "nothing matched": the caller defers instead of pinning to the country.
    from clear_pipeline.providers.clear_api import GraphQLErrors
    location.invalidate_locations_cache()
    with patch.object(location, "get_locations_by_level", side_effect=GraphQLErrors([{"message": "timeout"}])), \
         patch.object(location, "get_locations", side_effect=GraphQLErrors([{"message": "timeout"}])), \
         pytest.raises(location.LocationsUnavailableError):
        location.locate_in_country(POST, POST, "Sudan")


def test_a_per_input_rejection_degrades_instead_of_failing():
    # A 400 about this post (context too long) isn't an outage: failing would block
    # the drain on the same signal every run, never retired.
    import anthropic
    import httpx
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    provider = MagicMock()
    provider.complete_structured.side_effect = anthropic.BadRequestError(
        "prompt is too long", response=httpx.Response(400, request=request), body=None
    )
    with patch.object(location, "make_llm_provider", return_value=provider):
        assert location.locate_in_country(POST, POST, "Sudan") == "sd"


@pytest.mark.parametrize(
    ("status", "message", "unavailable"),
    [
        (400, "Your credit balance is too low", True),
        (400, "prompt is too long", False),
        (401, "invalid x-api-key", True),
        (402, "Insufficient credits", True),
        (404, "model not found", False),
        (529, "overloaded", True),
    ],
)
def test_is_llm_unavailable(status, message, unavailable):
    import httpx
    import openai
    from clear_pipeline.providers.llm import is_llm_unavailable
    request = httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions")
    exc = openai.OpenAI(api_key="x")._make_status_error(
        message, body=None, response=httpx.Response(status, request=request)
    )
    assert is_llm_unavailable(exc) is unavailable


def test_arabic_spelling_fails_when_no_llm_is_usable():
    provider = MagicMock()
    provider.complete_structured.side_effect = _provider_error()
    with patch.object(location, "make_llm_provider", return_value=provider), \
         pytest.raises(LLMUnavailableError):
        location.arabic_spelling("قصف في الفاشر", "El Fasher")


def _spelling_failing_with(error):
    """Fake LLM: the location call answers West Kordofan / Al Dibab; the Arabic
    spelling call raises `error`."""
    def answer(*, system, user, schema):
        if schema is location._LocationResolution:
            return schema(location_type="general", location_id="sd-wk", place_name="Al Dibab")
        raise error

    provider = MagicMock()
    provider.complete_structured.side_effect = answer
    return patch.object(location, "make_llm_provider", return_value=provider)


def test_an_outage_during_the_arabic_spelling_fails_the_locate():
    # Keeping the area would be saved, and the drain never locates the signal again.
    with (
        _spelling_failing_with(_provider_error()),
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        _geocoding(hit=_hit(11.5, 28.5)) as m,
        pytest.raises(LLMUnavailableError),
    ):
        location.locate_in_country(POST, POST, "Sudan", country_code="sd")
    m["geocode"].assert_not_called()


def test_a_failed_arabic_spelling_still_geocodes_the_latin_name():
    with (
        _spelling_failing_with(RuntimeError("model down")),
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        _geocoding(hit=_hit(11.5, 28.5)) as m,
    ):
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "l4-dibab"
    assert m["geocode"].call_args.kwargs["arabic_name"] is None


# ── geocode misses go to the unresolved place-name queue (#1308) ─────────────

def _locate_collecting(*, llm=None, country_code="sd", **geocoding):
    """Run locate_in_country inside a miss collector; returns (location, misses)."""
    fields = {"location_type": "general", "location_id": "sd-wk", "place_name": "Al Dibab"}
    patcher, _ = _llm_returning(**(llm or fields))
    with patcher, _geocoding(**geocoding), location.collect_geocode_misses() as misses:
        located = location.locate_in_country(POST, POST, "Sudan", country_code=country_code)
    return located, misses


def _miss(reason, name_ar=None):
    return location.GeocodeMiss("Al Dibab", name_ar, "SD", "sd-wk", reason)


def test_a_name_no_tier_can_place_is_a_miss():
    located, misses = _locate_collecting(hit=None)
    assert located == "sd-wk"
    assert misses == [_miss("NO_MATCH")]


def test_a_match_the_area_check_rejects_is_a_miss_outside_the_area():
    located, misses = _locate_collecting(hit=_hit(15.6, 32.5))
    assert located == "sd-wk"
    assert misses == [_miss("OUTSIDE_AREA")]


def test_a_name_the_gazetteer_knows_elsewhere_is_a_miss_outside_the_area():
    # The gazetteer filters by area on the server, so its own lookup shows nothing.
    _, misses = _locate_collecting(hit=None, known_elsewhere=True)
    assert misses == [_miss("OUTSIDE_AREA")]


@pytest.mark.parametrize("promo", [
    {"locationId": None, "abortedReason": "different_a2"},   # promote aborted
    {"locationId": None, "abortedReason": None},             # promote returned no id
])
def test_a_promote_that_gives_no_location_is_a_miss(promo):
    located, misses = _locate_collecting(hit=_hit(11.5, 28.5), promo=promo)
    assert located == "sd-wk"
    assert misses == [_miss("NO_MATCH")]


def test_a_miss_carries_the_posts_arabic_spelling():
    provider = MagicMock()
    provider.complete_structured.side_effect = lambda *, system, user, schema: (
        schema(location_type="general", location_id="sd-wk", place_name="Al Dibab")
        if schema is location._LocationResolution else schema(place_name_ar="الدبيب")
    )
    with (
        patch.object(location, "make_llm_provider", return_value=provider),
        patch.object(location, "arabic_lookup_applies", return_value=True),
        _geocoding(hit=None),
        location.collect_geocode_misses() as misses,
    ):
        location.locate_in_country(POST, POST, "Sudan", country_code="sd")
    assert misses == [_miss("NO_MATCH", "الدبيب")]


@pytest.mark.parametrize("case", [
    {"hit": _hit(11.5, 28.5)},                                   # geocoded: no miss
    {"hit": _hit(11.5, 28.5), "shape": None},                    # the area has no geometry
    {"tier_failed": True},                                       # a lookup tier failed
    {"geocode_error": RuntimeError("clear-api 502")},            # the geocoder blew up
    {"hit": None, "country_code": None},                         # no country code: not geocoded
    {"hit": None, "llm": {"location_type": "general", "location_id": "sd-wk"}},           # no place name
    {"hit": None, "llm": {"location_type": "general", "location_id": "sd-kh",
                          "place_name": "Khartoum"}},                                     # a state's own name
    {"hit": None, "llm": {"location_type": "general", "location_id": "sd-wk",
                          "place_name": "Sudan"}},                                        # the country's own name
    {"hit": None, "llm": {"location_type": "general", "location_id": "sd",
                          "place_name": "Al Dibab"}},                                     # the LLM chose the country
])
def test_these_exits_are_not_misses(case):
    _, misses = _locate_collecting(**case)
    assert misses == []


def test_without_a_collector_a_miss_is_only_logged():
    # A dry run or an evaluation calls locate_in_country with no drain around it.
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Al Dibab")
    with patcher, _geocoding(hit=None), patch.object(location, "gazetteer_knows") as knows:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd-wk"
    assert location._geocode_misses.get() is None
    # Nothing is recorded, so the lookup that only sets the reason is not made.
    knows.assert_not_called()


def test_a_collector_closes_with_its_block():
    with location.collect_geocode_misses() as outer:
        with location.collect_geocode_misses() as inner:
            location._note_miss("Kamo", None, "sd", "sd-wk", "NO_MATCH")
        location._note_miss("Aruri", None, "sd", "sd-wk", "NO_MATCH")
    assert [m.name for m in inner] == ["Kamo"]
    assert [m.name for m in outer] == ["Aruri"]


@contextmanager
def _drain_with_misses(misses, *, record_side_effect=None):
    """The drain with a connector whose locate reports `misses`."""
    def locate(_view):
        for miss in misses:
            location._note_miss(miss.name, miss.name_ar, miss.country_code, miss.area_id, miss.reason)
        return "sd-wk"

    with (
        _drain([_x_row()]) as m,
        patch.object(SudanWarXConnector, "locate", side_effect=locate),
        patch.object(stages, "record_unresolved_place_name", side_effect=record_side_effect) as record,
    ):
        yield {**m, "record": record}


def test_drain_records_each_miss_with_the_signal_id():
    with _drain_with_misses([_miss("OUTSIDE_AREA", "الدبيب")]) as m:
        stages._drain_signals_locked(MagicMock())

    m["record"].assert_called_once_with(
        signal_id="sig-x-1", name="Al Dibab", name_ar="الدبيب", country_code="SD",
        area_location_id="sd-wk", reason="OUTSIDE_AREA",
    )
    m["update"].assert_called_once_with("sig-x-1", "sd-wk")
    assert ("PROCESSED", ["sig-x-1"]) in m["marked"]


def test_drain_records_nothing_when_the_place_was_geocoded():
    with _drain_with_misses([]) as m:
        stages._drain_signals_locked(MagicMock())
    m["record"].assert_not_called()


@pytest.mark.parametrize("error", [RuntimeError("clear-api 502"), TimeoutError("timed out")])
def test_a_failed_record_never_fails_the_drain(error):
    with _drain_with_misses([_miss("NO_MATCH")], record_side_effect=error) as m:
        stages._drain_signals_locked(MagicMock())

    m["record"].assert_called_once()
    m["update"].assert_called_once_with("sig-x-1", "sd-wk")
    assert m["group"].called
    assert ("PROCESSED", ["sig-x-1"]) in m["marked"]


def test_the_record_client_sends_the_miss():
    from clear_pipeline.providers import clear_api

    row = {"id": "row-1", "status": "OPEN", "signalCount": 1}
    with (
        patch.object(clear_api, "_record_unresolved_supported", True),
        patch.object(clear_api, "_execute", return_value={"recordUnresolvedPlaceName": row}) as ex,
    ):
        result = clear_api.record_unresolved_place_name(
            signal_id="sig-x-1", name="Al Dibab", name_ar="الدبيب", country_code="sd",
            area_location_id="sd-wk", reason="OUTSIDE_AREA",
        )
    assert result == row
    query, variables = ex.call_args.args
    assert "recordUnresolvedPlaceName(input: $input)" in query
    assert variables == {"input": {
        "signalId": "sig-x-1", "name": "Al Dibab", "nameAr": "الدبيب", "countryCode": "SD",
        "areaLocationId": "sd-wk", "reason": "OUTSIDE_AREA",
    }}
    # One attempt, short timeout: the record must not hold the drain up.
    assert ex.call_args.kwargs == {"retries": 1, "timeout": clear_api._RECORD_UNRESOLVED_TIMEOUT}
    assert clear_api._RECORD_UNRESOLVED_TIMEOUT <= 10


def test_the_record_client_sends_seen_at_for_a_backfill():
    from clear_pipeline.providers import clear_api

    with (
        patch.object(clear_api, "_record_unresolved_supported", True),
        patch.object(clear_api, "_execute", return_value={"recordUnresolvedPlaceName": {}}) as ex,
    ):
        clear_api.record_unresolved_place_name(
            signal_id="s", name="Kamo", country_code="SD", area_location_id="sd-nd",
            seen_at="2026-08-03T10:00:00Z",
        )
    assert ex.call_args.args[1]["input"] == {
        "signalId": "s", "name": "Kamo", "countryCode": "SD", "areaLocationId": "sd-nd",
        "reason": "NO_MATCH", "seenAt": "2026-08-03T10:00:00Z",
    }


@pytest.mark.parametrize("message", [
    'clear-api 400 (non-retryable): Cannot query field "recordUnresolvedPlaceName" on type "Mutation".',
    'clear-api 400 (non-retryable): Unknown type "RecordUnresolvedPlaceNameInput".',
])
def test_the_record_client_stops_calling_an_older_clear_api(message):
    from clear_pipeline.providers import clear_api

    kwargs = {"signal_id": "s", "name": "Kamo", "country_code": "SD", "area_location_id": "sd-nd"}
    with (
        patch.object(clear_api, "_record_unresolved_supported", True),
        patch.object(clear_api, "_execute", side_effect=clear_api.ClearApiError(message)) as ex,
    ):
        assert clear_api.record_unresolved_place_name(**kwargs) is None
        assert clear_api.record_unresolved_place_name(**kwargs) is None
    # One rejected attempt, then no call for the rest of the process.
    assert ex.call_count == 1


def test_the_record_client_raises_other_errors_and_keeps_calling():
    from clear_pipeline.providers import clear_api

    kwargs = {"signal_id": "s", "name": "Kamo", "country_code": "SD", "area_location_id": "sd-nd"}
    with (
        patch.object(clear_api, "_record_unresolved_supported", True),
        patch.object(clear_api, "_execute", side_effect=clear_api.ClearApiError("clear-api 403: forbidden")) as ex,
    ):
        for _ in range(2):
            with pytest.raises(clear_api.ClearApiError):
                clear_api.record_unresolved_place_name(**kwargs)
    assert ex.call_count == 2


# ── the geocoder's trace: why a lookup found nothing ─────────────────────────

def _trace(name="X", *, gazetteer=None, gazetteer_error=None, search=None, accept=None,
           arabic_name=None, use_gazetteer=True):
    trace = geoparser.GeocodeTrace()
    gaz = MagicMock(return_value=gazetteer, side_effect=gazetteer_error)
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", use_gazetteer),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", gaz),
        patch.object(geoparser.nominatim, "search", search or MagicMock(return_value=None)),
    ):
        result = geoparser.geocode_place_name(
            name, expected_country_codes={"sd"}, accept=accept, arabic_name=arabic_name, trace=trace,
        )
    return result, trace


def test_trace_of_a_plain_miss_is_empty():
    result, trace = _trace()
    assert result is None
    assert (trace.outside, trace.failed) == (False, False)


def test_trace_marks_a_gazetteer_hit_the_caller_rejects():
    hit = {"name": "X", "latitude": "15", "longitude": "32", "countryCode": "SD", "featureClass": "P", "score": 1}
    _, trace = _trace(gazetteer=hit, accept=lambda lat, lng: False)
    assert (trace.outside, trace.failed) == (True, False)


def test_trace_marks_a_locationiq_result_the_caller_rejects():
    row = {"lat": "15", "lon": "32", "class": "place", "importance": 0.1, "address": {"country_code": "sd"}}
    _, trace = _trace(search=MagicMock(return_value=[row]), accept=lambda lat, lng: False, use_gazetteer=False)
    assert (trace.outside, trace.failed) == (True, False)


def test_trace_marks_an_arabic_match_outside_the_area():
    _, trace = _trace("Tendalti", gazetteer=_gaz("Tendalti", 13.12, 32.06), arabic_name="تندلتي",
                      accept=lambda lat, lng: False)
    assert trace.outside


def test_trace_marks_a_failed_gazetteer_lookup():
    _, trace = _trace(gazetteer_error=RuntimeError("clear-api 502"))
    assert trace.failed
    _, trace = _trace("Tendalti", gazetteer_error=RuntimeError("clear-api 502"), arabic_name="تندلتي")
    assert trace.failed


def test_trace_marks_a_failed_locationiq_search():
    def search(query, *, country_codes=None, limit=5, outcome=None):
        outcome["failed"] = True
        return None

    _, trace = _trace(search=MagicMock(side_effect=search))
    assert trace.failed


def test_an_untraced_lookup_calls_locationiq_as_before():
    search = MagicMock(return_value=None)
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", False),
        patch.object(geoparser.nominatim, "search", search),
    ):
        geoparser.geocode_place_name("X", expected_country_codes={"sd"})
    assert "outcome" not in search.call_args.kwargs


@pytest.mark.parametrize(("status", "failed"), [("error", True), ("no_result", False)])
def test_nominatim_reports_a_cached_error_as_failed_and_an_empty_answer_as_not(status, failed):
    from clear_pipeline.providers import nominatim

    outcome: dict = {}
    with (
        patch.object(nominatim.settings, "locationiq_api_key", "k"),
        patch.object(nominatim, "_cache_lookup", return_value={"status": status}),
    ):
        assert nominatim.search("Kamo", outcome=outcome) is None
    assert bool(outcome.get("failed")) is failed


def test_nominatim_reports_an_open_circuit_as_failed():
    from clear_pipeline.providers import nominatim

    outcome: dict = {}
    with (
        patch.object(nominatim.settings, "locationiq_api_key", "k"),
        patch.object(nominatim, "_cache_lookup", return_value=None),
        patch.object(nominatim, "_is_circuit_open", return_value=True),
    ):
        assert nominatim.search("Kamo", outcome=outcome) is None
    assert outcome == {"failed": True}


@pytest.mark.parametrize(("hits", "use_gazetteer", "known"), [
    ({"الدبيب": {"name": "Ad Dibab", "exact": True}}, True, True),    # an exact Arabic match
    ({"Al Dibab": {"name": "Ad Dibab", "exact": True}}, True, True),  # an exact Latin match
    ({"Al Dibab": {"name": "El Fasher", "exact": False}}, True, False),  # a fuzzy hit is another place
    ({}, True, False),
    ({"Al Dibab": {"name": "Ad Dibab", "exact": True}}, False, False),   # the gazetteer is off
    # GeoNames gives Kosti the Arabic name of another town: the Latin names disagree.
    ({"الدبيب": {"name": "Kosti", "exact": True}}, True, False),
    # A river or a road is not a place a point can stand for.
    ({"Al Dibab": {"name": "Ad Dibab", "exact": True, "featureClass": "H"}}, True, False),
    ({"الدبيب": {"name": "Ad Dibab", "exact": True, "featureClass": "R"}}, True, False),
])
def test_gazetteer_knows_counts_what_the_lookups_would_accept(hits, use_gazetteer, known):
    gaz = MagicMock(side_effect=lambda name, *, country_code=None: hits.get(name))
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", use_gazetteer),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", gaz),
    ):
        assert geoparser.gazetteer_knows("Al Dibab", "sd", arabic_name="الدبيب") is known
    if use_gazetteer and gaz.called:
        assert gaz.call_args.kwargs == {"country_code": "SD"}


def test_gazetteer_knows_asks_for_the_latin_name_alone_without_an_arabic_one():
    gaz = MagicMock(return_value=None)
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", gaz),
    ):
        assert geoparser.gazetteer_knows("Al Dibab", "SD") is False
    assert [c.args[0] for c in gaz.call_args_list] == ["Al Dibab"]


def test_gazetteer_knows_is_false_when_the_lookup_fails():
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", side_effect=RuntimeError("502")),
    ):
        assert geoparser.gazetteer_knows("Al Dibab", "SD") is False


# ── place aliases: use the alias's CLEAR location directly (#1311) ───────────

def _alias(lat, lng, *, location_id="loc-um-dabakir", name="Um Dabakir power station"):
    """A gazetteer hit that came from one of CLEAR's place aliases."""
    return {"geonamesId": 0, "name": name, "latitude": lat, "longitude": lng, "countryCode": "SD",
            "featureClass": None, "featureCode": None, "population": 0, "score": 1, "exact": True,
            "source": "ALIAS", "locationId": location_id}


def _alias_hit(lat, lng, **kwargs) -> GeoparseResult:
    hit = _hit(lat, lng)
    hit.location_id = kwargs.get("location_id", "loc-um-dabakir")
    return hit


def test_an_alias_hit_is_used_as_the_signals_location_with_no_promote():
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Um Dabakir")
    with patcher, _geocoding(hit=_alias_hit(11.5, 28.5)) as m, location.collect_geocode_misses() as misses:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "loc-um-dabakir"
    m["promote"].assert_not_called()
    assert misses == []


def test_an_alias_location_outside_the_area_is_rejected():
    # The same area check as for any hit: the alias's point is outside West Kordofan.
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Um Dabakir")
    with patcher, _geocoding(hit=_alias_hit(15.6, 32.5)) as m, location.collect_geocode_misses() as misses:
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd-wk"
    m["promote"].assert_not_called()
    assert [m.reason for m in misses] == ["OUTSIDE_AREA"]


def test_the_latin_tier_returns_an_alias_hit_with_its_location():
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", return_value=_alias(11.5, 28.5)),
        patch.object(geoparser.nominatim, "search") as search,
    ):
        result = geoparser.geocode_place_name("Um Dabakir", expected_country_codes={"sd"},
                                              accept=lambda lat, lng: True)
    assert (result.location_id, result.lat, result.lng) == ("loc-um-dabakir", 11.5, 28.5)
    search.assert_not_called()


def test_an_alias_hit_the_area_check_rejects_falls_through_to_the_next_tier():
    row = {"lat": "11.9", "lon": "28.2", "class": "place", "importance": 0.01,
           "address": {"country_code": "sd"}, "display_name": "Um Dabakir"}
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", return_value=_alias(15.6, 32.5)),
        patch.object(geoparser.nominatim, "search", return_value=[row]) as search,
    ):
        result = geoparser.geocode_place_name(
            "Um Dabakir", expected_country_codes={"sd"},
            accept=lambda lat, lng: WK_SHAPE.contains(Point(lng, lat)),
        )
    assert search.called
    assert result.location_id is None and (result.lat, result.lng) == (11.9, 28.2)


def test_a_landmark_name_uses_an_alias_and_skips_locationiq():
    # GeoNames holds few facilities, so a landmark name skips its GeoNames
    # hits. A place alias is a person's answer for the facility itself.
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", return_value=_alias(11.5, 28.5)),
        patch.object(geoparser.nominatim, "search") as search,
    ):
        result = geoparser.geocode_place_name("Um Dabakir Power Station", expected_country_codes={"sd"})
    assert result.kind == "landmark" and result.location_id == "loc-um-dabakir"
    search.assert_not_called()


def test_the_arabic_tier_uses_an_alias_whatever_its_latin_name():
    # The Latin-name check guards against GeoNames' own errors. An alias is a
    # person's mapping of this Arabic spelling, so it needs no such check.
    table = {"أم دباكر": _alias(13.1, 32.1, name="Kosti thermal plant")}
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", side_effect=_arabic_lookup(table)),
        patch.object(geoparser.nominatim, "search") as search,
    ):
        result = geoparser.geocode_place_name(
            "Um Dabakir", expected_country_codes={"sd"}, arabic_name="أم دباكر",
            accept=lambda lat, lng: WHITE_NILE.contains(Point(lng, lat)),
        )
    assert result.location_id == "loc-um-dabakir"
    search.assert_not_called()


def test_the_arabic_tier_rejects_an_alias_outside_the_area():
    table = {"أم دباكر": _alias(11.5, 28.5)}  # in West Kordofan, not White Nile
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", side_effect=_arabic_lookup(table)),
        patch.object(geoparser.nominatim, "search", return_value=None),
    ):
        trace = geoparser.GeocodeTrace()
        result = geoparser.geocode_place_name(
            "Um Dabakir", expected_country_codes={"sd"}, arabic_name="أم دباكر",
            accept=lambda lat, lng: WHITE_NILE.contains(Point(lng, lat)), trace=trace,
        )
    assert result is None and trace.outside


def test_a_landmarks_arabic_name_uses_an_alias_only():
    # "محطة أم دباكر": a facility the post names in Arabic.
    alias = {"محطة أم دباكر": _alias(13.1, 32.1)}
    geonames = {"محطة أم دباكر": _gaz("Um Dabakir", 13.1, 32.1)}
    for table, expected in ((alias, "loc-um-dabakir"), (geonames, None)):
        with (
            patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
            patch.object(geoparser.graphql, "resolve_gazetteer_location", side_effect=_arabic_lookup(table)),
            patch.object(geoparser.nominatim, "search", return_value=None),
        ):
            result = geoparser.geocode_place_name(
                "Um Dabakir Power Station", expected_country_codes={"sd"}, arabic_name="محطة أم دباكر",
            )
        assert (result.location_id if result else None) == expected


def test_the_arabic_spelling_is_bought_for_a_landmark_name():
    # A place alias can hold a facility's Arabic name, so the spelling is worth asking for.
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk", place_name="Al Dibab Airport")
    with (
        patcher,
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(location, "arabic_spelling", return_value="مطار الدبيب") as spelling,
        _geocoding(hit=_alias_hit(11.5, 28.5)) as m,
    ):
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "loc-um-dabakir"
    spelling.assert_called_once()
    assert m["geocode"].call_args.kwargs["arabic_name"] == "مطار الدبيب"


def test_the_gazetteer_client_asks_for_the_alias_fields():
    from clear_pipeline.providers import clear_api

    with (
        patch.object(clear_api, "_gazetteer_within_supported", True),
        patch.object(clear_api, "_gazetteer_alias_fields_supported", True),
        patch.object(clear_api, "_execute", return_value={"resolveGazetteerLocation": _alias(1, 2)}) as ex,
    ):
        hit = clear_api.resolve_gazetteer_location("Um Dabakir", country_code="SD", within_location_id="sd-wk")
    query = ex.call_args.args[0]
    assert "locationId" in query and "source" in query and "withinLocationId: $withinLocationId" in query
    assert (hit["source"], hit["locationId"]) == ("ALIAS", "loc-um-dabakir")


def test_the_gazetteer_client_drops_the_alias_fields_on_an_older_clear_api():
    from clear_pipeline.providers import clear_api

    hit = {"name": "Ad Dibab"}
    queries = []

    def fake_execute(query, variables, retries=1):
        queries.append(query)
        if "locationId" in query:
            raise clear_api.ClearApiError(
                'clear-api 400 (non-retryable): Cannot query field \\"locationId\\" on type \\"GazetteerHit\\".'
            )
        return {"resolveGazetteerLocation": hit}

    with (
        patch.object(clear_api, "_gazetteer_within_supported", True),
        patch.object(clear_api, "_gazetteer_alias_fields_supported", True),
        patch.object(clear_api, "_execute", side_effect=fake_execute),
    ):
        assert clear_api.resolve_gazetteer_location("Al Dibab", within_location_id="sd-wk") == hit
        assert clear_api.resolve_gazetteer_location("Al Dibab") == hit
    # One rejected attempt, then the fields aren't asked for again in this
    # process. The area argument is kept: it is a different feature.
    assert ["locationId" in q for q in queries] == [True, False, False]
    assert "withinLocationId" in queries[1] and "withinLocationId" not in queries[2]
    # Without the fields, a hit is a GeoNames hit.
    assert geoparser._alias_location_id(hit) is None


def test_the_gazetteer_client_raises_other_errors_and_keeps_the_alias_fields():
    from clear_pipeline.providers import clear_api

    with (
        patch.object(clear_api, "_gazetteer_alias_fields_supported", True),
        patch.object(clear_api, "_execute", side_effect=clear_api.ClearApiError("clear-api 403: forbidden")),
    ):
        with pytest.raises(clear_api.ClearApiError):
            clear_api.resolve_gazetteer_location("Al Dibab")
        assert clear_api._gazetteer_alias_fields_supported is True


def test_drain_records_a_miss_after_it_saves_the_location():
    # clear-api moves a miss on a name that was resolved in the meantime from
    # the signal's saved location, so the location is saved first.
    order = []
    with _drain_with_misses([_miss("NO_MATCH")]) as m:
        m["update"].side_effect = lambda *a, **k: order.append("save") or {"id": "sig-x-1", "generalLocation": None}
        m["record"].side_effect = lambda **k: order.append("record")
        stages._drain_signals_locked(MagicMock())
    assert order == ["save", "record"]


def test_drain_records_a_miss_when_no_location_resolves():
    def locate(_view):
        location._note_miss("Kamo", None, "SD", "sd", "NO_MATCH")
        return None

    with (
        _drain([_x_row()]) as m,
        patch.object(SudanWarXConnector, "locate", side_effect=locate),
        patch.object(stages, "record_unresolved_place_name") as record,
    ):
        stages._drain_signals_locked(MagicMock())
    record.assert_called_once()
    m["update"].assert_not_called()


def test_a_failed_save_records_nothing_and_leaves_the_post_new():
    with _drain_with_misses([_miss("NO_MATCH")]) as m:
        m["update"].side_effect = RuntimeError("clear-api 502")
        stages._drain_signals_locked(MagicMock())
    m["record"].assert_not_called()
    assert all(not ids for _, ids in m["marked"])


def test_gazetteer_knows_counts_an_alias_whatever_its_saved_name():
    # An Arabic-only alias outside the area: the scoped lookup hides it, and
    # its location's name need not agree with the model's English name.
    alias = {"الدبيب": _alias(15.6, 32.5, name="Kosti thermal plant")}
    with (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location",
                     side_effect=lambda name, *, country_code=None: alias.get(name)),
    ):
        assert geoparser.gazetteer_knows("Al Dibab", "SD", arabic_name="الدبيب") is True


@contextmanager
def _drain_with_a_late_alias(*, row_status, moved_to=None, reread_error=None):
    """The drain when clear-api answers a recorded miss with `row_status`."""
    moved = {"id": "l4-alias", "name": "Um Dabakir", "level": 4, "ancestorIds": ["sd", "sd-wk", "sd-wk-dibab"]}
    with (
        _drain_with_misses([_miss("NO_MATCH")]) as m,
        patch.object(stages, "get_signal_location", return_value=moved_to and moved,
                     side_effect=reread_error) as reread,
    ):
        m["record"].side_effect = None
        m["record"].return_value = {"id": "row-1", "status": row_status, "signalCount": 3}
        yield {**m, "reread": reread, "moved": moved}


def test_a_miss_on_a_name_resolved_meanwhile_groups_on_the_alias_location():
    # clear-api moved the signal as it recorded the miss: the event must be
    # keyed on where the signal is now, not on the state saved a moment ago.
    with _drain_with_a_late_alias(row_status="RESOLVED", moved_to=True) as m:
        stages._drain_signals_locked(MagicMock())
    m["reread"].assert_called_once_with("sig-x-1")
    assert m["group"].call_args.kwargs["created_signal"]["generalLocation"] == m["moved"]


def test_an_open_name_needs_no_second_read():
    with _drain_with_a_late_alias(row_status="OPEN") as m:
        stages._drain_signals_locked(MagicMock())
    m["reread"].assert_not_called()
    assert m["group"].call_args.kwargs["created_signal"]["generalLocation"]["id"] == "sd-wk"


@pytest.mark.parametrize("case", [{"reread_error": RuntimeError("clear-api 502")}, {"moved_to": None}])
def test_a_failed_second_read_keeps_the_saved_location(case):
    with _drain_with_a_late_alias(row_status="RESOLVED", **case) as m:
        stages._drain_signals_locked(MagicMock())
    assert m["group"].call_args.kwargs["created_signal"]["generalLocation"]["id"] == "sd-wk"
    assert ("PROCESSED", ["sig-x-1"]) in m["marked"]


def test_the_signal_location_client_reads_the_general_location():
    from clear_pipeline.providers import clear_api

    loc = {"id": "l4", "name": "Um Dabakir", "level": 4, "ancestorIds": ["sd"]}
    with patch.object(clear_api, "_execute", return_value={"signal": {"id": "s", "generalLocation": loc}}) as ex:
        assert clear_api.get_signal_location("s") == loc
    assert ex.call_args.args[1] == {"id": "s"}
    with patch.object(clear_api, "_execute", return_value={"signal": None}):
        assert clear_api.get_signal_location("gone") is None
