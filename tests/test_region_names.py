"""A region name gives no point (#1429).

"Darfur" is a region, not a place. GeoNames holds it with a centre point at
13.0, 25.0, inside North Darfur, so the area check accepted it and the signal
was pinned to a landmark in whatever district holds that centre. Three rules
stop it:

- the name the location answer is based on is checked like the other names of
  the post: a region phrase or a natural feature is not geocoded;
- a gazetteer match that is a state, a region or a mountain range is refused,
  and an exact one ends the lookup;
- a LocationIQ result that is a whole state or country is refused.
"""

from unittest.mock import MagicMock, patch

import pytest
from shapely.geometry import box

from clear_pipeline.providers import geoparser, location
from clear_pipeline.providers.geoparser import GeocodeTrace

SUDAN = {"id": "sd", "name": "Sudan", "level": 0, "parent": None}


def state(id_, name):
    return {"id": id_, "name": name, "level": 1, "parent": {"id": "sd"}}


def district(id_, name, state_id):
    return {"id": id_, "name": name, "level": 2, "parent": {"id": state_id}}


NORTH_DARFUR = state("sd-nd", "North Darfur")
LOCATIONS = [
    SUDAN,
    NORTH_DARFUR,
    state("sd-sd", "South Darfur"),
    state("sd-nk", "North Kordofan"),
    state("sd-sk", "South Kordofan"),
    state("sd-bn", "Blue Nile"),
    state("sd-rs", "Red Sea"),
    state("sd-kh", "Khartoum"),
    state("sd-no", "Northern"),
    district("sd-nd-fasher", "Al Fasher", "sd-nd"),
    district("sd-kh-north", "Khartoum North", "sd-kh"),
    district("sd-kh-sharg", "Sharg An Neel", "sd-kh"),
]
ND_SHAPE = box(22.0, 12.0, 27.0, 20.0)


@pytest.fixture(autouse=True)
def _locations():
    location.invalidate_locations_cache()
    with (
        patch.object(location, "get_locations", return_value=LOCATIONS),
        patch.object(location, "get_locations_by_level", return_value=[SUDAN]),
    ):
        yield
    location.invalidate_locations_cache()


# ── the name ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("name", [
    # The names the dev backfill of 11 Nov 2026 would have pinned to a point.
    "Darfur", "Kordofan", "Kordufan", "N. Kordofan", "eastern Sudan", "east", "North",
    "South Blue Nile", "North Red Sea", "North and West Kordofan border", "Red Sea Hills",
    "Nuba Mountains", "وسط دارفور",
    # And their like.
    "the Darfur region", "Kordofan axis", "دارفور", "كردفان", "ولاية شمال كردفان", "إقليم دارفور", "شرق",
    # A state's or the country's own name was refused before, and still is.
    "North Darfur", "Khartoum", "Sudan",
])
def test_a_region_is_not_a_place(name):
    assert not location._names_a_place(name, "Sudan")


@pytest.mark.parametrize("name", [
    "Tawila", "El Fasher", "Al Fasher", "Zamzam camp", "Port Sudan", "Wadi Halfa", "Jebel Awlia",
    # A district's own name is a place, whatever words it shares with a state.
    "Khartoum North", "Sharg An Neel",
    # An Arabic name is a place unless it is a region several states share.
    # "شرق النيل" is the district Sharg An Neel, not the east of a state.
    "طويلة", "الفاشر", "شرق النيل", "الخرطوم", "جنوب الحزام",
])
def test_a_place_is_a_place(name):
    assert location._names_a_place(name, "Sudan")


# ── the locator ──────────────────────────────────────────────────────────────

def _answer(**fields):
    provider = MagicMock()
    provider.complete_structured.side_effect = lambda *, system, user, schema: (
        schema(location_type="general", **fields) if schema is location._LocationResolution else schema()
    )
    return patch.object(location, "make_llm_provider", return_value=provider)


POST = "قصف مدفعي على مناطق متفرقة في دارفور"


def test_a_state_answer_with_a_region_as_its_place_stays_at_the_state():
    with (
        _answer(location_id="sd-nd", place_name="Darfur"),
        patch.object(location, "geocode_place_name") as geocode,
        patch.object(location, "named_places", return_value=[]),
        patch.object(location, "_area_has_geometry", return_value=True),
        location.collect_geocode_misses() as misses,
    ):
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd-nd"

    geocode.assert_not_called()
    # Not a miss: a reviewer has no place to give "Darfur".
    assert misses == []


def test_a_district_answer_with_a_region_as_its_place_falls_back_to_the_state():
    # Nothing confirms the district: "Darfur" does not geocode, and it is not
    # the district's own name.
    with (
        _answer(location_id="sd-nd-fasher", place_name="Darfur"),
        patch.object(location, "geocode_place_name") as geocode,
        patch.object(location, "named_places", return_value=[]),
        patch.object(location, "_area_has_geometry", return_value=True),
    ):
        assert location.locate_in_country(POST, POST, "Sudan", country_code="sd") == "sd-nd"
    geocode.assert_not_called()


def test_a_name_the_gazetteer_knows_as_an_area_is_not_a_miss():
    def names_an_area(name, *, trace, **_):
        trace.area = True
        return None

    geometry = {"type": "Polygon", "coordinates": [list(ND_SHAPE.exterior.coords)]}
    location._area_shapes_cache.clear()
    with (
        patch.object(location, "geocode_place_name", side_effect=names_an_area),
        patch.object(location, "arabic_lookup_applies", return_value=False),
        patch.object(location, "gazetteer_knows") as knows,
        patch.object(location, "get_location_with_geometry", return_value={"id": "sd-nd", "geometry": geometry}),
        location.collect_geocode_misses() as misses,
    ):
        assert location._geocode_within("الخرطوم", "sd-nd", "sd", text=POST) is None
    location._area_shapes_cache.clear()

    assert misses == []
    knows.assert_not_called()


@pytest.mark.parametrize(("is_area", "recorded"), [(True, 0), (False, 1)])
def test_a_state_name_that_the_area_lookup_missed_is_not_a_miss(is_area, recorded):
    # Inside a district the gazetteer finds nothing for "الخرطوم": the state's
    # centre point lies elsewhere. Asked for the whole country, it is a state.
    geometry = {"type": "Polygon", "coordinates": [list(ND_SHAPE.exterior.coords)]}
    location._area_shapes_cache.clear()
    with (
        patch.object(location, "geocode_place_name", return_value=None),
        patch.object(location, "arabic_lookup_applies", return_value=False),
        patch.object(location, "gazetteer_knows", return_value=False),
        patch.object(location, "gazetteer_names_area", return_value=is_area) as asked,
        patch.object(location, "get_location_with_geometry", return_value={"id": "sd-nd", "geometry": geometry}),
        location.collect_geocode_misses() as misses,
    ):
        assert location._geocode_within("الخرطوم", "sd-nd", "sd", text=POST) is None
    location._area_shapes_cache.clear()

    asked.assert_called_once_with("الخرطوم", "sd", arabic_name=None)
    assert len(misses) == recorded


def test_the_area_check_is_not_paid_for_when_no_miss_is_recorded():
    geometry = {"type": "Polygon", "coordinates": [list(ND_SHAPE.exterior.coords)]}
    location._area_shapes_cache.clear()
    with (
        patch.object(location, "geocode_place_name", return_value=None),
        patch.object(location, "arabic_lookup_applies", return_value=False),
        patch.object(location, "gazetteer_names_area") as asked,
        patch.object(location, "get_location_with_geometry", return_value={"id": "sd-nd", "geometry": geometry}),
    ):
        assert location._geocode_within("الخرطوم", "sd-nd", "sd", text=POST) is None
    location._area_shapes_cache.clear()
    asked.assert_not_called()


# ── the gazetteer ────────────────────────────────────────────────────────────

def hit(name, code, *, cls=None, exact=True, lat="13.0", lng="25.0", population=0):
    return {"name": name, "latitude": lat, "longitude": lng, "countryCode": "SD",
            "featureClass": cls or {"ADM1": "A", "ADM1H": "A", "ADM2": "A", "RGN": "L", "MTS": "T"}.get(code, "P"),
            "featureCode": code, "score": 1.0 if exact else 0.6, "exact": exact,
            "population": population, "source": "GEONAMES", "locationId": None}


def _gazetteer(answer):
    return (
        patch.object(geoparser.settings, "geoparser_use_gazetteer", True),
        patch.object(geoparser.graphql, "resolve_gazetteer_location", return_value=answer),
    )


@pytest.mark.parametrize(("name", "code"), [
    ("Darfur", "RGN"), ("Khartoum", "ADM1"), ("Kurdufan Wilayat", "ADM1H"), ("Jibāl an Nūbah", "MTS"),
])
def test_an_exact_match_on_an_area_gives_no_point_and_ends_the_lookup(name, code):
    use, gazetteer = _gazetteer(hit(name, code))
    trace = GeocodeTrace()
    with use, gazetteer, patch.object(geoparser.nominatim, "search") as locationiq:
        assert geoparser.geocode_place_name(name, expected_country_codes={"sd"}, trace=trace) is None

    # LocationIQ would answer with the state at its centre, or its capital.
    locationiq.assert_not_called()
    assert trace.area and not trace.failed


def test_an_area_ends_the_lookup_for_a_caller_with_no_trace():
    use, gazetteer = _gazetteer(hit("Darfur", "RGN"))
    with use, gazetteer, patch.object(geoparser.nominatim, "search") as locationiq:
        assert geoparser.geocode_place_name("Darfur", expected_country_codes={"sd"}) is None
    locationiq.assert_not_called()


def test_a_fuzzy_match_on_an_area_is_dropped_and_the_next_tier_is_asked():
    # "Sennar Sugar" fuzzily matches the state: that says nothing about the name.
    row = {"lat": "13.4", "lon": "33.6", "class": "place", "importance": 0.2,
           "address": {"village": "Sennar Sugar", "state": "Sennar", "country": "Sudan", "country_code": "sd"}}
    use, gazetteer = _gazetteer(hit("Sennar", "ADM1", exact=False))
    trace = GeocodeTrace()
    with use, gazetteer, patch.object(geoparser.nominatim, "search", return_value=[row]) as locationiq:
        result = geoparser.geocode_place_name("Sennar Sugar", expected_country_codes={"sd"}, trace=trace)

    locationiq.assert_called()
    assert (result.lat, result.lng) == (13.4, 33.6)
    assert not trace.area


def test_a_district_and_a_town_are_still_points():
    for answer in (hit("Tendalti", "ADM2", lat="13.1", lng="32.0"), hit("Ţawīlah", "PPL", lat="13.5", lng="24.9")):
        use, gazetteer = _gazetteer(answer)
        with use, gazetteer, patch.object(geoparser.nominatim, "search") as locationiq:
            result = geoparser.geocode_place_name(answer["name"], expected_country_codes={"sd"})
        assert result is not None and result.lat == float(answer["latitude"])
        locationiq.assert_not_called()


def test_an_arabic_spelling_that_is_a_state_gives_no_point():
    # "الخرطوم" matches Khartoum state. The same name without its article
    # would find the city: it is not asked.
    use, gazetteer = _gazetteer(hit("Khartoum", "ADM1", lat="15.8", lng="33.0"))
    with use, gazetteer as lookup, patch.object(geoparser.nominatim, "search") as locationiq:
        result = geoparser.geocode_place_name(
            "Khartoum city", expected_country_codes={"sd"}, arabic_name="الخرطوم",
        )
    assert result is None
    assert [call.args[0] for call in lookup.call_args_list] == ["الخرطوم"]
    locationiq.assert_not_called()


def test_a_place_alias_is_kept_whatever_its_feature():
    # A person mapped the spelling to a CLEAR location: none of the checks apply.
    alias = {**hit("Darfur", "RGN"), "source": "ALIAS", "locationId": "loc-1"}
    use, gazetteer = _gazetteer(alias)
    with use, gazetteer:
        assert geoparser.geocode_place_name("Darfur", expected_country_codes={"sd"}).location_id == "loc-1"


def test_a_state_is_no_exact_match_elsewhere_and_no_known_name():
    state_hit = hit("Khartoum", "ADM1", population=5_000_000)
    use, gazetteer = _gazetteer(state_hit)
    with use, gazetteer:
        assert geoparser.exact_gazetteer_match(
            "Khartoum", "sd", within_location_id="sd-kh", min_population=5000,
        ) is None
        assert not geoparser.gazetteer_knows("Khartoum", "sd")


def test_the_gazetteer_says_when_a_name_is_an_area():
    use, gazetteer = _gazetteer(hit("Khartoum", "ADM1"))
    with use, gazetteer as lookup:
        assert geoparser.gazetteer_names_area("Khartoum city", "sd", arabic_name="الخرطوم")
    assert lookup.call_args.kwargs == {"country_code": "SD"}  # the whole country, no area

    for answer in (hit("Ţawīlah", "PPL"), hit("Khartoum", "ADM1", exact=False), None,
                   {**hit("Darfur", "RGN"), "source": "ALIAS", "locationId": "loc-1"}):
        use, gazetteer = _gazetteer(answer)
        with use, gazetteer:
            assert not geoparser.gazetteer_names_area("Tawila", "sd")

    with patch.object(geoparser.settings, "geoparser_use_gazetteer", False):
        assert not geoparser.gazetteer_names_area("Khartoum", "sd")


# ── LocationIQ ───────────────────────────────────────────────────────────────

def row(lat, lon, address, *, importance=0.4, cls="boundary"):
    return {"lat": lat, "lon": lon, "class": cls, "type": "administrative", "importance": importance,
            "address": {**address, "country_code": "sd"}}


def test_a_result_that_is_a_whole_state_is_refused():
    # What LocationIQ answers "Darfur" with: the states, each at its centre.
    results = [
        row("16.0", "26.0", {"state": "North Darfur", "country": "Sudan"}, importance=0.48),
        row("11.0", "25.0", {"state": "South Darfur", "country": "Sudan"}, importance=0.47),
    ]
    assert geoparser._pick_best_nominatim_result(results, {"sd"}) is None


def test_the_country_itself_is_refused():
    assert geoparser._pick_best_nominatim_result([row("15.0", "30.0", {"country": "Sudan"})], {"sd"}) is None


def test_a_result_inside_a_state_is_kept_over_a_more_important_state():
    results = [
        row("15.87", "32.99", {"state": "Al Khartum State", "country": "Sudan"}, importance=0.63),
        row("15.56", "32.53", {"city": "Khartoum", "state": "Al Khartum State", "country": "Sudan"}, importance=0.47),
    ]
    assert geoparser._pick_best_nominatim_result(results, {"sd"})["lat"] == "15.56"


def test_a_result_with_no_address_to_judge_is_kept():
    bare = {"lat": "11.0", "lon": "29.7", "class": "power", "importance": 0.1, "address": {"country_code": "sd"}}
    assert geoparser._pick_best_nominatim_result([bare], {"sd"}) is bare
    no_address = {"lat": "11.0", "lon": "29.7", "class": "power", "importance": 0.1}
    assert geoparser._pick_best_nominatim_result([no_address], set()) is no_address
