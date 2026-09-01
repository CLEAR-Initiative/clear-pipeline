"""Unit tests for IDMC IDU's origin/destination pairing (`_split_by_location`,
`_classify_role`) and the per-entry location resolution in
`build_idmc_signal_input`. Plain dict fixtures, no network/DB — mirrors
test_signals_ingest_drain.py's style.
"""

from unittest.mock import patch

from clear_context_pipeline.providers.idmc import (
    _classify_role,
    _content_hash,
    _kind_from_locations_accuracy,
    _parse_coordinate,
    _split_by_location,
    build_idmc_signal_input,
    build_signal_content_update,
)


def _raw(**overrides) -> dict:
    row = {
        "id": 1,
        "figure": 100,
        "iso3": "SDN",
        "displacement_type": "Conflict",
        "role": "Recommended figure",
        "event_name": "Clashes in Darfur",
        "standard_popup_text": "Some description",
        "latitude": 13.578933333333332,
        "longitude": 24.743561,
        "locations_name": "Al Fasher, North Darfur State, Sudan",
        "locations_type": "Origin",
        "locations_coordinates": "13.30913, 25.517651",
        "locations_accuracy": "Locality",
        "displacement_start_date": "2026-01-01",
        "displacement_end_date": "2026-01-05",
        "source_url": "https://example.com",
        "created_at": "2026-01-06T00:00:00Z",
    }
    row.update(overrides)
    return row


def _parsed(**raw_overrides) -> dict:
    """A single-location record as `build_idmc_signal_input` actually
    receives it in production: the output of `_split_by_location` (even the
    n==1 case) with `content_hash` attached, exactly like `fetch_idu_records`
    does before handing a record to a connector — never `_parse_event`'s raw
    output directly, that's what surfaces `locations_coordinates`/
    `locations_accuracy` at the top level."""
    from clear_context_pipeline.providers.idmc import _parse_event

    result = _split_by_location(_parse_event(_raw(**raw_overrides)))[0]
    result["content_hash"] = _content_hash(result["raw"])
    return result


def _parsed_from(raw: dict) -> dict:
    from clear_context_pipeline.providers.idmc import _parse_event

    return _parse_event(raw)


# ── _parse_coordinate / _kind_from_locations_accuracy / _classify_role ───────


def test_parse_coordinate_valid_pair():
    assert _parse_coordinate("13.30913, 25.517651") == (13.30913, 25.517651)


def test_parse_coordinate_malformed_returns_none():
    assert _parse_coordinate("not a coordinate") is None


def test_parse_coordinate_empty_returns_none():
    assert _parse_coordinate("") is None


def test_kind_from_locations_accuracy_is_always_admin_for_known_values():
    # Every accuracy value seen in live IDU data describes an
    # administrative/settlement precision level, never a POI landmark.
    assert _kind_from_locations_accuracy("Locality") == "admin"
    assert _kind_from_locations_accuracy("District/Zone/Department (ADM2)") == "admin"
    assert _kind_from_locations_accuracy(None) == "admin"


def test_classify_role():
    assert _classify_role("Origin") == "origin"
    assert _classify_role("Destination") == "destination"
    assert _classify_role("Origin and destination") == "both"
    assert _classify_role("") == "neither"
    assert _classify_role("SomethingElse") == "neither"
    assert _classify_role(None) == "neither"


# ── _split_by_location: single location ──────────────────────────────────────


def test_single_location_keeps_idu_id_unchanged():
    parsed = _parsed()
    splits = _split_by_location(parsed)
    assert len(splits) == 1
    assert splits[0]["idu_id"] == "1"


def test_single_location_lat_lng_matches_centroid():
    parsed = _parsed()
    splits = _split_by_location(parsed)
    # lat/lng is always the row's centroid, unaffected by the split — the
    # locations_coordinates entry (13.30913, 25.517651) is NOT used here.
    assert splits[0]["lat"] == parsed["lat"] == 13.578933333333332
    assert splits[0]["lng"] == parsed["lng"] == 24.743561


# ── _split_by_location: clean 1:1 pair (real data — 174447) ──────────────────


def _one_origin_one_destination_raw() -> dict:
    # Real (id-altered) IDU row: "200 households displaced from Al
    # Jazirah to Al Fao" — one flow, unambiguous origin + destination.
    return _raw(
        id=174447,
        figure=1000,
        locations_name="Al Fao, Al Qadarif, Gedarif State, Sudan; Al Jazirah, Sudan",
        locations_type="Destination; Origin",
        locations_coordinates="14.11326, 34.088169; 14.66715, 33.222359",
        locations_accuracy=(
            "County/City/town/Village/Woreda (ADM3); State/Region/Province (ADM1)"
        ),
        latitude=14.390204999999998,
        longitude=33.655263999999995,
    )


def test_one_origin_one_destination_merges_into_single_signal():
    parsed = _parsed_from(_one_origin_one_destination_raw())
    splits = _split_by_location(parsed)
    assert len(splits) == 1
    assert splits[0]["idu_id"] == "174447"  # unchanged — not fragmented
    assert splits[0]["figure"] == 1000  # full, undivided


def test_one_origin_one_destination_combines_name_and_type_origin_first():
    parsed = _parsed_from(_one_origin_one_destination_raw())
    split = _split_by_location(parsed)[0]
    assert split["locations_name"] == "Al Jazirah, Sudan; Al Fao, Al Qadarif, Gedarif State, Sudan"
    assert split["locations_type"] == "Origin; Destination"
    assert split["locations_coordinates"] == "14.66715, 33.222359; 14.11326, 34.088169"
    assert split["raw"]["locations_name"] == split["locations_name"]


def test_one_origin_one_destination_lat_lng_stays_centroid():
    parsed = _parsed_from(_one_origin_one_destination_raw())
    split = _split_by_location(parsed)[0]
    assert split["lat"] == 14.390204999999998
    assert split["lng"] == 33.655263999999995


# ── _split_by_location: 1:N / N:1 fan-out ─────────────────────────────────────


def _three_location_raw() -> dict:
    # Real (id-altered) IDU row: Al Fasher ("Origin and destination")
    # fills the missing origin role since the other two are plain
    # Destination entries — see _classify_role/ambiguous-resolution.
    return _raw(
        figure=1750,
        locations_name=(
            "Al Fasher, North Darfur State, Sudan; "
            "Kabkabiya, North Darfur State, Sudan; "
            "Tawilah, Al Fasher, North Darfur State, Sudan"
        ),
        locations_type="Origin and destination; Destination; Destination",
        locations_coordinates="13.30913, 25.517651; 13.9138, 23.851801; 13.51387, 24.861231",
        locations_accuracy="Locality; Locality; Locality",
    )


def test_one_origin_two_destinations_fans_out_into_two_pairs():
    parsed = _parsed_from(_three_location_raw())
    splits = _split_by_location(parsed)
    assert len(splits) == 2  # 1 origin x 2 destinations, NOT 3 raw locations
    assert [s["idu_id"] for s in splits] == ["1:0", "1:1"]


def test_one_origin_two_destinations_figure_divided_by_pair_count():
    parsed = _parsed_from(_three_location_raw())
    splits = _split_by_location(parsed)
    figures = [s["figure"] for s in splits]
    assert figures == [875, 875]  # 1750 / 2 pairs, not / 3 locations
    assert sum(figures) == 1750


def test_one_origin_two_destinations_each_pair_names_the_shared_origin():
    parsed = _parsed_from(_three_location_raw())
    splits = _split_by_location(parsed)
    assert splits[0]["locations_name"] == (
        "Al Fasher, North Darfur State, Sudan; Kabkabiya, North Darfur State, Sudan"
    )
    assert splits[1]["locations_name"] == (
        "Al Fasher, North Darfur State, Sudan; Tawilah, Al Fasher, North Darfur State, Sudan"
    )
    assert splits[0]["locations_type"] == "Origin and destination; Destination"
    assert splits[1]["locations_type"] == "Origin and destination; Destination"


def test_n_origins_one_destination_symmetric_case():
    raw = _raw(
        figure=900,
        locations_name="Town A, Sudan; Town B, Sudan; Town C, Sudan",
        locations_type="Origin; Origin; Destination",
        locations_coordinates="1.0, 1.0; 2.0, 2.0; 3.0, 3.0",
        locations_accuracy="Locality; Locality; Locality",
    )
    parsed = _parsed_from(raw)
    splits = _split_by_location(parsed)
    assert len(splits) == 2  # 2 origins x 1 destination
    figures = [s["figure"] for s in splits]
    assert figures == [450, 450]
    assert splits[0]["locations_name"] == "Town A, Sudan; Town C, Sudan"
    assert splits[1]["locations_name"] == "Town B, Sudan; Town C, Sudan"


def test_raw_dicts_are_independent_objects():
    parsed = _parsed_from(_three_location_raw())
    splits = _split_by_location(parsed)
    assert splits[0]["raw"] is not splits[1]["raw"]
    assert splits[0]["raw"] is not parsed["raw"]
    # Original parsed dict must be untouched.
    assert parsed["figure"] == 1750


def test_figure_zero_all_shares_zero():
    parsed = _parsed_from(_three_location_raw() | {"figure": 0})
    splits = _split_by_location(parsed)
    assert [s["figure"] for s in splits] == [0, 0]


# ── _split_by_location: fallback to independent per-location split ──────────
# See TODO.md — M origins x N destinations (both >1) and any neither-role
# entry aren't paired; they fall back to the older equal-division-per-raw-
# location behavior instead of being dropped.


def test_neither_role_entry_falls_back_to_independent_split():
    raw = _one_origin_one_destination_raw() | {
        "locations_type": "Origin; SomethingElse",
    }
    parsed = _parsed_from(raw)
    splits = _split_by_location(parsed)
    # Falls back: one signal per raw location (2), not a pair.
    assert len(splits) == 2
    assert [s["idu_id"] for s in splits] == ["174447:0", "174447:1"]
    assert sum(s["figure"] for s in splits) == 1000
    # Index 0 is "Al Fao..." (the name array's own order) — single name,
    # not combined with another location.
    assert splits[0]["locations_name"] == "Al Fao, Al Qadarif, Gedarif State, Sudan"


def test_multiple_origins_and_multiple_destinations_falls_back():
    raw = _raw(
        figure=800,
        locations_name="O1, Sudan; O2, Sudan; D1, Sudan; D2, Sudan",
        locations_type="Origin; Origin; Destination; Destination",
        locations_coordinates="1,1; 2,2; 3,3; 4,4",
        locations_accuracy="Locality; Locality; Locality; Locality",
    )
    parsed = _parsed_from(raw)
    splits = _split_by_location(parsed)
    # 2 origins x 2 destinations isn't paired — falls back to 4 independent
    # single-location signals (TODO.md).
    assert len(splits) == 4
    assert sum(s["figure"] for s in splits) == 800


def test_unresolvable_ambiguous_entry_falls_back():
    # Both a plain origin AND a plain destination already present — an
    # "Origin and destination" entry has no missing role to fill.
    raw = _raw(
        figure=300,
        locations_name="A, Sudan; B, Sudan; C, Sudan",
        locations_type="Origin; Destination; Origin and destination",
        locations_coordinates="1,1; 2,2; 3,3",
        locations_accuracy="Locality; Locality; Locality",
    )
    parsed = _parsed_from(raw)
    splits = _split_by_location(parsed)
    assert len(splits) == 3
    assert sum(s["figure"] for s in splits) == 300


# ── _split_by_location: count mismatch fallback ──────────────────────────────


def test_locations_field_count_mismatch_drops_the_row():
    raw = _three_location_raw()
    # Only two locations_type entries for three names -> mismatch.
    raw["locations_type"] = "Origin; Destination"
    parsed = _parsed_from(raw)

    assert _split_by_location(parsed) == []


# ── build_idmc_signal_input: origin/destination resolution ──────────────────
# find_or_create_landmark_l4 returns {locationId, reused, pointType,
# abortedReason} — mocked with that shape throughout.


def _promo(location_id=None, aborted_reason=None):
    return {
        "locationId": location_id,
        "reused": False,
        "pointType": "L4",
        "abortedReason": aborted_reason,
    }


def test_resolves_origin_id_using_name_and_coordinate():
    parsed = _parsed(locations_type="Origin")
    with patch(
        "clear_context_pipeline.providers.idmc.find_or_create_landmark_l4",
        return_value=_promo("loc-123"),
    ) as mock_promote, patch(
        "clear_context_pipeline.providers.idmc.enrich_with_geoparser"
    ) as mock_geoparse:
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    mock_promote.assert_called_once_with(
        name="Al Fasher",
        lat=13.30913,
        lng=25.517651,
        kind="admin",
        source_lat=13.578933333333332,
        source_lng=24.743561,
    )
    assert input_data["originId"] == "loc-123"
    assert "destinationId" not in input_data
    assert "locationId" not in input_data
    mock_geoparse.assert_called_once()


def test_resolves_destination_id():
    parsed = _parsed(locations_type="Destination")
    with patch(
        "clear_context_pipeline.providers.idmc.find_or_create_landmark_l4",
        return_value=_promo("loc-456"),
    ), patch("clear_context_pipeline.providers.idmc.enrich_with_geoparser"):
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    assert input_data["destinationId"] == "loc-456"
    assert "originId" not in input_data


def test_origin_and_destination_sets_both_ids():
    parsed = _parsed(locations_type="Origin and destination")
    with patch(
        "clear_context_pipeline.providers.idmc.find_or_create_landmark_l4",
        return_value=_promo("loc-789"),
    ), patch("clear_context_pipeline.providers.idmc.enrich_with_geoparser"):
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    assert input_data["originId"] == "loc-789"
    assert input_data["destinationId"] == "loc-789"
    assert "locationId" not in input_data


def test_blank_locations_type_leaves_both_unset():
    parsed = _parsed(locations_type="")
    with patch(
        "clear_context_pipeline.providers.idmc.find_or_create_landmark_l4",
        return_value=_promo("loc-999"),
    ), patch("clear_context_pipeline.providers.idmc.enrich_with_geoparser"):
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    assert "originId" not in input_data
    assert "destinationId" not in input_data
    assert "locationId" not in input_data


def test_no_match_still_uses_origin_coordinate_for_lat_lng():
    # locations_type="Origin" (the default fixture) — lat/lng prefers the
    # origin's own coordinate whenever one is present in the data, even
    # though promotion itself found no matching/creatable location.
    parsed = _parsed()
    with patch(
        "clear_context_pipeline.providers.idmc.find_or_create_landmark_l4",
        return_value=_promo(None),
    ) as mock_promote, patch(
        "clear_context_pipeline.providers.idmc.enrich_with_geoparser"
    ) as mock_geoparse:
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    mock_promote.assert_called_once()
    assert "originId" not in input_data
    assert "destinationId" not in input_data
    assert input_data["lat"] == 13.30913
    assert input_data["lng"] == 25.517651
    mock_geoparse.assert_called_once()


def test_no_origin_role_falls_through_to_centroid_lat_lng():
    # Destination-only — no origin coordinate exists to prefer, so lat/lng
    # falls back to the row's shared centroid.
    parsed = _parsed(locations_type="Destination")
    with patch(
        "clear_context_pipeline.providers.idmc.find_or_create_landmark_l4",
        return_value=_promo("loc-456"),
    ), patch("clear_context_pipeline.providers.idmc.enrich_with_geoparser"):
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    assert input_data["lat"] == parsed["lat"] == 13.578933333333332
    assert input_data["lng"] == parsed["lng"] == 24.743561


def test_aborted_promotion_leaves_origin_destination_unset():
    parsed = _parsed(locations_type="Origin")
    with patch(
        "clear_context_pipeline.providers.idmc.find_or_create_landmark_l4",
        return_value=_promo(None, aborted_reason="different_a2"),
    ), patch("clear_context_pipeline.providers.idmc.enrich_with_geoparser"):
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    assert "originId" not in input_data
    assert "destinationId" not in input_data


def test_promotion_exception_is_caught_and_falls_through():
    parsed = _parsed(locations_type="Origin")
    with patch(
        "clear_context_pipeline.providers.idmc.find_or_create_landmark_l4",
        side_effect=RuntimeError("boom"),
    ), patch(
        "clear_context_pipeline.providers.idmc.enrich_with_geoparser"
    ) as mock_geoparse:
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    assert "originId" not in input_data
    assert "destinationId" not in input_data
    # The origin's coordinate is still preferred for lat/lng even though
    # the promotion call itself raised — the raw coordinate data is
    # independent of whether find_or_create_landmark_l4 succeeded.
    assert input_data["lat"] == 13.30913
    mock_geoparse.assert_called_once()


def test_missing_coordinate_skips_promotion_attempt_entirely():
    parsed = _parsed(locations_coordinates="")
    with patch(
        "clear_context_pipeline.providers.idmc.find_or_create_landmark_l4"
    ) as mock_promote, patch("clear_context_pipeline.providers.idmc.enrich_with_geoparser"):
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    mock_promote.assert_not_called()
    assert "originId" not in input_data
    assert "destinationId" not in input_data


# ── build_idmc_signal_input: pair entries (2 locations, one signal) ─────────


def test_pair_signal_resolves_both_ends_independently():
    parsed = _parsed_from(_one_origin_one_destination_raw())
    split = _split_by_location(parsed)[0]  # merged 1:1 pair
    split["content_hash"] = _content_hash(split["raw"])

    def fake_promote(*, name, lat, lng, kind, source_lat, source_lng):
        return _promo(f"loc-{name}")

    with patch(
        "clear_context_pipeline.providers.idmc.find_or_create_landmark_l4",
        side_effect=fake_promote,
    ) as mock_promote, patch("clear_context_pipeline.providers.idmc.enrich_with_geoparser"):
        input_data = build_idmc_signal_input(split, source_id="src-1")

    assert mock_promote.call_count == 2
    assert input_data["originId"] == "loc-Al Jazirah"
    assert input_data["destinationId"] == "loc-Al Fao"
    # lat/lng prefers the origin's (Al Jazirah's) own coordinate, not the
    # destination's and not the row's shared centroid.
    assert input_data["lat"] == 14.66715
    assert input_data["lng"] == 33.222359


def test_pair_signal_one_end_unresolvable_still_sets_the_other():
    parsed = _parsed_from(_one_origin_one_destination_raw())
    split = _split_by_location(parsed)[0]
    split["content_hash"] = _content_hash(split["raw"])

    def fake_promote(*, name, lat, lng, kind, source_lat, source_lng):
        # Only the origin (Al Jazirah) resolves; destination doesn't.
        return _promo("loc-origin" if name == "Al Jazirah" else None)

    with patch(
        "clear_context_pipeline.providers.idmc.find_or_create_landmark_l4",
        side_effect=fake_promote,
    ), patch("clear_context_pipeline.providers.idmc.enrich_with_geoparser"):
        input_data = build_idmc_signal_input(split, source_id="src-1")

    assert input_data["originId"] == "loc-origin"
    assert "destinationId" not in input_data


# ── real-API-shaped sample (id/event_id altered; otherwise a live 3-location
# ── row, incl. fields _parse_event doesn't extract — type/subtype/category/
# ── qualifier/centroid/event_codes — to confirm they're harmlessly ignored) ──


def _real_sample_raw() -> dict:
    return {
        "id": 900001,
        "iso3": "SDN",
        "role": "Recommended figure",
        "type": None,
        "year": 2024,
        "figure": 1750,
        "old_id": None,
        "country": "Sudan",
        "sources": "IOM Displacement Tracking Matrix (IOM DTM)",
        "subtype": None,
        "category": None,
        "centroid": "[13.578933333333332, 24.743561]",
        "event_id": 800001,
        "latitude": 13.578933333333332,
        "longitude": 24.743561,
        "qualifier": "approximately",
        "created_at": "2024-12-10T06:25:21.139712Z",
        "event_name": "Sudan: Non-International armed conflict (NIAC) - Countrywide - 2024",
        "source_url": "",
        "event_codes": "CE-2023-000066-SDN; MDRSD033",
        "subcategory": None,
        "event_end_date": "2024-12-31",
        "locations_name": (
            "Al Fasher, North Darfur State, Sudan; "
            "Kabkabiya, North Darfur State, Sudan; "
            "Tawilah, Al Fasher, North Darfur State, Sudan"
        ),
        "locations_type": "Origin and destination; Destination; Destination",
        "event_code_types": "Glide Number; IFRC Appeal ID",
        "event_start_date": "2024-01-01",
        "displacement_date": "2024-11-09",
        "displacement_type": "Conflict",
        "locations_accuracy": (
            "District/Zone/Department (ADM2); District/Zone/Department (ADM2); "
            "County/City/town/Village/Woreda (ADM3)"
        ),
        "standard_info_text": "<b> Sudan: 1,750 displacements, 07 November - 09 November </b>",
        "standard_popup_text": (
            "<b> Sudan: 1,750 displacements, 07 November - 09 November </b> <br> "
            "Approximately 350 households were displaced due to non-international "
            "armed conflict (niac) between 7 and 9 November from Al Fasher to "
            "Tawila and Kabkabiya, according to IOM Displacement Tracking Matrix "
            "(IOM DTM). <br> IOM Displacement Tracking Matrix (IOM DTM) - "
            "10 November 2024"
        ),
        "displacement_end_date": "2024-11-09",
        "displacement_occurred": "Displacement without preventive evacuations reported",
        "locations_coordinates": "13.30913, 25.517651; 13.9138, 23.851801; 13.51387, 24.861231",
        "displacement_start_date": "2024-11-07",
    }


def test_real_sample_pairs_despite_unmapped_fields():
    parsed = _parsed_from(_real_sample_raw())
    # Extra fields _parse_event doesn't extract (type, subtype, category,
    # qualifier, centroid, event_codes, ...) must not raise — they simply
    # aren't surfaced onto the parsed dict, but survive inside `raw`.
    assert parsed["figure"] == 1750
    assert parsed["raw"]["qualifier"] == "approximately"

    splits = _split_by_location(parsed)
    # 1 origin (Al Fasher, filling the missing role) x 2 destinations = 2
    # pairs, not 3 independent locations.
    assert len(splits) == 2
    assert [s["idu_id"] for s in splits] == ["900001:0", "900001:1"]
    assert sum(s["figure"] for s in splits) == 1750


def test_real_sample_splits_all_share_the_row_level_centroid():
    """Confirms the design choice directly against a live sample: even
    though the paired locations_coordinates entries are distinct real
    points, every output's top-level lat/lng stays the row's shared
    centroid (13.578933, 24.743561) — origin/destination precision comes
    from each pair's own name + coordinate via find_or_create_landmark_l4
    instead, not from overriding lat/lng."""
    parsed = _parsed_from(_real_sample_raw())
    splits = _split_by_location(parsed)
    assert len(splits) == 2
    for split in splits:
        assert split["lat"] == parsed["lat"] == 13.578933333333332
        assert split["lng"] == parsed["lng"] == 24.743561


# ── build_signal_content_update ──────────────────────────────────────────


def test_build_signal_content_update_carries_required_fields():
    input_data = {
        "sourceId": "src-1",
        "externalId": "idmc:174447",
        "rawData": {"figure": 1500},
        "publishedAt": "2026-01-06T00:00:00Z",
        "title": "Clashes in Darfur",
        "description": "Some description",
        "severity": 3,
        "contentHash": "hash123",
    }
    update_input = build_signal_content_update(input_data, "signal-abc")
    assert update_input["id"] == "signal-abc"
    assert update_input["contentHash"] == "hash123"
    assert update_input["rawData"] == {"figure": 1500}
    assert update_input["title"] == "Clashes in Darfur"
    assert update_input["description"] == "Some description"
    assert update_input["severity"] == 3


def test_build_signal_content_update_omits_absent_optional_fields():
    """The regression this guards against: an absent key must stay absent in
    the update payload, never default to None — sending an explicit null
    would clear a previously-resolved field (e.g. originId) on a revision
    where location resolution happened to fail transiently this poll."""
    input_data = {
        "rawData": {"figure": 1500},
        "title": "t",
        "description": None,
        "severity": 2,
        "contentHash": "hash123",
        # originId/destinationId/lat/lng/url/geoparsedData deliberately absent
    }
    update_input = build_signal_content_update(input_data, "signal-abc")
    for absent_field in ("url", "originId", "destinationId", "lat", "lng", "geoparsedData"):
        assert absent_field not in update_input


def test_build_signal_content_update_carries_present_optional_fields():
    input_data = {
        "rawData": {"figure": 1500},
        "title": "t",
        "description": "d",
        "severity": 2,
        "contentHash": "hash123",
        "url": "https://example.com",
        "originId": "loc-origin",
        "destinationId": "loc-dest",
        "lat": 13.6,
        "lng": 24.7,
        "geoparsedData": {"candidate": "Nyala"},
    }
    update_input = build_signal_content_update(input_data, "signal-abc")
    assert update_input["url"] == "https://example.com"
    assert update_input["originId"] == "loc-origin"
    assert update_input["destinationId"] == "loc-dest"
    assert update_input["lat"] == 13.6
    assert update_input["lng"] == 24.7
    assert update_input["geoparsedData"] == {"candidate": "Nyala"}


def test_build_signal_content_update_never_includes_location_id():
    """locationId isn't produced by build_idmc_signal_input at all (IDMC only
    ever sets originId/destinationId/lat/lng client-side; the general
    fallback location is resolved server-side from lat/lng), so it should
    never appear here either."""
    input_data = {"rawData": {}, "contentHash": "h"}
    update_input = build_signal_content_update(input_data, "signal-abc")
    assert "locationId" not in update_input
