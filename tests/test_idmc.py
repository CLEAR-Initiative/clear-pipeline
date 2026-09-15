"""Unit tests for IDMC IDU's signal building (`build_idmc_signal_input`) and
the update-payload adapter (`build_signal_content_update`). Plain dict
fixtures, no network/DB — mirrors test_signals_ingest_drain.py's style.
"""

import logging
from unittest.mock import patch

from clear_pipeline.providers.idmc import (
    _content_hash,
    _parse_coordinate,
    _parse_event,
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
    """A record as `build_idmc_signal_input` actually receives it in
    production: the output of `_parse_event` with `content_hash` attached,
    exactly like `fetch_idu_records` does before handing a record to a
    connector."""
    result = _parse_event(_raw(**raw_overrides))
    assert result is not None
    result["content_hash"] = _content_hash(result["raw"])
    return result


# Real (id-altered) IDU row with 3 named locations — used to confirm a
# multi-location row resolves to a single signal anchored on the row's own
# centroid, ignoring locations_coordinates.
def _multi_location_raw() -> dict:
    return _raw(
        id=900001,
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


def _parsed_from(raw: dict) -> dict:
    result = _parse_event(raw)
    assert result is not None
    result["content_hash"] = _content_hash(result["raw"])
    return result


# ── _parse_event: edge cases ──────────────────────────────────────────────


def test_missing_id_returns_none():
    assert _parse_event(_raw(id=None)) is None


def test_missing_figure_returns_none():
    assert _parse_event(_raw(figure=None)) is None


def test_malformed_figure_returns_none():
    assert _parse_event(_raw(figure="not-a-number")) is None


def test_malformed_latitude_discards_valid_longitude_too():
    """The lat/lng parse is one shared try/except — a bad latitude raises
    before the longitude line ever runs, so a perfectly good longitude is
    silently discarded along with it. Locking in current behavior."""
    result = _parse_event(_raw(latitude="not-a-float", longitude=24.5))
    assert result is not None
    assert result["lat"] is None
    assert result["lng"] is None


# ── _content_hash / _round_centroid: coordinate-noise rounding ────────────
# IDMC's backend recomputes latitude/longitude/centroid independently on
# every poll with float noise around 1e-11 to 1e-14 degrees — these tests
# guard the fingerprint against flagging that noise as a real revision.


def test_content_hash_ignores_latitude_longitude_float_noise():
    raw_a = _raw(latitude=13.578933333333332, longitude=24.743561)
    raw_b = _raw(latitude=13.578933333333335, longitude=24.743561000001)
    assert _content_hash(raw_a) == _content_hash(raw_b)


def test_content_hash_treats_int_and_float_coordinate_as_equal():
    raw_a = _raw(latitude=9, longitude=24.743561)
    raw_b = _raw(latitude=9.0, longitude=24.743561)
    assert _content_hash(raw_a) == _content_hash(raw_b)


def test_content_hash_ignores_centroid_noise():
    raw_a = _raw(centroid="[13.578933333333332, 24.743561]")
    raw_b = _raw(centroid="[13.5789333333349, 24.7435612222]")
    assert _content_hash(raw_a) == _content_hash(raw_b)


def test_content_hash_changes_on_real_content_change():
    raw_a = _raw(figure=100)
    raw_b = _raw(figure=200)
    assert _content_hash(raw_a) != _content_hash(raw_b)


# ── _parse_coordinate ─────────────────────────────────────────────────────


def test_parse_coordinate_valid_pair():
    assert _parse_coordinate("13.30913, 25.517651") == (13.30913, 25.517651)


def test_parse_coordinate_malformed_returns_none():
    assert _parse_coordinate("not a coordinate") is None


def test_parse_coordinate_empty_returns_none():
    assert _parse_coordinate("") is None


# ── build_idmc_signal_input: location resolution ──────────────────────────
# enrich_with_geoparser is mocked to a no-op throughout — it makes a real
# Nominatim network call otherwise.


def test_single_location_row_uses_row_centroid_for_lat_lng():
    parsed = _parsed()
    with patch("clear_pipeline.providers.idmc.enrich_with_geoparser"):
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    assert input_data["lat"] == 13.578933333333332
    assert input_data["lng"] == 24.743561
    assert "originId" not in input_data
    assert "destinationId" not in input_data
    assert "locationId" not in input_data


def test_multi_location_row_uses_row_centroid_ignoring_per_location_coordinates():
    """The signal's lat/lng always comes from the row's own centroid
    (latitude/longitude), regardless of how many locations the row's
    locations_coordinates field lists individually."""
    parsed = _parsed_from(_multi_location_raw())
    with patch("clear_pipeline.providers.idmc.enrich_with_geoparser"):
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    assert input_data["lat"] == 13.578933333333332
    assert input_data["lng"] == 24.743561
    assert "originId" not in input_data
    assert "destinationId" not in input_data


def test_sets_url_from_source_url():
    parsed = _parsed(source_url="https://example.com/174447")
    with patch("clear_pipeline.providers.idmc.enrich_with_geoparser"):
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    assert input_data["url"] == "https://example.com/174447"


def test_carries_title_description_severity_content_hash():
    parsed = _parsed(event_name="Clashes in Darfur", figure=15_000)
    with patch("clear_pipeline.providers.idmc.enrich_with_geoparser"):
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    assert input_data["externalId"] == "idmc:1"
    assert input_data["title"] == "Clashes in Darfur"
    assert input_data["description"] == "Some description"
    assert input_data["severity"] == 4  # figure >= 10_000
    assert input_data["contentHash"] == parsed["content_hash"]


def test_partial_coordinate_skips_lat_lng_and_logs_warning(caplog):
    parsed = _parsed(longitude=None)
    with patch("clear_pipeline.providers.idmc.enrich_with_geoparser"), \
         caplog.at_level(logging.WARNING):
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    assert "lat" not in input_data
    assert "lng" not in input_data
    assert "partial coordinate" in caplog.text


def test_missing_both_coordinates_skips_lat_lng_without_warning(caplog):
    parsed = _parsed(latitude=None, longitude=None)
    with patch("clear_pipeline.providers.idmc.enrich_with_geoparser"), \
         caplog.at_level(logging.WARNING):
        input_data = build_idmc_signal_input(parsed, source_id="src-1")

    assert "lat" not in input_data
    assert "lng" not in input_data
    assert "partial coordinate" not in caplog.text


def test_calls_enrich_with_geoparser():
    parsed = _parsed()
    with patch("clear_pipeline.providers.idmc.enrich_with_geoparser") as mock_geoparse:
        build_idmc_signal_input(parsed, source_id="src-1")

    mock_geoparse.assert_called_once()


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
    """lat/lng/geoparsedData stay absent when not in input_data — an absent
    key leaves that column alone, sending None would null it out. url (like
    title/description/severity) always syncs to the latest value instead, so
    it appears even when absent from input_data."""
    input_data = {
        "rawData": {"figure": 1500},
        "title": "t",
        "description": None,
        "severity": 2,
        "contentHash": "hash123",
        # lat/lng/geoparsedData deliberately absent
    }
    update_input = build_signal_content_update(input_data, "signal-abc")
    assert update_input["url"] is None
    for absent_field in ("lat", "lng", "geoparsedData"):
        assert absent_field not in update_input


def test_build_signal_content_update_carries_present_optional_fields():
    input_data = {
        "rawData": {"figure": 1500},
        "title": "t",
        "description": "d",
        "severity": 2,
        "contentHash": "hash123",
        "url": "https://example.com",
        "lat": 13.6,
        "lng": 24.7,
        "geoparsedData": {"candidate": "Nyala"},
    }
    update_input = build_signal_content_update(input_data, "signal-abc")
    assert update_input["url"] == "https://example.com"
    assert update_input["lat"] == 13.6
    assert update_input["lng"] == 24.7
    assert update_input["geoparsedData"] == {"candidate": "Nyala"}


def test_build_signal_content_update_never_includes_location_id():
    """locationId isn't produced by build_idmc_signal_input at all — it's
    resolved server-side from lat/lng — so it should never appear here."""
    input_data = {"rawData": {}, "contentHash": "h"}
    update_input = build_signal_content_update(input_data, "signal-abc")
    assert "locationId" not in update_input
