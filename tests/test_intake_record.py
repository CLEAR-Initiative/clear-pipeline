"""IntakeRecord — the source-agnostic silver contract the router forks on
(C-1445 slice 1; providers/intake_record.py). V0 is a byte-identical
projection: it retains the source signal_input and re-emits it unchanged,
exposing the routing surface as typed fields.
"""

import pytest

from clear_pipeline.providers.intake_record import IntakeRecord, to_signal_input


def _full_signal_input() -> dict:
    """A CreateSignalInput with every field a silver stage populates — the
    normalized surface, the location anchors, and the extract_event output."""
    return {
        "sourceId": "src-123",
        "externalId": "dataminr:abc",   # the <source>:-prefixed id — clear-api framing
        "rawData": {"alertId": "abc", "k": "v"},
        "publishedAt": "2026-11-01T00:00:00Z",
        "url": "http://example/abc",
        "title": "Clashes in Gezira",
        "description": "Heavy fighting reported overnight",
        "casualties": 4,
        "lat": 14.1,
        "lng": 33.4,
        "geoparsedData": {"display_name": "Gezira, Sudan"},
        # extract_event output (extracted_signal_fields)
        "severity": 3,
        "startDate": "2026-10-30",
        "startDateAccuracy": "DAY",
        "endDate": "2026-11-01",
        "endDateAccuracy": "DAY",
        "magnitude": 1200.0,
        "magnitudeUnit": "people",
        "actors": ["RSF", "SAF"],
        "countryIds": ["loc-sdn"],
        "rawS3Key": "raw/dataminr/2026/abc.json",
    }


def test_to_signal_input_returns_the_retained_dict_unchanged():
    si = _full_signal_input()
    rec = IntakeRecord.from_signal_input(si, external_id="abc", source="dataminr")
    # Byte-identical AND the same object — V0 is a pure pass-through on the
    # event-branch side, so the medallion is unchanged downstream.
    assert rec.to_signal_input() is si
    assert rec.to_signal_input() == si
    assert to_signal_input(rec) is si  # module-level seam mirrors the method


def test_typed_fields_project_the_whole_surface():
    si = _full_signal_input()
    rec = IntakeRecord.from_signal_input(
        si, external_id="abc", source="dataminr", published_at="2026-11-01T00:00:00Z"
    )
    assert rec.external_id == "abc"      # the row key, NOT "dataminr:abc"
    assert rec.source == "dataminr"
    assert rec.published_at == "2026-11-01T00:00:00Z"
    assert rec.title == "Clashes in Gezira"
    assert rec.lead == "Heavy fighting reported overnight"
    assert rec.url == "http://example/abc"
    assert rec.raw_data == {"alertId": "abc", "k": "v"}
    assert (rec.lat, rec.lng) == (14.1, 33.4)
    assert rec.geoparsed_data == {"display_name": "Gezira, Sudan"}
    assert rec.country_ids == ["loc-sdn"]
    assert rec.severity == 3
    assert rec.casualties == 4
    assert rec.event_span == ("2026-10-30", "2026-11-01")
    assert (rec.start_date_accuracy, rec.end_date_accuracy) == ("DAY", "DAY")
    assert rec.magnitude == 1200.0 and rec.magnitude_unit == "people"
    assert rec.actors == ["RSF", "SAF"]


def test_external_id_is_the_row_key_not_the_prefixed_signal_id():
    """The prefixed `externalId` ("dataminr:abc") is clear-api framing and stays
    only in `signal_input`; the record's `external_id` is the unprefixed row key."""
    si = _full_signal_input()
    rec = IntakeRecord.from_signal_input(si, external_id="abc", source="dataminr")
    assert rec.external_id == "abc"
    assert rec.signal_input["externalId"] == "dataminr:abc"


def test_published_at_falls_back_to_the_dict_when_not_passed():
    si = _full_signal_input()
    rec = IntakeRecord.from_signal_input(si, external_id="abc", source="dataminr")
    assert rec.published_at == "2026-11-01T00:00:00Z"  # from si["publishedAt"]


def test_minimal_input_leaves_optional_fields_none():
    """A sparse signal_input (no extraction, no coords) projects to Nones/empties,
    never KeyErrors — matching `.get(...)` semantics the silver df relied on."""
    si = {"externalId": "darfur24:1", "title": "t", "description": None, "locationId": "loc-x"}
    rec = IntakeRecord.from_signal_input(si, external_id="1", source="darfur24")
    assert rec.title == "t"
    assert rec.lead is None
    assert rec.severity is None
    assert rec.lat is None and rec.lng is None
    assert rec.geoparsed_data is None
    assert rec.location_id == "loc-x"
    assert rec.country_ids == []       # absent countryIds -> [], not None
    assert rec.actors is None
    assert rec.event_span == (None, None)
    assert rec.to_signal_input() is si


def test_unknown_severity_projects_to_none_not_an_invented_floor():
    si = {"externalId": "s:1", "title": "t", "severity": None}
    rec = IntakeRecord.from_signal_input(si, external_id="1", source="s")
    assert rec.severity is None


def test_routing_surface_excludes_the_retained_signal_input():
    """The router reads the bounded surface (§4), never the full document /
    retained dict."""
    si = _full_signal_input()
    rec = IntakeRecord.from_signal_input(si, external_id="abc", source="dataminr")
    surface = rec.routing_surface()
    assert "signal_input" not in surface
    assert "rawData" not in surface
    assert surface["title"] == "Clashes in Gezira"
    assert surface["event_start"] == "2026-10-30"
    assert surface["event_end"] == "2026-11-01"
    assert surface["severity"] == 3
    # Classification fields are None until the classify stage folds on (slice 3).
    assert surface["event_type"] is None
    assert surface["relevance"] is None


def test_classification_fields_default_none_and_are_settable():
    si = _full_signal_input()
    rec = IntakeRecord.from_signal_input(si, external_id="abc", source="dataminr")
    assert (rec.event_type, rec.relevance, rec.glide_code) == (None, None, None)
    rec.event_type, rec.relevance, rec.glide_code = "conflict", 0.9, "cv"
    assert rec.routing_surface()["event_type"] == "conflict"
    assert rec.routing_surface()["relevance"] == 0.9


@pytest.mark.parametrize("anchor", [
    {"locationId": "loc-1"},
    {"lat": 15.6, "lng": 32.5},
    {"geoparsedData": {"display_name": "Khartoum, Sudan"}},
    {"countryIds": ["loc-sdn"]},
])
def test_each_location_anchor_shape_round_trips(anchor):
    si = {"externalId": "s:1", "title": "t", **anchor}
    rec = IntakeRecord.from_signal_input(si, external_id="1", source="s")
    assert rec.to_signal_input() == si
