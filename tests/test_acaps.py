"""Unit tests for the ACAPS seasonal-calendar blob builder (no network)."""

from clear_pipeline.providers import acaps


def _entry(id_, iso, *, country_wide, event_type, adm1_names=None, months=None):
    return {
        "id": id_,
        "iso": [iso],
        "country": ["X"],
        "country_wide": country_wide,
        "adm1": "X.1_1, X.2_1",
        "adm1_eng_name": adm1_names,
        "months": months or ["March", "January"],
        "event": [event_type],
        "event_type": [event_type],
        "label": [event_type],
        "comment": "c",
        "source": "FEWS NET",
        "source_date": "2013-12-17",
        "source_link": "https://example.org",
    }


def test_months_sorted_to_calendar_order():
    e = _entry("1", "SDN", country_wide=True, event_type="Lean season",
               months=["September", "August", "October"])
    blobs = acaps.build_blobs([e], "SDN")
    assert blobs["country"][0]["months"] == ["August", "September", "October"]


def test_country_wide_goes_to_country_bucket():
    e = _entry("1", "SDN", country_wide=True, event_type="Outbreak")
    blobs = acaps.build_blobs([e], "SDN")
    assert len(blobs["country"]) == 1
    assert blobs["admin1"] == {}
    assert blobs["country"][0]["event_type"] == ["Outbreak"]


def test_subnational_expands_across_named_admin1s():
    e = _entry("2", "SDN", country_wide=False, event_type="Harvest",
               adm1_names=["Kassala", "Red Sea"])
    blobs = acaps.build_blobs([e], "SDN")
    assert blobs["country"] == []
    assert set(blobs["admin1"]) == {"Kassala", "Red Sea"}
    assert blobs["admin1"]["Kassala"][0]["id"] == "2"


def test_subnational_without_names_is_skipped():
    e = _entry("3", "SDN", country_wide=False, event_type="Livestock", adm1_names=None)
    blobs = acaps.build_blobs([e], "SDN")
    assert blobs["admin1"] == {}
    assert blobs["skipped_no_adm1_name"] == 1


def test_filters_by_iso3():
    entries = [
        _entry("1", "SDN", country_wide=True, event_type="Lean season"),
        _entry("2", "KEN", country_wide=True, event_type="Lean season"),
    ]
    blobs = acaps.build_blobs(entries, "SDN")
    assert len(blobs["country"]) == 1
    assert blobs["country"][0]["id"] == "1"
