"""Unit tests for the location-metadata resolution helpers (no network)."""

from clear_context_pipeline.defs.location_metadata import assets


_INDEX = {
    "pcode_to_id": {
        "0": {"SD": "id-sudan"},          # clear-api stores Sudan A0 as "SD"
        "1": {"SD05": "id-sd05"},
        "2": {"SD05002": "id-nyala"},
    },
    "iso2_to_id": {"SD": "id-sudan"},
    "name_to_id": {
        "0": {"sudan": "id-sudan"},
        "1": {},
        "2": {"gezira": "id-gezira"},     # normalised form of "El Gezira"
    },
}


def test_resolve_exact_pcode():
    assert assets._resolve(_INDEX, "SD05002", 2, None) == "id-nyala"


def test_resolve_iso2_fallback_at_admin0():
    # HAPI/DTM return "SDN"; clear-api has "SD" → matched on the ISO2 prefix.
    assert assets._resolve(_INDEX, "SDN", 0, "Sudan") == "id-sudan"


def test_resolve_name_fallback_when_pcode_missing():
    # A district whose pcode isn't in clear-api, matched by normalised name.
    assert assets._resolve(_INDEX, "SD99999", 2, "El Gezira") == "id-gezira"


def test_resolve_returns_none_when_nothing_matches():
    assert assets._resolve(_INDEX, "ZZ0000", 2, "Nowhere") is None


def test_to_batch_skips_unmatched():
    blobs = {
        "SD05002": {"admin_name": "Nyala", "v": 1},
        "ZZ0000": {"admin_name": "Nowhere", "v": 2},
    }
    batch, unmatched = assets._to_batch(blobs, type_="hapi_x", level=2, index=_INDEX)
    assert unmatched == 1
    assert len(batch) == 1
    assert batch[0] == {"locationId": "id-nyala", "type": "hapi_x", "data": {"admin_name": "Nyala", "v": 1}}


def test_parse_assessment_forms():
    assert assets._parse_assessment("") is None
    assert assets._parse_assessment("BA") == "BA"
    assert assets._parse_assessment("BA,FM") == ["BA", "FM"]
    assert assets._parse_assessment(" , ") is None


def test_normalise_name_strips_prefixes_and_punct():
    assert assets._normalise_name("Republic of Sudan") == "sudan"
    assert assets._normalise_name("Al Jazirah") == "jazirah"
    assert assets._normalise_name("El-Fasher!") == "fasher"
