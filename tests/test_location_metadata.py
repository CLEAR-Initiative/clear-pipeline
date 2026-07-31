"""Unit tests for the location-metadata resolution helpers (no network)."""

from clear_context_pipeline.defs.location_metadata import assets


# name_to_id is level -> iso2 -> normalised_name -> id (country-scoped): an admin1
# named "Northern" exists in BOTH Sudan (SD) and Afghanistan (AF), so a name hit
# must be scoped to the country being ingested.
_INDEX = {
    "pcode_to_id": {
        "0": {"SD": "id-sudan"},          # clear-api stores Sudan A0 as "SD"
        "1": {"SD05": "id-sd05"},
        "2": {"SD05002": "id-nyala"},
    },
    "iso2_to_id": {"SD": "id-sudan", "AF": "id-afg"},
    "name_to_id": {
        "0": {"SD": {"sudan": "id-sudan"}},
        "1": {"SD": {"northern": "id-sd-northern"}, "AF": {"northern": "id-af-northern"}},
        "2": {"SD": {"gezira": "id-gezira"}},   # normalised form of "El Gezira"
    },
}


def test_resolve_exact_pcode():
    assert assets._resolve(_INDEX, "SD05002", 2, None, "SD") == "id-nyala"


def test_resolve_iso2_fallback_at_admin0():
    # HAPI/DTM return "SDN"; clear-api has "SD" → matched via the country's ISO2.
    assert assets._resolve(_INDEX, "SDN", 0, "Sudan", "SD") == "id-sudan"


def test_resolve_name_fallback_within_country():
    # A district whose pcode isn't in clear-api, matched by name within Sudan.
    assert assets._resolve(_INDEX, "SD99999", 2, "El Gezira", "SD") == "id-gezira"


def test_resolve_name_is_country_scoped():
    # "Northern" exists in both SD and AF — it must resolve to the country being
    # ingested, never leak across.
    assert assets._resolve(_INDEX, "SD99", 1, "Northern", "SD") == "id-sd-northern"
    assert assets._resolve(_INDEX, "AF99", 1, "Northern", "AF") == "id-af-northern"


def test_resolve_unknown_country_never_guesses():
    # A country not in the index (country_iso2=None) must return None rather than
    # binding onto some other country's location.
    assert assets._resolve(_INDEX, "IRQ", 0, "Iraq", None) is None
    # And an ISO3 whose 2-char prefix collides with a real ISO2 (IR≈Iran) must
    # not resolve when its own country isn't mapped.
    assert assets._iso2_for("IRQ") is None


def test_resolve_returns_none_when_nothing_matches():
    assert assets._resolve(_INDEX, "ZZ0000", 2, "Nowhere", "SD") is None


def test_to_batch_skips_unmatched():
    blobs = {
        "SD05002": {"admin_name": "Nyala", "v": 1},
        "ZZ0000": {"admin_name": "Nowhere", "v": 2},
    }
    batch, unmatched = assets._to_batch(blobs, type_="hapi_x", level=2, index=_INDEX, country_iso2="SD")
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
