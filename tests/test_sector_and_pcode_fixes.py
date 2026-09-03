"""need_sectors soft-coercion (enrich) + pcode-first A0 resolution (clear_api)."""

from unittest.mock import patch

from clear_pipeline.defs.knowledgebase.enrich import ExtractedParameters
from clear_pipeline.providers import clear_api
from clear_pipeline.providers.llm import _to_strict_json_schema


# ── strict-schema transform: make Pydantic schemas OpenAI strict-valid ───────

def _every_object_is_strict(node) -> bool:
    """Every object node has additionalProperties:false and lists all keys required."""
    if isinstance(node, dict):
        if node.get("type") == "object" and "properties" in node:
            if node.get("additionalProperties") is not False:
                return False
            if set(node.get("required", [])) != set(node["properties"]):
                return False
        return all(_every_object_is_strict(v) for v in node.values())
    if isinstance(node, list):
        return all(_every_object_is_strict(v) for v in node)
    return True


def test_strict_schema_is_openai_valid_including_defs():
    # Pydantic's raw schema is NOT strict-valid (no additionalProperties:false,
    # no `required`) — the transform must fix it recursively, incl. $defs so the
    # provider can grammar-constrain decoding instead of free-generating.
    raw = ExtractedParameters.model_json_schema()
    assert raw.get("additionalProperties") is not False  # precondition
    strict = _to_strict_json_schema(raw)
    assert _every_object_is_strict(strict)
    assert "$defs" in strict  # LocationRef ref preserved…
    assert _every_object_is_strict(strict["$defs"])  # …and made strict too


def test_strict_schema_does_not_mutate_input():
    raw = ExtractedParameters.model_json_schema()
    _to_strict_json_schema(raw)
    assert raw.get("additionalProperties") is not False  # original untouched


# ── need_sectors: map synonyms, drop unknowns, never reject the extraction ──

def test_need_sectors_maps_synonyms_and_drops_unknown():
    p = ExtractedParameters(
        need_sectors=[
            "Nutrition", "Child Protection", "CCCM", "MHPSS",
            "Funding", "Access and Logistics", "WASH", "wash",
        ],
    )
    # Nutrition→Food Security, Child Protection→Protection, CCCM→Shelter,
    # MHPSS→Health; Funding + Access and Logistics dropped; WASH deduped.
    assert p.need_sectors == ["Food Security", "Protection", "Shelter", "Health", "WASH"]


def test_need_sectors_canonical_untouched():
    p = ExtractedParameters(need_sectors=["Shelter", "Health", "Education"])
    assert p.need_sectors == ["Shelter", "Health", "Education"]


def test_need_sectors_empty_default():
    assert ExtractedParameters().need_sectors == []


# ── resolve_country_location_id_by_iso3: pcode-first, name fallback ──────────

def test_resolve_by_iso3_uses_pcode():
    with (
        patch(
            "clear_pipeline.providers.clear_api.get_pipeline_countries",
            return_value=[{"name": "Venezuela (Bolivarian Republic of)", "iso3": "VEN", "pcode": "VE"}],
        ),
        patch(
            "clear_pipeline.providers.clear_api.resolve_location",
            return_value="loc-ven",
        ) as mock_resolve,
    ):
        result = clear_api.resolve_country_location_id_by_iso3("ven")
    assert result == "loc-ven"
    # pcode is passed (and wins over the long official name that exact-name misses)
    mock_resolve.assert_called_once_with(pcode="VE", name="Venezuela (Bolivarian Republic of)", admin_level=0)


def test_resolve_by_iso3_unknown_country_returns_none():
    with patch(
        "clear_pipeline.providers.clear_api.get_pipeline_countries",
        return_value=[{"name": "Sudan", "iso3": "SDN", "pcode": "SD"}],
    ):
        assert clear_api.resolve_country_location_id_by_iso3("xxx") is None
