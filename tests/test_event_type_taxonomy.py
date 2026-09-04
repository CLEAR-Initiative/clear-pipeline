"""Tests for the shared level_2 event-type vocabulary + coercion.

`coerce_event_types` is the single gate that keeps LLM-emitted event tags on the
disaster_types level_2 taxonomy (mirrored in event_categories.json), used by both
KB enrichment and datapoint extraction.
"""

from clear_pipeline.providers.classify import (
    coerce_event_types,
    level2_values,
)


def test_level2_values_are_the_taxonomy():
    vals = level2_values()
    # A stable, non-trivial vocabulary drawn from the taxonomy.
    assert "flood" in vals
    assert "tropical cyclone" in vals
    assert "battles" in vals
    # Consequences / activities are NOT event types.
    assert "displacement" not in vals
    assert "search-and-rescue" not in vals
    # Sorted + de-duplicated + lowercase.
    assert vals == sorted(set(vals))
    assert all(v == v.lower() for v in vals)


def test_drops_off_taxonomy_tags():
    assert coerce_event_types(["displacement", "search-and-rescue", "nonsense"]) == []


def test_passes_through_valid_level2():
    assert coerce_event_types(["flood", "drought"]) == ["flood", "drought"]


def test_maps_common_aliases():
    assert coerce_event_types(["Wildfire"]) == ["wild fire"]
    assert coerce_event_types(["disease outbreak"]) == ["epidemic"]
    assert coerce_event_types(["cyclone", "hurricane", "typhoon"]) == ["tropical cyclone"]
    assert coerce_event_types(["landslide"]) == ["land slide"]


def test_lowercases_dedupes_preserves_order():
    assert coerce_event_types(["FLOOD", "flood", "Drought"]) == ["flood", "drought"]


def test_mixed_valid_and_invalid():
    assert coerce_event_types(["flood", "displacement", "Wildfire", "xyz"]) == [
        "flood",
        "wild fire",
    ]


def test_non_list_passthrough_for_pydantic():
    # A non-list is returned unchanged so pydantic reports it as it would today.
    assert coerce_event_types("notalist") == "notalist"
    assert coerce_event_types(None) is None
