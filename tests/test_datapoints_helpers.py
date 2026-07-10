"""Tests for the pure helper functions in datapoints_extract.py.

Extraction assets themselves are integration-heavy (S3 + LLM +
clear-api). These tests target the helpers that do the deterministic
work in isolation: nested-dict walking, path lookup, numeric coercion,
and the location-ref collection routine that feeds the resolver.
"""

from unittest.mock import patch

import pytest

from clear_context_pipeline.defs.knowledgebase.datapoints_extract import (
    _collect_location_refs,
    _dig,
    _num_or_none,
    _resolve_all_locations,
)
from clear_context_pipeline.defs.knowledgebase.datapoints_schemas import LocationRef


class TestDig:
    """`_dig` is used to hoist hot totals out of the merged blob. If
    it silently returns wrong values, the dashboard's headline tiles
    lie without any downstream check catching it."""

    def test_returns_leaf_value_on_valid_path(self):
        obj = {"casualties": {"killed": {"total": {"value": 42}}}}
        assert _dig(obj, "casualties", "killed", "total") == {"value": 42}

    def test_returns_none_on_missing_key(self):
        assert _dig({"casualties": {}}, "casualties", "killed") is None

    def test_returns_none_when_intermediate_is_not_dict(self):
        # A path that runs into a non-dict must yield None, not crash.
        assert _dig({"casualties": "oops"}, "casualties", "killed") is None

    def test_returns_none_for_empty_input(self):
        assert _dig(None, "any", "path") is None
        assert _dig({}, "casualties", "killed") is None

    def test_returns_none_when_final_value_is_none(self):
        # Explicit null in the blob still resolves to None (not the string "None").
        assert _dig({"a": {"b": None}}, "a", "b") is None


class TestNumOrNone:
    """Extract the .value out of a NumericField and coerce to int for
    the denormalised hot columns."""

    def test_extracts_integer_from_numeric_field(self):
        assert _num_or_none({"value": 42000, "unit": "people"}) == 42000

    def test_truncates_floats_to_integer(self):
        # Hot columns are Int?; floats coerce via `int()` which truncates.
        # This matches Postgres's implicit float→int cast semantics.
        assert _num_or_none({"value": 42.7, "unit": "people"}) == 42

    def test_returns_none_when_value_missing(self):
        assert _num_or_none({"unit": "people"}) is None
        assert _num_or_none({"value": None}) is None

    def test_returns_none_for_non_dict_input(self):
        assert _num_or_none(None) is None
        assert _num_or_none(42) is None
        assert _num_or_none("42") is None

    def test_returns_none_for_uncoercible_string(self):
        assert _num_or_none({"value": "not-a-number"}) is None


class TestCollectLocationRefs:
    """The extractor emits LocationRef-shaped dicts nested inside per-
    sector, per-flow, and per-access blobs. `_collect_location_refs`
    walks the tree and pulls them out for de-dup + resolver lookup."""

    def test_collects_from_top_level(self):
        blob = {
            "timing_and_scope": {
                "locations": [
                    {"pcode": "SD01", "name": "Khartoum", "admin_level": 1},
                    {"pcode": "SD02"},
                ],
            },
        }
        refs: list[LocationRef] = []
        _collect_location_refs(blob, refs)
        assert len(refs) == 2
        assert refs[0].pcode == "SD01"
        assert refs[0].name == "Khartoum"
        assert refs[1].pcode == "SD02"
        assert refs[1].name is None

    def test_collects_from_deeply_nested_structures(self):
        # Simulates a Displacement flow with origin/destination refs.
        blob = {
            "displacement": {
                "flows": [
                    {
                        "origin": {"pcode": "SD01"},
                        "destination": {"pcode": "SD02"},
                        "value": {"value": 1000, "confidence": "reported"},
                    },
                ],
            },
        }
        refs: list[LocationRef] = []
        _collect_location_refs(blob, refs)
        pcodes = sorted(r.pcode for r in refs if r.pcode)
        assert pcodes == ["SD01", "SD02"]

    def test_ignores_non_location_dicts(self):
        # NumericField (value/unit/confidence/etc.) has fields foreign
        # to LocationRef — must NOT be misinterpreted as a location.
        blob = {
            "casualties": {
                "killed": {
                    "total": {
                        "value": 42,
                        "unit": "people",
                        "confidence": "reported",
                        "source_quote": "42 killed",
                    },
                },
            },
        }
        refs: list[LocationRef] = []
        _collect_location_refs(blob, refs)
        assert refs == []

    def test_handles_empty_and_none_gracefully(self):
        refs: list[LocationRef] = []
        _collect_location_refs(None, refs)
        _collect_location_refs({}, refs)
        _collect_location_refs([], refs)
        assert refs == []


class TestResolveAllLocations:
    """`_resolve_all_locations` de-dups the collected refs, calls
    clear-api's resolver once per unique tuple, and returns a
    resolved/unresolved split. Errors on individual lookups don't
    abort the batch."""

    def test_dedupes_repeated_refs_before_calling_resolver(self):
        # The same location may appear across many fields in one report.
        # Only ONE clear-api call should fire per unique tuple.
        refs = [
            LocationRef(pcode="SD01"),
            LocationRef(pcode="SD01"),
            LocationRef(pcode="SD01"),
        ]
        with patch(
            "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.resolve_location",
        ) as mock_resolve:
            mock_resolve.return_value = "loc-sd01"
            resolved, unresolved = _resolve_all_locations(refs)
        assert mock_resolve.call_count == 1
        assert resolved == ["loc-sd01"]
        assert unresolved == []

    def test_separates_resolved_and_unresolved(self):
        refs = [LocationRef(pcode="SD01"), LocationRef(pcode="SD-BAD")]
        with patch(
            "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.resolve_location",
        ) as mock_resolve:
            mock_resolve.side_effect = ["loc-sd01", None]
            resolved, unresolved = _resolve_all_locations(refs)
        assert resolved == ["loc-sd01"]
        assert unresolved == ["SD-BAD"]

    def test_transient_error_treated_as_unresolvable(self):
        # A 5xx / network hiccup on ONE lookup shouldn't kill the
        # whole batch — the ref just gets treated as unresolved.
        refs = [LocationRef(pcode="SD01"), LocationRef(pcode="SD02")]
        with patch(
            "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.resolve_location",
        ) as mock_resolve:
            mock_resolve.side_effect = ["loc-sd01", RuntimeError("network blip")]
            resolved, unresolved = _resolve_all_locations(refs)
        assert resolved == ["loc-sd01"]
        # SD02 falls through to unresolved via the exception handler.
        assert unresolved == ["SD02"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
