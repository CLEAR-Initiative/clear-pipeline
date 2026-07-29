"""Tests for the pure helper functions in datapoints_extract.py.

Extraction assets themselves are integration-heavy (S3 + LLM +
clear-api). These tests target the helpers that do the deterministic
work in isolation: nested-dict walking, path lookup, numeric coercion,
and the location-ref collection routine that feeds the resolver.
"""

from unittest.mock import patch

import pytest

from clear_context_pipeline.defs.knowledgebase.datapoints_extract import (
    _backfill_chunk_indices,
    _collect_location_refs,
    _collect_numeric_fields,
    _dig,
    _match_chunk_index,
    _num_or_none,
    _resolve_all_locations,
    _resolve_figure_scopes,
)
from clear_context_pipeline.defs.knowledgebase.datapoints_schemas import (
    Casualties,
    CasualtyDisaggregation,
    LocationRef,
    NumericField,
)


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


RESOLVE = "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.resolve_location"


def _nf(value, scope=None):
    """A NumericField dict as it appears in the merged blob (post
    model_dump), optionally with an LLM-emitted scope name."""
    return NumericField(
        value=value, unit="people", confidence="reported",
        source_quote="…", scope_location_name=scope,
    ).model_dump(mode="json")


class TestCollectNumericFields:
    """The scope resolver keys off finding every NumericField in the
    nested blob. Missing one means that figure never gets a scope id and
    silently drops out of location roll-ups."""

    def test_finds_fields_nested_in_domains(self):
        blob = {
            "casualties": {"killed": {"total": _nf(10, "El Fasher")}},
            "displacement": {"idp_stock": _nf(5000, "Kordofan")},
        }
        out: list[dict] = []
        _collect_numeric_fields(blob, out)
        assert len(out) == 2

    def test_finds_fields_inside_lists(self):
        blob = {"access": [{"incidents": _nf(3, "Nyala")}, {"incidents": _nf(4)}]}
        out: list[dict] = []
        _collect_numeric_fields(blob, out)
        assert len(out) == 2

    def test_ignores_non_numeric_dicts(self):
        # A TextField has no scope_location_name key, so it isn't picked up.
        blob = {"note": {"value": "some text", "confidence": "reported"}}
        out: list[dict] = []
        _collect_numeric_fields(blob, out)
        assert out == []


class TestResolveFigureScopes:
    """Figure Scope (schema v2): map each figure's scope_location_name to
    a locations id, in place. A wrong or missing id sends the figure to
    the wrong bucket or drops it — the exact failures ADR-0002 exists to
    prevent, so the resolution contract is worth pinning."""

    def test_resolves_name_to_id_in_place(self):
        blob = {"casualties": {"killed": {"total": _nf(10, "El Fasher")}}}
        with patch(RESOLVE, return_value="loc-elfasher") as m:
            figures, named, resolved = _resolve_figure_scopes(blob)
        assert blob["casualties"]["killed"]["total"]["scope_location_id"] == "loc-elfasher"
        assert (figures, named, resolved) == (1, 1, 1)
        m.assert_called_once_with(name="El Fasher")

    def test_null_name_stays_unscoped(self):
        # LLM abstained (no place) → id null, resolver never called.
        blob = {"casualties": {"killed": {"total": _nf(10, None)}}}
        with patch(RESOLVE) as m:
            figures, named, resolved = _resolve_figure_scopes(blob)
        assert blob["casualties"]["killed"]["total"]["scope_location_id"] is None
        assert (figures, named, resolved) == (1, 0, 0)
        m.assert_not_called()

    def test_unmatched_name_leaves_null_id(self):
        # Resolver can't match the name → id null, but it WAS named (so the
        # resolver-match-rate metric separates this from abstention).
        blob = {"displacement": {"idp_stock": _nf(5000, "Nowhereville")}}
        with patch(RESOLVE, return_value=None):
            figures, named, resolved = _resolve_figure_scopes(blob)
        assert blob["displacement"]["idp_stock"]["scope_location_id"] is None
        assert (figures, named, resolved) == (1, 1, 0)

    def test_llm_supplied_id_is_overwritten(self):
        # The LLM must not supply scope_location_id — a hallucinated one is
        # unconditionally replaced by the resolver's answer.
        f = _nf(10, "Kordofan")
        f["scope_location_id"] = "hallucinated-id"
        blob = {"casualties": {"killed": {"total": f}}}
        with patch(RESOLVE, return_value="loc-kordofan"):
            _resolve_figure_scopes(blob)
        assert blob["casualties"]["killed"]["total"]["scope_location_id"] == "loc-kordofan"

    def test_repeated_name_resolved_once(self):
        # Two figures scoped to the same place → one clear-api hit.
        blob = {
            "a": {"x": _nf(1, "Kassala")},
            "b": {"y": _nf(2, "Kassala")},
        }
        with patch(RESOLVE, return_value="loc-kassala") as m:
            figures, named, resolved = _resolve_figure_scopes(blob)
        assert (figures, named, resolved) == (2, 2, 2)
        assert m.call_count == 1  # deduped

    def test_resolver_error_treated_as_unscoped(self):
        blob = {"casualties": {"killed": {"total": _nf(10, "El Fasher")}}}
        with patch(RESOLVE, side_effect=RuntimeError("network blip")):
            figures, named, resolved = _resolve_figure_scopes(blob)
        assert blob["casualties"]["killed"]["total"]["scope_location_id"] is None
        assert (figures, named, resolved) == (1, 1, 0)

    def test_real_schema_shape_round_trips(self):
        # Guards against the collector missing a genuinely-constructed
        # NumericField (not a hand-built dict) after model_dump.
        cas = Casualties(killed=CasualtyDisaggregation(
            total=NumericField(value=8, unit="people", confidence="verified",
                               source_quote="…", scope_location_name="Zalingei"),
        )).model_dump(mode="json")
        blob = {"casualties": cas}
        with patch(RESOLVE, return_value="loc-zalingei"):
            figures, named, resolved = _resolve_figure_scopes(blob)
        assert (figures, named, resolved) == (1, 1, 1)
        assert blob["casualties"]["killed"]["total"]["scope_location_id"] == "loc-zalingei"


class TestNumericFieldScopeSchema:
    def test_scope_fields_default_null_and_are_present(self):
        d = NumericField(value=1, unit="people", confidence="reported",
                         source_quote="…").model_dump()
        assert d["scope_location_name"] is None
        assert d["scope_location_id"] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


# ── chunk_index backfill ──────────────────────────────────────────────

_CHUNKS = [
    {"chunk_index": 0, "page_start": 1, "page_end": 2, "text": "Cover page and table of contents. NRC situation report."},
    {"chunk_index": 1, "page_start": 2, "page_end": 3, "text": "In North Kordofan, 42,000 people were newly displaced during the reporting period."},
    {"chunk_index": 2, "page_start": 3, "page_end": 4, "text": "Health facilities in El Fasher were damaged; 3 people were killed in the attack."},
]


def test_match_chunk_index_exact_substring():
    idx = _match_chunk_index(
        "42,000 people were newly displaced", page_number=2, chunks=_CHUNKS,
    )
    assert idx == 1


def test_match_chunk_index_page_narrows_ambiguity():
    # The quote text also loosely resembles chunk 2, but page_number=3 keeps
    # both 1 and 2 in range; exact substring still resolves to chunk 2.
    idx = _match_chunk_index(
        "3 people were killed in the attack", page_number=4, chunks=_CHUNKS,
    )
    assert idx == 2


def test_match_chunk_index_fuzzy_when_no_exact():
    # Slightly paraphrased/whitespace-mangled quote → fuzzy longest-block match.
    idx = _match_chunk_index(
        "42,000  people   were newly   displaced during", page_number=None, chunks=_CHUNKS,
    )
    assert idx == 1


def test_match_chunk_index_no_match_returns_none():
    assert _match_chunk_index(
        "completely unrelated sentence about funding appeals", page_number=9, chunks=_CHUNKS,
    ) is None
    assert _match_chunk_index("", page_number=1, chunks=_CHUNKS) is None


def test_backfill_chunk_indices_walks_and_overwrites():
    merged = {
        "displacement": {
            "new_displacements": {
                "value": 42000, "unit": "people", "confidence": "reported",
                "source_quote": "42,000 people were newly displaced",
                "page_number": 2, "chunk_index": 99,  # LLM guess — must be overwritten
                "scope_location_name": "North Kordofan",
            },
        },
        "casualties": {
            "killed": {
                "total": {
                    "value": 3, "unit": "people", "confidence": "verified",
                    "source_quote": "3 people were killed in the attack",
                    "page_number": 4, "chunk_index": None,
                    "scope_location_name": "El Fasher",
                },
            },
        },
    }
    with_quote, matched = _backfill_chunk_indices(merged, _CHUNKS)
    assert (with_quote, matched) == (2, 2)
    assert merged["displacement"]["new_displacements"]["chunk_index"] == 1
    assert merged["casualties"]["killed"]["total"]["chunk_index"] == 2


def test_backfill_chunk_indices_nulls_unmatched_and_quoteless():
    merged = {
        "needs_and_funding": {
            "overall_affected": {
                "value": 1, "unit": "people", "confidence": "media",
                "source_quote": "a sentence that appears in no chunk at all",
                "page_number": 1, "chunk_index": 7,
                "scope_location_name": "Sudan",
            },
        },
        "displacement": {
            "idp_stock": {
                "value": 5, "unit": "people", "confidence": "reported",
                "source_quote": "", "page_number": None, "chunk_index": 4,
                "scope_location_name": None,
            },
        },
    }
    with_quote, matched = _backfill_chunk_indices(merged, _CHUNKS)
    assert (with_quote, matched) == (1, 0)  # only the affected figure had a quote; it didn't match
    assert merged["needs_and_funding"]["overall_affected"]["chunk_index"] is None
    assert merged["displacement"]["idp_stock"]["chunk_index"] is None
