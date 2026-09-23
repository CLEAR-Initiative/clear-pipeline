"""Unit tests for the analysis Frame (ADR-0007) — canonicalisation, identity,
and the frame -> KnowledgebaseFilters mapping. Pure logic, no I/O."""

from clear_pipeline.defs.situation.frame import (
    Frame,
    build_rag_filters,
    country_frame,
)


class TestCanonicalisation:
    def test_arrays_sorted_deduped_and_emptied(self):
        f = Frame.build(
            window_start="2026-01-01",
            location_ids=["b", "a", "b", ""],
            event_types=["FL", "EQ", "EQ"],
            need_sectors=[],
        )
        assert f.location_ids == ("a", "b")   # sorted, de-duped, "" dropped
        assert f.event_types == ("EQ", "FL")
        assert f.need_sectors == ()

    def test_order_insensitive_identity(self):
        a = Frame.build(window_start="2026-01-01", location_ids=["x", "y"])
        b = Frame.build(window_start="2026-01-01", location_ids=["y", "x"])
        assert a == b            # frames are equal regardless of input order
        assert hash(a) == hash(b)

    def test_window_end_none_is_rolling(self):
        f = Frame.build(window_start="2026-01-01", window_end=None)
        assert f.window_end is None
        assert f.upsert_kwargs()["window_end"] is None


class TestCountryDefault:
    def test_single_location_no_narrowing_is_country_default(self):
        assert country_frame("sudan-a0", window_start="2026-01-01", window_end="2026-12-31").is_country_default

    def test_event_types_make_it_not_country_default(self):
        f = Frame.build(window_start="2026-01-01", location_ids=["sudan-a0"], event_types=["FL"])
        assert not f.is_country_default

    def test_multiple_locations_not_country_default(self):
        f = Frame.build(window_start="2026-01-01", location_ids=["a", "b"])
        assert not f.is_country_default


class TestUpsertKwargs:
    def test_shape_matches_provider(self):
        f = Frame.build(
            window_start="2026-03-01",
            window_end="2026-03-31",
            location_ids=["l1"],
            event_types=["FL"],
            need_sectors=["health"],
        )
        assert f.upsert_kwargs() == {
            "location_ids": ["l1"],
            "event_types": ["FL"],
            "need_sectors": ["health"],
            "window_start": "2026-03-01",
            "window_end": "2026-03-31",
        }


class TestBuildRagFilters:
    def test_country_scope_uses_country_location_id_not_literal_locations(self):
        f = country_frame("sudan-a0", window_start="2026-01-01", window_end="2026-12-31")
        filters = build_rag_filters(f, country_scope_id="sudan-a0")
        assert filters == {"countryLocationId": "sudan-a0"}
        assert "locationIds" not in filters  # subtree expansion, not literal match

    def test_single_location_frame_expands_to_subtree(self):
        # A single-location custom frame (e.g. a country-level analysis) scopes by
        # its subtree via countryLocationId — a literal locationIds=[A0] would miss
        # the admin-2-tagged chunks.
        f = Frame.build(window_start="2026-01-01", location_ids=["sudan-a0"])
        assert build_rag_filters(f) == {"countryLocationId": "sudan-a0"}

    def test_multi_location_frame_uses_location_ids(self):
        f = Frame.build(window_start="2026-01-01", location_ids=["khartoum", "darfur"])
        filters = build_rag_filters(f)
        assert filters == {"locationIds": ["darfur", "khartoum"]}  # canonical order

    def test_event_types_and_need_sectors_pass_through(self):
        f = Frame.build(
            window_start="2026-01-01",
            location_ids=["l1"],
            event_types=["FL"],
            need_sectors=["health", "wash"],
        )
        filters = build_rag_filters(f)
        assert filters["eventTypes"] == ["FL"]
        assert filters["needSectors"] == ["health", "wash"]

    def test_time_range_off_by_default(self):
        f = Frame.build(window_start="2026-01-01", window_end="2026-12-31", location_ids=["l1"])
        assert "timeRange" not in build_rag_filters(f)

    def test_time_range_included_when_requested(self):
        f = Frame.build(window_start="2026-01-01", window_end="2026-12-31", location_ids=["l1"])
        filters = build_rag_filters(f, include_time_range=True)
        assert filters["timeRange"] == {"from": "2026-01-01", "to": "2026-12-31"}

    def test_time_range_rolling_has_no_to(self):
        f = Frame.build(window_start="2026-01-01", window_end=None, location_ids=["l1"])
        filters = build_rag_filters(f, include_time_range=True)
        assert filters["timeRange"] == {"from": "2026-01-01"}

    def test_empty_frame_yields_no_filters(self):
        f = Frame.build(window_start="2026-01-01")
        assert build_rag_filters(f) is None
