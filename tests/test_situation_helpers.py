"""Tests for the deterministic helpers in the situation-analysis path.

Covers:
  - `_field_value`, `_dig` shims that hoist QualityEnvelope values out
    of an aggregated-datapoints blob.
  - `_build_datapoints` — Component 1's deterministic assembly.
  - `_build_sources` — Component 7's chronological source ordering.
  - `fetch_rag_context` — RAG search wrapper: dedup, formatting,
    error → empty fallback.
  - `_format_hits_for_prompt` — the LLM-facing string shape.

Everything here is pure Python + clear_api mocks; no Dagster, no LLM.
"""

from unittest.mock import patch

import pytest

from clear_context_pipeline.defs.situation.generate import (
    _build_datapoints,
    _build_sources,
    _calendar_year_window,
    _fetch_report_meta,
    _field_value,
)
from clear_context_pipeline.defs.situation.rag_helper import (
    _format_hits_for_prompt,
    _pages_range,
    fetch_rag_context,
)
from clear_context_pipeline.defs.situation.schemas import Source


# ────────────────────────────────────────────────────────────────────
# _field_value — safe QualityEnvelope value extraction
# ────────────────────────────────────────────────────────────────────


class TestFieldValue:
    def test_extracts_numeric_value(self):
        data = {"idp_stock": {"value": 45000, "unit": "people"}}
        assert _field_value(data, "idp_stock") == 45000.0

    def test_coerces_int_to_float(self):
        # Value should ALWAYS be float — the caller decides truncation.
        assert _field_value({"x": {"value": 42}}, "x") == 42.0
        assert isinstance(_field_value({"x": {"value": 42}}, "x"), float)

    def test_returns_none_for_missing_field(self):
        assert _field_value({"other": {"value": 5}}, "idp_stock") is None

    def test_returns_none_for_non_dict_field(self):
        # Set-union fields carry {"values": [...]} not {"value": N}.
        # A caller mistakenly reading them as numeric must get None.
        assert _field_value({"event_types": {"values": ["conflict"]}}, "event_types") is None

    def test_returns_none_when_value_is_null(self):
        assert _field_value({"x": {"value": None}}, "x") is None

    def test_returns_none_for_uncoercible_value(self):
        assert _field_value({"x": {"value": "not-a-number"}}, "x") is None


# ────────────────────────────────────────────────────────────────────
# _calendar_year_window — Jan 1 → Dec 31 UTC
# ────────────────────────────────────────────────────────────────────


class TestCalendarYearWindow:
    def test_returns_jan1_dec31_utc(self):
        start, end = _calendar_year_window(2026)
        assert start.startswith("2026-01-01T00:00:00")
        assert end.startswith("2026-12-31T23:59:59")

    def test_handles_leap_year(self):
        # Feb 29 exists in 2024; the window still ends on Dec 31.
        start, end = _calendar_year_window(2024)
        assert start.startswith("2024-01-01")
        assert end.startswith("2024-12-31")


# ────────────────────────────────────────────────────────────────────
# _build_datapoints — hoists from aggregated_datapoints
# ────────────────────────────────────────────────────────────────────


class TestBuildDatapoints:
    def test_null_aggregated_returns_empty_defaults(self):
        # A country-year with zero ingested reports → all-null values
        # + empty envelope. Dashboard renders "no data yet".
        dp = _build_datapoints(None)
        assert dp.population_displaced is None
        assert dp.population_in_need is None
        assert dp.population_affected is None
        assert dp.returnees is None
        assert dp.number_of_events == 0
        assert dp.funding_required_usd is None
        assert dp.funding_received_usd is None
        assert dp.envelope.report_count is None

    def test_hoists_all_six_headline_numbers(self):
        aggregated = {
            "dataQualityScore": 0.85,
            "newestSourceAt": "2026-07-10T00:00:00Z",
            "oldestSourceAt": "2026-01-15T00:00:00Z",
            "reportCount": 42,
            "data": {
                "idp_stock":            {"value": 6_500_000, "unit": "people"},
                "returnee_stock":       {"value": 200_000,   "unit": "people"},
                "funding_required_usd": {"value": 2_500_000_000, "unit": "USD"},
                "funding_received_usd": {"value": 1_100_000_000, "unit": "USD"},
                "overall_pin":          {"value": 25_000_000,     "unit": "people"},
                "overall_affected":     {"value": 30_000_000,     "unit": "people"},
            },
        }
        dp = _build_datapoints(aggregated)
        assert dp.population_displaced == 6_500_000.0
        assert dp.population_in_need == 25_000_000.0  # from overall_pin
        # Population Affected is a distinct, wider figure than PIN.
        assert dp.population_affected == 30_000_000.0  # from overall_affected
        assert dp.returnees == 200_000.0
        assert dp.funding_required_usd == 2_500_000_000.0
        assert dp.funding_received_usd == 1_100_000_000.0
        # number_of_events is currently a proxy for reportCount — the
        # doc comment on the helper explains this approximation.
        assert dp.number_of_events == 42
        assert dp.envelope.quality_score == 0.85
        assert dp.envelope.report_count == 42

    def test_null_within_data_blob_produces_null_datapoint(self):
        # Some fields present, others missing. Missing ones stay null;
        # do NOT default to zero (would poison the dashboard).
        aggregated = {
            "reportCount": 5,
            "data": {"idp_stock": {"value": 100000}},
        }
        dp = _build_datapoints(aggregated)
        assert dp.population_displaced == 100000.0
        assert dp.population_in_need is None
        assert dp.population_affected is None
        assert dp.returnees is None
        assert dp.funding_required_usd is None


# ────────────────────────────────────────────────────────────────────
# _build_sources — chronological ordering
# ────────────────────────────────────────────────────────────────────


class TestBuildSources:
    def test_sorts_newest_first(self):
        report_meta = {
            "old-report": {
                "reportTitle": "January sitrep",
                "sourceUrl": "https://reliefweb.int/old",
                "publishedAt": "2026-01-15T00:00:00Z",
            },
            "new-report": {
                "reportTitle": "July sitrep",
                "sourceUrl": "https://reliefweb.int/new",
                "publishedAt": "2026-07-10T00:00:00Z",
            },
            "mid-report": {
                "reportTitle": "April sitrep",
                "sourceUrl": "https://reliefweb.int/mid",
                "publishedAt": "2026-04-20T00:00:00Z",
            },
        }
        sources = _build_sources(
            ["old-report", "new-report", "mid-report"], report_meta,
        )
        assert [s.report_id for s in sources.reports] == [
            "new-report", "mid-report", "old-report",
        ]

    def test_falls_back_to_report_id_when_meta_missing(self):
        # Reports may have contributed to aggregation but not yet have
        # a report_datapoints row (fresh pipeline race). Ensure the
        # sources tab still shows a row rather than dropping the id.
        sources = _build_sources(["unknown-report"], {})
        assert len(sources.reports) == 1
        assert sources.reports[0].report_id == "unknown-report"
        assert sources.reports[0].report_title == "unknown-report"
        assert sources.reports[0].source_url == ""

    def test_empty_input_returns_empty_sources(self):
        sources = _build_sources([], {})
        assert sources.reports == []

    def test_undated_reports_sort_to_bottom(self):
        # Empty publishedAt is falsy → sorts below any ISO date.
        report_meta = {
            "dated": {"reportTitle": "A", "sourceUrl": "", "publishedAt": "2026-07-10"},
            "undated": {"reportTitle": "B", "sourceUrl": "", "publishedAt": ""},
        }
        sources = _build_sources(["undated", "dated"], report_meta)
        assert sources.reports[0].report_id == "dated"
        assert sources.reports[1].report_id == "undated"


# ────────────────────────────────────────────────────────────────────
# _fetch_report_meta — batch metadata lookup
# ────────────────────────────────────────────────────────────────────


class TestFetchReportMeta:
    def test_returns_empty_on_no_ids(self):
        # No GraphQL calls for the empty case.
        with patch(
            "clear_context_pipeline.providers.clear_api._execute",
        ) as mock_execute:
            result = _fetch_report_meta([])
        assert result == {}
        mock_execute.assert_not_called()

    def test_isolates_per_id_failures(self):
        # One bad lookup mustn't drop the whole batch. Result should
        # include the successful ones + omit the failed.
        def side_effect(query, variables):
            rid = variables["id"]
            if rid == "bad":
                raise RuntimeError("simulated 500")
            return {
                "reportDatapoint": {
                    "reportTitle": f"Report {rid}",
                    "sourceUrl": f"https://example.com/{rid}",
                    "publishedAt": "2026-07-01",
                },
            }
        with patch(
            "clear_context_pipeline.providers.clear_api._execute",
            side_effect=side_effect,
        ):
            result = _fetch_report_meta(["good", "bad", "another"])
        assert set(result.keys()) == {"good", "another"}
        assert result["good"]["reportTitle"] == "Report good"


# ────────────────────────────────────────────────────────────────────
# fetch_rag_context — RAG search wrapper
# ────────────────────────────────────────────────────────────────────


class TestFetchRagContext:
    def test_empty_hits_returns_empty_context(self):
        with patch(
            "clear_context_pipeline.defs.situation.rag_helper.clear_api.search_knowledgebase",
            return_value=[],
        ):
            ctx = fetch_rag_context(query="anything")
        assert ctx.is_empty
        assert ctx.hit_count == 0
        assert ctx.formatted_for_prompt == ""

    def test_dedups_report_ids_preserving_rrf_order(self):
        # RRF returns hits ranked by relevance; a report contributing
        # multiple chunks should collapse to one id in FIRST-SEEN order.
        hits = [
            {"reportId": "r-1", "reportTitle": "T1", "publishedAt": "2026-01", "pageStart": 1, "pageEnd": 1, "chunkText": "a"},
            {"reportId": "r-2", "reportTitle": "T2", "publishedAt": "2026-02", "pageStart": 1, "pageEnd": 1, "chunkText": "b"},
            {"reportId": "r-1", "reportTitle": "T1 again", "publishedAt": "2026-01", "pageStart": 5, "pageEnd": 5, "chunkText": "c"},
        ]
        with patch(
            "clear_context_pipeline.defs.situation.rag_helper.clear_api.search_knowledgebase",
            return_value=hits,
        ):
            ctx = fetch_rag_context(query="anything")
        assert ctx.contributing_report_ids == ["r-1", "r-2"]
        assert ctx.hit_count == 3

    def test_search_failure_degrades_to_empty(self):
        # A transient search error mustn't kill the whole situation
        # analysis — the caller degrades to "no evidence for this
        # component" and moves on.
        with patch(
            "clear_context_pipeline.defs.situation.rag_helper.clear_api.search_knowledgebase",
            side_effect=RuntimeError("clear-api 500"),
        ):
            ctx = fetch_rag_context(query="anything")
        assert ctx.is_empty
        assert ctx.contributing_report_ids == []

    def test_forwards_filters_to_underlying_search(self):
        with patch(
            "clear_context_pipeline.defs.situation.rag_helper.clear_api.search_knowledgebase",
        ) as mock_search:
            mock_search.return_value = []
            fetch_rag_context(
                query="Sudan health",
                filters={"needSectors": ["Health"]},
                limit=5,
            )
        call = mock_search.call_args
        assert call.kwargs["filters"] == {"needSectors": ["Health"]}
        assert call.kwargs["limit"] == 5


# ────────────────────────────────────────────────────────────────────
# _format_hits_for_prompt / _pages_range — LLM-facing formatting
# ────────────────────────────────────────────────────────────────────


class TestFormatHitsForPrompt:
    def test_numbers_hits_from_r1(self):
        # [R1], [R2], ... — the marker scheme downstream citation
        # refinement (Phase E) will target. Even without citations,
        # the numbering makes prompts readable in logs.
        formatted = _format_hits_for_prompt([
            {"reportId": "a", "reportTitle": "First", "publishedAt": "2026-01-01",
             "pageStart": 1, "pageEnd": 1, "chunkText": "alpha"},
            {"reportId": "b", "reportTitle": "Second", "publishedAt": "2026-02-01",
             "pageStart": 2, "pageEnd": 3, "chunkText": "beta"},
        ])
        assert "[R1]" in formatted
        assert "[R2]" in formatted
        assert "First" in formatted
        assert "alpha" in formatted

    def test_pages_range_single_page(self):
        assert _pages_range({"pageStart": 5, "pageEnd": 5}) == " (p.5)"

    def test_pages_range_multi_page(self):
        assert _pages_range({"pageStart": 3, "pageEnd": 7}) == " (pp.3–7)"

    def test_pages_range_missing_page(self):
        # Missing pageStart → no citation suffix. Dashboard falls
        # back to report-level citation.
        assert _pages_range({}) == ""

    def test_source_class_construction(self):
        # Guards the shape the sources component ships to the dashboard.
        s = Source(
            report_id="r1", report_title="Sudan sitrep",
            source_url="https://…", published_at="2026-07-01",
        )
        assert s.model_dump()["published_at"] == "2026-07-01"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
