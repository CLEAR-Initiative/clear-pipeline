"""End-to-end tests for `generate_and_upsert_for_country_year` and
the Dagster `weekly_situation_analyses` asset.

Everything below the SUT is mocked (clear_api, LLM provider). The
tests verify:
  - Country resolution failure → returns None cleanly (no upsert).
  - Aggregated fetch failure → continues with empty datapoints.
  - SITUATION_SKIP_NARRATIVE kill-switch → deterministic-only row.
  - Full happy path → payload has all 7 components, upsert called
    with the right shape.
  - Cascade behaviour on the Dagster asset: iterates all POC
    countries and returns per-country summaries.
"""

import os
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from clear_context_pipeline.defs.situation.generate import (
    generate_and_upsert_for_country_month,
    generate_and_upsert_for_country_year,
    weekly_situation_analyses,
)
from clear_context_pipeline.defs.situation.schemas import (
    HazardsAndVulnerabilities,
    ContextRisks,
    AISummary,
    DisplacementNarrative,
    Sectors,
)


AGGREGATED_STUB = {
    "id": "agg-123",
    "reportCount": 10,
    "dataQualityScore": 0.75,
    "newestSourceAt": "2026-07-10T00:00:00Z",
    "oldestSourceAt": "2026-01-15T00:00:00Z",
    "contributingReportIds": ["r-1", "r-2", "r-3"],
    "data": {
        "idp_stock": {"value": 6_500_000, "unit": "people"},
        "returnees": {"value": 200_000, "unit": "people"},
        "funding_required_usd": {"value": 2_500_000_000, "unit": "USD"},
        "funding_received_usd": {"value": 1_100_000_000, "unit": "USD"},
        "overall_pin": {"value": 25_000_000, "unit": "people"},
    },
}


@pytest.fixture(autouse=True)
def _clean_env():
    # SITUATION_SKIP_NARRATIVE leaks between tests otherwise. Every
    # test starts with the kill-switch off unless it opts in.
    os.environ.pop("SITUATION_SKIP_NARRATIVE", None)
    yield
    os.environ.pop("SITUATION_SKIP_NARRATIVE", None)


def _patch_narrative_generators(stub_source_ids: list[str] | None = None):
    """Common setup: stub every LLM-driven generator to return its
    empty default so tests focus on orchestration, not narrative
    quality. Individual generator tests live in test_situation_narrative
    and test_situation_sectors."""
    src_ids = stub_source_ids or []
    return {
        "generate_ai_summary":
            patch(
                "clear_context_pipeline.defs.situation.generate.generate_ai_summary",
                return_value=AISummary(text="Sample summary", source_report_ids=src_ids),
            ),
        "generate_context_risks":
            patch(
                "clear_context_pipeline.defs.situation.generate.generate_context_risks",
                return_value=ContextRisks(),
            ),
        "generate_hazards_and_vulnerabilities":
            patch(
                "clear_context_pipeline.defs.situation.generate.generate_hazards_and_vulnerabilities",
                return_value=HazardsAndVulnerabilities(),
            ),
        "generate_displacement_narrative":
            patch(
                "clear_context_pipeline.defs.situation.generate.generate_displacement_narrative",
                return_value=DisplacementNarrative(),
            ),
        "generate_all_sectors":
            patch(
                "clear_context_pipeline.defs.situation.generate.generate_all_sectors",
                return_value=Sectors(),
            ),
    }


class TestGenerateAndUpsertForCountryYear:
    def test_country_resolution_failure_returns_none_without_upsert(self):
        # Fresh env — no locations backfilled yet. The generator
        # skips cleanly rather than upserting an orphan row.
        with (
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.resolve_country_location_id",
                return_value=None,
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.upsert_situation_analysis",
            ) as mock_upsert,
        ):
            result = generate_and_upsert_for_country_year(
                country_name="Sudan", year=2026,
            )
        assert result is None
        mock_upsert.assert_not_called()

    def test_aggregated_fetch_failure_still_ships_analysis(self):
        # A hiccup fetching the yearly bucket shouldn't fail the
        # whole analysis — we ship datapoints as their all-null
        # defaults + populate whatever narrative components succeed.
        patches = _patch_narrative_generators()
        with (
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.resolve_country_location_id",
                return_value="sudan-a0",
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.get_aggregated_datapoint",
                side_effect=RuntimeError("clear-api 500"),
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate._fetch_report_meta",
                return_value={},
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.make_llm_provider",
            ) as mock_make_llm,
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.upsert_situation_analysis",
                return_value={
                    "situationAnalysisId": "sit-1",
                    "countryLocationId": "sudan-a0",
                    "supersededPrevious": False,
                },
            ) as mock_upsert,
            patches["generate_ai_summary"],
            patches["generate_context_risks"],
            patches["generate_hazards_and_vulnerabilities"],
            patches["generate_displacement_narrative"],
            patches["generate_all_sectors"],
        ):
            mock_make_llm.return_value.model = "claude-sonnet-4-6"
            result = generate_and_upsert_for_country_year(
                country_name="Sudan", year=2026,
            )
        assert result is not None
        assert result["country_location_id"] == "sudan-a0"
        # Upsert still happens — payload just has all-null datapoints.
        mock_upsert.assert_called_once()
        payload = mock_upsert.call_args.kwargs["data"]
        assert payload["datapoints"]["population_displaced"] is None

    def test_upsert_sends_window_kind_and_a_matchable_window_start(self):
        # REGRESSION GUARD. clear-api keys the bucket on
        # (country, window_kind, window_start, schema_version) and REJECTS
        # a missing window_kind — a row written without it is a row no
        # reader can find.
        #
        # window_start must be exactly midnight Jan 1 UTC: clear-api's
        # `calendarYearStart` derives the same instant independently, and
        # the read matches on equality. window_end is deliberately NOT
        # matched on — this side sends 23:59:59.000 and the TS side used
        # to look for 23:59:59.999, which silently found nothing.
        patches = _patch_narrative_generators()
        with (
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.resolve_country_location_id",
                return_value="sudan-a0",
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.get_aggregated_datapoint",
                return_value=None,
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate._fetch_report_meta",
                return_value={},
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.make_llm_provider",
            ),
            patches["generate_ai_summary"],
            patches["generate_context_risks"],
            patches["generate_hazards_and_vulnerabilities"],
            patches["generate_displacement_narrative"],
            patches["generate_all_sectors"],
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.upsert_situation_analysis",
            ) as mock_upsert,
        ):
            generate_and_upsert_for_country_year(country_name="Sudan", year=2026)

        kwargs = mock_upsert.call_args.kwargs
        assert kwargs["window_kind"] == "yearly"
        assert kwargs["window_start"] == "2026-01-01T00:00:00+00:00"

    def test_month_wrapper_sends_monthly_window_kind_and_first_of_month(self):
        # The monthly wrapper reads/writes the monthly-A0 bucket. window_kind
        # must be "monthly" and window_start exactly midnight on the 1st —
        # matching clear-api's `monthOf` start (the equality the read + the
        # invalidation cascade both key on).
        patches = _patch_narrative_generators()
        with (
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.resolve_country_location_id",
                return_value="sudan-a0",
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.get_aggregated_datapoint",
                return_value=None,
            ) as mock_agg,
            patch(
                "clear_context_pipeline.defs.situation.generate._fetch_report_meta",
                return_value={},
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.make_llm_provider",
            ),
            patches["generate_ai_summary"],
            patches["generate_context_risks"],
            patches["generate_hazards_and_vulnerabilities"],
            patches["generate_displacement_narrative"],
            patches["generate_all_sectors"],
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.upsert_situation_analysis",
            ) as mock_upsert,
        ):
            generate_and_upsert_for_country_month(
                country_name="Sudan", year=2026, month=7,
            )

        # Reads the monthly bucket for the same window.
        assert mock_agg.call_args.kwargs["window_kind"] == "monthly"
        assert mock_agg.call_args.kwargs["window_start"] == "2026-07-01T00:00:00+00:00"
        # Writes it back with the monthly window_kind + start.
        kwargs = mock_upsert.call_args.kwargs
        assert kwargs["window_kind"] == "monthly"
        assert kwargs["window_start"] == "2026-07-01T00:00:00+00:00"

    def test_skip_narrative_kill_switch_ships_deterministic_only(self):
        # SITUATION_SKIP_NARRATIVE=1 → no LLM calls, no narrative
        # components. `generated_by_model` marks the row as
        # deterministic-only so an audit can spot and re-run it.
        os.environ["SITUATION_SKIP_NARRATIVE"] = "1"
        with (
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.resolve_country_location_id",
                return_value="sudan-a0",
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.get_aggregated_datapoint",
                return_value=AGGREGATED_STUB,
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate._fetch_report_meta",
                return_value={},
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.make_llm_provider",
            ) as mock_make_llm,
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.upsert_situation_analysis",
                return_value={
                    "situationAnalysisId": "sit-2",
                    "countryLocationId": "sudan-a0",
                    "supersededPrevious": True,
                },
            ) as mock_upsert,
        ):
            result = generate_and_upsert_for_country_year(
                country_name="Sudan", year=2026,
            )
        # No LLM provider constructed when the switch is on.
        mock_make_llm.assert_not_called()
        assert result is not None
        assert result["generated_by_model"].startswith("deterministic:")
        # Deterministic components ARE populated (datapoints, sources).
        payload = mock_upsert.call_args.kwargs["data"]
        assert payload["datapoints"]["population_displaced"] == 6_500_000.0
        # Narrative components stay at empty defaults.
        assert payload["ai_summary"]["text"] == ""
        assert payload["hazards_and_vulnerabilities"]["hazards"] == []

    def test_happy_path_ships_all_seven_components_with_dedupe_source_ids(self):
        # Full flow: datapoints from aggregated, narrative from stubs,
        # sources chronologically ordered, source_report_ids union'd
        # across everything without duplicates.
        report_meta = {
            "r-1": {"reportTitle": "Report 1", "sourceUrl": "https://…/1", "publishedAt": "2026-01-15"},
            "r-2": {"reportTitle": "Report 2", "sourceUrl": "https://…/2", "publishedAt": "2026-07-01"},
            "r-3": {"reportTitle": "Report 3", "sourceUrl": "https://…/3", "publishedAt": "2026-04-20"},
        }
        # AI summary reports r-4 (new source not in aggregated) —
        # exercises the dedupe-preserving-order path.
        patches = _patch_narrative_generators(stub_source_ids=["r-2", "r-4"])
        with (
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.resolve_country_location_id",
                return_value="sudan-a0",
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.get_aggregated_datapoint",
                return_value=AGGREGATED_STUB,
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate._fetch_report_meta",
                return_value=report_meta,
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.make_llm_provider",
            ) as mock_make_llm,
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.upsert_situation_analysis",
                return_value={
                    "situationAnalysisId": "sit-3",
                    "countryLocationId": "sudan-a0",
                    "supersededPrevious": False,
                },
            ) as mock_upsert,
            patches["generate_ai_summary"],
            patches["generate_context_risks"],
            patches["generate_hazards_and_vulnerabilities"],
            patches["generate_displacement_narrative"],
            patches["generate_all_sectors"],
        ):
            mock_make_llm.return_value.model = "claude-sonnet-4-6"
            result = generate_and_upsert_for_country_year(
                country_name="Sudan", year=2026,
            )

        mock_upsert.assert_called_once()
        kwargs = mock_upsert.call_args.kwargs

        # 1. All 7 component keys present in payload.
        payload = kwargs["data"]
        assert set(payload.keys()) == {
            "datapoints", "ai_summary", "context_risks",
            "hazards_and_vulnerabilities", "displacement", "sectors", "sources",
        }

        # 2. Datapoints hoisted correctly.
        assert payload["datapoints"]["population_displaced"] == 6_500_000.0
        assert payload["datapoints"]["envelope"]["report_count"] == 10

        # 3. Sources chronological (r-2 July → r-3 April → r-1 January).
        report_ids_in_order = [s["report_id"] for s in payload["sources"]["reports"]]
        assert report_ids_in_order == ["r-2", "r-3", "r-1"]

        # 4. source_report_ids denormalised UNION, deduped first-seen.
        # aggregated contributes ["r-1", "r-2", "r-3"]; AI summary
        # contributes ["r-2", "r-4"] → r-4 appended at end, no r-2 dup.
        assert kwargs["source_report_ids"] == ["r-1", "r-2", "r-3", "r-4"]

        # 5. aggregated_datapoint_id linkage recorded.
        assert kwargs["aggregated_datapoint_id"] == "agg-123"
        # 6. Model marker reflects the actual LLM.
        assert kwargs["generated_by_model"] == "claude-sonnet-4-6"

        # Summary carries the identity + counts back to the caller.
        assert result["situation_analysis_id"] == "sit-3"
        assert result["superseded_previous"] is False

    def test_upsert_clear_api_error_returns_none_without_crashing(self):
        # 4xx from clear-api on the situation upsert — bad payload
        # shape. Log, return None, don't retry.
        patches = _patch_narrative_generators()
        from clear_context_pipeline.providers.clear_api import ClearApiError
        with (
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.resolve_country_location_id",
                return_value="sudan-a0",
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.get_aggregated_datapoint",
                return_value=AGGREGATED_STUB,
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate._fetch_report_meta",
                return_value={},
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.make_llm_provider",
            ) as mock_make_llm,
            patch(
                "clear_context_pipeline.defs.situation.generate.clear_api.upsert_situation_analysis",
                side_effect=ClearApiError("400 bad request"),
            ),
            patches["generate_ai_summary"],
            patches["generate_context_risks"],
            patches["generate_hazards_and_vulnerabilities"],
            patches["generate_displacement_narrative"],
            patches["generate_all_sectors"],
        ):
            mock_make_llm.return_value.model = "claude-sonnet-4-6"
            result = generate_and_upsert_for_country_year(
                country_name="Sudan", year=2026,
            )
        assert result is None


class TestWeeklySituationAnalysesAsset:
    """The asset is a thin loop over `_POC_COUNTRIES` calling the
    helper. These tests exercise the loop shape, not the helper (the
    helper has its own class above)."""

    def test_iterates_all_poc_countries(self):
        # POC scope is Sudan only. The asset now writes TWO snapshots per
        # country — yearly + current-month — so Sudan yields two calls.
        with (
            patch(
                "clear_context_pipeline.defs.situation.generate.generate_and_upsert_for_country_year",
            ) as mock_year,
            patch(
                "clear_context_pipeline.defs.situation.generate.generate_and_upsert_for_country_month",
            ) as mock_month,
        ):
            mock_year.return_value = {
                "country_name": "Sudan", "window_kind": "yearly",
                "situation_analysis_id": "sit-year",
            }
            mock_month.return_value = {
                "country_name": "Sudan", "window_kind": "monthly",
                "situation_analysis_id": "sit-month",
            }
            ctx = dg.build_asset_context()
            result = weekly_situation_analyses(
                ctx, reliefweb_weekly_datapoint_aggregations={},
            )
        # Sudan-only for POC → one yearly + one monthly call.
        assert mock_year.call_count == 1
        assert mock_month.call_count == 1
        assert mock_year.call_args.kwargs["country_name"] == "Sudan"
        assert mock_month.call_args.kwargs["country_name"] == "Sudan"
        assert {r["situation_analysis_id"] for r in result} == {"sit-year", "sit-month"}

    def test_helper_returning_none_is_dropped_from_summaries(self):
        # Both helpers return None (e.g. country resolver failed). The asset
        # keeps going and omits the country from the summary list.
        with (
            patch(
                "clear_context_pipeline.defs.situation.generate.generate_and_upsert_for_country_year",
                return_value=None,
            ),
            patch(
                "clear_context_pipeline.defs.situation.generate.generate_and_upsert_for_country_month",
                return_value=None,
            ),
        ):
            ctx = dg.build_asset_context()
            result = weekly_situation_analyses(
                ctx, reliefweb_weekly_datapoint_aggregations={},
            )
        assert result == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
