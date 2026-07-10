"""Tests for reliefweb_weekly_datapoint_aggregations — the asset that
triggers clear-api's four-tier refresh.

The asset itself is a thin coordinator: check first-run state → pick
window → call refresh mutation → surface the result. Tests cover the
window-selection branches and the failure-mode fallbacks that matter
for correctness (existence-check hiccups must not silently
under-refresh).
"""

import os
from unittest.mock import patch

import dagster as dg
import pytest

from clear_context_pipeline.defs.knowledgebase.datapoints_aggregate import (
    reliefweb_weekly_datapoint_aggregations,
)


def _build_op_context():
    """Dagster requires a real BaseDirectExecutionContext for direct
    asset invocation — a MagicMock trips the isinstance check.
    `build_asset_context()` produces a lightweight one with in-memory
    log capture and metadata accumulation, exactly what these tests
    need."""
    return dg.build_asset_context()


class TestAggregationAsset:
    def test_skips_when_no_new_reports_landed(self):
        # If the extraction asset produced no summaries, there's
        # nothing to trigger a refresh over — bail early rather than
        # calling clear-api and racking up a no-op mutation.
        with patch(
            "clear_context_pipeline.defs.knowledgebase.datapoints_aggregate.clear_api.refresh_aggregated_datapoints",
        ) as mock_refresh:
            result = reliefweb_weekly_datapoint_aggregations(
                _build_op_context(), reliefweb_weekly_datapoints=[],
            )
        mock_refresh.assert_not_called()
        assert result["skipped"] is True
        assert result["computed_buckets"] == 0

    def test_first_run_uses_initial_window(self):
        # Fresh pipeline: no current aggregated_datapoints rows exist,
        # so has_aggregated_datapoints returns False → asset must pick
        # the wider initial window and log mode="initial-backfill".
        os.environ.pop("KB_AGGREGATION_LOOKBACK_DAYS", None)
        os.environ.pop("KB_AGGREGATION_INITIAL_LOOKBACK_DAYS", None)

        with (
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_aggregate.clear_api.has_aggregated_datapoints",
                return_value=False,
            ),
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_aggregate.clear_api.refresh_aggregated_datapoints",
            ) as mock_refresh,
        ):
            mock_refresh.return_value = {
                "computedBuckets": 5,
                "supersededBuckets": 0,
                "schemaVersion": "v1",
            }
            ctx = _build_op_context()
            reliefweb_weekly_datapoint_aggregations(
                ctx, reliefweb_weekly_datapoints=[{"report_id": "r1"}],
            )
            # The `from_iso` argument should be ~90 days before `to_iso`
            call_kwargs = mock_refresh.call_args.kwargs
            from_dt = _parse_iso(call_kwargs["from_iso"])
            to_dt = _parse_iso(call_kwargs["to_iso"])
            days_between = (to_dt - from_dt).days
            assert 89 <= days_between <= 91, f"initial window should be ~90d, got {days_between}"

    def test_subsequent_run_uses_weekly_window(self):
        # Populated cache: has_aggregated_datapoints returns True →
        # asset picks the narrower weekly window.
        os.environ.pop("KB_AGGREGATION_LOOKBACK_DAYS", None)

        with (
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_aggregate.clear_api.has_aggregated_datapoints",
                return_value=True,
            ),
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_aggregate.clear_api.refresh_aggregated_datapoints",
            ) as mock_refresh,
        ):
            mock_refresh.return_value = {
                "computedBuckets": 2,
                "supersededBuckets": 2,
                "schemaVersion": "v1",
            }
            reliefweb_weekly_datapoint_aggregations(
                _build_op_context(), reliefweb_weekly_datapoints=[{"report_id": "r1"}],
            )
            call_kwargs = mock_refresh.call_args.kwargs
            from_dt = _parse_iso(call_kwargs["from_iso"])
            to_dt = _parse_iso(call_kwargs["to_iso"])
            days_between = (to_dt - from_dt).days
            assert 6 <= days_between <= 8, f"weekly window should be ~7d, got {days_between}"

    def test_existence_check_failure_falls_back_to_initial_window(self):
        # If has_aggregated_datapoints throws (clear-api hiccup), we
        # MUST fall back to the wider window rather than silently
        # under-refreshing. The safer bias here is "recompute more"
        # since aggregation is idempotent.
        with (
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_aggregate.clear_api.has_aggregated_datapoints",
                side_effect=RuntimeError("clear-api 500"),
            ),
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_aggregate.clear_api.refresh_aggregated_datapoints",
            ) as mock_refresh,
        ):
            mock_refresh.return_value = {
                "computedBuckets": 1,
                "supersededBuckets": 0,
                "schemaVersion": "v1",
            }
            reliefweb_weekly_datapoint_aggregations(
                _build_op_context(), reliefweb_weekly_datapoints=[{"report_id": "r1"}],
            )
            call_kwargs = mock_refresh.call_args.kwargs
            from_dt = _parse_iso(call_kwargs["from_iso"])
            to_dt = _parse_iso(call_kwargs["to_iso"])
            assert (to_dt - from_dt).days >= 89

    def test_env_overrides_take_effect(self):
        # KB_AGGREGATION_LOOKBACK_DAYS overrides the compile-time
        # default. Verified by setting an atypical value and checking
        # the window matches.
        os.environ["KB_AGGREGATION_LOOKBACK_DAYS"] = "14"
        try:
            with (
                patch(
                    "clear_context_pipeline.defs.knowledgebase.datapoints_aggregate.clear_api.has_aggregated_datapoints",
                    return_value=True,
                ),
                patch(
                    "clear_context_pipeline.defs.knowledgebase.datapoints_aggregate.clear_api.refresh_aggregated_datapoints",
                ) as mock_refresh,
            ):
                mock_refresh.return_value = {
                    "computedBuckets": 0, "supersededBuckets": 0, "schemaVersion": "v1",
                }
                reliefweb_weekly_datapoint_aggregations(
                    _build_op_context(), reliefweb_weekly_datapoints=[{"report_id": "r1"}],
                )
                call_kwargs = mock_refresh.call_args.kwargs
                from_dt = _parse_iso(call_kwargs["from_iso"])
                to_dt = _parse_iso(call_kwargs["to_iso"])
                assert 13 <= (to_dt - from_dt).days <= 15
        finally:
            os.environ.pop("KB_AGGREGATION_LOOKBACK_DAYS", None)


def _parse_iso(s: str):
    """ISO-8601 → datetime (handles both `Z` and `+00:00` forms)."""
    from datetime import datetime

    return datetime.fromisoformat(s.replace("Z", "+00:00"))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
