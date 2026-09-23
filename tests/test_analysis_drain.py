"""Unit tests for the on-demand analysis drain helpers (ADR-0007 §4) — frame
construction, window labelling, and the leverage-structured-datapoints-when-
available rule (decision #2). Pure logic + a mocked clear_api."""

from unittest.mock import patch

from clear_pipeline.defs.analysis.stages import (
    _frame_from_request,
    _period_label,
    _resolve_frame_aggregated,
)
from clear_pipeline.defs.situation.frame import Frame


class TestFrameFromRequest:
    def test_builds_canonicalised_frame(self):
        frame = _frame_from_request({
            "id": "req-1",
            "windowStart": "2026-03-01",
            "windowEnd": "2026-03-31",
            "locationIds": ["b", "a"],
            "eventTypes": ["FL"],
            "needSectors": [],
        })
        assert frame.location_ids == ("a", "b")   # canonicalised
        assert frame.event_types == ("FL",)
        assert frame.window_start == "2026-03-01"
        assert frame.window_end == "2026-03-31"

    def test_missing_arrays_default_empty(self):
        frame = _frame_from_request({"id": "r", "windowStart": "2026-01-01"})
        assert frame.location_ids == ()
        assert frame.window_end is None


class TestPeriodLabel:
    def test_fixed_window(self):
        f = Frame.build(window_start="2026-01-01T00:00:00", window_end="2026-06-30T23:59:59")
        assert _period_label(f) == "2026-01-01 to 2026-06-30"

    def test_rolling_window(self):
        f = Frame.build(window_start="2026-01-01T00:00:00", window_end=None)
        assert _period_label(f) == "2026-01-01 to present"


class TestResolveFrameAggregated:
    def test_single_location_fixed_window_fetches_aggregated(self):
        # A single-location fixed-window frame leverages clear-api's bucket /
        # on-demand roll-up (decision #2).
        f = Frame.build(window_start="2026-01-01", window_end="2026-03-31", location_ids=["khartoum"])
        with patch(
            "clear_pipeline.defs.analysis.stages.clear_api.get_aggregated_datapoint",
            return_value={"id": "agg-1", "reportCount": 3},
        ) as mock_agg:
            result = _resolve_frame_aggregated(f)
        assert result == {"id": "agg-1", "reportCount": 3}
        kwargs = mock_agg.call_args.kwargs
        assert kwargs["location_id"] == "khartoum"
        assert kwargs["window_start"] == "2026-01-01"
        assert kwargs["window_end"] == "2026-03-31"

    def test_multi_location_frame_falls_back_to_kb_only(self):
        f = Frame.build(window_start="2026-01-01", window_end="2026-03-31", location_ids=["a", "b"])
        with patch("clear_pipeline.defs.analysis.stages.clear_api.get_aggregated_datapoint") as mock_agg:
            assert _resolve_frame_aggregated(f) is None
        mock_agg.assert_not_called()

    def test_rolling_frame_has_no_bucket(self):
        f = Frame.build(window_start="2026-01-01", window_end=None, location_ids=["khartoum"])
        with patch("clear_pipeline.defs.analysis.stages.clear_api.get_aggregated_datapoint") as mock_agg:
            assert _resolve_frame_aggregated(f) is None
        mock_agg.assert_not_called()

    def test_aggregated_fetch_failure_degrades_to_none(self):
        f = Frame.build(window_start="2026-01-01", window_end="2026-03-31", location_ids=["khartoum"])
        with patch(
            "clear_pipeline.defs.analysis.stages.clear_api.get_aggregated_datapoint",
            side_effect=RuntimeError("clear-api 500"),
        ):
            assert _resolve_frame_aggregated(f) is None  # KB narrative fills in
