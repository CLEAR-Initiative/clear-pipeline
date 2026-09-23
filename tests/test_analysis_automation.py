"""Unit tests for the analysis automation drain (ADR-0007 §5) — rolling-frame
construction and the min-cadence-by-frame grouping. Pure logic."""

from clear_pipeline.defs.analysis.automation import (
    _frame_from_automation,
    _frame_group_key,
)


class TestFrameFromAutomation:
    def test_is_rolling_and_canonicalised(self):
        f = _frame_from_automation({
            "id": "a1", "windowStart": "2026-01-01",
            "locationIds": ["b", "a"], "eventTypes": ["FL"], "needSectors": [],
        })
        assert f.window_end is None          # automations are rolling ("to present")
        assert f.location_ids == ("a", "b")  # canonicalised
        assert f.event_types == ("FL",)


class TestFrameGrouping:
    def test_same_frame_groups_together(self):
        a = _frame_from_automation({"id": "a", "windowStart": "2026-01-01", "locationIds": ["x"]})
        b = _frame_from_automation({"id": "b", "windowStart": "2026-01-01", "locationIds": ["x"]})
        assert _frame_group_key(a) == _frame_group_key(b)  # daily+weekly on same frame → one run

    def test_order_insensitive(self):
        a = _frame_from_automation({"id": "a", "windowStart": "2026-01-01", "locationIds": ["x", "y"]})
        b = _frame_from_automation({"id": "b", "windowStart": "2026-01-01", "locationIds": ["y", "x"]})
        assert _frame_group_key(a) == _frame_group_key(b)

    def test_different_window_separates(self):
        a = _frame_from_automation({"id": "a", "windowStart": "2026-01-01", "locationIds": ["x"]})
        b = _frame_from_automation({"id": "b", "windowStart": "2026-02-01", "locationIds": ["x"]})
        assert _frame_group_key(a) != _frame_group_key(b)

    def test_different_event_types_separate(self):
        a = _frame_from_automation({"id": "a", "windowStart": "2026-01-01", "locationIds": ["x"], "eventTypes": ["FL"]})
        b = _frame_from_automation({"id": "b", "windowStart": "2026-01-01", "locationIds": ["x"], "eventTypes": ["EQ"]})
        assert _frame_group_key(a) != _frame_group_key(b)
