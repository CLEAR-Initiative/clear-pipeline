"""Tests for event-onset (startedAt) extraction from signal text.

Locks the regex extractor's behavior: absolute + relative date phrasings
anchored to the signal timestamp, the plausibility window (no future onsets, no
long-past background mentions), earliest-wins, and the earliest_onset_iso helper
the pipeline uses to keep an event's onset at the earliest across its signals.
"""

from clear_pipeline.providers.signal import (
    earliest_onset_iso,
    extract_event_start_from_text,
)

_REF = "2026-03-20T12:00:00Z"  # reference "collected at" for relative phrases


class TestExtractEventStart:
    def test_iso_date(self):
        assert extract_event_start_from_text(
            "Floods began on 2026-03-10 across the region", reference_ts=_REF,
        ) == "2026-03-10"

    def test_days_ago_relative_to_reference(self):
        assert extract_event_start_from_text(
            "The quake struck 3 days ago", reference_ts=_REF,
        ) == "2026-03-17"

    def test_yesterday(self):
        assert extract_event_start_from_text("Clashes erupted yesterday", reference_ts=_REF) == "2026-03-19"

    def test_last_week(self):
        assert extract_event_start_from_text("Displacement started last week", reference_ts=_REF) == "2026-03-13"

    def test_day_month_year(self):
        assert extract_event_start_from_text(
            "Fighting since 3 March 2026 has displaced thousands", reference_ts=_REF,
        ) == "2026-03-03"

    def test_month_day_no_year_infers_reference_year(self):
        assert extract_event_start_from_text("Began March 3 in the capital", reference_ts=_REF) == "2026-03-03"

    def test_month_day_no_year_rolls_back_when_future(self):
        # ref is 2026-01-10; "December 20" with no year would be future in 2026 →
        # roll back to 2025-12-20 (21 days before the reference, in-window).
        assert extract_event_start_from_text(
            "Started December 20 after weeks of tension", reference_ts="2026-01-10T00:00:00Z",
        ) == "2025-12-20"

    def test_earliest_wins_across_multiple_mentions(self):
        assert extract_event_start_from_text(
            "Worsened yesterday, but the flooding began 5 days ago", reference_ts=_REF,
        ) == "2026-03-15"

    def test_future_date_dropped(self):
        # An onset can't post-date the report — a future ISO date is not an onset.
        assert extract_event_start_from_text("Planned for 2026-03-25", reference_ts=_REF) is None

    def test_background_mention_dropped(self):
        # Older than the lookback window → background context, not this onset.
        assert extract_event_start_from_text(
            "Rooted in the 2020-01-01 conflict", reference_ts=_REF,
        ) is None

    def test_no_date_returns_none(self):
        assert extract_event_start_from_text("Heavy rainfall reported in the area", reference_ts=_REF) is None

    def test_bare_month_year_not_misparsed_as_day(self):
        # "March 2026" is a month reference, not "March 20" — must not yield a day.
        assert extract_event_start_from_text("Conditions in March 2026 remain dire", reference_ts=_REF) is None

    def test_casualty_number_not_a_date(self):
        assert extract_event_start_from_text("At least 12 killed and 30 injured", reference_ts=_REF) is None

    def test_none_and_empty_texts(self):
        assert extract_event_start_from_text(None, "", reference_ts=_REF) is None


class TestEarliestOnsetIso:
    def test_picks_earliest_ignoring_none(self):
        assert earliest_onset_iso("2026-03-10", None, "2026-03-05") == "2026-03-05"

    def test_all_none(self):
        assert earliest_onset_iso(None, None) is None

    def test_single_value(self):
        assert earliest_onset_iso(None, "2026-03-01") == "2026-03-01"
