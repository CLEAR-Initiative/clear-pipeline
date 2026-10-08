"""The pipeline must not invent population/severity/end-date values — when a
figure is unknown it is left null rather than defaulted to a constant.
These lock in that behaviour for the pure resolver helpers in providers/event.py."""

from clear_pipeline.providers import event


class TestPopulationDisplaced:
    def test_null_when_no_figure(self):
        # No Claude-extracted figure → None (was a hardcoded 1670 default).
        assert event._resolve_population_displaced(None) is None

    def test_null_when_zero_or_negative(self):
        assert event._resolve_population_displaced(0) is None
        assert event._resolve_population_displaced(-5) is None

    def test_passes_through_a_real_figure(self):
        assert event._resolve_population_displaced(4200) == 4200


class TestSignalStats:
    def test_population_null_when_unknown(self):
        # No actual value and no per-event-type historical stat (glide_code=None)
        # → population_affected is None, not a 33_000 default.
        stats = event._resolve_signal_stats(
            actual_casualties=None, actual_population=None, glide_code=None
        )
        assert stats["population_affected"] is None
        assert stats["casualties"] is None

    def test_actual_population_passes_through(self):
        stats = event._resolve_signal_stats(
            actual_casualties=12, actual_population=50_000, glide_code=None
        )
        assert stats["population_affected"] == 50_000
        assert stats["casualties"] == 12


class TestEventSeverity:
    def test_null_when_signals_lack_severity_and_no_fallback(self):
        # A signal with null severity + no Claude fallback → event severity None
        # (no invented floor).
        assert event._compute_event_severity([{"severity": None}], None) is None

    def test_uses_claude_fallback_when_signal_severity_missing(self):
        assert event._compute_event_severity([{"severity": None}], 4) == 4

    def test_mean_when_all_signals_have_severity(self):
        assert event._compute_event_severity([{"severity": 4}, {"severity": 2}], None) == 3

    def test_averages_only_known_severities_ignoring_nulls(self):
        # Mixed: some signals have a severity, some are null. Null signals belong
        # to the event but are NOT counted in the mean (null = unknown, not low);
        # the Claude fallback is ignored because a known severity exists.
        assert event._compute_event_severity(
            [{"severity": 4}, {"severity": 2}, {"severity": None}], claude_fallback=1
        ) == 3

    def test_gdacs_red_survives_darfur24_null(self):
        # The #95 regression this fixes: a GDACS red alert (5) must not be thrown
        # away when a Darfur24 news signal (null severity) joins the same event.
        assert event._compute_event_severity(
            [{"severity": 5}, {"severity": None}], claude_fallback=2
        ) == 5
