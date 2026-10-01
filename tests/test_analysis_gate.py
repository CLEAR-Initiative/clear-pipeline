"""Tests for the analysis regeneration gate (ADR-0008): the pure
`decide_generation` logic plus the skip wiring in both drains."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from clear_pipeline.defs.analysis.gate import GateDecision, decide_generation

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


class TestDecideGeneration:
    def test_forced_always_generates(self):
        d = decide_generation(current={"generatedAt": _iso(NOW)}, latest_evidence_at=_iso(NOW), now=NOW, force=True)
        assert d == GateDecision(True, "forced")

    def test_no_existing_analysis_generates(self):
        d = decide_generation(current=None, latest_evidence_at=None, now=NOW)
        assert d == GateDecision(True, "no-existing-analysis")

    def test_within_24h_skips_even_with_new_evidence(self):
        gen = NOW - timedelta(hours=5)
        d = decide_generation(current={"generatedAt": _iso(gen)}, latest_evidence_at=_iso(NOW), now=NOW)
        assert d == GateDecision(False, "within-24h-floor")

    def test_past_floor_with_new_evidence_generates(self):
        gen = NOW - timedelta(hours=30)
        newer = NOW - timedelta(hours=1)
        d = decide_generation(current={"generatedAt": _iso(gen)}, latest_evidence_at=_iso(newer), now=NOW)
        assert d == GateDecision(True, "new-evidence")

    def test_past_floor_no_new_evidence_skips(self):
        gen = NOW - timedelta(hours=30)
        older = NOW - timedelta(hours=40)
        d = decide_generation(current={"generatedAt": _iso(gen)}, latest_evidence_at=_iso(older), now=NOW)
        assert d == GateDecision(False, "no-new-evidence")

    def test_past_floor_no_watermark_skips(self):
        gen = NOW - timedelta(hours=30)
        d = decide_generation(current={"generatedAt": _iso(gen)}, latest_evidence_at=None, now=NOW)
        assert d == GateDecision(False, "no-new-evidence")

    def test_evidence_equal_to_generated_at_skips(self):
        gen = NOW - timedelta(hours=30)
        d = decide_generation(current={"generatedAt": _iso(gen)}, latest_evidence_at=_iso(gen), now=NOW)
        assert d == GateDecision(False, "no-new-evidence")

    def test_naive_generated_at_treated_as_utc(self):
        # A timestamp with no tz must not raise on the aware/naive subtraction.
        gen_naive = (NOW - timedelta(hours=5)).replace(tzinfo=None).isoformat()
        d = decide_generation(current={"generatedAt": gen_naive}, latest_evidence_at=_iso(NOW), now=NOW)
        assert d == GateDecision(False, "within-24h-floor")

    def test_z_suffix_timestamps_parsed(self):
        gen = (NOW - timedelta(hours=30)).isoformat().replace("+00:00", "Z")
        latest = (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        d = decide_generation(current={"generatedAt": gen}, latest_evidence_at=latest, now=NOW)
        assert d.generate


class TestOnDemandDrainGate:
    def test_skip_touches_synced_and_marks_generated_without_generating(self):
        from clear_pipeline.defs.analysis import stages

        req = {"id": "r1", "windowStart": "2026-01-01", "windowEnd": "2026-06-30", "locationIds": ["a"]}
        with patch.object(stages, "evidence_gate", return_value=GateDecision(False, "within-24h-floor")), \
             patch.object(stages, "touch_synced") as touch, \
             patch.object(stages.clear_api, "mark_analysis_request_generated", return_value={"id": "r1"}) as mark, \
             patch.object(stages, "_run_frame_generation") as gen:
            out = stages._process_one_request(MagicMock(), req)
        assert out == stages._GENERATED
        gen.assert_not_called()           # no LLM generation on a skip
        touch.assert_called_once()        # lastSyncedAt bumped
        mark.assert_called_once_with("r1")  # existing fresh row IS the answer

    def test_generate_path_threads_force(self):
        from clear_pipeline.defs.analysis import stages

        req = {"id": "r2", "windowStart": "2026-01-01", "windowEnd": "2026-06-30", "locationIds": ["a"], "force": True}
        with patch.object(stages, "evidence_gate", return_value=GateDecision(True, "forced")), \
             patch.object(stages, "_run_frame_generation", return_value={"analysisId": "an-1"}) as gen, \
             patch.object(stages.clear_api, "mark_analysis_request_generated", return_value={"id": "r2"}):
            out = stages._process_one_request(MagicMock(), req)
        assert out == stages._GENERATED
        assert gen.call_args.kwargs["force"] is True


class TestAutomationDrainGate:
    def test_skip_advances_schedule_without_generating(self):
        from clear_pipeline.defs.analysis import automation

        frame = automation._frame_from_automation({"id": "au1", "windowStart": "2026-01-01", "locationIds": ["a"]})
        with patch.object(automation, "evidence_gate", return_value=GateDecision(False, "no-new-evidence")), \
             patch.object(automation, "touch_synced") as touch, \
             patch.object(automation.clear_api, "mark_analysis_automations_ran", return_value=1) as mark, \
             patch.object(automation, "_run_frame_generation") as gen:
            out = automation._process_frame(MagicMock(), frame, ["au1"], "2026-10-01T12:00:00+00:00")
        assert out is False               # no generation
        gen.assert_not_called()
        touch.assert_called_once()        # lastSyncedAt bumped
        mark.assert_called_once_with(["au1"])  # schedule still advances (R3b)
