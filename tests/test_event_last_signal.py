"""Tests for keeping an event's lastSignalCreatedAt monotonic during grouping.

Signals arrive out of order (a backdated ACLED/IDMC record, a delayed Dataminr
item). When one joins an existing event, the update must not move the event's
lastSignalCreatedAt back to the older signal's publishedAt — that value decides
whether the event stays in grouping's active window and the alert window.
"""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from clear_pipeline.providers import event as ev
from clear_pipeline.providers.event import _later_iso


class TestLaterIso:
    def test_keeps_current_when_candidate_is_older(self):
        assert _later_iso("2026-05-10T00:00:00.000Z", "2026-05-05T00:00:00+00:00") == "2026-05-10T00:00:00.000Z"

    def test_takes_candidate_when_newer(self):
        assert _later_iso("2026-05-10T00:00:00.000Z", "2026-05-11T00:00:00+00:00") == "2026-05-11T00:00:00+00:00"

    def test_compares_across_offsets_not_as_strings(self):
        # 2026-05-10T01:00+02:00 is 23:00Z on the 9th: older, though it sorts later as a string.
        assert _later_iso("2026-05-10T00:00:00.000Z", "2026-05-10T01:00:00+02:00") == "2026-05-10T00:00:00.000Z"

    def test_naive_candidate_read_as_utc(self):
        assert _later_iso("2026-05-10T00:00:00.000Z", "2026-05-10T00:00:01") == "2026-05-10T00:00:01"

    @pytest.mark.parametrize("current", [None, "", "not-a-date"])
    def test_falls_back_to_candidate_without_a_usable_current(self, current):
        assert _later_iso(current, "2026-05-05T00:00:00Z") == "2026-05-05T00:00:00Z"

    def test_keeps_current_when_candidate_unparseable(self):
        assert _later_iso("2026-05-10T00:00:00.000Z", "garbage") == "2026-05-10T00:00:00.000Z"


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


@pytest.fixture
def grouping(monkeypatch):
    """Run `_match_and_act` against one matching active event, with every
    collaborator past the signal-attach stubbed out. Returns the recorded
    update_event calls."""
    now = datetime.now(UTC)
    target = {"id": "evt-1", "lastSignalCreatedAt": _iso(now), "types": ["FL"]}
    calls: list[tuple[str, dict]] = []

    monkeypatch.setattr(ev, "_get_active_events", lambda: [target])
    monkeypatch.setattr(ev, "_event_matches", lambda e, a, l: True)
    monkeypatch.setattr(ev, "update_event", lambda eid, data: calls.append((eid, data)) or target)
    monkeypatch.setattr(ev, "_rewrite_event", lambda *a: (None, []))
    monkeypatch.setattr(ev, "_compute_event_severity", lambda *a: None)
    monkeypatch.setattr(ev, "_resolve_population_displaced", lambda **k: None)
    monkeypatch.setattr(ev, "_merge_event_stats", lambda *a: {})
    monkeypatch.setattr(ev, "_invalidate_events_cache", lambda: None)
    monkeypatch.setattr(ev, "extract_event_start_from_text", lambda *a, **k: None)

    def run(published_at: datetime) -> list[tuple[str, dict]]:
        ev._match_and_act(
            signal_id="sig-1",
            signal_title="Flooding in the district",
            signal_description=None,
            classification=SimpleNamespace(summary="Flooding"),
            admin2_id="admin2-1",
            level_2="flood",
            glide_code="FL",
            ts=published_at.isoformat(),
            location_name="Somewhere",
            primary=None,
            resolved_stats={"casualties": None, "population_affected": None},
        )
        return calls

    return SimpleNamespace(run=run, now=now, target=target)


def test_older_signal_leaves_last_signal_created_at_unchanged(grouping):
    calls = grouping.run(grouping.now - timedelta(days=5))
    eid, attach = calls[0]
    assert eid == "evt-1"
    assert attach["signalIds"] == ["sig-1"]
    assert attach["lastSignalCreatedAt"] == grouping.target["lastSignalCreatedAt"]


def test_newer_signal_advances_last_signal_created_at(grouping):
    newer = grouping.now + timedelta(hours=1)
    calls = grouping.run(newer)
    assert calls[0][1]["lastSignalCreatedAt"] == newer.isoformat()
