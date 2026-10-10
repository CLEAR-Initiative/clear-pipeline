"""`providers/event._rewrite_fields` and the grouping branches of
`_match_and_act` that use it. clear-api, the LLM and Redis are faked."""

from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.providers import event as ev


def m(mid, *, day=1, severity=3, casualties=None):
    return {"id": mid, "title": "Clash", "description": "details", "severity": severity,
            "casualties": casualties, "publishedAt": f"2026-09-{day:02d}T00:00:00Z",
            "source": {"name": "acled"}}


def rewrite(severity=4, population_displaced=1200):
    return ev.EventRewrite(title="New title", description="New desc", severity=severity,
                           population_displaced=population_displaced)


class FakeLLM:
    def __init__(self, fail=False):
        self.fail = fail

    def complete_structured(self, *, system, user, schema):
        if self.fail:
            raise RuntimeError("LLM down")
        return rewrite()


# ── _rewrite_fields ─────────────────────────────────────────────────────────


def test_every_member_severity_wins_over_the_rewrite():
    out = ev._rewrite_fields([m("a", severity=2), m("b", severity=4)], rewrite(severity=5), None)
    assert (out["severity"], out["rank"]) == (3, pytest.approx(0.6))


def test_rewrite_severity_used_when_a_member_lacks_one():
    out = ev._rewrite_fields([m("a", severity=None)], rewrite(severity=5), fallback_severity=2)
    assert out["severity"] == 5


def test_fallback_severity_used_without_a_rewrite_severity():
    assert ev._rewrite_fields([m("a", severity=None)], None, fallback_severity=2)["severity"] == 2
    assert ev._rewrite_fields([m("a", severity=None)], rewrite(severity=None), 2)["severity"] == 2


def test_no_members_returns_nothing():
    assert ev._rewrite_fields([], rewrite(), fallback_severity=2) == {
        "title": "New title", "description": "New desc", "populationDisplaced": "1200",
    }
    assert ev._rewrite_fields([], None, fallback_severity=2) == {}


def test_without_a_rewrite_no_text_or_displacement_and_no_nulls():
    out = ev._rewrite_fields([m("a", severity=None)], None, fallback_severity=None)
    assert out == {}


def test_rewrite_without_displacement_leaves_it_out():
    # ADR-0010: unknown stays null; no invented default.
    out = ev._rewrite_fields([m("a")], rewrite(population_displaced=None), None)
    assert "populationDisplaced" not in out


# ── _match_and_act (characterization) ───────────────────────────────────────


@pytest.fixture
def group():
    def _group(*, members, target=None, llm=None):
        update_event = MagicMock(side_effect=lambda eid, data: {"id": eid, **data})
        create_event = MagicMock(side_effect=lambda data: {"id": "new", **data})
        with (
            patch.object(ev, "_get_active_events", return_value=[target] if target else []),
            patch.object(ev, "_event_matches", return_value=True),
            patch.object(ev, "update_event", update_event),
            patch.object(ev, "create_event", create_event),
            patch.object(ev.graphql, "event_members", return_value=members),
            patch.object(ev, "make_llm_provider", return_value=llm or FakeLLM()),
            patch.object(ev, "_redis", MagicMock()),
        ):
            ev._match_and_act(
                signal_id="s-new", signal_title="Clash", signal_description="details",
                classification=MagicMock(summary="details"), admin2_id="d1", level_2="conflict",
                glide_code="rc", ts="2026-09-03T00:00:00Z", location_name="El Fasher",
                primary=None, resolved_stats={"casualties": 2, "population_affected": 100},
            )
        return update_event, create_event
    return _group


def target(**over):
    return {"id": "e1", "casualties": 5, "populationAffected": "50", "severity": 1, **over}


def test_add_branch_with_rewrite_writes_text_severity_displacement_and_merged_stats(group):
    update_event, _ = group(members=[m("a", severity=2), m("b", severity=4)], target=target())
    final = update_event.call_args_list[-1].args[1]
    assert (final["title"], final["description"]) == ("New title", "New desc")
    assert (final["severity"], final["rank"]) == (3, pytest.approx(0.6))
    assert final["populationDisplaced"] == "1200"
    assert (final["casualties"], final["populationAffected"]) == (7, "100")


def test_add_branch_failed_rewrite_keeps_stored_text_and_displacement(group):
    update_event, _ = group(members=[m("a", severity=2)], target=target(), llm=FakeLLM(fail=True))
    final = update_event.call_args_list[-1].args[1]
    for key in ("title", "description", "populationDisplaced"):
        assert key not in final
    assert final["severity"] == 2  # every member has one, so no LLM needed
    assert final["casualties"] == 7


def test_add_branch_failed_rewrite_without_member_severity_leaves_severity_alone(group):
    update_event, _ = group(members=[m("a", severity=None)], target=target(), llm=FakeLLM(fail=True))
    final = update_event.call_args_list[-1].args[1]
    assert "severity" not in final and "rank" not in final


def test_create_branch_with_rewrite(group):
    update_event, create_event = group(members=[m("s-new", severity=None)])
    assert create_event.call_count == 1
    final = update_event.call_args.args[1]
    assert (final["title"], final["severity"], final["populationDisplaced"]) == ("New title", 4, "1200")


def test_create_branch_failed_rewrite_writes_no_displacement(group):
    # ADR-0010: no rewrite figure, no invented default.
    update_event, _ = group(members=[m("s-new", severity=None)], llm=FakeLLM(fail=True))
    assert all("populationDisplaced" not in c.args[1] for c in update_event.call_args_list)


# ── group_signal records the glide (signals.glideCode) ──────────────────────


def test_group_signal_records_its_glide_before_attaching():
    calls: list[str] = []
    set_glide = MagicMock(side_effect=lambda sid, glide: calls.append(f"glide:{sid}:{glide}"))
    act = MagicMock(side_effect=lambda **kw: calls.append("attach") or {"id": "e1"})
    classification = MagicMock(disaster_types=["ba"], type_level_1="conflict", type_level_2="battles",
                               relevance=0.9, summary="s")
    with (
        patch.object(ev.graphql, "set_signal_glide_code", set_glide),
        patch.object(ev, "resolve_signal_admin2", return_value=None),
        patch.object(ev, "_match_and_act", act),
    ):
        ev.group_signal("s1", "Clash", "details", None, classification, {"casualties": 3})
    assert calls == ["glide:s1:ba", "attach"]


def test_group_signal_glide_write_failure_fails_grouping():
    classification = MagicMock(disaster_types=["ba"], type_level_1="conflict", type_level_2="battles",
                               relevance=0.9, summary="s")
    act = MagicMock()
    with (
        patch.object(ev.graphql, "set_signal_glide_code", side_effect=RuntimeError("api down")),
        patch.object(ev, "_match_and_act", act),
        pytest.raises(RuntimeError),
    ):
        ev.group_signal("s1", "Clash", "details", None, classification, {"casualties": 3})
    act.assert_not_called()
