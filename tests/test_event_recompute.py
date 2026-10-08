"""`providers/event.recompute_event`: an event's aggregates rebuilt from its
live members. clear-api, the LLM and Redis are faked."""

from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.providers import event as ev


class FakeApi:
    def __init__(self, members, state=None):
        self.members = members
        self.state = {"id": "e1", "title": "Old title", "description": "Old desc", "severity": 3,
                      "types": ["rc"], "rewriteMembersHash": None,
                      "generalLocation": {"name": "El Fasher"}, **(state or {})}
        self.writes: list[dict] = []
        self.snapshots: list[list[dict]] = []
        self.member_calls: list[int | None] = []
        self.glide_calls: list[bool] = []

    def event_members(self, event_id, first=None, *, with_glide=False):
        self.glide_calls.append(with_glide)
        self.member_calls.append(first)
        newest_first = sorted(self.members, key=lambda m: m["publishedAt"], reverse=True)
        return newest_first[:first] if first else newest_first

    def get_event_recompute_state(self, event_id):
        return self.state

    def set_event_aggregates(self, event_id, data, members):
        self.snapshots.append(members)
        self.writes.append(data)
        # Persist the hash/text like clear-api would, for idempotency checks.
        self.state.update({k: v for k, v in data.items() if k in ("rewriteMembersHash", "title", "description", "severity")})
        return {"id": event_id}


class FakeLLM:
    def __init__(self, fail=False):
        self.calls: list[str] = []
        self.fail = fail

    def complete_structured(self, *, system, user, schema):
        self.calls.append(user)
        if self.fail:
            raise RuntimeError("LLM down")
        return schema(title="New title", description="New desc", severity=4, population_displaced=1200)


def m(mid, *, day=1, severity=3, casualties=None, title="Clash", description="details", revision=0,
      glide=None):
    return {"id": mid, "title": title, "description": description, "severity": severity,
            "casualties": casualties, "publishedAt": f"2026-09-{day:02d}T00:00:00Z",
            "source": {"name": "acled"}, "revision": revision, "glideCode": glide}


def text(member):
    return member["title"], member["description"]


@pytest.fixture
def run():
    def _run(api, llm=None):
        llm = llm or FakeLLM()
        redis = api.redis = MagicMock()
        with (
            patch.object(ev.graphql, "event_members", side_effect=api.event_members),
            patch.object(ev.graphql, "get_event_recompute_state", side_effect=api.get_event_recompute_state),
            patch.object(ev.graphql, "set_event_aggregates", side_effect=api.set_event_aggregates),
            patch.object(ev, "make_llm_provider", return_value=llm),
            patch.object(ev, "_redis", redis),
        ):
            called = ev.recompute_event("e1", text)
        return called, llm, redis
    return _run


def test_sum_max_mean_rank(run):
    api = FakeApi([m("a", severity=2, casualties=10), m("b", day=2, severity=4, casualties=5)])
    run(api)
    w = api.writes[-1]
    assert w["casualties"] == 15
    assert w["severity"] == 3  # round(mean(2, 4))
    assert w["rank"] == pytest.approx(0.6)
    assert w["populationAffected"] is not None


def test_casualties_null_when_nothing_resolves(run):
    # flood glide has no historical fatality stats, and no member reports any
    api = FakeApi([m("a", title="Flood in Nyala", description="rising water")], {"types": ["fl"]})
    run(api)
    assert "casualties" in api.writes[-1]
    assert api.writes[-1]["casualties"] is None


def test_defaults_use_the_event_type_and_never_classify(run):
    api = FakeApi([m("a"), m("b", day=2)], {"types": ["fl"]})  # no figures in the text
    with (
        patch("clear_pipeline.providers.classify.classify_locally", side_effect=AssertionError),
        patch("clear_pipeline.providers.signal_classifier.classify_signal", side_effect=AssertionError),
    ):
        run(api)
    # ADR-0010: no figure and no flood stat -> null, not an invented default.
    assert api.writes[-1]["populationAffected"] is None


def test_retracted_member_is_not_counted(run):
    # eventMembers only returns live members: the retracted one is simply absent.
    api = FakeApi([m("a", casualties=10)])
    run(api)
    assert api.writes[-1]["casualties"] == 10


def test_zero_live_members_clears_aggregates_without_llm(run):
    api = FakeApi([])
    called, llm, _ = run(api)
    w = api.writes[-1]
    assert called is False and llm.calls == []
    assert (w["casualties"], w["populationAffected"], w["severity"], w["rank"]) == (None, None, None, 0.0)
    assert w["populationDisplaced"] is None  # no live member backs the figure
    assert "title" not in w and "description" not in w


def test_membership_change_runs_one_rewrite_and_stores_the_hash(run):
    members = [m("a"), m("b", day=2)]
    api = FakeApi(members)
    called, llm, _ = run(api)
    w = api.writes[-1]
    assert called is True and len(llm.calls) == 1
    assert (w["title"], w["description"]) == ("New title", "New desc")
    assert w["populationDisplaced"] == "1200"
    assert w["rewriteMembersHash"] == ev.members_hash(api.event_members("e1"))


def test_rewrite_without_a_displacement_figure_clears_it(run):
    # Absolute write: the stored figure may have come from a now-retracted member.
    class NoFigureLLM(FakeLLM):
        def complete_structured(self, *, system, user, schema):
            self.calls.append(user)
            return schema(title="New title", description="New desc", severity=4, population_displaced=None)

    api = FakeApi([m("a")])
    run(api, NoFigureLLM())
    w = api.writes[-1]
    assert "populationDisplaced" in w and w["populationDisplaced"] is None


def test_unchanged_membership_makes_no_llm_call_and_keeps_text(run):
    members = [m("a", severity=None), m("b", day=2)]
    api = FakeApi(members, {"severity": 2})
    api.state["rewriteMembersHash"] = ev.members_hash(api.event_members("e1"))
    called, llm, _ = run(api)
    w = api.writes[-1]
    assert called is False and llm.calls == []
    for key in ("title", "description", "rewriteMembersHash", "populationDisplaced"):
        assert key not in w
    assert w["severity"] == 3  # ADR-0010: the known severities are averaged; the null one is skipped


def test_unchanged_membership_falls_back_to_event_severity_when_no_member_has_one(run):
    members = [m("a", severity=None), m("b", day=2, severity=None)]
    api = FakeApi(members, {"severity": 2})
    api.state["rewriteMembersHash"] = ev.members_hash(api.event_members("e1"))
    run(api)
    assert api.writes[-1]["severity"] == 2


def test_idempotent(run):
    api = FakeApi([m("a", casualties=4), m("b", day=2, casualties=1)])
    run(api)
    first = dict(api.writes[-1])
    called, llm, _ = run(api)
    assert called is False and llm.calls == []
    for key in ("casualties", "populationAffected", "severity", "rank"):
        assert api.writes[-1][key] == first[key]


def test_members_hash_changes_with_membership_and_revision():
    base = ev.members_hash([m("a"), m("b")])
    assert ev.members_hash([m("b"), m("a")]) == base
    assert ev.members_hash([m("a")]) != base
    assert ev.members_hash([m("a"), m("b", revision=1)]) != base


def test_revised_member_reruns_the_rewrite(run):
    api = FakeApi([m("a"), m("b", day=2)])
    api.state["rewriteMembersHash"] = ev.members_hash(api.event_members("e1"))
    api.members[1] = m("b", day=2, description="4,000 displaced", revision=1)  # same id, in place
    called, llm, _ = run(api)
    assert called is True and "4,000 displaced" in llm.calls[0]
    assert api.writes[-1]["rewriteMembersHash"] == ev.members_hash(api.event_members("e1"))


def test_retracted_member_in_the_newest_50_reruns_the_rewrite(run):
    api = FakeApi([m("a"), m("b", day=2)])
    api.state["rewriteMembersHash"] = ev.members_hash(api.event_members("e1"))
    api.members.pop()  # eventMembers drops retracted signals
    called, _, _ = run(api)
    assert called is True


def test_change_outside_the_newest_50_makes_no_llm_call(run):
    members = [m(f"s{i:02d}", casualties=1) for i in range(51)]
    for i, member in enumerate(members):
        member["publishedAt"] = f"2026-09-01T00:{i:02d}:00Z"
    api = FakeApi(members)
    api.state["rewriteMembersHash"] = ev.members_hash(api.event_members("e1"))
    api.members[0] = {**members[0], "casualties": 9, "revision": 1}  # the oldest: not in the prompt
    called, llm, _ = run(api)
    assert called is False and llm.calls == []
    assert api.writes[-1]["casualties"] == 59  # aggregates still cover every member


def test_rewrite_prompt_takes_the_50_newest_but_aggregates_cover_all(run):
    members = [m(f"s{i:02d}", day=1 + i % 28, casualties=1) for i in range(60)]
    for i, member in enumerate(members):
        member["publishedAt"] = f"2026-09-01T00:{i:02d}:00Z"
    api = FakeApi(members)
    _, llm, _ = run(api)
    assert api.member_calls == [None, 50]
    assert api.writes[-1]["casualties"] == 60
    prompt = llm.calls[0]
    assert "2026-09-01T00:59:00Z" in prompt and "2026-09-01T00:10:00Z" in prompt
    assert "2026-09-01T00:09:00Z" not in prompt


def test_llm_failure_writes_deterministic_fields_only_then_raises(run):
    api = FakeApi([m("a", casualties=7)])
    with pytest.raises(RuntimeError, match="LLM down"):
        run(api, FakeLLM(fail=True))
    api.redis.delete.assert_called_with(ev.ACTIVE_EVENTS_CACHE_KEY)
    w = api.writes[-1]
    assert w["casualties"] == 7
    for key in ("title", "description", "rewriteMembersHash"):
        assert key not in w, "text kept and hash not advanced, so the next run retries"
    assert api.state["title"] == "Old title"


def test_cache_invalidated_even_when_the_write_fails(run):
    api = FakeApi([m("a")])
    api.set_event_aggregates = MagicMock(side_effect=RuntimeError("clear-api 500"))
    with pytest.raises(RuntimeError, match="500"):
        run(api)
    api.redis.delete.assert_called_with(ev.ACTIVE_EVENTS_CACHE_KEY)


def test_write_carries_the_member_snapshot_the_totals_came_from(run):
    api = FakeApi([m("a", revision=2, casualties=1), m("b", day=2, revision=5, casualties=2)])
    run(api)
    assert [(s["id"], s["revision"]) for s in api.snapshots[-1]] == [("b", 5), ("a", 2)]
    assert api.writes[-1]["casualties"] == 3


def test_stale_members_propagates_over_an_llm_failure_and_invalidates_the_cache(run):
    api = FakeApi([m("a")])
    api.set_event_aggregates = MagicMock(side_effect=ev.graphql.ClearApiStaleMembers([{"message": "stale", "extensions": {"code": "STALE_EVENT_MEMBERS"}}]))
    with pytest.raises(ev.graphql.ClearApiStaleMembers):
        run(api, FakeLLM(fail=True))
    api.redis.delete.assert_called_with(ev.ACTIVE_EVENTS_CACHE_KEY)


def test_cache_invalidated_after_every_write(run):
    api = FakeApi([m("a")])
    _, _, redis = run(api)
    redis.delete.assert_called_with(ev.ACTIVE_EVENTS_CACHE_KEY)


def test_grouping_rewrite_still_swallows_llm_errors():
    with (
        patch.object(ev.graphql, "event_members", return_value=[m("a")]),
        patch.object(ev, "make_llm_provider", return_value=FakeLLM(fail=True)),
    ):
        rewrite, signals = ev._rewrite_event("e1", "El Fasher", "flood")
    assert rewrite is None and len(signals) == 1


def test_grouping_rewrite_uses_the_50_newest_live_members_oldest_first():
    calls = []

    def members(event_id, first=None):
        calls.append(first)
        return [m("new", day=3), m("mid", day=2), m("old", day=1)]  # newest first, as clear-api returns

    llm = FakeLLM()
    with patch.object(ev.graphql, "event_members", side_effect=members), \
         patch.object(ev, "make_llm_provider", return_value=llm):
        ev._rewrite_event("e1", "El Fasher", "flood")
    assert calls == [50]
    prompt = llm.calls[0]
    assert prompt.index("2026-09-01") < prompt.index("2026-09-02") < prompt.index("2026-09-03")


# ── Per-member glide (signals.glideCode) ────────────────────────────────────


def test_member_glide_resolves_its_own_fallback(run):
    # Battles: `ba` armed clash (q75 4 / pop 23 532) vs the event's `bo`
    # non-state actor overtakes territory (q75 10 / pop 10 602).
    api = FakeApi([m("a", glide="ba"), m("b", day=2, glide="bo")], {"types": ["bo"]})
    run(api)
    w = api.writes[-1]
    assert w["casualties"] == 4 + 10
    assert w["populationAffected"] == "23532"


def test_member_without_glide_falls_back_to_the_event_type(run):
    api = FakeApi([m("a", glide=None), m("b", day=2, glide="bo")], {"types": ["bo"]})
    run(api)
    assert api.writes[-1]["casualties"] == 10 + 10


def test_only_the_aggregate_read_requests_glides(run):
    api = FakeApi([m("a")])
    run(api)
    # The rewrite's prompt read never needs them.
    assert api.glide_calls == [True, False]
