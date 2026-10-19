"""Grouping → recompute parity: for members that didn't change, `recompute_event`
reproduces the casualties / populationAffected grouping wrote, because each
member's stats fallback uses the glide grouping recorded for it. clear-api, the
LLM and Redis are faked with one in-memory store shared by both paths."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from clear_pipeline.providers import event as ev
from clear_pipeline.providers.classify import SignalClassification


class Store:
    """In-memory clear-api: signals, events and their memberships."""

    def __init__(self):
        self.signals: dict[str, dict] = {}
        self.events: dict[str, dict] = {}
        self.members: dict[str, list[str]] = {}
        self.aggregates: dict = {}

    def create_event(self, data):
        eid = f"e{len(self.events) + 1}"
        self.events[eid] = {"id": eid, **{k: v for k, v in data.items() if k != "signalIds"}}
        self.members[eid] = list(data["signalIds"])
        return dict(self.events[eid])

    def update_event(self, eid, data):
        self.members[eid] += [s for s in data.get("signalIds", []) if s not in self.members[eid]]
        self.events[eid].update({k: v for k, v in data.items() if k != "signalIds"})
        return dict(self.events[eid])

    def event_members(self, eid, first=None, *, with_glide=False):
        rows = [dict(self.signals[s]) for s in reversed(self.members[eid])]
        if not with_glide:
            for r in rows:
                r.pop("glideCode", None)
        return rows[:first] if first else rows

    def set_signal_glide_code(self, sid, glide):
        self.signals[sid]["glideCode"] = glide
        return {"id": sid, "glideCode": glide}

    def recompute_state(self, eid):
        return {**self.events[eid], "rewriteMembersHash": None}

    def set_event_aggregates(self, eid, data, members):
        self.aggregates = data
        return {"id": eid}


@contextmanager
def _lock(*_args, **_kwargs):
    yield True


def _llm():
    llm = MagicMock()
    llm.complete_structured.side_effect = lambda **kw: kw["schema"](title="t", description="d")
    return llm


def _fakes(store: Store):
    return (
        patch.object(ev, "create_event", side_effect=store.create_event),
        patch.object(ev, "update_event", side_effect=store.update_event),
        patch.object(ev, "_get_active_events", side_effect=lambda: [dict(e) for e in store.events.values()]),
        patch.object(ev, "_event_matches", return_value=True),  # one district, one level_2
        patch.object(ev, "resolve_signal_admin2", return_value="d1"),
        patch.object(ev, "redis_lock", _lock),
        patch.object(ev, "_redis", MagicMock()),
        patch.object(ev, "make_llm_provider", return_value=_llm()),
        patch.object(ev.graphql, "event_members", side_effect=store.event_members),
        patch.object(ev.graphql, "set_signal_glide_code", side_effect=store.set_signal_glide_code),
        patch.object(ev.graphql, "get_event_recompute_state", side_effect=store.recompute_state),
        patch.object(ev.graphql, "set_event_aggregates", side_effect=store.set_event_aggregates),
    )


def _group(store: Store, sid: str, glides: list[str], *, casualties=None, day=1):
    title, description = f"Fighting near Mellit ({sid})", "Clashes reported in the area"
    ts = f"2026-09-{day:02d}T00:00:00Z"
    store.signals[sid] = {"id": sid, "title": title, "description": description,
                          "casualties": casualties, "severity": 3, "publishedAt": ts}
    classification = SignalClassification(
        disaster_types=glides, relevance=0.9, severity=3, summary=description,
        type_level_1="conflict", type_level_2="battles",
    )
    ev.group_signal(sid, title, description, ts, classification,
                    {**store.signals[sid], "events": []})


def test_recompute_reproduces_grouping_for_unchanged_mixed_glide_members():
    # Battles, mixed level_3: `bo` (q75 10 / pop 10 602) creates the event;
    # `ba` (pop 23 532) joins with an actual casualty count; a third signal has
    # no glide, so grouping defaults it to "ot" (no casualty stat / pop 19 281).
    store = Store()
    fakes = _fakes(store)
    for f in fakes:
        f.start()
    try:
        _group(store, "s1", ["bo"], day=1)
        _group(store, "s2", ["ba"], casualties=5, day=2)
        _group(store, "s3", [], day=3)
        grouped = dict(store.events["e1"])

        ev.recompute_event("e1", lambda m: (m["title"], m["description"]))
    finally:
        for f in fakes:
            f.stop()

    assert [store.signals[s]["glideCode"] for s in ("s1", "s2", "s3")] == ["bo", "ba", "ot"]
    assert (grouped["casualties"], grouped["populationAffected"]) == (15, "23532")
    assert store.aggregates["casualties"] == grouped["casualties"]
    assert store.aggregates["populationAffected"] == grouped["populationAffected"]
