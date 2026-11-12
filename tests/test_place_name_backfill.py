"""The place-name backfill (clear_pipeline.backfill.place_names).

`plan` locates past signals again and writes a file; `apply` writes the file
to clear-api. The tests hold to three promises:

- `plan` writes nothing: no location is created, no signal moves;
- a signal moves only to a finer place inside its present location, and never
  when a person corrected it;
- a name enters the queue only when enough signals name it.
"""

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from shapely.geometry import box

from clear_pipeline.backfill import place_names as backfill
from clear_pipeline.providers import location
from clear_pipeline.providers.geoparser import GeoparseResult

SUDAN = {"id": "sd", "name": "Sudan", "level": 0, "parent": None}
WEST_KORDOFAN = {"id": "sd-wk", "name": "West Kordofan", "level": 1, "parent": {"id": "sd"}}
DIBAB = {"id": "sd-wk-dibab", "name": "Al Dibab", "level": 2, "parent": {"id": "sd-wk"}}
KHARTOUM = {"id": "sd-kh", "name": "Khartoum", "level": 1, "parent": {"id": "sd"}}
BAHRI = {"id": "sd-kh-bahri", "name": "Bahri", "level": 2, "parent": {"id": "sd-kh"}}
LOCATIONS = [SUDAN, WEST_KORDOFAN, DIBAB, KHARTOUM, BAHRI]
INDEX = backfill.LocationIndex(LOCATIONS)

POST = "مقتل 84 شخصاً في هجومين بطائرات مسيرة، 80 في منطقة الدبيب بغرب كردفان"
WK_SHAPE = box(27.0, 10.0, 30.0, 13.0)


def at(loc: dict) -> dict:
    return {"id": loc["id"], "name": loc["name"], "level": loc["level"], "ancestorIds": []}


def signal(id_="s1", *, general=WEST_KORDOFAN, published="2026-09-10T08:00:00.000Z", **overrides) -> dict:
    return {
        "id": id_,
        "externalId": f"x:{id_}",
        "title": POST[:60],
        "description": POST,
        "publishedAt": published,
        "url": None,
        "rawS3Key": None,
        "retracted": False,
        "locationCorrectedAt": None,
        "locationChallenge": None,
        "source": {"id": "src-x", "name": "sudan-war-x"},
        "originLocation": None,
        "destinationLocation": None,
        "generalLocation": at(general) if general else None,
        **overrides,
    }


# ── the country's locations ──────────────────────────────────────────────────

def test_inside_reads_the_parent_chain():
    assert INDEX.inside("sd-wk-dibab", "sd-wk")
    assert INDEX.inside("sd-wk-dibab", "sd")
    assert INDEX.inside("sd-wk", "sd-wk")
    assert not INDEX.inside("sd-wk-dibab", "sd-kh")
    assert not INDEX.inside("unknown", "sd")


def test_state_of_is_none_for_the_country():
    assert INDEX.state_of("sd-wk-dibab") == "sd-wk"
    assert INDEX.state_of("sd-wk") == "sd-wk"
    assert INDEX.state_of("sd") is None


# ── which signals ────────────────────────────────────────────────────────────

@pytest.mark.parametrize(("overrides", "reason"), [
    ({}, None),
    ({"generalLocation": at(SUDAN)}, None),
    ({"retracted": True}, "retracted"),
    ({"locationCorrectedAt": "2026-09-11T00:00:00.000Z"}, "corrected_by_a_person"),
    ({"locationChallenge": {"id": "c1"}}, "open_location_challenge"),
    ({"originLocation": at(KHARTOUM)}, "has_origin_or_destination"),
    ({"destinationLocation": at(KHARTOUM)}, "has_origin_or_destination"),
    ({"generalLocation": None}, "no_location"),
    ({"generalLocation": at(DIBAB)}, "district_or_finer"),
])
def test_skip_reason(overrides, reason):
    assert backfill.skip_reason(signal(**overrides)) == reason


def test_candidates_are_the_source_and_the_dates_asked_for_newest_first():
    rows = [
        signal("old", published="2026-07-31T23:59:59.000Z"),
        signal("first", published="2026-08-01T00:00:00.000Z"),
        signal("newest", published="2026-09-30T10:00:00.000Z"),
        signal("after", published="2026-10-01T00:00:00.000Z"),
        signal("other", source={"id": "d", "name": "dataminr"}),
        signal("fine", general=DIBAB),
        signal("fixed", locationCorrectedAt="2026-09-11T00:00:00.000Z"),
    ]
    chosen, left_out = backfill.choose_candidates(
        rows, sources={"sudan-war-x"},
        since=datetime(2026, 8, 1, tzinfo=timezone.utc), until=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )
    assert [r["id"] for r in chosen] == ["newest", "first"]
    # Another source and other dates are not counted: they were not asked for.
    assert dict(left_out) == {"district_or_finer": 1, "corrected_by_a_person": 1}


def test_parse_time_reads_iso_and_epoch_milliseconds():
    expected = datetime(2026, 9, 10, 8, 0, tzinfo=timezone.utc)
    assert backfill.parse_time("2026-09-10T08:00:00.000Z") == expected
    assert backfill.parse_time(str(int(expected.timestamp() * 1000))) == expected
    assert backfill.parse_time("2026-09-10") == datetime(2026, 9, 10, tzinfo=timezone.utc)
    assert backfill.parse_time("") is None and backfill.parse_time("soon") is None


# ── the outcome for one signal ───────────────────────────────────────────────

POINT = {"name": "Al Dibab", "lat": 11.5, "lng": 28.5, "kind": "admin"}


def test_a_point_inside_the_present_state_is_a_move():
    hits = [backfill._Hit("planned-point:1", "sd-wk", "Al Dibab")]
    outcome, proposed = backfill.decide(at(WEST_KORDOFAN), "planned-point:1", hits, {"planned-point:1": POINT}, INDEX)
    assert outcome == "move"
    assert proposed["kind"] == "point" and proposed["point"] == POINT and proposed["areaId"] == "sd-wk"


def test_a_point_in_another_state_is_not_a_move():
    # The signal sits in Khartoum; the locator now reads West Kordofan.
    hits = [backfill._Hit("planned-point:1", "sd-wk", "Al Dibab")]
    outcome, _ = backfill.decide(at(KHARTOUM), "planned-point:1", hits, {"planned-point:1": POINT}, INDEX)
    assert outcome == "differs"


@pytest.mark.parametrize(("current", "located", "outcome"), [
    (SUDAN, "sd-wk", "move"),            # country → state
    (SUDAN, "sd-wk-dibab", "move"),      # country → district
    (WEST_KORDOFAN, "sd-wk-dibab", "move"),
    (WEST_KORDOFAN, "sd-wk", "same"),
    (WEST_KORDOFAN, "sd", "differs"),    # coarser
    (WEST_KORDOFAN, "sd-kh", "differs"), # another state
    (WEST_KORDOFAN, "sd-kh-bahri", "differs"),
    (WEST_KORDOFAN, "gone", "differs"),  # a location the index does not know
    (WEST_KORDOFAN, None, "no_location"),
])
def test_an_area_answer(current, located, outcome):
    assert backfill.decide(at(current), located, [], {}, INDEX)[0] == outcome


def test_an_alias_location_inside_the_present_state_is_a_move():
    # A landmark a reviewer mapped the spelling to; made after the index was read.
    hits = [backfill._Hit("landmark-9", "sd-wk", "Al-Laeit")]
    outcome, proposed = backfill.decide(at(WEST_KORDOFAN), "landmark-9", hits, {}, INDEX)
    assert outcome == "move"
    assert proposed == {"kind": "alias", "locationId": "landmark-9", "name": "Al-Laeit", "level": None,
                        "areaId": "sd-wk", "areaName": "West Kordofan"}


# ── plan: the real locator, and nothing written ──────────────────────────────

@contextmanager
def locator(*, place_name="Al Dibab", location_id="sd-wk", hit=None, known_elsewhere=False):
    """The real `locate_in_country`, with the model, the gazetteer and
    clear-api replaced. `promote` is clear-api's landmark mutation."""
    provider = MagicMock()
    provider.complete_structured.side_effect = lambda *, system, user, schema: (
        schema(location_type="general", location_id=location_id, place_name=place_name)
        if schema is location._LocationResolution else schema()
    )

    def geocode(name, *, expected_country_codes=None, accept=None, arabic_name=None,
                within_location_id=None, trace=None):
        if hit and accept(hit.lat, hit.lng):
            return hit
        return None

    promote = MagicMock(return_value={"locationId": "l4-real", "reused": False, "abortedReason": None})
    geometry = {"type": "Polygon", "coordinates": [list(WK_SHAPE.exterior.coords)]}
    location.invalidate_locations_cache()
    with (
        patch.object(location, "get_locations", return_value=LOCATIONS),
        patch.object(location, "get_locations_by_level", return_value=[SUDAN]),
        patch.object(location, "make_llm_provider", return_value=provider),
        patch.object(location, "geocode_place_name", side_effect=geocode),
        patch.object(location, "gazetteer_knows", return_value=known_elsewhere),
        patch.object(location, "named_places", return_value=[]),
        patch.object(location, "find_or_create_landmark_l4", promote),
        patch.object(location, "get_location_with_geometry", return_value={"id": "sd-wk", "geometry": geometry}),
    ):
        yield promote
    location.invalidate_locations_cache()


def geocoded(lat, lng) -> GeoparseResult:
    return GeoparseResult(
        candidate="Al Dibab", kind="admin", field="body", lat=lat, lng=lng, country_code="sd",
        osm_class=None, osm_type="geonames:PPL", importance=1.0, display_name="Al Dibab",
    )


def test_plan_finds_a_point_and_creates_no_location():
    with (
        locator(hit=geocoded(11.5, 28.5)) as promote,
        patch.object(backfill, "get_signal_for_relocate", return_value=signal()),
        backfill.planning() as points,
    ):
        entry = backfill.plan_signal("s1", points, INDEX)

    promote.assert_not_called()
    assert entry["outcome"] == "move"
    assert entry["current"] == {"id": "sd-wk", "name": "West Kordofan", "level": 1}
    assert entry["proposed"]["point"] == {"name": "Al Dibab", "lat": 11.5, "lng": 28.5, "kind": "admin"}
    assert entry["proposed"]["areaId"] == "sd-wk"
    assert entry["misses"] == []
    json.dumps(entry)  # the plan file holds it as it is


def test_plan_keeps_the_name_the_locator_could_not_place():
    with (
        locator(hit=None, known_elsewhere=True),
        patch.object(backfill, "get_signal_for_relocate", return_value=signal()),
        backfill.planning() as points,
    ):
        entry = backfill.plan_signal("s1", points, INDEX)

    assert entry["outcome"] == "same"
    assert entry["misses"] == [{
        "name": "Al Dibab", "nameAr": None, "countryCode": "SD", "areaId": "sd-wk",
        "areaName": "West Kordofan", "stateId": "sd-wk", "reason": "OUTSIDE_AREA",
    }]


def test_plan_pays_for_no_model_call_on_a_signal_it_must_not_touch():
    corrected = signal(locationChallenge={"id": "c1"})
    with (
        locator() as promote,
        patch.object(location, "make_llm_provider") as provider,
        patch.object(backfill, "get_signal_for_relocate", return_value=corrected),
        backfill.planning() as points,
    ):
        entry = backfill.plan_signal("s1", points, INDEX)

    assert (entry["outcome"], entry["reason"]) == ("skipped", "open_location_challenge")
    provider.assert_not_called()
    promote.assert_not_called()


def test_planning_puts_the_locator_back():
    geocode, promote = location._geocode_within, location.find_or_create_landmark_l4
    with backfill.planning():
        assert location._geocode_within is not geocode
        assert location.find_or_create_landmark_l4 is not promote
    assert location._geocode_within is geocode
    assert location.find_or_create_landmark_l4 is promote


# ── the queue threshold ──────────────────────────────────────────────────────

def miss(name, *, state="sd-wk", area="sd-wk", reason="NO_MATCH", name_ar=None) -> dict:
    return {"name": name, "nameAr": name_ar, "countryCode": "SD", "areaId": area,
            "areaName": "West Kordofan", "stateId": state, "reason": reason}


def planned(id_, *, outcome="same", misses=(), proposed=None, current=WEST_KORDOFAN) -> dict:
    return {"type": "signal", "signalId": id_, "source": "sudan-war-x",
            "publishedAt": "2026-09-10T08:00:00.000Z", "current": at(current),
            "outcome": outcome, "proposed": proposed, "misses": list(misses)}


def test_spellings_that_differ_in_case_accents_and_punctuation_share_a_row():
    assert backfill.name_key("Al-Laeit  Jar Al-Nabi") == backfill.name_key("al laéit jar al nabi")
    assert backfill.name_key("اللعيت") == "اللعيت"


def test_a_name_counts_once_per_signal_and_per_state():
    entries = {e["signalId"]: e for e in [
        planned("a", misses=[miss("Azwag Tibba"), miss("azwag tibba")]),
        planned("b", misses=[miss("Azwag-Tibba")]),
        planned("c", misses=[miss("Azwag Tibba", state="sd-kh")]),
    ]}
    named = backfill.signals_per_name(entries)
    assert named[("SD", "azwag tibba", "sd-wk")] == {"a", "b"}
    assert named[("SD", "azwag tibba", "sd-kh")] == {"c"}


# ── apply ────────────────────────────────────────────────────────────────────

@contextmanager
def clear_api(row=None, *, promo=None, recorded_status="OPEN"):
    with (
        patch.object(backfill, "get_signal_for_relocate", return_value=row) as read,
        patch.object(backfill, "find_or_create_landmark_l4",
                     return_value=promo or {"locationId": "l4-real", "abortedReason": None}) as promote,
        patch.object(backfill, "update_signal_location") as update,
        patch.object(backfill, "record_unresolved_place_name",
                     return_value={"id": "row-1", "status": recorded_status, "signalCount": 2}) as record,
    ):
        yield {"read": read, "promote": promote, "update": update, "record": record}


POINT_MOVE = planned("s1", outcome="move", proposed={"kind": "point", "point": POINT, "areaId": "sd-wk"})
AREA_MOVE = planned("s1", outcome="move", proposed={"kind": "area", "locationId": "sd-wk-dibab"})


def test_apply_creates_the_landmark_and_moves_the_signal():
    with clear_api(signal()) as api:
        assert backfill.apply_move(POINT_MOVE) == "moved"
    api["promote"].assert_called_once_with(**POINT)
    api["update"].assert_called_once_with("s1", "l4-real")


def test_apply_moves_to_an_area_without_a_landmark():
    with clear_api(signal()) as api:
        assert backfill.apply_move(AREA_MOVE) == "moved"
    api["promote"].assert_not_called()
    api["update"].assert_called_once_with("s1", "sd-wk-dibab")


@pytest.mark.parametrize(("row", "reason"), [
    (None, "not_found"),
    (signal(locationCorrectedAt="2026-11-01T00:00:00.000Z"), "corrected_by_a_person"),
    (signal(locationChallenge={"id": "c1"}), "open_location_challenge"),
    (signal(retracted=True), "retracted"),
    (signal(general=DIBAB), "district_or_finer"),
    (signal(general=KHARTOUM), "location_changed_since_the_plan"),
])
def test_apply_leaves_a_signal_that_changed_since_the_plan(row, reason):
    with clear_api(row) as api:
        assert backfill.apply_move(POINT_MOVE) == reason
    api["promote"].assert_not_called()
    api["update"].assert_not_called()


def test_apply_leaves_the_signal_when_the_point_gives_no_location():
    with clear_api(signal(), promo={"locationId": None, "abortedReason": "different_a2"}) as api:
        assert backfill.apply_move(POINT_MOVE) == "point_gave_no_location"
    api["update"].assert_not_called()


def test_apply_records_a_name_with_the_date_of_the_post():
    entry = planned("a", misses=[miss("Azwag Tibba", name_ar="ازوق تبة")])
    with clear_api(signal("a")) as api:
        result = backfill.apply_entry(entry, move=False, misses=entry["misses"])

    api["record"].assert_called_once_with(
        signal_id="a", name="Azwag Tibba", name_ar="ازوق تبة", country_code="SD",
        area_location_id="sd-wk", reason="NO_MATCH", seen_at="2026-09-10T08:00:00+00:00",
    )
    assert result == {"type": "applied", "signalId": "a", "recorded": [
        {"name": "Azwag Tibba", "key": ["SD", "azwag tibba", "sd-wk"], "status": "OPEN"},
    ]}
    api["update"].assert_not_called()  # no move was asked for


# ── the files ────────────────────────────────────────────────────────────────

def write_plan(path, entries, *, host="dev-api.example"):
    lines = [{"type": "header", "host": host, "country": "Sudan", "sources": ["sudan-war-x"],
              "since": "2026-08-01T00:00:00+00:00", "until": None}, *entries]
    path.write_text("".join(json.dumps(line, ensure_ascii=False) + "\n" for line in lines), encoding="utf-8")


def test_the_last_line_for_a_signal_wins(tmp_path):
    plan = tmp_path / "plan.jsonl"
    write_plan(plan, [
        {"type": "signal", "signalId": "a", "outcome": "error", "error": "timeout"},
        planned("a", outcome="same"),
    ])
    _, entries = backfill.read_plan(plan)
    assert entries["a"]["outcome"] == "same"


def test_apply_refuses_a_plan_made_against_another_host(tmp_path, monkeypatch):
    plan = tmp_path / "plan.jsonl"
    write_plan(plan, [AREA_MOVE], host="dev-api.example")
    monkeypatch.setenv("CLEAR_API_URL", "https://api.example/graphql")
    with clear_api(signal()) as api, pytest.raises(SystemExit, match="made against dev-api.example"):
        backfill.main(["apply", "--plan", str(plan)])
    api["update"].assert_not_called()


def test_apply_writes_each_signal_once_across_two_runs(tmp_path, monkeypatch, capsys):
    plan = tmp_path / "plan.jsonl"
    write_plan(plan, [
        {**AREA_MOVE, "misses": [miss("Azwag Tibba")]},
        planned("s2", misses=[miss("Azwag Tibba")]),
        planned("s3", misses=[miss("Once Only")]),
    ])
    monkeypatch.setenv("CLEAR_API_URL", "https://dev-api.example/graphql")
    with clear_api(signal()) as api:
        assert backfill.main(["apply", "--plan", str(plan)]) == 0
        assert backfill.main(["apply", "--plan", str(plan)]) == 0

    api["update"].assert_called_once_with("s1", "sd-wk-dibab")
    # "Once Only" has one signal: under the threshold of two.
    assert [c.kwargs["signal_id"] for c in api["record"].call_args_list] == ["s1", "s2"]
    out = capsys.readouterr().out
    assert "Applying 2 signals" in out and "moved: 1" in out
    assert "Applying 0 signals to dev-api.example (1 moves and 2 names done before)" in out


def test_a_lower_threshold_later_records_only_the_new_names(tmp_path, monkeypatch):
    plan = tmp_path / "plan.jsonl"
    write_plan(plan, [
        {**AREA_MOVE, "misses": [miss("Azwag Tibba"), miss("Once Only")]},
        planned("s2", misses=[miss("Azwag Tibba")]),
    ])
    monkeypatch.setenv("CLEAR_API_URL", "https://dev-api.example/graphql")
    with clear_api(signal()) as api:
        assert backfill.main(["apply", "--plan", str(plan)]) == 0
        assert backfill.main(["apply", "--plan", str(plan), "--min-signals", "1"]) == 0

    # The move is not tried again, and no name is recorded twice.
    api["update"].assert_called_once_with("s1", "sd-wk-dibab")
    assert [(c.kwargs["signal_id"], c.kwargs["name"]) for c in api["record"].call_args_list] == [
        ("s1", "Azwag Tibba"), ("s2", "Azwag Tibba"), ("s1", "Once Only"),
    ]


def test_a_signal_that_fails_is_tried_again_on_the_next_run(tmp_path, monkeypatch, capsys):
    plan = tmp_path / "plan.jsonl"
    write_plan(plan, [AREA_MOVE])
    monkeypatch.setenv("CLEAR_API_URL", "https://dev-api.example/graphql")
    with clear_api(signal()) as api:
        api["update"].side_effect = [RuntimeError("clear-api 502"), None]
        assert backfill.main(["apply", "--plan", str(plan)]) == 1
        assert backfill.main(["apply", "--plan", str(plan)]) == 0
    assert api["update"].call_count == 2
    assert "1 signals failed" in capsys.readouterr().out


def test_a_last_line_cut_short_is_removed_before_the_next_write(tmp_path):
    log = tmp_path / "plan.jsonl.applied"
    log.write_bytes(b'{"signalId": "a", "move": "moved"}\n{"signalId": "b", "mo')

    assert backfill.read_lines(log) == [{"signalId": "a", "move": "moved"}]
    assert log.read_bytes().endswith(b'"mo')  # reading alone changes nothing
    assert backfill.read_lines(log, repair=True) == [{"signalId": "a", "move": "moved"}]
    backfill._Lines(log).write({"signalId": "b", "move": "moved"})
    assert [line["signalId"] for line in backfill.read_lines(log)] == ["a", "b"]


def test_a_whole_last_line_with_no_newline_is_kept(tmp_path):
    log = tmp_path / "plan.jsonl.applied"
    log.write_bytes(b'{"signalId": "a"}\n{"signalId": "b"}')

    assert [line["signalId"] for line in backfill.read_lines(log, repair=True)] == ["a", "b"]
    backfill._Lines(log).write({"signalId": "c"})
    assert [line["signalId"] for line in backfill.read_lines(log)] == ["a", "b", "c"]


def test_a_bad_line_in_the_middle_stops_the_command(tmp_path):
    plan = tmp_path / "plan.jsonl"
    plan.write_bytes(b'{"type": "header"}\n{"signalId": "a", "outc\n{"signalId": "b"}\n')
    with pytest.raises(SystemExit, match="line 2: not valid JSON"):
        backfill.read_lines(plan, repair=True)
    assert plan.read_bytes().count(b"\n") == 3  # left as it was


def test_summary_counts_moves_and_the_names_that_enter_the_queue(tmp_path, capsys):
    plan = tmp_path / "plan.jsonl"
    write_plan(plan, [
        {**POINT_MOVE, "proposed": {**POINT_MOVE["proposed"], "level": 4}, "misses": [miss("Azwag Tibba")]},
        planned("s2", misses=[miss("Azwag Tibba", name_ar="ازوق تبة")]),
        planned("s3", misses=[miss("Once Only")]),
    ])
    assert backfill.main(["summary", "--plan", str(plan)]) == 0
    out = capsys.readouterr().out
    assert "move: 1" in out and "same: 2" in out
    assert "level 1 → level 4: 1" in out
    assert "Unplaced names: 2, named by 3 signals." in out
    assert "named by 2 signals or more: 1 names, 2 signals" in out
    assert "   2  Azwag Tibba" in out and "Once Only" not in out


def test_apply_moves_signals_inside_one_replay_run(tmp_path, monkeypatch):
    """clear-api logs a move as a replay only when the client says so
    (clear-api ADR-0020). Every move of a plan belongs to one run."""
    from clear_pipeline.providers import clear_api as client

    plan = tmp_path / "plan.jsonl"
    write_plan(plan, [AREA_MOVE])
    monkeypatch.setenv("CLEAR_API_URL", "https://dev-api.example/graphql")
    seen: list[tuple[str, str | None]] = []
    with clear_api(signal()) as api:
        api["update"].side_effect = lambda *_a: seen.append((client._run_kind.get(), client._run_id.get()))
        assert backfill.main(["apply", "--plan", str(plan)]) == 0
    assert seen == [("REPLAY", "place-names-backfill:plan.jsonl")]
    assert client._run_kind.get() == "LIVE"  # the block ends with the run


def test_a_move_that_a_person_held_back_is_not_applied(tmp_path, monkeypatch, capsys):
    plan = tmp_path / "plan.jsonl"
    write_plan(plan, [{**AREA_MOVE, "outcome": "held", "misses": [miss("Azwag Tibba")]},
                      planned("s2", misses=[miss("Azwag Tibba")])])
    monkeypatch.setenv("CLEAR_API_URL", "https://dev-api.example/graphql")
    with clear_api(signal()) as api:
        assert backfill.main(["apply", "--plan", str(plan)]) == 0
        assert backfill.main(["summary", "--plan", str(plan)]) == 0

    api["update"].assert_not_called()
    assert api["record"].call_count == 2  # its name is still recorded
    assert "held: 1" in capsys.readouterr().out
