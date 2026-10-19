"""sudan-war-x signals get a Sudanese location before they are grouped.

X posts reach clear-api via ``POST /api/x/ingest`` with no location, so the
drain used to group every one into its own location-less event — invisible
under any country filter, and never merged with other posts about the same
strike (grouping keys on district). The drain now resolves a location from the
(mostly Arabic) text, scoped to Sudan, falling back to Sudan itself.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.defs.signals import stages
from clear_pipeline.defs.signals.connectors import (
    LocatingSource,
    ManualConnector,
    SudanWarXConnector,
)
from clear_pipeline.providers import location

SUDAN = {"id": "sd", "name": "Sudan", "level": 0, "parent": None}
CHAD = {"id": "td", "name": "Chad", "level": 0, "parent": None}
WEST_KORDOFAN = {"id": "sd-wk", "name": "West Kordofan", "level": 1, "parent": {"id": "sd"}}
DIBAB = {"id": "sd-wk-dibab", "name": "Al Dibab", "level": 2, "parent": {"id": "sd-wk"}}
KHARTOUM = {"id": "sd-kh", "name": "Khartoum", "level": 1, "parent": {"id": "sd"}}
SUDAN_POINT = {"id": "sd-pt", "name": "Point 12.89, 23.47", "level": 4, "parent": {"id": "sd-wk-dibab"}}
OUADDAI = {"id": "td-ou", "name": "Ouaddai", "level": 1, "parent": {"id": "td"}}
LOCATIONS = [SUDAN, CHAD, WEST_KORDOFAN, DIBAB, KHARTOUM, SUDAN_POINT, OUADDAI]

POST = "مقتل 84 شخصاً في هجومين بطائرات مسيرة في السودان، 80 في منطقة الدبيب بغرب كردفان"


@pytest.fixture(autouse=True)
def _locations():
    location.invalidate_locations_cache()
    with (
        patch.object(location, "get_locations", return_value=LOCATIONS),
        patch.object(location, "get_locations_by_level", return_value=[SUDAN, CHAD]),
    ):
        yield
    location.invalidate_locations_cache()


def _llm_returning(**fields):
    provider = MagicMock()
    provider.complete_structured.return_value = location._LocationResolution(**fields)
    return patch.object(location, "make_llm_provider", return_value=provider), provider


# ── country-scoped resolution ────────────────────────────────────────────────

def test_scoped_locations_are_the_country_and_its_admin_areas_only():
    scoped = {loc["id"] for loc in location._country_scoped_locations("Sudan")}
    # Chad's rows and Sudan's coordinate-named L4 points are left out.
    assert scoped == {"sd", "sd-wk", "sd-wk-dibab", "sd-kh"}


def test_scoped_locations_for_unknown_country_is_empty():
    assert location._country_scoped_locations("Atlantis") == []


def test_scoped_resolution_only_offers_the_country_to_the_llm():
    patcher, provider = _llm_returning(location_type="general", location_id="sd-wk-dibab")
    with patcher:
        result = location.resolve_signal_location(POST, POST, None, country="Sudan")

    assert result["location_id"] == "sd-wk-dibab"
    prompt = provider.complete_structured.call_args.kwargs["user"]
    assert '"Al Dibab": "sd-wk-dibab"' in prompt
    assert "Ouaddai" not in prompt and "Point 12.89" not in prompt


def test_unscoped_resolution_still_offers_every_location():
    patcher, provider = _llm_returning(location_type="general", location_id="td-ou")
    with patcher:
        location.resolve_signal_location("Clashes in Ouaddai", None, None)
    assert "Ouaddai" in provider.complete_structured.call_args.kwargs["user"]


# ── locate_in_country ────────────────────────────────────────────────────────

def test_locate_in_country_returns_the_matched_district():
    patcher, _ = _llm_returning(location_type="general", location_id="sd-wk-dibab")
    with patcher:
        assert location.locate_in_country(POST, POST, "Sudan") == "sd-wk-dibab"


def test_locate_in_country_pins_displacement_to_its_origin():
    patcher, _ = _llm_returning(
        location_type="displacement", origin_id="sd-wk-dibab", destination_id="sd-kh",
    )
    with patcher:
        assert location.locate_in_country(POST, POST, "Sudan") == "sd-wk-dibab"


def test_locate_in_country_falls_back_to_the_country_when_nothing_matches():
    patcher, _ = _llm_returning(location_type="general")
    with patcher:
        assert location.locate_in_country("قصف مستمر", None, "Sudan") == "sd"


def test_locate_in_country_falls_back_when_the_llm_fails():
    provider = MagicMock()
    provider.complete_structured.side_effect = RuntimeError("model down")
    with patch.object(location, "make_llm_provider", return_value=provider):
        assert location.locate_in_country(POST, POST, "Sudan") == "sd"


def test_locate_in_country_drops_ids_outside_the_scope():
    # A hallucinated / out-of-scope id (Chad's) is rejected, not persisted.
    patcher, _ = _llm_returning(location_type="general", location_id="td-ou")
    with patcher:
        assert location.locate_in_country(POST, POST, "Sudan") == "sd"


# ── connectors ───────────────────────────────────────────────────────────────

def test_sudan_war_x_locates_and_manual_does_not():
    assert isinstance(SudanWarXConnector(), LocatingSource)
    # Analyst-created signals carry the analyst's chosen location; never guess.
    assert not isinstance(ManualConnector("manual"), LocatingSource)


# ── drain ────────────────────────────────────────────────────────────────────

def _x_row(**overrides) -> dict:
    row = {
        "id": "sig-x-1",
        "externalId": "x:2112241240762978429",
        "source": {"id": "ds-x", "name": "sudan-war-x"},
        "title": POST[:60] + "…",
        "description": POST,
        "severity": None,
        "casualties": None,
        "publishedAt": "2026-10-19T17:55:29Z",
        "status": "NEW",
        "revision": 0,
        "rawS3Key": None,
        "originLocation": None,
        "destinationLocation": None,
        "generalLocation": None,
        "events": [],
    }
    row.update(overrides)
    return row


@contextmanager
def _drain(rows, *, locate_to="sd-wk-dibab", update_side_effect=None):
    @contextmanager
    def fake_lock(key, **_kwargs):
        yield True

    batches = [rows, []]
    marked: list[tuple[str, list[str]]] = []

    def mark(items, status):
        marked.append((status, [i["id"] for i in items]))
        return len(items)

    def update(signal_id, location_id):
        return {
            "id": signal_id,
            "generalLocation": {"id": location_id, "name": "Al Dibab", "level": 2,
                                "ancestorIds": ["sd", "sd-wk"]},
        }

    with (
        patch.object(stages, "pending_signals", side_effect=lambda first: batches.pop(0) if batches else []),
        patch.object(stages, "pending_recomputes", return_value=[]),
        patch.object(stages, "mark_signals_processed", side_effect=mark),
        patch.object(stages, "redis_lock", side_effect=fake_lock),
        patch.object(stages, "classify_signal", return_value=MagicMock(relevance=0.9)),
        patch.object(stages, "group_signal", return_value={"id": "evt-1"}) as group,
        patch.object(stages, "enqueue_translation"),
        patch.object(stages, "sync_event_cards", return_value={"synced": 1, "skipped": 0}),
        patch.object(stages, "update_signal_location",
                     side_effect=update_side_effect or update) as update_loc,
        patch.object(SudanWarXConnector, "locate", return_value=locate_to) as locate,
        patch.object(stages, "_redis", MagicMock(**{"incr.return_value": 1})),
    ):
        yield {"group": group, "update": update_loc, "locate": locate, "marked": marked}


def test_drain_locates_an_unplaced_x_post_before_grouping():
    with _drain([_x_row()]) as m:
        stages._drain_signals_locked(MagicMock())

    m["update"].assert_called_once_with("sig-x-1", "sd-wk-dibab")
    grouped_row = m["group"].call_args.kwargs["created_signal"]
    assert grouped_row["generalLocation"]["id"] == "sd-wk-dibab"
    assert grouped_row["generalLocation"]["ancestorIds"] == ["sd", "sd-wk"]
    assert ("PROCESSED", ["sig-x-1"]) in m["marked"]


def test_drain_keeps_an_x_post_that_already_has_a_location():
    located = {"id": "sd-kh", "name": "Khartoum", "level": 1, "ancestorIds": ["sd"]}
    with _drain([_x_row(generalLocation=located)]) as m:
        stages._drain_signals_locked(MagicMock())

    m["locate"].assert_not_called()
    m["update"].assert_not_called()
    assert m["group"].call_args.kwargs["created_signal"]["generalLocation"] == located


def test_drain_groups_unlocated_when_nothing_resolves():
    with _drain([_x_row()], locate_to=None) as m:
        stages._drain_signals_locked(MagicMock())

    m["update"].assert_not_called()
    assert m["group"].call_args.kwargs["created_signal"]["generalLocation"] is None
    assert ("PROCESSED", ["sig-x-1"]) in m["marked"]


def test_drain_leaves_the_post_new_when_persisting_the_location_fails():
    # A clear-api blip must not let the post group location-less: it stays NEW
    # and the next drain retries it.
    with _drain([_x_row()], update_side_effect=RuntimeError("clear-api 502")) as m:
        stages._drain_signals_locked(MagicMock())

    m["group"].assert_not_called()
    assert all(not ids for _, ids in m["marked"])
