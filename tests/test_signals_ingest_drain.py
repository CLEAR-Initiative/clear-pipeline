"""Unit tests for the stage-based signal pipeline.

Per-source ingest (factory) feeds shared, source-agnostic drain stages
(classify_group → alert → translate, in stages.py). Mocks the clear-api / S3
boundaries so these run without a live backend. Covers: connector registry +
flags, the ingest factory shape, per-source projection dispatch, the
classify_group drain-loop control flow, and the translation-hash helper.
"""

import json
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.defs.signals import factory, lake, stages
from clear_pipeline.defs.signals.connectors import (
    CONNECTORS,
    CONNECTORS_BY_SOURCE,
    DRAINED_SOURCES,
    ACLEDConnector,
    Darfur24Connector,
    DataminrConnector,
    GDACSConnector,
    IDMCConnector,
    ManualConnector,
    SignalSource,
    SudanWarXConnector,
)
from clear_pipeline.providers.translation_hash import (
    HASH_FIELDS,
    compute_source_hashes,
    stale_fields,
)


def _run():
    # Call the inner drain directly — the outer _drain_signals wraps it in a
    # single-flight Redis lock, which these mocked tests don't exercise.
    return stages._drain_signals_locked(MagicMock())


def _batched(batches):
    def pending(first):
        return batches.pop(0) if batches else []
    return pending


def _recorder(sink):
    def mark(items, status):
        sink.append((status, [i["id"] for i in items]))
        return len(items)
    return mark


@pytest.fixture(autouse=True)
def _no_recomputes():
    """The recompute lane is empty unless a test says otherwise."""
    with patch.object(stages, "pending_recomputes", return_value=[]):
        yield


# ── connector registry + capability flags ────────────────────────────────────

# (polled, drained) per source. idmc is pushed by gx, so it is not polled here.
EXPECTED_FLAGS = {
    "dataminr": (True, True),
    "acled": (True, True),
    "gdacs": (True, True),
    "darfur24": (True, True),
    "idmc": (False, True),
    "dtm": (True, False),
    "manual": (False, True),
    "sudan-war-x": (False, True),
}


def test_registry_flags():
    by_name = {c.source: c for c in CONNECTORS}
    assert all(isinstance(c, SignalSource) for c in CONNECTORS)
    assert {n: (c.polled, c.drained) for n, c in by_name.items()} == EXPECTED_FLAGS
    assert DRAINED_SOURCES == frozenset(n for n, (_, d) in EXPECTED_FLAGS.items() if d)
    assert "idmc" in DRAINED_SOURCES


def test_connectors_by_source_map():
    assert isinstance(CONNECTORS_BY_SOURCE["dataminr"], DataminrConnector)
    assert isinstance(CONNECTORS_BY_SOURCE["acled"], ACLEDConnector)
    assert isinstance(CONNECTORS_BY_SOURCE["gdacs"], GDACSConnector)
    assert isinstance(CONNECTORS_BY_SOURCE["darfur24"], Darfur24Connector)
    assert isinstance(CONNECTORS_BY_SOURCE["manual"], ManualConnector)
    assert isinstance(CONNECTORS_BY_SOURCE["idmc"], IDMCConnector)
    assert isinstance(CONNECTORS_BY_SOURCE["sudan-war-x"], SudanWarXConnector)


# ── to_content_update_input dispatch ──────────────────────────────────────────

def test_non_revising_connectors_return_none_for_content_update():
    dummy_input = {"rawData": {}, "contentHash": "h"}
    dummy_created = {"id": "sig-1"}
    for connector in (DataminrConnector(), ACLEDConnector(), GDACSConnector(), Darfur24Connector()):
        assert connector.to_content_update_input(dummy_input, dummy_created) is None


# ── IDMC: downloaded by gx, drained from its S3 file ────────────────────────

IDMC_INGEST_METHODS = (
    "poll", "external_id", "published_at", "raw_bytes", "api_source_id",
    "to_signal_input", "last_synced", "set_watermark", "post_create",
    "to_content_update_input",
)


def test_idmc_connector_has_no_ingest_methods():
    for name in IDMC_INGEST_METHODS:
        assert not hasattr(IDMCConnector, name), name


def test_idmc_connector_flags():
    assert (IDMCConnector.polled, IDMCConnector.drained) == (False, True)


# A record as gx writes it to raw/idmc/<created_at day>/<idu_id>.json.
IDMC_RECORD = {
    "idu_id": "174447",
    "title": "Clashes in Al Fasher",
    "description": "1,500 people displaced",
    "created_at": "2026-01-06T00:00:00Z",
    "locations_name": "Al Fasher, North Darfur State, Sudan",
    "source_url": "https://example.com/idu",
    "lat": 13.57,
    "lng": 24.74,
}


def _idmc_row(**over):
    row = {
        "id": "sig-idmc-1",
        "externalId": "idmc:174447",
        "source": {"name": "idmc"},
        "title": "Clashes in Al Fasher",
        "publishedAt": "2026-01-06T00:00:00Z",
        "rawS3Key": "raw/idmc/2026-01-06/174447.json",
        "generalLocation": {"name": "North Darfur"},
    }
    row.update(over)
    return row


def _fake_s3(payload: bytes):
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=payload))}
    return s3


def test_project_idmc_reads_its_s3_file():
    s3 = _fake_s3(json.dumps(IDMC_RECORD).encode())
    with patch.object(lake, "s3_client", return_value=s3):
        connector, view = stages._project(_idmc_row())
    s3.get_object.assert_called_once()
    assert s3.get_object.call_args.kwargs["Key"] == "raw/idmc/2026-01-06/174447.json"
    assert isinstance(connector, IDMCConnector)
    assert view.external_id == "174447"
    assert view.title == "Clashes in Al Fasher"
    assert view.description == "1,500 people displaced"
    assert view.location_name == "Al Fasher, North Darfur State, Sudan"
    assert view.url == "https://example.com/idu"
    assert (view.lat, view.lng) == (13.57, 24.74)
    assert view.timestamp == "2026-01-06T00:00:00Z"


def test_idmc_location_falls_back_to_the_row():
    record = {**IDMC_RECORD, "locations_name": None}
    view = IDMCConnector().project(record, _idmc_row())
    assert view.location_name == "North Darfur"


def test_project_idmc_without_s3_key_fails_loudly():
    # gx always records rawS3Key; a row without one goes through the normal
    # failure path (retried, then FAILED).
    with patch.object(lake, "s3_client") as s3, pytest.raises(ValueError, match="no rawS3Key"):
        stages._project(_idmc_row(rawS3Key=None))
    s3.assert_not_called()


def test_project_polled_with_s3_key_still_reads_s3():
    s3 = _fake_s3(b"{}")
    created = {"id": "a1", "source": {"name": "acled"}, "rawS3Key": "raw/acled/2026-01-01/a1.json"}
    with patch.object(lake, "s3_client", return_value=s3), \
         patch.object(ACLEDConnector, "project", return_value="VIEW") as project:
        connector, view = stages._project(created)
    s3.get_object.assert_called_once()
    assert isinstance(connector, ACLEDConnector)
    assert view == "VIEW"
    assert project.call_args.args[0] == {}


def test_project_manual_and_sudan_war_x_never_touch_s3():
    for name in ("manual", "sudan-war-x"):
        with patch.object(lake, "s3_client") as s3:
            _connector, view = stages._project({"id": "m", "source": {"name": name}, "title": "t"})
        s3.assert_not_called()
        assert view.title == "t"


def test_idmc_has_no_ingest_defs():
    assert factory.build_source_assets(IDMCConnector()) == []
    # the polled sources are unchanged
    for c in (DataminrConnector(), ACLEDConnector(), GDACSConnector(), Darfur24Connector()):
        assert len(factory.build_source_assets(c)) == 2


def test_dtm_unchanged():
    dtm = CONNECTORS_BY_SOURCE["dtm"]
    assert dtm.polled and not dtm.drained
    assert len(factory.build_source_assets(dtm)) == 2


def test_loaded_definitions_have_no_raw_idmc_but_keep_the_gx_idmc_assets():
    # Subprocess: loading the defs folder in-process breaks when an earlier
    # test has torn down its DagsterInstance.
    code = (
        "from clear_pipeline.definitions import defs\n"
        "d = defs()\n"
        "print(json.dumps({'keys': sorted(k.to_user_string() for k in d.resolve_all_asset_keys()),"
        " 'sensors': sorted(s.name for s in d.sensors or [])}))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", "import json\n" + code],
        capture_output=True, text=True, check=True,
    ).stdout
    loaded = json.loads(out.strip().splitlines()[-1])
    keys, sensors = set(loaded["keys"]), set(loaded["sensors"])
    assert not any("raw_idmc" in k for k in keys)
    assert "idmc_poll_sensor" not in sensors
    assert {"idmc_bronze", "idmc_reconcile", "idmc_push"} <= keys
    assert "raw_dataminr" in keys


# ── ingest factory (per-source, polled only) ─────────────────────────────────

def test_factory_builds_ingest_for_polled_only():
    def names(defs):
        return {str(getattr(d, "key", getattr(d, "name", "?"))) for d in defs}

    dm = names(factory.build_source_assets(DataminrConnector()))
    assert any("raw_dataminr" in n for n in dm)
    assert any("dataminr_poll_sensor" in n for n in dm)
    assert not any("signals_processed" in n for n in dm)  # drains are shared stages now

    # manual / sudan-war-x are not polled → no ingest defs
    assert factory.build_source_assets(ManualConnector()) == []
    assert factory.build_source_assets(SudanWarXConnector()) == []


def test_raw_key_is_source_date_partitioned_and_slash_safe():
    assert lake.raw_key("acled", "2026-08-13T10:00:00Z", "a/b") == "raw/acled/2026-08-13/a_b.json"
    assert lake.raw_key("gdacs", "", "x").startswith("raw/gdacs/unknown/")


# ── per-source projection dispatch (stages._project) ─────────────────────────

def test_project_manual_from_signal_row():
    created = {
        "id": "m1",
        "source": {"name": "manual"},
        "title": "Reported shelling",
        "description": "analyst note",
        "publishedAt": "2026-08-13T09:00:00Z",
        "originLocation": {"name": "Khartoum"},
        # no rawS3Key — manual has no lake blob
    }
    result = stages._project(created)
    assert result is not None
    connector, view = result
    assert isinstance(connector, ManualConnector)
    assert view.external_id == "m1"
    assert view.title == "Reported shelling"
    assert view.location_name == "Khartoum"


def test_project_sudan_war_x_from_signal_row():
    # Regression for expo-533: X posts pushed via clear-api's POST /api/x/ingest
    # land as NEW `sudan-war-x` rows with no rawS3Key. They used to hit the
    # "unknown_source" branch and be marked FAILED; now they project from the
    # row exactly like manual signals.
    created = {
        "id": "sig-x-1",
        "externalId": "x:2094734567902953601",
        "source": {"name": "sudan-war-x"},
        "title": "RSF shelling reported in Omdurman this morning, several…",
        "description": "RSF shelling reported in Omdurman this morning, several casualties",
        "url": "https://x.com/someone/status/2094734567902953601",
        "publishedAt": "2026-09-02T12:00:00Z",
        "generalLocation": {"name": "Omdurman"},
        "rawData": {"author": {"username": "someone"}, "metrics": {"likes": 3}},
        # no rawS3Key — pushed rows have no lake blob
    }
    result = stages._project(created)
    assert result != "unknown_source"
    connector, view = result
    assert isinstance(connector, SudanWarXConnector)
    assert view.external_id == "sig-x-1"
    assert view.title.startswith("RSF shelling reported in Omdurman")
    assert view.description.endswith("several casualties")
    assert view.timestamp == "2026-09-02T12:00:00Z"
    assert view.location_name == "Omdurman"


def test_project_polled_without_blob_returns_no_blob():
    # Dataminr signal with no rawS3Key → permanent skip reason (legacy Celery row).
    assert stages._project({"id": "s1", "source": {"name": "dataminr"}}) == "no_blob"


def test_project_unknown_source_returns_reason():
    assert stages._project({"id": "x", "source": {"name": "mystery"}}) == "unknown_source"


# ── classify_group drain-loop control flow ───────────────────────────────────

def test_drain_marks_processed_and_terminates_on_empty():
    marked: list[tuple[str, list[str]]] = []
    batches = [[{"id": "s1"}, {"id": "s2"}], []]
    with (
        patch.object(stages, "pending_signals", side_effect=_batched(batches)),
        patch.object(stages, "mark_signals_processed", side_effect=_recorder(marked)),
        patch.object(stages, "_process_one_signal", return_value=stages._PROCESSED),
    ):
        result = _run()

    assert ("PROCESSED", ["s1", "s2"]) in marked
    assert result.metadata["processed"] == 2


def test_drain_all_requeue_stops_without_marking():
    # Every row is a transient requeue (lock contention) — stays NEW, nothing marked.
    with (
        patch.object(stages, "pending_signals", side_effect=lambda first: [{"id": "s1"}]),
        patch.object(stages, "mark_signals_processed") as mark,
        patch.object(stages, "_process_one_signal", return_value=stages._REQUEUE),
    ):
        result = _run()

    assert result.metadata["requeued"] == 1
    for call in mark.call_args_list:
        assert call.args[0] == []  # nothing flipped; loop still terminated


def test_drain_legacy_no_blob_backlog_does_not_deadlock():
    # The cutover bug: the queue head is all legacy no-blob rows. They must be
    # marked PROCESSED (leave the queue) so a real signal behind them still drains,
    # instead of an all-skip first batch breaking the loop forever.
    marked: list[tuple[str, list[str]]] = []
    batches = [[{"id": "legacy1"}, {"id": "legacy2"}], [{"id": "real"}], []]

    def outcome(created, touched_events):
        return stages._PROCESSED if created["id"] == "real" else stages._DROP_DONE

    with (
        patch.object(stages, "pending_signals", side_effect=_batched(batches)),
        patch.object(stages, "mark_signals_processed", side_effect=_recorder(marked)),
        patch.object(stages, "_process_one_signal", side_effect=outcome),
    ):
        result = _run()

    assert ("PROCESSED", ["legacy1", "legacy2"]) in marked  # drops leave the queue
    assert ("PROCESSED", ["real"]) in marked                # signal behind them drained
    assert result.metadata["dropped"] == 2
    assert result.metadata["processed"] == 1


def test_drain_transient_failure_requeues_not_failed():
    # First failure (attempt 1) → requeue (leave NEW) so a transient blip isn't a
    # permanent drop. Nothing is marked.
    marked: list[tuple[str, list[str]]] = []
    with (
        patch.object(stages, "pending_signals", side_effect=_batched([[{"id": "x"}], []])),
        patch.object(stages, "mark_signals_processed", side_effect=_recorder(marked)),
        patch.object(stages, "_process_one_signal", side_effect=RuntimeError("boom")),
        patch.object(stages._redis, "incr", return_value=1),
        patch.object(stages._redis, "expire"),
    ):
        result = _run()

    assert result.metadata["requeued"] == 1
    assert result.metadata["failed"] == 0
    assert all(ids == [] for _status, ids in marked)  # nothing marked terminal


def test_drain_marks_failed_after_max_attempts_and_keeps_going():
    def process(created, touched_events):
        if created["id"] == "bad":
            raise RuntimeError("boom")
        return stages._PROCESSED

    marked: list[tuple[str, list[str]]] = []
    batches = [[{"id": "ok"}, {"id": "bad"}], []]
    with (
        patch.object(stages, "pending_signals", side_effect=_batched(batches)),
        patch.object(stages, "mark_signals_processed", side_effect=_recorder(marked)),
        patch.object(stages, "_process_one_signal", side_effect=process),
        patch.object(stages._redis, "incr", return_value=stages._MAX_SIGNAL_ATTEMPTS),
        patch.object(stages._redis, "expire"),
    ):
        result = _run()

    assert ("PROCESSED", ["ok"]) in marked          # good signal still drained
    assert ("FAILED", ["bad"]) in marked            # bad one FAILED at max attempts
    assert result.metadata["processed"] == 1
    assert result.metadata["failed"] == 1


def test_drain_syncs_event_cards_for_touched_events_once():
    # ADR-0006: the drain refreshes the incident-tier KB card once per event it
    # touched (deduped), via sync_event_cards, after the batch loop.
    def process(created, touched_events):
        touched_events.add(f"event-of-{created['id']}")
        return stages._PROCESSED

    calls: list[list[str]] = []
    with (
        patch.object(stages, "pending_signals", side_effect=_batched([[{"id": "s1"}, {"id": "s2"}], []])),
        patch.object(stages, "mark_signals_processed", side_effect=_recorder([])),
        patch.object(stages, "_process_one_signal", side_effect=process),
        patch.object(stages, "sync_event_cards", side_effect=lambda ids: calls.append(sorted(ids)) or {"synced": len(ids), "skipped": 0}),
    ):
        _run()

    assert calls == [["event-of-s1", "event-of-s2"]]  # one call, both touched events


def test_drain_event_card_sync_failure_never_fails_the_drain():
    def process(created, touched_events):
        touched_events.add("e1")
        return stages._PROCESSED

    with (
        patch.object(stages, "pending_signals", side_effect=_batched([[{"id": "s1"}], []])),
        patch.object(stages, "mark_signals_processed", side_effect=_recorder([])),
        patch.object(stages, "_process_one_signal", side_effect=process),
        patch.object(stages, "sync_event_cards", side_effect=RuntimeError("clear-api down")),
    ):
        result = _run()  # must NOT raise
    assert result.metadata["processed"] == 1


# ── translate drain — no repeated LLM calls on a stuck entity ────────────────

def test_translate_unparseable_entity_invoked_once_per_run():
    from clear_pipeline.providers import translate as tp

    calls = {"n": 0}

    def fake_tu(entity_type, entity_id, canonical):
        calls["n"] += 1
        return tp.UNPARSEABLE  # rows cleared inside translate_and_upsert

    # pending_translations keeps returning the same row; without the per-run `seen`
    # guard the loop would re-invoke the model _MAX_BATCHES times.
    row = {"entityType": "event", "entityId": "e1", "locale": "ar"}
    with (
        patch.object(stages, "pending_translations", side_effect=lambda first: [row]),
        patch.dict(stages._CANONICAL_FETCH, {"event": lambda eid: {"title": "t", "description": "d"}}),
        patch.object(stages, "translate_and_upsert", side_effect=fake_tu),
    ):
        result = stages._drain_translations(MagicMock())

    assert calls["n"] == 1  # invoked once, not 50×
    assert result.metadata["cleared"] == 1


def test_translate_unknown_entity_type_is_dropped():
    with (
        patch.object(stages, "pending_translations",
                     side_effect=lambda first: [{"entityType": "widget", "entityId": "w1", "locale": "ar"}]),
        patch.object(stages, "mark_translated") as mark,
    ):
        result = stages._drain_translations(MagicMock())

    mark.assert_called_with("widget", "w1", "ar")  # row cleared so it can't poison the queue
    assert result.metadata["cleared"] == 1


# ── single-inference: group_signal reuses classify_locally's prediction ──────

def test_classify_locally_carries_taxonomy_for_group_reuse():
    from clear_pipeline.providers.classify import SignalClassification, classify_locally

    # group_signal reads glide (disaster_types[0]) + type_level_2 off the
    # classification instead of re-running the model, so classify_locally must
    # carry them.
    assert "type_level_2" in SignalClassification.model_fields
    c = classify_locally(title="Flooding displaces thousands", description="Heavy rains in the region")
    assert c.type_level_2 is not None
    assert c.disaster_types and c.disaster_types[0]  # glide code


# ── translation-hash helper (must agree with clear-api's TS helper) ──────────

def test_translation_hash_event_fields_and_staleness():
    assert HASH_FIELDS["event"] == ("title", "description")
    h1 = compute_source_hashes("event", {"title": "A", "description": "B"})
    assert set(h1) == {"title", "description"}
    assert all(v.startswith("sha256:") for v in h1.values())
    # unchanged → no stale fields; changed title → only title stale
    assert stale_fields(h1, h1) == []
    h2 = compute_source_hashes("event", {"title": "A2", "description": "B"})
    assert stale_fields(h2, h1) == ["title"]
    # cold start (no stored hashes) → all fields stale
    assert set(stale_fields(h1, None)) == {"title", "description"}


def test_drain_chunks_the_event_card_sync():
    # E5: a large touched set is chunked so one giant call can't time out and
    # lose everything; a chunk failure doesn't stop later chunks.
    def process(created, touched_events):
        touched_events.add(f"e-{created['id']}")
        return stages._PROCESSED

    calls: list[list[str]] = []
    with (
        patch.object(stages, "pending_signals", side_effect=_batched([[{"id": "1"}, {"id": "2"}, {"id": "3"}], []])),
        patch.object(stages, "mark_signals_processed", side_effect=_recorder([])),
        patch.object(stages, "_process_one_signal", side_effect=process),
        patch.object(stages, "_SYNC_CHUNK_SIZE", 2),
        patch.object(stages, "sync_event_cards", side_effect=lambda ids: calls.append(list(ids)) or {"synced": len(ids), "skipped": 0}),
    ):
        _run()

    assert len(calls) == 2  # 3 events, chunk size 2 → 2 chunks
    assert sorted(x for c in calls for x in c) == ["e-1", "e-2", "e-3"]


def test_drain_dedups_same_event_across_signals_and_batches():
    # The dedup rests on the set: two signals in DIFFERENT batches that group into
    # the SAME event → one card, one id, synced once at drain end.
    def process(created, touched_events):
        touched_events.add("e-shared")  # both signals → same event
        return stages._PROCESSED

    calls: list[list[str]] = []
    with (
        patch.object(stages, "pending_signals", side_effect=_batched([[{"id": "s1"}], [{"id": "s2"}], []])),
        patch.object(stages, "mark_signals_processed", side_effect=_recorder([])),
        patch.object(stages, "_process_one_signal", side_effect=process),
        patch.object(stages, "sync_event_cards", side_effect=lambda ids: calls.append(sorted(ids)) or {"synced": len(ids), "skipped": 0}),
    ):
        _run()

    assert calls == [["e-shared"]]  # deduped across both batches → one embed


# ── recompute lane (signals changed after grouping) ──────────────────────────

def _rc(sid, events, revision=1, source="idmc"):
    return {"id": sid, "revision": revision, "source": {"name": source},
            "events": [{"id": e} for e in events]}


def _items_recorder(sink):
    def mark(items, status):
        sink.append((status, [(i["id"], i["revision"]) for i in items]))
        return len(items)
    return mark


def _run_lanes(new_batches, recompute_batches, *, recompute=None, mark=None, incr=1):
    calls: list[tuple] = []
    marked: list = []

    def pending_new(first):
        calls.append(("pending_signals",))
        return new_batches.pop(0) if new_batches else []

    def pending_rc(first):
        calls.append(("pending_recomputes",))
        return recompute_batches.pop(0) if recompute_batches else []

    def recompute_event(event_id, member_text):
        calls.append(("recompute_event", event_id))
        return recompute(event_id) if recompute else False

    with (
        patch.object(stages, "pending_signals", side_effect=pending_new),
        patch.object(stages, "pending_recomputes", side_effect=pending_rc),
        patch.object(stages, "recompute_event", side_effect=recompute_event),
        patch.object(stages, "mark_signals_processed", side_effect=mark or _items_recorder(marked)),
        patch.object(stages, "_process_one_signal", return_value=stages._PROCESSED),
        patch.object(stages, "_sync_event_cards") as sync,
        patch.object(stages._redis, "incr", return_value=incr) as redis_incr,
        patch.object(stages._redis, "expire"),
    ):
        result = _run()
    return result, calls, marked, sync, redis_incr


def test_recompute_lane_recomputes_each_event_once_then_marks_with_revision():
    rows = [_rc("s1", ["e1", "e2"], revision=3), _rc("s2", ["e1"], revision=1)]
    result, calls, marked, sync, _ = _run_lanes([], [rows, []])
    assert [c for c in calls if c[0] == "recompute_event"] == [("recompute_event", "e1"), ("recompute_event", "e2")]
    assert marked == [("PROCESSED", [("s1", 3), ("s2", 1)])]
    assert result.metadata["recomputed_events"] == 2
    assert sync.call_args.args[1] == {"e1", "e2"}


def test_recompute_lane_enqueues_translations_only_for_rewritten_events():
    rows = [_rc("s1", ["e1", "e2"])]
    with patch.object(stages, "_enqueue_translations") as enqueue:
        _run_lanes([], [rows, []], recompute=lambda event_id: event_id == "e1")  # only e1 rewritten
    enqueue.assert_called_once_with("event", "e1")


def test_recompute_lane_failure_still_syncs_the_new_lanes_kb_cards():
    def group(created, touched_events):
        touched_events.add("e-new")
        return stages._PROCESSED

    with (
        patch.object(stages, "pending_signals", side_effect=[[{"id": "n1", "revision": 0}], []]),
        patch.object(stages, "pending_recomputes", side_effect=RuntimeError("clear-api timeout")),
        patch.object(stages, "mark_signals_processed", side_effect=lambda items, status: len(items)),
        patch.object(stages, "_process_one_signal", side_effect=group),
        patch.object(stages, "_sync_event_cards") as sync,
        pytest.raises(RuntimeError, match="clear-api timeout"),
    ):
        _run()
    assert sync.call_args.args[1] == {"e-new"}


def test_new_lane_runs_before_the_recompute_lane():
    _, calls, _, _, _ = _run_lanes([[{"id": "n1", "revision": 0}], []], [[_rc("s1", ["e1"])], []])
    first_rc = calls.index(("pending_recomputes",))
    assert all(c == ("pending_signals",) for c in calls[:first_rc])


def test_recompute_row_without_events_is_just_marked():
    _, calls, marked, _, _ = _run_lanes([], [[_rc("s1", [])], []])
    assert not any(c[0] == "recompute_event" for c in calls)
    assert marked == [("PROCESSED", [("s1", 1)])]


def test_partial_failure_marks_only_fully_recomputed_rows_and_counts_one_attempt():
    def recompute(event_id):
        if event_id == "bad":
            raise RuntimeError("LLM down")
        return False

    rows = [_rc("ok", ["good"]), _rc("mixed", ["good", "bad"])]
    result, _, marked, _, incr = _run_lanes([], [rows, rows, []], recompute=recompute)
    assert marked == [("PROCESSED", [("ok", 1)])]
    incr.assert_called_once_with("signal:attempts:mixed")  # once per run, even if refetched
    assert result.metadata["recompute_failed"] == 0


def test_failing_row_goes_failed_after_max_attempts_through_items():
    def recompute(event_id):
        raise RuntimeError("boom")

    _, _, marked, _, _ = _run_lanes([], [[_rc("s1", ["e1"], revision=2)], []], recompute=recompute,
                                    incr=stages._MAX_SIGNAL_ATTEMPTS)
    assert marked == [("FAILED", [("s1", 2)])]


def test_failing_recomputes_do_not_block_first_grouping():
    def recompute(event_id):
        raise RuntimeError("boom")

    new = [[{"id": f"n{i}", "revision": 0} for i in range(3)], []]
    failing = [_rc(f"r{i}", [f"e{i}"]) for i in range(250)]
    result, _, marked, _, _ = _run_lanes(new, [failing[:200], failing[:200], []], recompute=recompute)
    assert ("PROCESSED", [("n0", 0), ("n1", 0), ("n2", 0)]) in marked
    assert result.metadata["processed"] == 3


def test_llm_budget_defers_events_and_leaves_their_rows_unmarked():
    with patch.object(stages.settings, "signal_max_signals_per_run", 1):
        result, calls, marked, _, incr = _run_lanes(
            [[{"id": "n1", "revision": 0}], []], [[_rc("s1", ["e1"])], []],
        )
    assert not any(c[0] == "recompute_event" for c in calls)
    assert marked == [("PROCESSED", [("n1", 0)])]
    assert result.metadata["recompute_deferred"] == 1
    incr.assert_not_called()


def test_mark_conflicts_are_counted():
    def mark(items, status):
        return 0  # every row changed since it was fetched

    result, _, _, _, _ = _run_lanes([], [[_rc("s1", ["e1"])], []], mark=mark)
    assert result.metadata["mark_conflicts"] == 1


def test_drain_metadata_keys():
    result, *_ = _run_lanes([], [])
    assert set(result.metadata) == {
        "processed", "dropped", "requeued", "failed", "mark_conflicts",
        "recompute_rows", "recomputed_events", "recompute_failed", "recompute_deferred",
    }


# ── other sources are unaffected ─────────────────────────────────────────────

def test_non_idmc_sources_mark_with_revision_0_and_never_recompute():
    rows = [{"id": f"{s}-1", "source": {"name": s}, "revision": 0, "events": []}
            for s in ("acled", "dataminr", "gdacs", "darfur24", "manual", "sudan-war-x")]
    _, calls, marked, _, _ = _run_lanes([rows, []], [[]])
    assert marked == [("PROCESSED", [(r["id"], 0) for r in rows])]
    assert not any(c[0] == "recompute_event" for c in calls)


def test_member_text_for_an_acled_member_comes_from_its_blob():
    blob = json.dumps({"acled_id": "SDN1", "title": "Clashes in El Fasher", "description": "12 killed"}).encode()
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=blob))}
    member = {"id": "a1", "source": {"name": "acled"}, "rawS3Key": "raw/acled/2026-09-01/SDN1.json",
              "title": "db title", "description": "db desc"}
    with patch.object(lake, "s3_client", return_value=s3):
        assert stages._member_text(member) == ("Clashes in El Fasher", "12 killed")
    s3.get_object.assert_called_once()


def test_member_text_falls_back_to_db_fields_without_a_blob():
    from botocore.exceptions import ClientError

    member = {"id": "a1", "source": {"name": "acled"}, "title": "db title", "description": "db desc"}
    with patch.object(lake, "s3_client") as s3:
        assert stages._member_text(member) == ("db title", "db desc")  # no rawS3Key
    s3.assert_not_called()

    missing = MagicMock()
    missing.get_object.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
    with patch.object(lake, "s3_client", return_value=missing):
        assert stages._member_text({**member, "rawS3Key": "raw/acled/x.json"}) == ("db title", "db desc")


def test_member_text_raises_on_other_s3_errors():
    from botocore.exceptions import ClientError

    denied = MagicMock()
    denied.get_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
    member = {"id": "a1", "source": {"name": "acled"}, "rawS3Key": "raw/acled/x.json"}
    with patch.object(lake, "s3_client", return_value=denied), pytest.raises(ClientError):
        stages._member_text(member)
