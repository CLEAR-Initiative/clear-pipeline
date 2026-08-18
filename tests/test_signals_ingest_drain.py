"""Unit tests for the stage-based signal pipeline.

Per-source ingest (factory) feeds shared, source-agnostic drain stages
(classify_group → alert → translate, in stages.py). Mocks the clear-api / S3
boundaries so these run without a live backend. Covers: connector registry +
flags, the ingest factory shape, per-source projection dispatch, the
classify_group drain-loop control flow, and the translation-hash helper.
"""

from unittest.mock import MagicMock, patch

from clear_context_pipeline.defs.signals import factory, lake, stages
from clear_context_pipeline.defs.signals.connectors import (
    CONNECTORS,
    CONNECTORS_BY_SOURCE,
    DRAINED_SOURCES,
    ACLEDConnector,
    Darfur24Connector,
    DataminrConnector,
    GDACSConnector,
    ManualConnector,
    SignalSource,
)
from clear_context_pipeline.providers.translation_hash import (
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
    def mark(ids, status):
        sink.append((status, list(ids)))
    return mark


# ── connector registry + capability flags ────────────────────────────────────

def test_registry_flags_all_drained():
    by_name = {c.source: c for c in CONNECTORS}
    assert all(isinstance(c, SignalSource) for c in CONNECTORS)
    assert {"dataminr", "acled", "gdacs", "darfur24", "manual"} <= set(by_name)
    # every source now feeds the shared stages (darfur24 is no longer a tracer)
    assert all(c.drained for c in CONNECTORS)
    assert DRAINED_SOURCES == frozenset({"dataminr", "acled", "gdacs", "darfur24", "manual"})
    # only manual is non-polled
    assert not by_name["manual"].polled
    assert all(by_name[s].polled for s in ("dataminr", "acled", "gdacs", "darfur24"))


def test_connectors_by_source_map():
    assert isinstance(CONNECTORS_BY_SOURCE["dataminr"], DataminrConnector)
    assert isinstance(CONNECTORS_BY_SOURCE["acled"], ACLEDConnector)
    assert isinstance(CONNECTORS_BY_SOURCE["gdacs"], GDACSConnector)
    assert isinstance(CONNECTORS_BY_SOURCE["darfur24"], Darfur24Connector)
    assert isinstance(CONNECTORS_BY_SOURCE["manual"], ManualConnector)


# ── ingest factory (per-source, polled only) ─────────────────────────────────

def test_factory_builds_ingest_for_polled_only():
    def names(defs):
        return {str(getattr(d, "key", getattr(d, "name", "?"))) for d in defs}

    dm = names(factory.build_source_assets(DataminrConnector()))
    assert any("raw_dataminr" in n for n in dm)
    assert any("dataminr_poll_sensor" in n for n in dm)
    assert not any("signals_processed" in n for n in dm)  # drains are shared stages now

    # manual is not polled → no ingest defs
    assert factory.build_source_assets(ManualConnector()) == []


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

    def outcome(created):
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
    def process(created):
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


# ── translate drain — no repeated LLM calls on a stuck entity ────────────────

def test_translate_unparseable_entity_invoked_once_per_run():
    from clear_context_pipeline.providers import translate as tp

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
    from clear_context_pipeline.providers.classify import SignalClassification, classify_locally

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
