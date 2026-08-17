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
    return stages._drain_signals(MagicMock())


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


def test_project_polled_without_blob_is_skipped():
    # Dataminr signal with no rawS3Key → not ours (e.g. legacy Celery row).
    assert stages._project({"id": "s1", "source": {"name": "dataminr"}}) is None


def test_project_unknown_source_is_skipped():
    assert stages._project({"id": "x", "source": {"name": "mystery"}}) is None


# ── classify_group drain-loop control flow ───────────────────────────────────

def test_drain_marks_processed_and_terminates_on_empty():
    marked: list[tuple[str, list[str]]] = []
    batches = [[{"id": "s1"}, {"id": "s2"}], []]
    with (
        patch.object(stages, "pending_signals", side_effect=_batched(batches)),
        patch.object(stages, "mark_signals_processed", side_effect=_recorder(marked)),
        patch.object(stages, "_process_one_signal", return_value="processed"),
    ):
        result = _run()

    assert ("PROCESSED", ["s1", "s2"]) in marked
    assert result.metadata["processed"] == 2


def test_drain_stops_on_skip_only_batch_no_infinite_loop():
    with (
        patch.object(stages, "pending_signals", side_effect=lambda first: [{"id": "s1"}]),
        patch.object(stages, "mark_signals_processed") as mark,
        patch.object(stages, "_process_one_signal", return_value="skipped"),
    ):
        result = _run()

    assert result.metadata["skipped"] == 1
    for call in mark.call_args_list:
        assert call.args[0] == []  # nothing flipped; loop still terminated


def test_drain_marks_failed_signals_and_keeps_going():
    def process(created):
        if created["id"] == "bad":
            raise RuntimeError("boom")
        return "processed"

    marked: list[tuple[str, list[str]]] = []
    batches = [[{"id": "ok"}, {"id": "bad"}], []]
    with (
        patch.object(stages, "pending_signals", side_effect=_batched(batches)),
        patch.object(stages, "mark_signals_processed", side_effect=_recorder(marked)),
        patch.object(stages, "_process_one_signal", side_effect=process),
    ):
        result = _run()

    assert ("PROCESSED", ["ok"]) in marked
    assert ("FAILED", ["bad"]) in marked
    assert result.metadata["processed"] == 1
    assert result.metadata["failed"] == 1


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
