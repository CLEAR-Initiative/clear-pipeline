"""Unit tests for the hotline-enrichment drain (defs/ground/stages.py).

Mirrors tests/test_signals_ingest_drain.py's approach: patch.object on the
stage module's imported provider functions, and call the inner
_drain_hotline_enrich_locked directly (bypassing the Redis single-flight
lock wrapper) via a MagicMock context. _process_one_message is tested
separately with redis_lock faked, since (unlike _process_one_signal in the
signals tests) its draft-before-classification write ordering is the thing
under test.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from clear_pipeline.defs.ground import stages
from clear_pipeline.defs.ground.schemas import HotlineEnrichment


def _run():
    return stages._drain_hotline_enrich_locked(MagicMock())


def message(id_, *, thread_id="t1", classification=None, **overrides):
    return {
        "id": id_,
        "text": "some hotline text",
        "sentAt": "2026-09-15T10:00:00Z",
        "senderRef": "s_abc123",
        "hasMedia": False,
        "classification": classification,
        "threadId": thread_id,
        **overrides,
    }


def enrichment(**overrides):
    defaults = dict(
        classification="field_report",
        title="Flooding near Nyala",
        severity=3,
        disaster_type="fl",
        uncertainty_marker=None,
    )
    return HotlineEnrichment(**{**defaults, **overrides})


@contextmanager
def _lock_acquired(*_args, **_kwargs):
    yield True


@contextmanager
def _lock_not_acquired(*_args, **_kwargs):
    yield False


class _FakeTransientError(Exception):
    """Stand-in for a TRANSIENT_LLM_ERRORS member — avoids constructing a
    real anthropic/openai exception (awkward constructors) just to test
    the requeue-without-attempts-increment branch."""


# ── drain-loop control flow ───────────────────────────────────────────────


def test_drain_no_active_sources_returns_zero_counts():
    with (
        patch.object(stages, "pipeline_ground_source_ids", return_value=[]),
        patch.object(stages, "make_llm_provider", return_value=MagicMock()),
    ):
        result = _run()
    assert result.metadata == {"processed": 0, "requeued": 0, "failed": 0}


def test_drain_processes_only_unclassified_messages():
    rows = [message("m1", classification=None), message("m2", classification="chatter")]
    with (
        patch.object(stages, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(stages, "make_llm_provider", return_value=MagicMock()),
        patch.object(stages, "ground_messages_for_classification", return_value=rows),
        patch.object(stages, "_process_one_message", return_value=stages._PROCESSED) as process,
    ):
        result = _run()

    assert result.metadata["processed"] == 1
    process.assert_called_once()
    assert process.call_args.args[1]["id"] == "m1"


def test_drain_holds_out_untranscribed_voice_messages():
    rows = [
        message("voice_pending", voiceMediaKeys=["ground/gs1/a.ogg"], transcript=None),
        message("voice_ready", voiceMediaKeys=["ground/gs1/b.ogg"], transcript="we need water"),
        message("text_only"),
    ]
    with (
        patch.object(stages, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(stages, "make_llm_provider", return_value=MagicMock()),
        patch.object(stages, "ground_messages_for_classification", return_value=rows),
        patch.object(stages, "_process_one_message", return_value=stages._PROCESSED) as process,
    ):
        result = _run()

    assert result.metadata["processed"] == 2
    processed_ids = {call.args[1]["id"] for call in process.call_args_list}
    assert processed_ids == {"voice_ready", "text_only"}


def test_drain_transient_error_requeues_without_consuming_attempts():
    with (
        patch.object(stages, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(stages, "make_llm_provider", return_value=MagicMock()),
        patch.object(stages, "ground_messages_for_classification", return_value=[message("m1")]),
        patch.object(stages, "TRANSIENT_LLM_ERRORS", (_FakeTransientError,)),
        patch.object(stages, "_process_one_message", side_effect=_FakeTransientError("boom")),
        patch.object(stages._redis, "incr") as incr,
    ):
        result = _run()

    assert result.metadata == {"processed": 0, "requeued": 1, "failed": 0}
    incr.assert_not_called()


def test_drain_generic_failure_requeues_then_fails_after_max_attempts():
    with (
        patch.object(stages, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(stages, "make_llm_provider", return_value=MagicMock()),
        patch.object(stages, "ground_messages_for_classification", return_value=[message("bad")]),
        patch.object(stages, "_process_one_message", side_effect=RuntimeError("boom")),
        patch.object(stages._redis, "incr", return_value=1),
        patch.object(stages._redis, "expire"),
    ):
        result = _run()
    assert result.metadata == {"processed": 0, "requeued": 1, "failed": 0}

    with (
        patch.object(stages, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(stages, "make_llm_provider", return_value=MagicMock()),
        patch.object(stages, "ground_messages_for_classification", return_value=[message("bad")]),
        patch.object(stages, "_process_one_message", side_effect=RuntimeError("boom")),
        patch.object(stages._redis, "incr", return_value=stages._MAX_MESSAGE_ATTEMPTS),
        patch.object(stages._redis, "expire"),
    ):
        result = _run()
    assert result.metadata == {"processed": 0, "requeued": 0, "failed": 1}


def test_drain_stops_at_per_run_processed_cap():
    rows = [message("m1"), message("m2")]
    with (
        patch.object(stages, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(stages, "make_llm_provider", return_value=MagicMock()),
        patch.object(stages, "ground_messages_for_classification", return_value=rows),
        patch.object(stages, "_MAX_PROCESSED_PER_RUN", 1),
        patch.object(stages, "_process_one_message", return_value=stages._PROCESSED) as process,
    ):
        result = _run()

    assert result.metadata["processed"] == 1
    process.assert_called_once()  # second message left for next run


# ── _process_one_message: write ordering + guard rails ────────────────────


def test_process_one_message_writes_draft_before_classification():
    calls: list[str] = []

    def record_draft(_inputs):
        calls.append("draft")

    def record_classification(_inputs):
        calls.append("classification")

    with (
        patch.object(stages, "redis_lock", _lock_acquired),
        patch.object(stages, "_enrich_one_message", return_value=enrichment()),
        patch.object(stages, "_geoparse_one_message", return_value="loc_1"),
        patch.object(stages, "upsert_ground_thread_drafts", side_effect=record_draft),
        patch.object(stages, "upsert_ground_message_classifications", side_effect=record_classification),
    ):
        outcome = stages._process_one_message(MagicMock(), message("m1", thread_id="t1"))

    assert outcome == stages._PROCESSED
    assert calls == ["draft", "classification"]


def test_process_one_message_skips_missing_thread_id():
    outcome = stages._process_one_message(MagicMock(), message("m1", thread_id=None))
    assert outcome == stages._DROP_FAILED


def test_process_one_message_requeues_on_lock_contention():
    with (
        patch.object(stages, "redis_lock", _lock_not_acquired),
        patch.object(stages, "_enrich_one_message") as enrich_mock,
    ):
        outcome = stages._process_one_message(MagicMock(), message("m1"))

    assert outcome == stages._REQUEUE
    enrich_mock.assert_not_called()


# ── transcript preferred over text once present ────────────────────────────


def test_enrich_one_message_prefers_transcript_over_text():
    with patch.object(stages, "build_hotline_enrich_prompt") as build_prompt:
        stages._enrich_one_message(
            MagicMock(complete_structured=MagicMock(return_value=enrichment())),
            message("m1", text="", transcript="we need water", hasMedia=True),
        )
    assert build_prompt.call_args.args[0] == "we need water"


def test_enrich_one_message_falls_back_to_text_without_transcript():
    with patch.object(stages, "build_hotline_enrich_prompt") as build_prompt:
        stages._enrich_one_message(
            MagicMock(complete_structured=MagicMock(return_value=enrichment())),
            message("m1", text="plain text", transcript=None),
        )
    assert build_prompt.call_args.args[0] == "plain text"


def test_process_one_message_geoparses_the_transcript_when_present():
    with (
        patch.object(stages, "redis_lock", _lock_acquired),
        patch.object(stages, "_enrich_one_message", return_value=enrichment()),
        patch.object(stages, "_geoparse_one_message", return_value="loc_1") as geoparse,
        patch.object(stages, "upsert_ground_thread_drafts"),
        patch.object(stages, "upsert_ground_message_classifications"),
    ):
        stages._process_one_message(
            MagicMock(), message("m1", text="", transcript="flooding in Nyala")
        )
    geoparse.assert_called_once_with("flooding in Nyala")
