"""Unit tests for the hotline voice-note transcription drain
(defs/ground/transcribe.py).

Mirrors tests/test_ground_hotline_enrich.py's approach: patch.object on the
module's imported provider functions, call the inner
_drain_ground_transcribe_locked directly (bypassing the Redis single-flight
lock wrapper) via a MagicMock context.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from clear_pipeline.defs.ground import transcribe


def _run():
    return transcribe._drain_ground_transcribe_locked(MagicMock())


def message(id_, *, voice_media_keys=None, transcript=None, **overrides):
    return {
        "id": id_,
        "text": "",
        "sentAt": "2026-09-15T10:00:00Z",
        "senderRef": "s_abc123",
        "hasMedia": bool(voice_media_keys),
        "voiceMediaKeys": voice_media_keys or [],
        "transcript": transcript,
        "classification": None,
        "threadId": "t1",
        **overrides,
    }


@contextmanager
def _lock_acquired(*_args, **_kwargs):
    yield True


@contextmanager
def _lock_not_acquired(*_args, **_kwargs):
    yield False


class _FakeTransientError(Exception):
    """Stand-in for a TRANSIENT_LLM_ERRORS member — avoids constructing a
    real openai exception (awkward constructor) just to test the
    requeue-without-attempts-increment branch."""


# ── drain-loop control flow ───────────────────────────────────────────────


def test_drain_no_active_sources_returns_zero_counts():
    with patch.object(transcribe, "pipeline_ground_source_ids", return_value=[]):
        result = _run()
    assert result.metadata == {"processed": 0, "requeued": 0, "failed": 0}


def test_drain_processes_only_untranscribed_voice_messages():
    rows = [
        message("no_voice"),
        message("already_transcribed", voice_media_keys=["a.ogg"], transcript="hello"),
        message("pending", voice_media_keys=["b.ogg"], transcript=None),
    ]
    with (
        patch.object(transcribe, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(transcribe, "ground_messages_for_classification", return_value=rows),
        patch.object(
            transcribe, "_process_one_message", return_value=transcribe._PROCESSED
        ) as process,
    ):
        result = _run()

    assert result.metadata["processed"] == 1
    process.assert_called_once()
    assert process.call_args.args[0]["id"] == "pending"


def test_drain_transient_error_requeues_without_consuming_attempts():
    with (
        patch.object(transcribe, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(
            transcribe,
            "ground_messages_for_classification",
            return_value=[message("m1", voice_media_keys=["a.ogg"])],
        ),
        patch.object(transcribe, "TRANSIENT_LLM_ERRORS", (_FakeTransientError,)),
        patch.object(transcribe, "_process_one_message", side_effect=_FakeTransientError("boom")),
        patch.object(transcribe._redis, "incr") as incr,
    ):
        result = _run()

    assert result.metadata == {"processed": 0, "requeued": 1, "failed": 0}
    incr.assert_not_called()


def test_drain_generic_failure_requeues_then_fails_after_max_attempts():
    with (
        patch.object(transcribe, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(
            transcribe,
            "ground_messages_for_classification",
            return_value=[message("bad", voice_media_keys=["a.ogg"])],
        ),
        patch.object(transcribe, "_process_one_message", side_effect=RuntimeError("boom")),
        patch.object(transcribe._redis, "incr", return_value=1),
        patch.object(transcribe._redis, "expire"),
    ):
        result = _run()
    assert result.metadata == {"processed": 0, "requeued": 1, "failed": 0}

    with (
        patch.object(transcribe, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(
            transcribe,
            "ground_messages_for_classification",
            return_value=[message("bad", voice_media_keys=["a.ogg"])],
        ),
        patch.object(transcribe, "_process_one_message", side_effect=RuntimeError("boom")),
        patch.object(transcribe._redis, "incr", return_value=transcribe._MAX_MESSAGE_ATTEMPTS),
        patch.object(transcribe._redis, "expire"),
    ):
        result = _run()
    assert result.metadata == {"processed": 0, "requeued": 0, "failed": 1}


def test_drain_stops_at_per_run_processed_cap():
    rows = [
        message("m1", voice_media_keys=["a.ogg"]),
        message("m2", voice_media_keys=["b.ogg"]),
    ]
    with (
        patch.object(transcribe, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(transcribe, "ground_messages_for_classification", return_value=rows),
        patch.object(transcribe, "_MAX_PROCESSED_PER_RUN", 1),
        patch.object(
            transcribe, "_process_one_message", return_value=transcribe._PROCESSED
        ) as process,
    ):
        result = _run()

    assert result.metadata["processed"] == 1
    process.assert_called_once()  # second message left for next run


# ── _process_one_message ────────────────────────────────────────────────


def test_process_one_message_writes_the_transcript():
    with (
        patch.object(transcribe, "redis_lock", _lock_acquired),
        patch.object(transcribe, "_transcribe_one_message", return_value="we need water"),
        patch.object(transcribe, "upsert_ground_message_transcripts") as upsert,
    ):
        outcome = transcribe._process_one_message(message("m1", voice_media_keys=["a.ogg"]))

    assert outcome == transcribe._PROCESSED
    upsert.assert_called_once_with([{"messageId": "m1", "transcript": "we need water"}])


def test_process_one_message_requeues_on_lock_contention():
    with (
        patch.object(transcribe, "redis_lock", _lock_not_acquired),
        patch.object(transcribe, "_transcribe_one_message") as transcribe_mock,
    ):
        outcome = transcribe._process_one_message(message("m1", voice_media_keys=["a.ogg"]))

    assert outcome == transcribe._REQUEUE
    transcribe_mock.assert_not_called()


# ── _transcribe_one_message: S3 fetch + Whisper + join ─────────────────────


def test_transcribe_one_message_fetches_each_key_and_joins_transcripts():
    s3 = MagicMock()
    s3.get_object.side_effect = [
        {"Body": MagicMock(read=MagicMock(return_value=b"audio-a"))},
        {"Body": MagicMock(read=MagicMock(return_value=b"audio-b"))},
    ]
    with (
        patch.object(transcribe, "_s3_client", return_value=s3),
        patch.dict("os.environ", {"S3_BUCKET": "clear-ground"}),
        patch.object(
            transcribe, "transcribe_audio", side_effect=["first note", "second note"]
        ) as transcribe_audio,
    ):
        result = transcribe._transcribe_one_message(
            message("m1", voice_media_keys=["ground/gs1/a.ogg", "ground/gs1/b.ogg"])
        )

    assert result == "first note\n\nsecond note"
    assert s3.get_object.call_args_list[0].kwargs == {
        "Bucket": "clear-ground",
        "Key": "ground/gs1/a.ogg",
    }
    transcribe_audio.assert_any_call(b"audio-a", "a.ogg")
    transcribe_audio.assert_any_call(b"audio-b", "b.ogg")


def test_transcribe_one_message_drops_empty_parts_when_joining():
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=b"audio"))}
    with (
        patch.object(transcribe, "_s3_client", return_value=s3),
        patch.dict("os.environ", {"S3_BUCKET": "clear-ground"}),
        patch.object(transcribe, "transcribe_audio", return_value=""),
    ):
        result = transcribe._transcribe_one_message(
            message("m1", voice_media_keys=["ground/gs1/silent.ogg"])
        )
    assert result == ""
