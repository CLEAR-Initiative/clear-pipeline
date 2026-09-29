"""Unit tests for the hotline voice-note transcription drain
(defs/ground/transcribe.py).

Mirrors tests/test_ground_hotline_enrich.py's approach: patch.object on the
module's imported provider functions, call the inner
_drain_ground_transcribe_locked directly (bypassing the Redis single-flight
lock wrapper) via a MagicMock context.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.defs.ground import transcribe


@pytest.fixture(autouse=True)
def _patch_redis(fake_redis):
    with patch.object(transcribe, "_redis", fake_redis):
        yield


def _drain_patches(rows, **process_kwargs):
    """One active source whose awaiting-transcript page is `rows`, and a
    stubbed `_process_one_message`."""
    return (
        patch.object(transcribe, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(transcribe, "ground_messages_for_classification", return_value=rows),
        patch.object(transcribe, "_process_one_message", **process_kwargs),
    )


def _run():
    return transcribe._drain_ground_transcribe_locked(MagicMock())


def message(id_, *, voice_media_keys=None, transcript=None, **overrides):
    return {
        "id": id_,
        "text": "",
        "sentAt": "2026-09-15T10:00:00Z",
        "senderRef": "s_abc123",
        "hasMedia": True,
        "hasVoice": True,
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

_EMPTY = {"processed": 0, "requeued": 0, "failed": 0, "parked": 0, "not_ready": 0}


def test_drain_no_active_sources_returns_zero_counts():
    with patch.object(transcribe, "pipeline_ground_source_ids", return_value=[]):
        result = _run()
    assert result.metadata == _EMPTY


def test_drain_asks_the_server_for_voice_notes_awaiting_transcript():
    # Filtering client-side over the source's oldest 2000 messages stalls
    # once those are done — the filter has to be server-side.
    with (
        patch.object(transcribe, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(transcribe, "ground_messages_for_classification", return_value=[]) as fetch,
    ):
        _run()
    fetch.assert_called_once_with("gs1", limit=transcribe._FETCH_LIMIT, awaiting_transcript=True)


def test_drain_skips_voice_notes_whose_media_is_not_stored_yet(fake_redis):
    rows = [message("not_stored", voice_media_keys=[]), message("ready", voice_media_keys=["a.ogg"])]
    p = _drain_patches(rows, return_value=transcribe._PROCESSED)
    with p[0], p[1], p[2] as process:
        result = _run()

    assert result.metadata == {**_EMPTY, "processed": 1, "not_ready": 1}
    assert [call.args[0]["id"] for call in process.call_args_list] == ["ready"]
    assert fake_redis.store == {}  # no attempt spent on the not-ready one


def test_drain_transient_error_requeues_without_consuming_attempts(fake_redis):
    p = _drain_patches([message("m1", voice_media_keys=["a.ogg"])], side_effect=_FakeTransientError("boom"))
    with p[0], p[1], p[2], patch.object(transcribe, "TRANSIENT_LLM_ERRORS", (_FakeTransientError,)):
        result = _run()

    assert result.metadata == {**_EMPTY, "requeued": 1}
    assert fake_redis.store == {}


def test_drain_generic_failure_requeues_then_parks_after_max_attempts(fake_redis):
    key = "ground:transcribe:attempts:bad"
    for attempt in range(1, transcribe._MAX_MESSAGE_ATTEMPTS):
        p = _drain_patches([message("bad", voice_media_keys=["a.ogg"])], side_effect=RuntimeError("boom"))
        with p[0], p[1], p[2]:
            result = _run()
        assert result.metadata["requeued"] == 1
        assert fake_redis.store[key] == attempt

    p = _drain_patches([message("bad", voice_media_keys=["a.ogg"])], side_effect=RuntimeError("boom"))
    with p[0], p[1], p[2]:
        result = _run()
    assert result.metadata == {**_EMPTY, "failed": 1}


def test_drain_skips_a_parked_message_without_calling_it(fake_redis):
    fake_redis.store["ground:transcribe:attempts:bad"] = transcribe._MAX_MESSAGE_ATTEMPTS
    rows = [message("bad", voice_media_keys=["a.amr"]), message("good", voice_media_keys=["b.ogg"])]
    p = _drain_patches(rows, return_value=transcribe._PROCESSED)
    with p[0], p[1], p[2] as process:
        result = _run()

    assert result.metadata == {**_EMPTY, "processed": 1, "parked": 1}
    assert [call.args[0]["id"] for call in process.call_args_list] == ["good"]


def test_drain_stops_at_per_run_cap():
    rows = [message("m1", voice_media_keys=["a.ogg"]), message("m2", voice_media_keys=["b.ogg"])]
    p = _drain_patches(rows, return_value=transcribe._PROCESSED)
    with p[0], p[1], p[2] as process, patch.object(transcribe, "_MAX_ATTEMPTED_PER_RUN", 1):
        result = _run()

    assert result.metadata["processed"] == 1
    process.assert_called_once()  # second message left for next run


def test_drain_failures_count_against_the_per_run_cap():
    rows = [message(f"m{i}", voice_media_keys=["a.ogg"]) for i in range(5)]
    p = _drain_patches(rows, side_effect=RuntimeError("boom"))
    with p[0], p[1], p[2] as process, patch.object(transcribe, "_MAX_ATTEMPTED_PER_RUN", 2):
        result = _run()

    assert process.call_count == 2
    assert result.metadata["requeued"] == 2


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
