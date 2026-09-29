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

import pytest

from clear_pipeline.defs.ground import attempts, stages
from clear_pipeline.defs.ground.schemas import HotlineEnrichment


@pytest.fixture(autouse=True)
def _patch_redis(fake_redis):
    with patch.object(stages, "_redis", fake_redis):
        yield


@pytest.fixture(autouse=True)
def mark_failed():
    """clear-api's markGroundMessagesFailed, stubbed for every test so a
    give-up never reaches the network."""
    with patch.object(attempts, "mark_ground_messages_failed", return_value=1) as mark:
        yield mark


def _drain_patches(rows, **process_kwargs):
    """The patches every drain-loop test needs: one active source whose
    unclassified page is `rows`, and a stubbed `_process_one_message`."""
    return (
        patch.object(stages, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(stages, "make_llm_provider", return_value=MagicMock()),
        patch.object(stages, "ground_messages_for_classification", return_value=rows),
        patch.object(stages, "_process_one_message", **process_kwargs),
    )


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
    assert result.metadata == {"processed": 0, "requeued": 0, "failed": 0, "parked": 0}


def test_drain_asks_the_server_for_unclassified_messages_only():
    # The unfiltered query returns the source's oldest 2000 messages; once
    # those are all classified, message #2001 would never be seen. The
    # filter has to be server-side for the window to advance. It's also
    # what keeps marked-failed messages (enrichFailedAt, or a voice note's
    # transcribeFailedAt) away from _process_one_message: the server drops
    # them from the unclassifiedOnly queue, and the drain has no other
    # filter for them.
    with (
        patch.object(stages, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(stages, "make_llm_provider", return_value=MagicMock()),
        patch.object(stages, "ground_messages_for_classification", return_value=[]) as fetch,
    ):
        _run()
    fetch.assert_called_once_with("gs1", limit=stages._FETCH_LIMIT, unclassified_only=True)


def test_drain_holds_out_voice_messages_until_transcribed():
    rows = [
        message("voice_pending", hasVoice=True, voiceMediaKeys=["ground/gs1/a.ogg"], transcript=None),
        # Media not stored yet: voiceMediaKeys still empty, but hasVoice is
        # set at row creation — must not be enriched as a text message.
        message("voice_media_not_stored", hasVoice=True, voiceMediaKeys=[], transcript=None),
        message("voice_ready", hasVoice=True, voiceMediaKeys=["ground/gs1/b.ogg"], transcript="we need water"),
        message("text_only", hasVoice=False),
    ]
    p = _drain_patches(rows, return_value=stages._PROCESSED)
    with p[0], p[1], p[2], p[3] as process:
        result = _run()

    assert result.metadata["processed"] == 2
    processed_ids = {call.args[1]["id"] for call in process.call_args_list}
    assert processed_ids == {"voice_ready", "text_only"}


def test_drain_transient_error_requeues_without_consuming_attempts(fake_redis):
    p = _drain_patches([message("m1")], side_effect=_FakeTransientError("boom"))
    with p[0], p[1], p[2], p[3], patch.object(stages, "TRANSIENT_LLM_ERRORS", (_FakeTransientError,)):
        result = _run()

    assert result.metadata == {"processed": 0, "requeued": 1, "failed": 0, "parked": 0}
    assert fake_redis.store == {}


def _fail_until_exhausted(fake_redis):
    """Run the drain until message "bad" has used its last attempt."""
    key = "ground:attempts:bad"
    for attempt in range(1, stages._MAX_MESSAGE_ATTEMPTS):
        p = _drain_patches([message("bad")], side_effect=RuntimeError("boom"))
        with p[0], p[1], p[2], p[3]:
            result = _run()
        assert result.metadata["requeued"] == 1
        assert fake_redis.store[key] == attempt

    p = _drain_patches([message("bad")], side_effect=RuntimeError("boom"))
    with p[0], p[1], p[2], p[3]:
        return _run()


def test_drain_requeues_then_marks_failed_after_max_attempts(fake_redis, mark_failed):
    result = _fail_until_exhausted(fake_redis)

    assert result.metadata == {"processed": 0, "requeued": 0, "failed": 1, "parked": 0}
    mark_failed.assert_called_once_with(
        [{"messageId": "bad", "stage": "ENRICH", "error": "RuntimeError: boom"}]
    )
    # Counter reset, so a reviewer's retry gets a fresh set of attempts
    # instead of being skipped as parked.
    assert "ground:attempts:bad" not in fake_redis.store


def test_drain_parks_in_redis_when_marking_fails(fake_redis, mark_failed):
    mark_failed.side_effect = RuntimeError("clear-api down")
    result = _fail_until_exhausted(fake_redis)  # must not raise

    assert result.metadata["failed"] == 1
    assert fake_redis.store["ground:attempts:bad"] == stages._MAX_MESSAGE_ATTEMPTS
    assert fake_redis.ttls["ground:attempts:bad"] == attempts.ATTEMPTS_TTL_SECONDS

    # Still unclassified and unmarked on the server, so the next page
    # returns it — the park keeps it from another paid call.
    p = _drain_patches([message("bad")], return_value=stages._PROCESSED)
    with p[0], p[1], p[2], p[3] as process:
        result = _run()
    assert result.metadata["parked"] == 1
    process.assert_not_called()


def test_drain_skips_a_parked_message_without_calling_it(fake_redis):
    fake_redis.store["ground:attempts:bad"] = stages._MAX_MESSAGE_ATTEMPTS
    p = _drain_patches([message("bad"), message("good")], return_value=stages._PROCESSED)
    with p[0], p[1], p[2], p[3] as process:
        result = _run()

    assert result.metadata == {"processed": 1, "requeued": 0, "failed": 0, "parked": 1}
    assert [call.args[1]["id"] for call in process.call_args_list] == ["good"]


def test_drain_attempt_ttl_is_set_once_not_refreshed(fake_redis):
    p = _drain_patches([message("bad")], side_effect=RuntimeError("boom"))
    with p[0], p[1], p[2], p[3], patch.object(fake_redis, "expire", wraps=fake_redis.expire) as expire:
        _run()
        _run()
    expire.assert_called_once_with("ground:attempts:bad", attempts.ATTEMPTS_TTL_SECONDS)


def test_drain_marks_a_message_with_no_thread_failed_immediately(fake_redis, mark_failed):
    p = _drain_patches([message("orphan", thread_id=None)], return_value=stages._DROP_FAILED)
    with p[0], p[1], p[2], p[3]:
        result = _run()
    assert result.metadata["failed"] == 1
    mark_failed.assert_called_once_with(
        [{"messageId": "orphan", "stage": "ENRICH", "error": "message has no threadId"}]
    )
    assert fake_redis.store == {}


def test_drain_parks_a_message_with_no_thread_when_marking_fails(fake_redis, mark_failed):
    mark_failed.side_effect = RuntimeError("clear-api down")
    p = _drain_patches([message("orphan", thread_id=None)], return_value=stages._DROP_FAILED)
    with p[0], p[1], p[2], p[3]:
        result = _run()
    assert result.metadata["failed"] == 1
    assert fake_redis.store["ground:attempts:orphan"] == stages._MAX_MESSAGE_ATTEMPTS


def test_drain_stops_at_per_run_cap():
    rows = [message("m1"), message("m2")]
    p = _drain_patches(rows, return_value=stages._PROCESSED)
    with p[0], p[1], p[2], p[3] as process, patch.object(stages, "_MAX_ATTEMPTED_PER_RUN", 1):
        result = _run()

    assert result.metadata["processed"] == 1
    process.assert_called_once()  # second message left for next run


def test_drain_failures_count_against_the_per_run_cap():
    rows = [message(f"m{i}") for i in range(5)]
    p = _drain_patches(rows, side_effect=RuntimeError("boom"))
    with p[0], p[1], p[2], p[3] as process, patch.object(stages, "_MAX_ATTEMPTED_PER_RUN", 2):
        result = _run()

    assert process.call_count == 2
    assert result.metadata["requeued"] == 2


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


# ── geoparse: country-scoped, never creates a location ─────────────────────


def test_geoparse_scopes_to_the_hotline_country_and_resolves_existing_location():
    geo = MagicMock(candidate="Nyala", kind="admin", importance=0.8, lat=12.0, lng=24.9)
    with (
        patch.object(stages.settings, "ground_hotline_country_codes", "SD"),
        patch.object(stages, "geoparse_signal", return_value=geo) as geoparse,
        patch.object(stages, "geoparse_to_dict", return_value={}),
        patch.object(stages, "resolve_location", return_value="loc_nyala") as resolve,
    ):
        location_id = stages._geoparse_one_message("flooding in Nyala")

    assert location_id == "loc_nyala"
    assert geoparse.call_args.kwargs["expected_country_codes"] == {"sd"}
    resolve.assert_called_once_with(name="Nyala")
    assert not hasattr(stages, "find_or_create_landmark_l4")


def test_geoparse_lookup_failure_is_swallowed():
    geo = MagicMock(candidate="Nyala", kind="admin", importance=0.8)
    with (
        patch.object(stages, "geoparse_signal", return_value=geo),
        patch.object(stages, "geoparse_to_dict", return_value={}),
        patch.object(stages, "resolve_location", side_effect=RuntimeError("api down")),
    ):
        assert stages._geoparse_one_message("flooding in Nyala") is None


# ── schema: disaster_type degrades instead of failing the enrichment ───────


@pytest.mark.parametrize(("raw", "expected"), [("FL", "fl"), (" fl ", "fl"), ("flood", None), (None, None)])
def test_disaster_type_is_normalised_or_dropped(raw, expected):
    assert enrichment(disaster_type=raw).disaster_type == expected
