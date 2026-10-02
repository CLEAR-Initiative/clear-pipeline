"""Unit tests for the one-off hotline draft backfill (defs/ground/backfill.py).

Same approach as tests/test_ground_hotline_enrich.py: patch.object on the
module's imported provider functions and call the inner `_backfill_locked`
with a MagicMock context, redis_lock faked.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from clear_pipeline.defs.ground import backfill
from clear_pipeline.defs.ground.schemas import HotlineEnrichment


@contextmanager
def _lock_acquired(*_args, **_kwargs):
    yield True


@contextmanager
def _lock_not_acquired(*_args, **_kwargs):
    yield False


def thread(id_, *, draft_title=None, review_state="unverified"):
    return {"id": id_, "reviewState": review_state, "draftTitle": draft_title}


def message(id_, thread_id, *, classification="field_report", **overrides):
    return {
        "id": id_,
        "text": "Checkpoint on the road out of Mukjar",
        "threadId": thread_id,
        "classification": classification,
        "hasVoice": False,
        "transcript": None,
        **overrides,
    }


def enrichment():
    return HotlineEnrichment(
        classification="field_report",
        title="Checkpoint near Mukjar turning back trucks",
        severity=3,
        disaster_type=None,
        uncertainty_marker=None,
    )


def _run(threads, messages, *, lock=_lock_acquired, enrich=None):
    with (
        patch.object(backfill, "pipeline_ground_source_ids", return_value=["gs1"]),
        patch.object(backfill, "make_llm_provider", return_value=MagicMock()),
        patch.object(backfill, "ground_thread_drafts_for_source", return_value=threads),
        patch.object(backfill, "ground_messages_for_classification", return_value=messages),
        patch.object(backfill, "redis_lock", lock),
        patch.object(
            backfill, "_enrich_one_message", **(enrich or {"return_value": enrichment()})
        ) as enrich_mock,
        patch.object(backfill, "_geoparse_one_message", return_value="loc_1"),
        patch.object(backfill, "upsert_ground_thread_drafts") as write_drafts,
    ):
        result = backfill._backfill_locked(MagicMock())
    return result, enrich_mock, write_drafts


def test_writes_a_draft_for_a_classified_thread_without_one():
    result, _, write_drafts = _run([thread("t1")], [message("m1", "t1")])

    assert result.metadata == {"written": 1, "skipped": 0, "failed": 0, "remaining": 0}
    write_drafts.assert_called_once_with([{
        "threadId": "t1",
        "draftTitle": "Checkpoint near Mukjar turning back trucks",
        "draftSeverity": 3,
        "draftLocationId": "loc_1",
        "draftDisasterType": None,
    }])


def test_never_touches_the_existing_classification():
    assert not hasattr(backfill, "upsert_ground_message_classifications")


def test_skips_threads_that_already_have_a_draft_or_are_reviewed():
    threads = [
        thread("drafted", draft_title="Already drafted"),
        thread("promoted", review_state="approved_public"),
    ]
    messages = [message("m1", "drafted"), message("m2", "promoted")]
    result, enrich, _ = _run(threads, messages)

    assert result.metadata["written"] == 0
    enrich.assert_not_called()


def test_leaves_unclassified_messages_to_the_drain():
    result, enrich, _ = _run([thread("t1")], [message("m1", "t1", classification=None)])
    assert result.metadata["written"] == 0
    enrich.assert_not_called()


def test_skips_voice_notes_without_a_transcript():
    msgs = [message("m1", "t1", hasVoice=True, transcript=None)]
    result, enrich, _ = _run([thread("t1")], msgs)
    assert result.metadata["written"] == 0
    enrich.assert_not_called()


def test_one_draft_per_thread_from_its_oldest_message():
    msgs = [message("m1", "t1"), message("m2", "t1")]
    _, enrich, write_drafts = _run([thread("t1")], msgs)

    assert enrich.call_count == 1
    assert enrich.call_args.args[1]["id"] == "m1"
    write_drafts.assert_called_once()


def test_a_failure_is_counted_and_the_rest_still_run():
    threads = [thread("t1"), thread("t2")]
    msgs = [message("m1", "t1"), message("m2", "t2")]
    result, _, write_drafts = _run(
        threads, msgs, enrich={"side_effect": [RuntimeError("boom"), enrichment()]},
    )

    assert result.metadata == {"written": 1, "skipped": 0, "failed": 1, "remaining": 1}
    assert write_drafts.call_args.args[0][0]["threadId"] == "t2"


def test_lock_contention_skips_without_calling_the_llm():
    result, enrich, _ = _run([thread("t1")], [message("m1", "t1")], lock=_lock_not_acquired)
    assert result.metadata == {"written": 0, "skipped": 1, "failed": 0, "remaining": 1}
    enrich.assert_not_called()


def test_stops_at_the_per_run_cap():
    threads = [thread(f"t{i}") for i in range(3)]
    msgs = [message(f"m{i}", f"t{i}") for i in range(3)]
    with patch.object(backfill, "_MAX_ATTEMPTED_PER_RUN", 2):
        result, enrich, _ = _run(threads, msgs)

    assert enrich.call_count == 2
    assert result.metadata == {"written": 2, "skipped": 0, "failed": 0, "remaining": 1}
