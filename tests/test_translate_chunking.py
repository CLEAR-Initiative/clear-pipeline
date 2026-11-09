"""Bulk translation of large entities (#1382).

One call used to translate a whole situation analysis into every locale. It ran
12-21 minutes, hit the output-token cap and came back as unparseable JSON, the
6-minute lock expired under it (so 3-4 drains translated the same analysis at
once), and the queue rows were cleared, so the analysis was never translated.

Covered here: a cut-off reply is reported as truncation and never parsed; a
large entity is split into per-locale calls sized by `_CALL_BUDGET_CHARS`; each
call is written as soon as it returns; a failed entity is retried a bounded
number of times; the lock is renewed for as long as the work runs. clear-api /
the LLM / Redis are mocked.
"""

import json
import logging
import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.defs.signals import stages
from clear_pipeline.providers import redis_lock as rl
from clear_pipeline.providers import translate as tp
from clear_pipeline.providers.llm import (
    STOP_LENGTH,
    AnthropicProvider,
    FallbackProvider,
    OpenAICompatibleProvider,
)
from clear_pipeline.providers.translation_hash import HASH_FIELDS, compute_source_hashes

FIELDS = list(HASH_FIELDS["analysis"])
EVENT = {"title": "Flood in Kassala", "description": "The Gash river burst its banks."}


def _analysis(chars_per_field: int = 5_000) -> dict:
    """A prose projection whose 8 components are each ~`chars_per_field` long."""
    return {field: {"text": f"{field} " + "x" * chars_per_field} for field in FIELDS}


def _echo(locales, fields, canonical):
    """A model reply that 'translates' every requested field for every locale."""
    return {loc: {f: {"text": f"[{loc}] {f}"} for f in fields} for loc in locales}


@contextmanager
def _env(*, stored=None, redis=None, locales=("ar", "fr", "es")):
    """Patch everything translate_and_upsert touches except the model call.
    Yields the mocks, plus the lock's call arguments."""
    lock_calls: list[dict] = []

    @contextmanager
    def lock(key, **kwargs):
        lock_calls.append({"key": key, **kwargs})
        yield True

    with (
        patch.object(tp, "redis_lock", lock),
        patch.object(tp, "shared_client", return_value=redis if redis is not None else MagicMock()),
        patch.object(tp, "configured_target_locales", return_value=list(locales)),
        patch.object(tp.clear_api, "get_translations", return_value=stored or []),
        patch.object(tp.clear_api, "upsert_translations") as upsert,
        patch.object(tp.clear_api, "mark_translated") as mark,
        patch.object(tp.clear_api, "enqueue_translation") as enqueue,
    ):
        yield SimpleNamespace(upsert=upsert, mark=mark, enqueue=enqueue, lock_calls=lock_calls)


def _final_rows(upsert) -> dict[str, dict]:
    """The last row written per locale (each write carries the merged row)."""
    rows: dict[str, dict] = {}
    for call in upsert.call_args_list:
        for row in call.args[2]:
            rows[row["locale"]] = row
    return rows


# ── _translate_call: truncation is named, not reported as a parse failure ────

def _llm(reply: str, *, stop=None, output_tokens=None):
    llm = MagicMock()
    llm.complete_text.return_value = reply
    llm.last_stop_reason = stop
    llm.last_usage = None if output_tokens is None else {"input_tokens": 10, "output_tokens": output_tokens}
    llm.model = "qwen/test"
    return llm


def test_cut_off_reply_is_reported_as_truncation_and_not_parsed(caplog):
    # A balanced JSON object is returned on purpose: the stop reason alone must
    # decide, because a cut-off reply can still contain a parseable prefix.
    llm = _llm('{"fr": {"title": "Inondation"}}', stop=STOP_LENGTH, output_tokens=8192)
    with (
        patch.object(tp, "make_llm_provider", return_value=llm),
        caplog.at_level(logging.WARNING, logger=tp.logger.name),
    ):
        out, truncated = tp._translate_call("event", EVENT, ["fr"], ["title"], entity_id="e1")

    assert (out, truncated) == (None, True)
    assert "reply cut off at max_tokens=8192" in caplog.text
    assert "output_tokens=8192" in caplog.text and "model=qwen/test" in caplog.text
    assert "could not parse" not in caplog.text


def test_unparseable_reply_logs_the_stop_reason_and_token_count(caplog):
    llm = _llm("I cannot help with that", stop="stop", output_tokens=7)
    with (
        patch.object(tp, "make_llm_provider", return_value=llm),
        caplog.at_level(logging.WARNING, logger=tp.logger.name),
    ):
        out, truncated = tp._translate_call("event", EVENT, ["fr"], ["title"], entity_id="e1")

    assert (out, truncated) == (None, False)
    assert "could not parse model JSON (1 locales × 1 fields; stop=stop output_tokens=7" in caplog.text


def test_token_cap_is_sized_to_the_payload():
    llm = _llm(json.dumps({"fr": {"title": "x"}}))
    with patch.object(tp, "make_llm_provider", return_value=llm):
        tp._translate_call("event", EVENT, ["fr"], ["title"])
        small = llm.complete_text.call_args.kwargs["max_tokens"]
        tp._translate_call("analysis", _analysis(40_000), ["ar"], FIELDS)
        huge = llm.complete_text.call_args.kwargs["max_tokens"]

    assert small == tp._MIN_CALL_MAX_TOKENS  # a runaway on a short entity stops early
    assert huge == tp._TRANSLATE_MAX_TOKENS  # never above the ceiling


def test_translate_entity_keeps_its_single_call_contract():
    llm = _llm(json.dumps({"fr": {"title": "Inondation"}, "ar": "not an object"}))
    with patch.object(tp, "make_llm_provider", return_value=llm):
        assert tp.translate_entity("event", EVENT, ["en", "fr", "ar"], ["title"]) == {
            "fr": {"title": "Inondation"},
        }
        assert tp.translate_entity("event", EVENT, ["en"], ["title"]) is None
    assert llm.complete_text.call_count == 1


# ── planning ──────────────────────────────────────────────────────────────────

def test_small_entity_is_one_call_for_every_locale():
    stale = {"ar": ["title", "description"], "fr": ["title"]}
    assert tp._plan_calls(EVENT, stale) == [(["ar", "fr"], ["description", "title"])]


def test_large_entity_is_one_locale_per_call_within_the_budget():
    canonical = _analysis(5_000)
    calls = tp._plan_calls(canonical, {"ar": list(FIELDS), "fr": list(FIELDS)})

    assert {tuple(locales) for locales, _ in calls} == {("ar",), ("fr",)}
    for locale in ("ar", "fr"):
        groups = [fields for locales, fields in calls if locales == [locale]]
        assert sorted(f for group in groups for f in group) == sorted(FIELDS)  # nothing lost
        assert len(groups) > 1
        assert all(tp._payload_chars(canonical, g) <= tp._CALL_BUDGET_CHARS for g in groups)


def test_a_field_over_the_budget_gets_a_call_of_its_own():
    canonical = {"a": "x" * 100, "big": "y" * (tp._CALL_BUDGET_CHARS + 1), "c": "z" * 100}
    assert tp._pack_fields(canonical, ["a", "big", "c"]) == [["a"], ["big"], ["c"]]


def test_split_call_goes_per_locale_then_halves_the_fields():
    assert tp._split_call(["ar", "fr"], ["a", "b"]) == [(["ar"], ["a", "b"]), (["fr"], ["a", "b"])]
    assert tp._split_call(["ar"], ["a", "b", "c"]) == [(["ar"], ["a"]), (["ar"], ["b", "c"])]
    assert tp._split_call(["ar"], ["a"]) == []


# ── translate_and_upsert: small entities are unchanged ───────────────────────

def test_small_entity_one_call_one_write_and_a_renewed_lock():
    call = MagicMock(side_effect=lambda et, canonical, locales, fields, entity_id=None: (
        _echo(locales, fields, canonical), False,
    ))
    with _env(locales=("ar", "fr")) as env, patch.object(tp, "_translate_call", call):
        outcome = tp.translate_and_upsert("event", "e1", EVENT)

    assert outcome == tp.TRANSLATED
    assert call.call_count == 1
    hashes = compute_source_hashes("event", EVENT)
    env.upsert.assert_called_once()
    assert {row["locale"]: row["sourceHashes"] for row in env.upsert.call_args.args[2]} == {
        "ar": hashes, "fr": hashes,
    }
    env.enqueue.assert_not_called()
    # The lock must outlast the call: it is renewed while the work runs.
    assert env.lock_calls == [{
        "key": "translate:event:e1", "ttl_seconds": tp._TRANSLATE_LOCK_TTL_SECONDS,
        "wait_seconds": 0, "renew": True,
    }]


# ── translate_and_upsert: large entities ─────────────────────────────────────

def test_large_analysis_is_translated_in_small_calls_and_fully_written():
    canonical = _analysis()
    seen: list[tuple[list[str], list[str]]] = []

    def call(et, canon, locales, fields, entity_id=None):
        seen.append((locales, fields))
        return _echo(locales, fields, canon), False

    with _env() as env, patch.object(tp, "_translate_call", side_effect=call):
        outcome = tp.translate_and_upsert("analysis", "a1", canonical)

    assert outcome == tp.TRANSLATED
    assert all(len(locales) == 1 for locales, _ in seen)  # never 3 locales × 8 fields again
    assert all(tp._payload_chars(canonical, fields) <= tp._CALL_BUDGET_CHARS for _, fields in seen)
    assert env.upsert.call_count == len(seen)  # written call by call, not at the end
    # Each write deletes the locale's queue row in clear-api, so an unfinished
    # locale is put back after every write but its last one.
    per_locale = len(seen) // 3
    assert env.enqueue.call_count == 3 * (per_locale - 1)
    rows = _final_rows(env.upsert)
    assert set(rows) == {"ar", "fr", "es"}
    for locale, row in rows.items():
        assert row["sourceHashes"] == compute_source_hashes("analysis", canonical)
        assert row["data"] == {f: {"text": f"[{locale}] {f}"} for f in FIELDS}


def test_cut_off_call_is_split_and_retried_smaller():
    canonical = _analysis()
    seen: list[list[str]] = []

    def call(et, canon, locales, fields, entity_id=None):
        seen.append(fields)
        if len(fields) > 1:
            return None, True  # cut off at the token cap
        return _echo(locales, fields, canon), False

    with _env(locales=("ar",)) as env, patch.object(tp, "_translate_call", side_effect=call):
        outcome = tp.translate_and_upsert("analysis", "a1", canonical)

    assert outcome == tp.TRANSLATED
    assert sorted(f for fields in seen if len(fields) == 1 for f in fields) == sorted(FIELDS)
    assert _final_rows(env.upsert)["ar"]["sourceHashes"] == compute_source_hashes("analysis", canonical)


def test_failed_fields_stay_stale_and_the_entity_is_left_queued(fake_redis, caplog):
    canonical = _analysis()

    def call(et, canon, locales, fields, entity_id=None):
        if locales == ["ar"] and "sectors" in fields:
            return None, False  # malformed JSON for this one call
        return _echo(locales, fields, canon), False

    with (
        _env(redis=fake_redis, locales=("ar", "fr")) as env,
        patch.object(tp, "_translate_call", side_effect=call),
        caplog.at_level(logging.WARNING, logger=tp.logger.name),
    ):
        outcome = tp.translate_and_upsert("analysis", "a1", canonical)

    assert outcome == tp.RETRY
    rows = _final_rows(env.upsert)
    fresh = compute_source_hashes("analysis", canonical)
    assert rows["fr"]["sourceHashes"] == fresh
    # ar: what translated is kept; the failed call's fields have no hash, so
    # the next attempt translates only those.
    assert "sectors" not in rows["ar"]["sourceHashes"]
    assert set(rows["ar"]["sourceHashes"]) < set(fresh)
    # ar is still queued for the retry; fr finished, so its last write cleared it.
    assert env.enqueue.call_args_list[-1].args == ("analysis", "a1", "ar")
    env.mark.assert_not_called()
    assert fake_redis.store["translate:attempts:analysis:a1"] == 1
    assert "attempt 1 of 3, left queued" in caplog.text
    assert "sectors (no usable reply)" in caplog.text
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]  # no GlitchTip issue yet


def test_a_run_that_dies_part_way_leaves_the_unfinished_locale_queued():
    canonical = _analysis()
    events: list[str] = []
    done = {"n": 0}

    def call(et, canon, locales, fields, entity_id=None):
        done["n"] += 1
        if done["n"] == 2:
            raise ConnectionError("provider went away")  # transport errors propagate
        return _echo(locales, fields, canon), False

    with _env(locales=("ar",)) as env, patch.object(tp, "_translate_call", side_effect=call):
        env.upsert.side_effect = lambda *a: events.append("upsert")
        env.enqueue.side_effect = lambda *a: events.append("enqueue")
        with pytest.raises(ConnectionError):
            tp.translate_and_upsert("analysis", "a1", canonical)

    # The first call was written (which deleted the queue row) and the locale
    # was put straight back, so the next drain finishes it.
    assert events == ["upsert", "enqueue"]


def test_next_attempt_translates_only_what_is_still_stale():
    canonical = _analysis()
    fresh = compute_source_hashes("analysis", canonical)
    done = [f for f in FIELDS if f != "sectors"]
    stored = [{
        "locale": "ar",
        "data": {f: {"text": f"[ar] {f}"} for f in done},
        "sourceHashes": {f: fresh[f] for f in done},
    }]
    call = MagicMock(side_effect=lambda et, canon, locales, fields, entity_id=None: (
        _echo(locales, fields, canon), False,
    ))
    with _env(stored=stored, locales=("ar",)) as env, patch.object(tp, "_translate_call", call):
        outcome = tp.translate_and_upsert("analysis", "a1", canonical)

    assert outcome == tp.TRANSLATED
    assert [c.args[3] for c in call.call_args_list] == [["sectors"]]
    row = _final_rows(env.upsert)["ar"]
    assert row["sourceHashes"] == fresh
    assert set(row["data"]) == set(FIELDS)  # earlier translations kept


def test_gives_up_after_the_attempt_limit_and_clears_the_queue(fake_redis, caplog):
    canonical = _analysis()
    fake_redis.store["translate:attempts:analysis:a1"] = tp._MAX_ATTEMPTS - 1

    with (
        _env(redis=fake_redis, locales=("ar",)) as env,
        patch.object(tp, "_translate_call", return_value=(None, True)),  # always cut off
        caplog.at_level(logging.WARNING, logger=tp.logger.name),
    ):
        outcome = tp.translate_and_upsert("analysis", "a1", canonical)

    assert outcome == tp.UNPARSEABLE
    env.upsert.assert_not_called()
    env.enqueue.assert_not_called()
    env.mark.assert_called_once_with("analysis", "a1", "ar")  # dropped, cannot re-bill
    assert "translate:attempts:analysis:a1" not in fake_redis.store  # a re-enqueue starts fresh
    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1 and "gave up after 3 attempts" in errors[0]
    # The error names truncation; it is not reported as a JSON parse failure.
    assert "sectors (reply cut off at the token cap)" in errors[0]
    assert "could not parse" not in errors[0]


def test_success_clears_the_attempt_counter(fake_redis):
    fake_redis.store["translate:attempts:event:e1"] = 2
    call = MagicMock(side_effect=lambda et, canon, locales, fields, entity_id=None: (
        _echo(locales, fields, canon), False,
    ))
    with _env(redis=fake_redis, locales=("ar",)), patch.object(tp, "_translate_call", call):
        assert tp.translate_and_upsert("event", "e1", EVENT) == tp.TRANSLATED
    assert "translate:attempts:event:e1" not in fake_redis.store


def test_redis_down_means_give_up_not_an_unbounded_retry_loop():
    broken = MagicMock()
    broken.incr.side_effect = ConnectionError("redis down")
    with (
        _env(redis=broken, locales=("ar",)) as env,
        patch.object(tp, "_translate_call", return_value=(None, False)),
    ):
        outcome = tp.translate_and_upsert("event", "e1", EVENT)

    assert outcome == tp.UNPARSEABLE
    env.mark.assert_called_once_with("event", "e1", "ar")


def test_a_field_the_model_left_out_is_not_marked_current(fake_redis):
    def call(et, canon, locales, fields, entity_id=None):
        return {"ar": {"title": "فيضان"}}, False  # description missing

    with _env(redis=fake_redis, locales=("ar",)) as env, patch.object(tp, "_translate_call", side_effect=call):
        outcome = tp.translate_and_upsert("event", "e1", EVENT)

    assert outcome == tp.RETRY
    row = _final_rows(env.upsert)["ar"]
    assert row["data"] == {"title": "فيضان"}
    assert set(row["sourceHashes"]) == {"title"}


def test_fields_with_no_text_are_recorded_without_a_model_call():
    canonical = {"title": "Flood in Kassala", "summary": None, "scenarios": [], "needs": {"wash": ""}}
    call = MagicMock(side_effect=lambda et, canon, locales, fields, entity_id=None: (
        {loc: {f: f"[{loc}] {f}" for f in fields} for loc in locales}, False,
    ))
    with _env(locales=("ar",)) as env, patch.object(tp, "_translate_call", call):
        outcome = tp.translate_and_upsert("crisis", "c1", canonical)

    assert outcome == tp.TRANSLATED
    assert [c.args[3] for c in call.call_args_list] == [["title"]]  # only the field with text
    env.upsert.assert_called_once()
    row = env.upsert.call_args.args[2][0]
    # null, not the empty canonical value: clear-api shows canonical for a null overlay.
    assert row["data"] == {"title": "[ar] title", "summary": None, "scenarios": None, "needs": None}
    assert row["sourceHashes"] == compute_source_hashes("crisis", canonical)


def test_entity_with_no_text_at_all_is_written_without_any_call():
    canonical = {field: {} for field in FIELDS}
    call = MagicMock()
    with _env(locales=("ar", "fr")) as env, patch.object(tp, "_translate_call", call):
        outcome = tp.translate_and_upsert("analysis", "a1", canonical)

    assert outcome == tp.TRANSLATED
    call.assert_not_called()
    env.upsert.assert_called_once()
    assert [row["locale"] for row in env.upsert.call_args.args[2]] == ["ar", "fr"]
    assert all(row["data"] == dict.fromkeys(FIELDS) for row in env.upsert.call_args.args[2])
    env.enqueue.assert_not_called()


# ── stage: RETRY leaves the rows queued ──────────────────────────────────────

def test_stage_counts_a_retry_as_requeued_and_invokes_it_once_per_run():
    calls = {"n": 0}

    def fake_tu(entity_type, entity_id, canonical, requested_locales=None):
        calls["n"] += 1
        return tp.RETRY

    row = {"entityType": "analysis", "entityId": "a1", "locale": "ar"}
    with (
        patch.object(stages, "pending_translations",
                     side_effect=lambda first, entity_type=None: [] if entity_type else [row]),
        patch.dict(stages._CANONICAL_FETCH, {"analysis": lambda aid: _analysis(10)}),
        patch.object(stages, "translate_and_upsert", side_effect=fake_tu),
    ):
        result = stages._drain_translations(MagicMock())

    assert calls["n"] == 1  # the still-queued row is not re-invoked in the same run
    assert result.metadata["requeued"] == 1
    assert result.metadata["cleared"] == 0 and result.metadata["translated"] == 0


# ── redis_lock(renew=True) ────────────────────────────────────────────────────

class _LockRedis:
    """Just enough Redis for redis_lock: SET NX EX plus the two Lua scripts."""

    def __init__(self):
        self.value = None
        self.extends: list[int] = []
        self.released = False
        self.lost = False

    def set(self, key, token, nx=False, ex=None):
        if self.value is not None:
            return False
        self.value = token
        return True

    def eval(self, script, numkeys, key, token, *args):
        if script == rl._EXTEND_SCRIPT:
            if self.lost or self.value != token:
                return 0
            self.extends.append(int(args[0]))
            return 1
        self.released = self.value == token
        self.value = None
        return 1


def _wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_renewed_lock_is_extended_while_held_and_released_after():
    client = _LockRedis()
    with patch.object(rl, "_client", return_value=client):
        with rl.redis_lock("k", ttl_seconds=1, wait_seconds=0, renew=True) as acquired:
            assert acquired
            assert _wait_for(lambda: len(client.extends) >= 2)  # outlasts the TTL
        extends_at_exit = len(client.extends)
        time.sleep(0.5)

    assert client.released
    assert set(client.extends) == {1}  # each renewal resets the full TTL
    assert len(client.extends) == extends_at_exit  # the renewer stopped with the lock


def test_renewal_stops_once_the_lock_is_lost(caplog):
    client = _LockRedis()
    client.lost = True
    with patch.object(rl, "_client", return_value=client), caplog.at_level(logging.WARNING, logger=rl.logger.name):
        with rl.redis_lock("k", ttl_seconds=1, wait_seconds=0, renew=True):
            assert _wait_for(lambda: "Lost k" in caplog.text)
    assert client.extends == []


def test_lock_is_not_renewed_by_default_or_when_not_acquired():
    client = _LockRedis()
    with patch.object(rl, "_client", return_value=client), patch.object(rl.threading, "Thread") as thread:
        with rl.redis_lock("k", ttl_seconds=1, wait_seconds=0) as acquired:
            assert acquired
        client.value = "held-by-a-peer"
        with rl.redis_lock("k", ttl_seconds=1, wait_seconds=0, renew=True) as acquired:
            assert not acquired
    thread.assert_not_called()


# ── providers record why the reply stopped ───────────────────────────────────

@pytest.mark.parametrize(("finish_reason", "expected"), [
    ("length", STOP_LENGTH), ("stop", "stop"), (None, None),
])
def test_openai_compatible_records_the_stop_reason(finish_reason, expected):
    p = OpenAICompatibleProvider(role="translate", model="m", base_url="http://x", api_key="k")
    p._client = MagicMock()
    p._client.chat.completions.create.return_value = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="hi"), finish_reason=finish_reason)],
        usage=SimpleNamespace(prompt_tokens=4, completion_tokens=2),
    )
    assert p.last_stop_reason is None
    assert p.complete_text(system="s", user="u") == "hi"  # recorded, never raised
    assert p.last_stop_reason == expected


def test_anthropic_max_tokens_is_normalised_to_stop_length():
    p = AnthropicProvider(role="translate", model="claude-haiku-4-5", api_key="k")
    p._client = MagicMock()
    p._client.messages.create.return_value = SimpleNamespace(
        content=[SimpleNamespace(type="text", text="hello")],
        usage=SimpleNamespace(input_tokens=5, output_tokens=2),
        stop_reason="max_tokens",
    )
    assert p.complete_text(system="s", user="u") == "hello"
    assert p.last_stop_reason == STOP_LENGTH


def test_fallback_reports_the_stop_reason_of_the_provider_that_served():
    import openai

    primary = MagicMock(model="primary", provider_name="p", role="translate", last_stop_reason="stop")
    primary.complete_text.side_effect = openai.APIConnectionError(request=MagicMock())
    fallback = MagicMock(model="fallback", provider_name="f", role="translate", last_stop_reason=STOP_LENGTH)
    fallback.complete_text.return_value = "cut off"
    p = FallbackProvider(primary, fallback)

    assert p.last_stop_reason == "stop"  # nothing served yet: the primary's
    assert p.complete_text(system="s", user="u") == "cut off"
    assert p.last_stop_reason == STOP_LENGTH
