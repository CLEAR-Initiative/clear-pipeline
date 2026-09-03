"""Tests for the insights telemetry layer (providers/insights.py).

Network is never touched: ``_post`` is monkeypatched to collect request
bodies, and providers are minimal doubles that call ``insights.capture`` the
way the real ones do.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from clear_pipeline.providers import insights
from clear_pipeline.providers.insights import TelemetryProvider


class _Out(BaseModel):
    answer: str


class _FakeProvider:
    """Behaves like a real provider: captures raw text + usage after the
    'API' returns, then returns / raises."""

    def __init__(self, *, fail: Exception | None = None, model="fake-model"):
        self.role = "signal"
        self.model = model
        self.provider_name = "fake"
        self._fail = fail

    def complete_structured(self, *, system, user, schema, **_):
        insights.capture(
            model=self.model,
            raw_response='{"answer": "42"}',
            usage={"input_tokens": 10, "output_tokens": 5,
                   "cache_read_tokens": None, "cache_create_tokens": None},
        )
        if self._fail:
            raise self._fail
        return schema(answer="42")

    def complete_text(self, *, system, user, **_):
        insights.capture(model=self.model, raw_response="plain", usage={"input_tokens": 3})
        return "plain"


@pytest.fixture
def posted(monkeypatch):
    """Enable telemetry, stub the HTTP layer, collect (path, body) pairs."""
    calls: list[tuple[str, dict]] = []

    def fake_post(path, body):
        calls.append((path, body))
        return {"id": "run-1"} if path == "/api/runs" else {"id": "call-1"}

    monkeypatch.setattr(insights.settings, "insights_api_url", "http://insights.test")
    monkeypatch.setattr(insights.settings, "insights_ingest_token", "tok")
    monkeypatch.setattr(insights.settings, "pipeline_env", "test")
    monkeypatch.setattr(insights, "_post", fake_post)
    insights.reset_run_cache()
    yield calls
    insights.reset_run_cache()


def _call_bodies(calls):
    return [b for p, b in calls if p == "/api/calls"]


class TestWrapper:
    def test_success_records_one_row_with_usage_and_parsed(self, posted):
        llm = TelemetryProvider(_FakeProvider())
        out = llm.complete_structured(system="sys", user="usr", schema=_Out)
        assert out.answer == "42"

        assert posted[0][0] == "/api/runs"
        run = posted[0][1]
        assert run["env"] == "test" and run["pipeline_repo"] == "clear-pipeline"

        (row,) = _call_bodies(posted)
        assert row["run_id"] == "run-1"
        assert row["stage"] == "signal"  # unscoped → role
        assert row["prompt_version"] == insights.UNVERSIONED
        assert row["model"] == "fake-model"
        assert row["system_prompt"] == "sys" and row["user_prompt"] == "usr"
        assert row["raw_response"] == '{"answer": "42"}'
        assert row["parsed_response"] == {"answer": "42"}
        assert row["parse_error"] is None
        assert row["input_tokens"] == 10 and row["output_tokens"] == 5
        assert isinstance(row["latency_ms"], int)

    def test_failure_records_row_and_reraises(self, posted):
        llm = TelemetryProvider(_FakeProvider(fail=ValueError("boom")))
        with pytest.raises(ValueError, match="boom"):
            llm.complete_structured(system="s", user="u", schema=_Out)
        (row,) = _call_bodies(posted)
        assert row["parse_error"] == "ValueError: boom"
        assert row["parsed_response"] is None
        assert row["input_tokens"] == 10  # usage still captured before the raise

    def test_scope_labels_calls_and_nests(self, posted):
        llm = TelemetryProvider(_FakeProvider())
        with insights.scope(stage="signal.rewrite", prompt_version="rewrite-v3", event_id="ev-1"):
            llm.complete_text(system="s", user="u")
            with insights.scope(signal_id="sig-9"):  # inherits stage/version/event
                llm.complete_text(system="s", user="u")
        llm.complete_text(system="s", user="u")  # scope restored → defaults

        rows = _call_bodies(posted)
        assert [r["stage"] for r in rows] == ["signal.rewrite", "signal.rewrite", "signal"]
        assert rows[0]["event_id"] == "ev-1" and rows[0]["signal_id"] is None
        assert rows[1]["event_id"] == "ev-1" and rows[1]["signal_id"] == "sig-9"
        assert rows[2]["prompt_version"] == insights.UNVERSIONED
        assert rows[2]["event_id"] is None

    def test_run_is_created_once_per_process(self, posted):
        llm = TelemetryProvider(_FakeProvider())
        llm.complete_text(system="s", user="u")
        llm.complete_text(system="s", user="u")
        assert [p for p, _ in posted].count("/api/runs") == 1

    def test_model_property_delegates(self, posted):
        inner = _FakeProvider(model="m1")
        llm = TelemetryProvider(inner)
        inner.model = "m2"  # e.g. FallbackProvider switching served model
        assert llm.model == "m2"

    def test_telemetry_failure_never_breaks_the_call(self, posted, monkeypatch):
        def exploding_record(**_):
            raise RuntimeError("dashboard down")

        monkeypatch.setattr(insights, "record_call", exploding_record)
        llm = TelemetryProvider(_FakeProvider())
        assert llm.complete_text(system="s", user="u") == "plain"


class TestDisabled:
    def test_wrap_is_identity_when_disabled(self, monkeypatch):
        monkeypatch.setattr(insights.settings, "insights_ingest_token", "")
        inner = _FakeProvider()
        assert insights.wrap(inner) is inner

    def test_capture_outside_wrapper_is_noop(self):
        insights.capture(model="m", raw_response="x", usage={"input_tokens": 1})  # must not raise

    def test_record_call_noop_when_disabled(self, monkeypatch):
        monkeypatch.setattr(insights.settings, "insights_ingest_token", "")
        called = []
        monkeypatch.setattr(insights, "_post", lambda *a: called.append(a))
        insights.record_call(
            stage="s", prompt_version="v", model="m", system_prompt="a",
            user_prompt="b", raw_response="c", latency_ms=1,
        )
        assert called == []


class TestCapture:
    def test_usage_accumulates_across_attempts(self):
        cap = insights.CallCapture()
        cap.add_usage({"input_tokens": 10, "output_tokens": 2})
        cap.add_usage({"input_tokens": 4, "output_tokens": None, "cache_read_tokens": 7})
        assert cap.usage() == {
            "input_tokens": 14, "output_tokens": 2,
            "cache_read_tokens": 7, "cache_create_tokens": None,
        }

    def test_usage_from_anthropic_maps_cache_fields(self):
        class U:
            input_tokens = 1
            output_tokens = 2
            cache_read_input_tokens = 3
            cache_creation_input_tokens = 4

        assert insights.usage_from_anthropic(U()) == {
            "input_tokens": 1, "output_tokens": 2,
            "cache_read_tokens": 3, "cache_create_tokens": 4,
        }

    def test_usage_from_openai_tolerates_missing_details(self):
        class U:
            prompt_tokens = 5
            completion_tokens = 6
            prompt_tokens_details = None

        assert insights.usage_from_openai(U()) == {
            "input_tokens": 5, "output_tokens": 6,
            "cache_read_tokens": None, "cache_create_tokens": None,
        }

    def test_empty_raw_response_gets_placeholder(self, monkeypatch):
        bodies = []
        monkeypatch.setattr(insights.settings, "insights_api_url", "http://x")
        monkeypatch.setattr(insights.settings, "insights_ingest_token", "t")
        monkeypatch.setattr(insights, "_post", lambda p, b: bodies.append(b) or {"id": "r"})
        insights.reset_run_cache()
        insights.record_call(
            stage="s", prompt_version="v", model="m", system_prompt="a",
            user_prompt="b", raw_response="", latency_ms=1, parse_error="APIError: x",
        )
        insights.reset_run_cache()
        assert bodies[-1]["raw_response"].startswith("(no response")
