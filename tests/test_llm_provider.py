"""Tests for pure helpers in the LLM provider.

The providers themselves are network-bound; these target the
deterministic parsing helpers in isolation.
"""

import json

import pytest

from clear_pipeline.providers.llm import (
    FallbackProvider,
    OpenAICompatibleProvider,
    _strip_code_fence,
    make_llm_provider,
)


class TestDisableThinkingWiring:
    """`disable_thinking` must suppress reasoning on BOTH wire providers: Ollama
    (`think: false`) and OpenRouter (`reasoning.enabled: false`). Sending only
    one leaves a reasoning model emitting reasoning tokens on the other, which
    empties `content` on a short-output role (the enrich/context bug)."""

    def _provider(self, **kw):
        return OpenAICompatibleProvider(
            role="context", model="nvidia/nemotron-3-nano-30b-a3b",
            base_url="https://openrouter.ai/api/v1", api_key="sk-test", **kw,
        )

    def test_disable_thinking_sets_both_provider_switches(self):
        p = self._provider(disable_thinking=True)
        body = p._extra_kwargs["extra_body"]
        assert body["think"] is False                    # Ollama
        assert body["reasoning"] == {"enabled": False}   # OpenRouter

    def test_no_extra_body_when_thinking_left_on(self):
        p = self._provider(disable_thinking=False)
        assert p._extra_kwargs == {}

    def test_require_parameters_is_independent_of_thinking(self):
        p = self._provider(disable_thinking=False, require_parameters=True)
        body = p._extra_kwargs["extra_body"]
        assert body == {"provider": {"require_parameters": True}}
        assert "think" not in body and "reasoning" not in body

    def test_no_think_reaches_an_openrouter_fallback(self, monkeypatch):
        # Regression: the enrich error was an empty response from the FALLBACK
        # (nvidia/nemotron-3-nano-30b-a3b). When the fallback is itself a
        # reasoning model, the role's NO_THINK must disable reasoning on it too,
        # not just the primary.
        env = {
            "LLM_CONTEXT_PROVIDER": "openai_compat",
            "LLM_CONTEXT_MODEL": "nvidia/nemotron-3-ultra-550b-a55b:free",
            "LLM_CONTEXT_API_KEY": "sk-primary",
            "LLM_CONTEXT_BASE_URL": "https://openrouter.ai/api/v1",
            "LLM_CONTEXT_NO_THINK": "true",
            "LLM_CONTEXT_FALLBACK_PROVIDER": "openai_compat",
            "LLM_CONTEXT_FALLBACK_MODEL": "nvidia/nemotron-3-nano-30b-a3b",
            "LLM_CONTEXT_FALLBACK_API_KEY": "sk-fallback",
            "LLM_CONTEXT_FALLBACK_BASE_URL": "https://openrouter.ai/api/v1",
        }
        for k, v in env.items():
            monkeypatch.setenv(k, v)

        provider = make_llm_provider("context")
        assert isinstance(provider, FallbackProvider)
        for p in (provider._primary, provider._fallback):
            assert p._extra_kwargs["extra_body"]["reasoning"] == {"enabled": False}
            assert p._extra_kwargs["extra_body"]["think"] is False


class TestStripCodeFence:
    """Some OpenRouter-hosted models (observed: google/gemma-4-26b-a4b)
    wrap valid JSON in a markdown ```json fence despite response_format=
    json_schema, which fails model_validate_json on the backticks. The
    stripper unwraps it so the good content parses; it must no-op on
    well-behaved (unfenced) output."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            # The exact gemma-4 shape from the eval logs.
            ('```json\n{"context":"across the country."}\n```',
             '{"context":"across the country."}'),
            # Fence with no language tag.
            ('```\n{"a":1}\n```', '{"a":1}'),
            # Single-line fence with an inline language tag.
            ('```json {"inline":1} ```', '{"inline":1}'),
            # Leading/trailing whitespace around the whole fenced block.
            ('  ```json\n{"pad":2}\n```  ', '{"pad":2}'),
        ],
    )
    def test_unwraps_fenced_json(self, raw, expected):
        out = _strip_code_fence(raw)
        assert out == expected
        assert json.loads(out) == json.loads(expected)  # parses cleanly

    def test_noop_on_unfenced_json(self):
        clean = '{"clean":true,"n":3}'
        assert _strip_code_fence(clean) == clean

    def test_noop_on_plain_text(self):
        # A non-JSON, non-fenced string is returned unchanged (the caller's
        # model_validate_json will raise the real error, not the stripper).
        assert _strip_code_fence("not json at all") == "not json at all"

    def test_handles_empty(self):
        assert _strip_code_fence("") == ""


# ── FallbackProvider + circuit breaker ────────────────────────────────

from clear_pipeline.providers.llm import EmptyResponseError


class _FakeProvider:
    """Minimal LLMProvider double. The first `fail_times` calls raise a provider
    error (EmptyResponseError ∈ _FALLBACK_ERRORS), then it succeeds. `raises`
    overrides the exception type (e.g. TypeError for a programming error)."""

    def __init__(self, name, *, fail_times=0, tag="ok", raises=EmptyResponseError):
        self.role = "context"
        self.model = name
        self.provider_name = name
        self._fail_times = fail_times
        self._tag = tag
        self._raises = raises
        self.calls = 0

    def complete_text(self, **kw):
        self.calls += 1
        if self.calls <= self._fail_times:
            raise self._raises(f"{self.model} boom {self.calls}")
        return self._tag

    complete_structured = complete_text


class TestFallbackProvider:
    def test_uses_primary_when_it_works(self):
        p, f = _FakeProvider("p", tag="P"), _FakeProvider("f", tag="F")
        fb = FallbackProvider(p, f)
        assert fb.complete_text(system="", user="") == "P"
        assert f.calls == 0  # fallback untouched

    def test_falls_back_on_primary_failure(self):
        p = _FakeProvider("p", fail_times=1)      # fails once
        f = _FakeProvider("f", tag="F")
        fb = FallbackProvider(p, f)
        assert fb.complete_text(system="", user="") == "F"  # served by fallback
        assert f.calls == 1

    def test_circuit_opens_after_consecutive_failures(self):
        # Primary always fails. After open_after=2, the breaker trips and the
        # primary is no longer even called — every request goes to fallback.
        p = _FakeProvider("p", fail_times=999)
        f = _FakeProvider("f", tag="F")
        fb = FallbackProvider(p, f, open_after=2, cooldown_seconds=300)
        for _ in range(5):
            assert fb.complete_text(system="", user="") == "F"
        # Primary tried only up to the trip point (2), then skipped entirely.
        assert p.calls == 2
        assert f.calls == 5

    def test_recovery_resets_failure_count(self):
        # 1 failure then successes — breaker never trips (needs 2 consecutive).
        p = _FakeProvider("p", fail_times=1, tag="P")
        f = _FakeProvider("f", tag="F")
        fb = FallbackProvider(p, f, open_after=2)
        assert fb.complete_text(system="", user="") == "F"  # 1st fails → fallback
        assert fb.complete_text(system="", user="") == "P"  # recovers
        assert fb.complete_text(system="", user="") == "P"  # stays on primary

    def test_programming_error_propagates_not_falls_back(self):
        # A TypeError from our own code is NOT a provider failure — it must
        # propagate, not silently trip the breaker and double spend on Claude.
        p = _FakeProvider("p", fail_times=1, raises=TypeError)
        f = _FakeProvider("f", tag="F")
        fb = FallbackProvider(p, f)
        with pytest.raises(TypeError):
            fb.complete_text(system="", user="")
        assert f.calls == 0  # fallback never reached

    def test_model_reports_the_serving_provider(self):
        # `llm.model` is persisted as provenance AFTER the call, so it must name
        # who actually served it — primary when healthy, fallback once it fails.
        p = _FakeProvider("cheap", fail_times=1, tag="P")
        f = _FakeProvider("claude", tag="F")
        fb = FallbackProvider(p, f, open_after=2)
        assert fb.model == "cheap"                 # before any call, defaults to primary
        fb.complete_text(system="", user="")       # primary fails → fallback serves
        assert fb.model == "claude"                # provenance = the model that produced it
        fb.complete_text(system="", user="")       # primary recovers
        assert fb.model == "cheap"
