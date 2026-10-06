"""``last_usage`` on the LLM providers (clear-api ADR-0010: a Worker reports
what its model cost). Mocked SDK clients."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from pydantic import BaseModel

from clear_pipeline.providers.llm import (
    AnthropicProvider,
    EmptyResponseError,
    FallbackProvider,
    OpenAICompatibleProvider,
    _pydantic_to_anthropic_tool,
)


class Out(BaseModel):
    answer: str


def test_anthropic_records_usage_after_each_call():
    p = AnthropicProvider(role="narrative", model="claude-sonnet-5-5", api_key="k")
    p._client = MagicMock()
    p._client.messages.create.return_value = SimpleNamespace(
        content=[SimpleNamespace(type="tool_use", name=_pydantic_to_anthropic_tool(Out)["name"], input={"answer": "x"})],
        usage=SimpleNamespace(input_tokens=120, output_tokens=30),
    )
    assert p.last_usage is None
    assert p.complete_structured(system="s", user="u", schema=Out).answer == "x"
    assert p.last_usage == {"input_tokens": 120, "output_tokens": 30}

    p._client.messages.create.return_value = SimpleNamespace(
        content=[SimpleNamespace(type="text", text="hello")],
        usage=SimpleNamespace(input_tokens=5, output_tokens=2),
    )
    assert p.complete_text(system="s", user="u") == "hello"
    assert p.last_usage == {"input_tokens": 5, "output_tokens": 2}


def test_openai_compatible_records_usage_including_the_repair_call():
    p = OpenAICompatibleProvider(role="narrative", model="m", base_url="http://x", api_key="k")
    p._client = MagicMock()
    bad = SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="not json"))],
                          usage=SimpleNamespace(prompt_tokens=10, completion_tokens=1))
    good = SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content='{"answer":"y"}'))],
                           usage=SimpleNamespace(prompt_tokens=12, completion_tokens=3))
    p._client.chat.completions.create.side_effect = [bad, good]
    assert p.complete_structured(system="s", user="u", schema=Out).answer == "y"
    assert p.last_usage == {"input_tokens": 22, "output_tokens": 4}


def test_fallback_reports_the_serving_providers_usage():
    primary = MagicMock(model="primary", provider_name="p", role="narrative", last_usage={"input_tokens": 1, "output_tokens": 1})
    fallback = MagicMock(model="fallback", provider_name="f", role="narrative", last_usage={"input_tokens": 7, "output_tokens": 7})
    fb = FallbackProvider(primary, fallback)
    primary.complete_text.return_value = "ok"
    assert fb.complete_text(system="s", user="u") == "ok"
    assert fb.last_usage == {"input_tokens": 1, "output_tokens": 1}
    primary.complete_text.side_effect = EmptyResponseError("hung")
    fallback.complete_text.return_value = "ok2"
    assert fb.complete_text(system="s", user="u") == "ok2"
    assert fb.last_usage == {"input_tokens": 7, "output_tokens": 7}
