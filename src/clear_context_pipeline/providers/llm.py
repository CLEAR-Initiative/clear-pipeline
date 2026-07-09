"""LLM provider abstraction — Anthropic (v1) + OpenAI-compatible (v2 OSS).

Every LLM call in the knowledge-base pipeline goes through
``make_llm_provider(role)`` so the choice of provider and model is
driven entirely by env vars. Two roles today:

  - ``context``    — cheap, per-chunk contextualization. Uses prompt
                     caching heavily. v1: Claude Haiku 4.5.
                     v2: Llama 3.3 8B via Together / Fireworks.
  - ``extraction`` — structured extraction of locations / time range /
                     event types / need sectors. Needs solid JSON-mode
                     compliance. v1: Claude Sonnet 4.6.
                     v2: Llama 3.3 70B (Together / Fireworks) with
                     ``response_format={"type": "json_schema", …}``.

Both implementations expose ``complete_structured`` returning an
instance of the caller's Pydantic schema. Downstream code never
branches on which provider answered — the schema round-trips give us
identical typing regardless of source.

Cache semantics:
  - AnthropicProvider uses ``cache_control`` on the system prompt when
    ``cache_key`` is provided. Callers set ``cache_key`` to something
    stable across many requests (typically the doc id) so Anthropic
    reuses the KV cache for the doc-level context on chunks 2..N.
  - OpenAICompatibleProvider ignores ``cache_key`` — most OSS-hosted
    providers don't yet expose a caching API. When Together / Fireworks
    stabilise their caching interfaces this class flips the same
    parameter on the request; call sites need no change.

Retry semantics:
  - Both providers wrap the outbound call in ``tenacity`` with
    exponential backoff on RateLimit / connection errors. Structured-
    output parse failures retry the LLM call once with the previous
    (raw) output appended to the user prompt as feedback — that
    boosts JSON compliance materially on Llama models without any
    guided-grammar plumbing.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Literal, Protocol, TypeVar

import anthropic
import openai
from pydantic import BaseModel, ValidationError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

LLMRole = Literal["context", "extraction"]

# Pydantic bound so `complete_structured` returns the exact subclass the
# caller passed in — not just BaseModel.
TModel = TypeVar("TModel", bound=BaseModel)


class LLMProvider(Protocol):
    """Role-scoped LLM client.

    Instances are cheap to hold and safe to share across coroutines /
    threads — providers create their own HTTP session internally.
    """

    role: LLMRole
    model: str
    provider_name: str

    def complete_structured(
        self,
        *,
        system: str,
        user: str,
        schema: type[TModel],
        max_tokens: int = 1024,
        cache_key: str | None = None,
    ) -> TModel:
        """Call the model and return an instance of ``schema``.

        Args:
            system: system-level instructions (constraints, role framing).
            user:   the actual per-request payload.
            schema: Pydantic model the response must satisfy. Providers
                    coerce whatever raw output they get (tool_use for
                    Anthropic, JSON mode for OpenAI-compatible) through
                    ``schema.model_validate_json``.
            max_tokens: cap on completion length. Kept explicit so an
                        oversized extraction can be diagnosed rather
                        than silently truncated at the provider default.
            cache_key:  opaque identifier for a shareable prefix. When
                        provided AND supported by the provider, the
                        ``system`` block is cached under this key so
                        subsequent calls with the same key skip the
                        prefix cost.
        """
        ...


# ────────────────────────────────────────────────────────────────────
# Anthropic — v1 default, unlocks prompt caching for the context step
# ────────────────────────────────────────────────────────────────────


class AnthropicProvider:
    """Native Anthropic implementation.

    Uses `messages.create` with a `tools` block wired to the caller's
    Pydantic schema — Claude's tool_use path is stricter about JSON
    shape than plain "respond with JSON" prompting and gives us free
    validation before we hit ``model_validate_json``.
    """

    provider_name = "anthropic"

    def __init__(self, *, role: LLMRole, model: str, api_key: str) -> None:
        self.role = role
        self.model = model
        self._client = anthropic.Anthropic(api_key=api_key, timeout=600.0)

    @retry(
        retry=retry_if_exception_type(
            (
                anthropic.RateLimitError,
                anthropic.APIConnectionError,
                anthropic.InternalServerError,
            ),
        ),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def complete_structured(
        self,
        *,
        system: str,
        user: str,
        schema: type[TModel],
        max_tokens: int = 1024,
        cache_key: str | None = None,
    ) -> TModel:
        # `system` accepts either a string or a list of content blocks;
        # the list form is what unlocks per-block cache_control. We
        # always send the list form so the shape is stable, and only
        # attach cache_control when the caller opted in.
        system_blocks: list[dict[str, Any]] = [
            {"type": "text", "text": system},
        ]
        if cache_key is not None:
            # Anthropic doesn't take our key verbatim; the cache is
            # content-addressed. We log the caller's intent for
            # diagnostics, but the actual dedup happens automatically
            # from the block bytes.
            system_blocks[0]["cache_control"] = {"type": "ephemeral"}
            logger.debug(
                "[LLM] anthropic prompt-cache enabled for role=%s cache_key=%s",
                self.role, cache_key,
            )

        tool = _pydantic_to_anthropic_tool(schema)
        response = self._client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            system=system_blocks,
            tools=[tool],
            tool_choice={"type": "tool", "name": tool["name"]},
            messages=[{"role": "user", "content": user}],
        )

        for block in response.content:
            if block.type == "tool_use" and block.name == tool["name"]:
                return schema.model_validate(block.input)

        # Fallback: the model returned free text instead of the tool
        # (extremely rare with `tool_choice` forcing the tool). Try to
        # parse as raw JSON so we don't drop the call entirely.
        text = "".join(getattr(b, "text", "") for b in response.content).strip()
        return schema.model_validate_json(text)


def _pydantic_to_anthropic_tool(schema: type[BaseModel]) -> dict[str, Any]:
    """Convert a Pydantic model into an Anthropic tool schema.

    Uses the model's own JSON schema so field types, enums, and
    descriptions all round-trip — Claude sees exactly what a caller
    typed on the Python side.
    """
    return {
        "name": f"emit_{schema.__name__.lower()}",
        "description": schema.__doc__ or f"Emit a {schema.__name__} object.",
        "input_schema": schema.model_json_schema(),
    }


# ────────────────────────────────────────────────────────────────────
# OpenAI-compatible — v2 OSS default via Together / Fireworks / vLLM
# ────────────────────────────────────────────────────────────────────


class OpenAICompatibleProvider:
    """Provider for any /v1/chat/completions endpoint.

    Together AI, Fireworks, DeepInfra, and self-hosted vLLM all expose
    the OpenAI wire format. Model name differs — that's what
    ``LLM_<role>_MODEL`` sets. Base URL differs — that's what
    ``LLM_<role>_BASE_URL`` sets.

    Structured output is done via ``response_format={"type":
    "json_schema", …}`` when the endpoint supports it (Together
    exposes this for their Llama endpoints), falling back to plain
    ``json_object`` mode with a Pydantic-guided retry.
    """

    provider_name = "openai_compat"

    def __init__(
        self,
        *,
        role: LLMRole,
        model: str,
        base_url: str,
        api_key: str,
        json_schema_mode: bool = True,
    ) -> None:
        self.role = role
        self.model = model
        self._json_schema_mode = json_schema_mode
        self._client = openai.OpenAI(base_url=base_url, api_key=api_key, timeout=600.0)

    @retry(
        retry=retry_if_exception_type(
            (
                openai.RateLimitError,
                openai.APIConnectionError,
                openai.InternalServerError,
            ),
        ),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def complete_structured(
        self,
        *,
        system: str,
        user: str,
        schema: type[TModel],
        max_tokens: int = 1024,
        cache_key: str | None = None,
    ) -> TModel:
        # cache_key is intentionally unused today — most OSS-hosted
        # providers don't expose caching yet. Kept in the signature so
        # switching back and forth doesn't touch call sites.
        del cache_key

        response_format: dict[str, Any]
        if self._json_schema_mode:
            response_format = {
                "type": "json_schema",
                "json_schema": {
                    "name": schema.__name__,
                    "schema": schema.model_json_schema(),
                    "strict": True,
                },
            }
        else:
            response_format = {"type": "json_object"}

        raw = self._client.chat.completions.create(
            model=self.model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format=response_format,  # type: ignore[arg-type]
        )
        text = (raw.choices[0].message.content or "").strip()
        try:
            return schema.model_validate_json(text)
        except (ValidationError, json.JSONDecodeError) as exc:
            # Single-shot repair: hand the previous raw output back with
            # the validation error attached. Llama tends to converge on
            # the second attempt when told exactly what it violated.
            logger.warning(
                "[LLM] %s parse failed for %s, retrying with feedback: %s",
                self.model, schema.__name__, exc,
            )
            repair = self._client.chat.completions.create(
                model=self.model,
                max_tokens=max_tokens,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                    {"role": "assistant", "content": text},
                    {
                        "role": "user",
                        "content": (
                            "The previous response failed JSON schema validation: "
                            f"{exc}. Re-emit ONLY the JSON, matching the schema exactly."
                        ),
                    },
                ],
                response_format=response_format,  # type: ignore[arg-type]
            )
            repaired_text = (repair.choices[0].message.content or "").strip()
            return schema.model_validate_json(repaired_text)


# ────────────────────────────────────────────────────────────────────
# Factory
# ────────────────────────────────────────────────────────────────────


def make_llm_provider(role: LLMRole) -> LLMProvider:
    """Instantiate the LLM provider configured for ``role``.

    Reads env vars scoped by role so ``context`` and ``extraction`` can
    point at different providers / models without conflict:

      LLM_<ROLE>_PROVIDER   = "anthropic" | "openai_compat"
      LLM_<ROLE>_MODEL      = e.g. "claude-haiku-4-5", "meta-llama/…"
      LLM_<ROLE>_API_KEY    = provider API key
      LLM_<ROLE>_BASE_URL   = required for "openai_compat" only
    """
    prefix = f"LLM_{role.upper()}_"
    provider = os.environ.get(f"{prefix}PROVIDER", "anthropic").strip().lower()
    model = _require_env(f"{prefix}MODEL")
    api_key = _require_env(f"{prefix}API_KEY")

    if provider == "anthropic":
        return AnthropicProvider(role=role, model=model, api_key=api_key)
    if provider == "openai_compat":
        base_url = _require_env(f"{prefix}BASE_URL")
        return OpenAICompatibleProvider(
            role=role, model=model, base_url=base_url, api_key=api_key,
        )
    raise ValueError(
        f"Unsupported {prefix}PROVIDER={provider!r}. "
        "Expected 'anthropic' or 'openai_compat'.",
    )


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing env var {name}. Set it in .env or export it.")
    return value
