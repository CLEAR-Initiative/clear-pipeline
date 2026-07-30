"""LLM provider abstraction — Anthropic (v1) + OpenAI-compatible (v2 OSS).

Every LLM call in the knowledge-base pipeline goes through
``make_llm_provider(role)`` so the choice of provider and model is
driven entirely by env vars. Four roles today, split so each task runs
on a model matched to its stakes (cheap/tolerant → Haiku, accuracy- or
quality-critical → Sonnet), all still tfvars-tunable:

  - ``context``    — cheap, per-chunk contextualization. Uses prompt
                     caching heavily. Haiku.
  - ``extraction`` — per-chunk structured tagging (locations / time range /
                     event types / need sectors). Highest volume, tolerant
                     (has a validation-repair retry) → Haiku.
  - ``datapoints`` — the 6-domain numeric figure extraction. Accuracy-
                     critical (wrong numbers = wrong humanitarian data) →
                     Sonnet.
  - ``narrative``  — situation-analysis prose + sector generation. Low
                     volume, high quality bar → Sonnet.

``datapoints`` and ``narrative`` fall back to the ``extraction`` role's
env when their own vars aren't set, so a code deploy that lands before
the infra apply adding the per-role vars keeps working — and stays on
whatever ``extraction`` pointed at (Sonnet, pre-split) until that apply.

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
import threading
import time
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

LLMRole = Literal["context", "extraction", "datapoints", "narrative"]

# Per-request timeout (seconds). A flaky/hung model must fail FAST so it can be
# retried or fall back — never block the pipeline. 600s (the old value) let a
# single hung endpoint stall for 10 min × retries. 90s is ample for these
# completions (≤1500 output tokens) while turning a hang into a quick failure.
# Tune with LLM_TIMEOUT_SECONDS.
DEFAULT_TIMEOUT_SECONDS = float(os.environ.get("LLM_TIMEOUT_SECONDS", "90"))


class EmptyResponseError(RuntimeError):
    """A provider returned an empty/whitespace body (or no choices). Treated as
    a failure — not a successful empty string — so a configured fallback
    re-serves the call and the breaker counts it, rather than the context step
    silently embedding a blank prefix and poisoning that chunk."""


# Exceptions that mean "the provider/transport failed" and should trigger the
# fallback + count against the circuit breaker. Deliberately EXCLUDES our own
# programming errors (TypeError / ValueError / KeyError / AttributeError) so a
# bug in this code fails loudly instead of silently doubling spend on Claude.
_FALLBACK_ERRORS: tuple[type[BaseException], ...] = (
    openai.APIError,
    anthropic.APIError,
    ValidationError,
    json.JSONDecodeError,
    EmptyResponseError,
)

# New roles fall back to `extraction`'s env vars when their own aren't set.
# Keeps a code deploy safe if it lands before the infra apply that adds the
# per-role vars: pre-apply, `extraction` is still Sonnet, so datapoints /
# narrative correctly stay on Sonnet via this fallback.
_ROLE_ENV_FALLBACK: dict[LLMRole, LLMRole] = {
    "datapoints": "extraction",
    "narrative": "extraction",
}

# Pydantic bound so `complete_structured` returns the exact subclass the
# caller passed in — not just BaseModel.
TModel = TypeVar("TModel", bound=BaseModel)


def _strip_code_fence(text: str) -> str:
    """Unwrap a markdown ```` ```json … ``` ```` fence around a JSON body.

    Some OpenRouter-hosted models (observed: ``google/gemma-4-26b-a4b-it``)
    return valid JSON but wrap it in a code fence despite
    ``response_format={"type": "json_schema"}``, which then fails
    ``model_validate_json`` at "line 1 column 1" (the backticks). We strip a
    leading fence line (```` ``` ```` or ```` ```json ````) and a trailing
    ```` ``` ````. No-op when the content isn't fenced, so well-behaved
    models are unaffected."""
    s = text.strip()
    if not s.startswith("```"):
        return text
    if s.endswith("```"):
        s = s[:-3].rstrip()  # drop the closing fence
    s = s[3:]  # drop the opening ```
    # Drop an optional language tag (e.g. "json") that runs until the first
    # whitespace/newline — handles both ```json\n{…} and ```json {…} shapes.
    i = 0
    while i < len(s) and s[i].isalpha():
        i += 1
    return s[i:].strip()


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

    def complete_text(
        self,
        *,
        system: str,
        user: str,
        max_tokens: int = 1024,
        cache_key: str | None = None,
    ) -> str:
        """Call the model and return its raw text response (stripped).

        For steps whose output is a single free-text string (e.g. the
        contextual-retrieval prefix), where a JSON schema buys nothing and
        only adds a parse that models fond of ```json fences or empty bodies
        fail on. No structured-output support is required, so this works with
        any chat model. ``cache_key`` behaves as in ``complete_structured``.
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

    def __init__(
        self, *, role: LLMRole, model: str, api_key: str,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.role = role
        self.model = model
        self._client = anthropic.Anthropic(api_key=api_key, timeout=timeout)

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
    def complete_text(
        self,
        *,
        system: str,
        user: str,
        max_tokens: int = 1024,
        cache_key: str | None = None,
    ) -> str:
        system_blocks: list[dict[str, Any]] = [{"type": "text", "text": system}]
        if cache_key is not None:
            system_blocks[0]["cache_control"] = {"type": "ephemeral"}
        response = self._client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            system=system_blocks,
            messages=[{"role": "user", "content": user}],
        )
        text = _strip_code_fence("".join(getattr(b, "text", "") for b in response.content))
        if not text:
            raise EmptyResponseError(f"empty text response from {self.model}")
        return text


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
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.role = role
        self.model = model
        self._json_schema_mode = json_schema_mode
        self._client = openai.OpenAI(base_url=base_url, api_key=api_key, timeout=timeout)

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
        if not raw.choices:
            raise EmptyResponseError(f"no choices from {self.model}")
        text = _strip_code_fence(raw.choices[0].message.content or "")
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
            repaired_text = _strip_code_fence(repair.choices[0].message.content or "")
            return schema.model_validate_json(repaired_text)

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
    def complete_text(
        self,
        *,
        system: str,
        user: str,
        max_tokens: int = 1024,
        cache_key: str | None = None,
    ) -> str:
        # No response_format — plain chat completion. No JSON to parse, so any
        # chat model works regardless of structured-output support.
        del cache_key  # not cached on OSS routes yet (see complete_structured)
        raw = self._client.chat.completions.create(
            model=self.model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        if not raw.choices:
            raise EmptyResponseError(f"no choices from {self.model}")
        # Cheap OSS models wrap free text in ```fences``` (the same reason
        # complete_structured strips them). And an empty/whitespace body is a
        # FAILURE, not a successful "" — returning it would silently poison this
        # chunk's context embedding, so raise to trigger the fallback + breaker.
        text = _strip_code_fence(raw.choices[0].message.content or "")
        if not text:
            raise EmptyResponseError(f"empty text response from {self.model}")
        return text


# ────────────────────────────────────────────────────────────────────
# Resilience — never let a flaky model block the pipeline
# ────────────────────────────────────────────────────────────────────


class FallbackProvider:
    """Primary model with a reliable fallback, so a flaky/hung primary can
    never stall the pipeline or drop a unit of work.

    On a primary **provider failure** (timeout after retries, empty response,
    unparseable JSON — the ``_FALLBACK_ERRORS`` set; our own programming errors
    deliberately propagate instead), the same call is transparently re-served by
    the ``fallback`` provider (typically Claude). A per-provider **circuit
    breaker** trips after ``open_after`` consecutive primary failures and
    routes straight to the fallback for ``cooldown_seconds`` — so once a model
    is clearly down we stop paying its timeout on every call, and the batch
    keeps moving at fallback speed instead of grinding.

    Duck-types ``LLMProvider`` (``complete_structured`` / ``complete_text``).
    """

    def __init__(
        self,
        primary: LLMProvider,
        fallback: LLMProvider,
        *,
        open_after: int = 2,
        cooldown_seconds: float = 300.0,
    ) -> None:
        self._primary = primary
        self._fallback = fallback
        self._open_after = open_after
        self._cooldown = cooldown_seconds
        self._consecutive_failures = 0
        self._open_until = 0.0
        self._lock = threading.Lock()
        self.role = primary.role
        # The model that ACTUALLY served the most recent call, guarded by the
        # same lock as the breaker state.
        self._served_model = primary.model
        self.provider_name = f"{primary.provider_name}->fallback:{fallback.provider_name}"

    @property
    def model(self) -> str:
        """Model that served the most recent call. Call sites persist `llm.model`
        as provenance (`extracted_by_model`, the debug snapshot, `generated_by_
        model`) AFTER the call, so returning the last-served model keeps that
        honest: primary healthy → primary; breaker open → the fallback that
        actually produced the row. Reflects whichever call finished last under a
        provider shared across concurrent calls — fine for the sequential
        per-report extraction path this runs on."""
        with self._lock:
            return self._served_model

    def _breaker_open(self) -> bool:
        with self._lock:
            return time.monotonic() < self._open_until

    def _record(self, *, ok: bool) -> None:
        with self._lock:
            if ok:
                self._consecutive_failures = 0
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._open_after:
                self._open_until = time.monotonic() + self._cooldown
                self._consecutive_failures = 0
                logger.warning(
                    "[LLM] circuit OPEN for %s — routing to fallback %s for %.0fs",
                    self._primary.model, self._fallback.model, self._cooldown,
                )

    def _call(self, method: str, kwargs: dict[str, Any]) -> Any:
        if not self._breaker_open():
            try:
                result = getattr(self._primary, method)(**kwargs)
                self._record(ok=True)
                with self._lock:
                    self._served_model = self._primary.model
                return result
            except _FALLBACK_ERRORS as exc:
                # Only provider/transport/parse/empty failures fall back. A
                # TypeError/ValueError from our own code propagates instead of
                # silently tripping the breaker and doubling spend on Claude.
                self._record(ok=False)
                logger.warning(
                    "[LLM] primary %s failed (%s) — falling back to %s",
                    self._primary.model, exc, self._fallback.model,
                )
        result = getattr(self._fallback, method)(**kwargs)
        with self._lock:
            self._served_model = self._fallback.model
        return result

    def complete_structured(self, **kwargs: Any) -> Any:
        return self._call("complete_structured", kwargs)

    def complete_text(self, **kwargs: Any) -> Any:
        return self._call("complete_text", kwargs)


# ────────────────────────────────────────────────────────────────────
# Factory
# ────────────────────────────────────────────────────────────────────


def make_llm_provider(role: LLMRole) -> LLMProvider:
    """Instantiate the LLM provider configured for ``role``.

    Reads env vars scoped by role so each role can point at a different
    provider / model without conflict:

      LLM_<ROLE>_PROVIDER   = "anthropic" | "openai_compat"
      LLM_<ROLE>_MODEL      = e.g. "claude-haiku-4-5", "meta-llama/…"
      LLM_<ROLE>_API_KEY    = provider API key
      LLM_<ROLE>_BASE_URL   = required for "openai_compat" only

    For ``datapoints`` / ``narrative`` a missing role-scoped var falls back
    to the ``extraction`` role's equivalent (see ``_ROLE_ENV_FALLBACK``).
    """
    def _env(suffix: str, *, required: bool) -> str | None:
        val = os.environ.get(f"LLM_{role.upper()}_{suffix}")
        if not val and role in _ROLE_ENV_FALLBACK:
            val = os.environ.get(f"LLM_{_ROLE_ENV_FALLBACK[role].upper()}_{suffix}")
        if not val and required:
            raise RuntimeError(
                f"Missing env var LLM_{role.upper()}_{suffix}. "
                "Set it in .env or export it.",
            )
        return val

    provider = (_env("PROVIDER", required=False) or "anthropic").strip().lower()
    model = _env("MODEL", required=True)
    api_key = _env("API_KEY", required=True)
    assert model is not None and api_key is not None  # narrow for type-checkers

    def _construct(prov: str, mdl: str, key: str, base_url: str | None) -> LLMProvider:
        if prov == "anthropic":
            return AnthropicProvider(role=role, model=mdl, api_key=key)
        if prov == "openai_compat":
            if not base_url:
                raise RuntimeError(
                    f"openai_compat needs a BASE_URL (role {role}).",
                )
            return OpenAICompatibleProvider(
                role=role, model=mdl, base_url=base_url, api_key=key,
            )
        raise ValueError(
            f"Unsupported provider {prov!r} for role {role}. "
            "Expected 'anthropic' or 'openai_compat'.",
        )

    primary = _construct(
        provider, model, api_key,
        _env("BASE_URL", required=(provider == "openai_compat")),
    )

    # Optional reliable fallback. When a (typically cheap/OSS) primary fails or
    # hangs — after its short timeout + retries — the same call is re-served by
    # this provider so the pipeline never stalls or drops work. A circuit
    # breaker then routes straight to the fallback while the primary stays down.
    # Configure with LLM_<ROLE>_FALLBACK_{PROVIDER,MODEL,API_KEY,BASE_URL}.
    fb_model = _env("FALLBACK_MODEL", required=False)
    fb_key = _env("FALLBACK_API_KEY", required=False)
    if bool(fb_model) != bool(fb_key):
        # A half-set fallback (e.g. a typo'd key var) would otherwise silently
        # run a bare cheap primary with NO fallback and NO breaker — the exact
        # opposite of what ADR-0003 makes mandatory for a non-Claude backfill.
        logger.warning(
            "[LLM] role=%s has a PARTIAL fallback config (FALLBACK_MODEL=%s, "
            "FALLBACK_API_KEY set=%s) — fallback + circuit breaker are DISABLED. "
            "Set both, or neither.",
            role, fb_model or "(unset)", bool(fb_key),
        )
    if fb_model and fb_key:
        fb_provider = (_env("FALLBACK_PROVIDER", required=False) or "anthropic").strip().lower()
        fallback = _construct(
            fb_provider, fb_model, fb_key, _env("FALLBACK_BASE_URL", required=False),
        )
        logger.info(
            "[LLM] role=%s primary=%s with fallback=%s", role, model, fb_model,
        )
        return FallbackProvider(primary, fallback)
    return primary
