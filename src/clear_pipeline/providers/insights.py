"""Per-LLM-call telemetry for the clear-pipeline-insights dashboard.

Every LLM call made through ``make_llm_provider`` is reported as one row to
the insights ingest API (``POST /api/calls``), grouped under a pipeline run
(``POST /api/runs``). The dashboard computes cost from model + token counts,
so the pipeline never sends a price.

Three pieces live here so ``llm.py`` stays a thin provider layer:

- **Client** — ``ensure_run`` / ``record_call``. Fire-and-forget: every error
  is logged and swallowed so telemetry can never break the pipeline. Disabled
  (all no-ops) unless both ``INSIGHTS_API_URL`` and ``INSIGHTS_INGEST_TOKEN``
  are set.
- **Scope** — a contextvar the *caller* sets to label calls with the pipeline
  stage, prompt version and the signal/event being processed. Providers don't
  know any of that; assets and stage functions do::

      with insights.scope(stage="crisis.narrative", prompt_version=CRISIS_PROMPT_VERSION):
          llm.complete_structured(...)

  Unscoped calls are labelled with the LLM role and ``"unversioned"``.
- **Capture** — a contextvar the *provider* fills with the raw response text
  and token usage right after the API returns. ``TelemetryProvider`` (the
  wrapper ``make_llm_provider`` applies outermost) opens a capture around each
  call, times it, and posts the row — including failures, which land with
  ``parse_error`` set so a broken prompt is visible on the dashboard.

Repo: github.com/CLEAR-Initiative/clear-pipeline-insights
"""

from __future__ import annotations

import contextlib
import getpass
import logging
import os
import subprocess
import threading
import time
from collections.abc import Iterator
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

import httpx

from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 3.0
PIPELINE_REPO = "clear-pipeline"
DEFAULT_RUN_NAME = "live"
UNVERSIONED = "unversioned"
_NO_RESPONSE = "(no response — see parse_error)"
_MAX_ERROR_CHARS = 2000

# Roles whose configured model is recorded in the run's config blob so a run
# can be read back as "which model served which role". Mirrors LLMRole in
# llm.py; kept as strings here to avoid importing the provider module.
_ROLES = ("context", "extraction", "datapoints", "narrative", "signal", "translate", "vision")


# ────────────────────────────────────────────────────────────────────
# Client
# ────────────────────────────────────────────────────────────────────

_run_id: str | None = None
_run_id_lock = threading.Lock()


def enabled() -> bool:
    return bool(settings.insights_ingest_token and settings.insights_api_url)


def resolve_env() -> str:
    """``PIPELINE_ENV`` (set by infra: dev / staging / prod), else
    ``local-<user>`` so laptop runs are distinguishable on the dashboard."""
    if settings.pipeline_env:
        return settings.pipeline_env
    try:
        user = getpass.getuser()
    except Exception:  # noqa: BLE001 — best-effort label only
        user = "unknown"
    return f"local-{user}"


def _git_sha() -> str | None:
    # Deployed images have no .git — infra can pass the built SHA/tag instead.
    for var in ("GIT_SHA", "IMAGE_TAG"):
        val = os.environ.get(var)
        if val:
            return val
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=2, check=True,
        )
        return result.stdout.strip() or None
    except Exception:  # noqa: BLE001
        return None


def _role_models() -> dict[str, str | None]:
    return {role: os.environ.get(f"LLM_{role.upper()}_MODEL") for role in _ROLES}


def _post(path: str, body: dict[str, Any]) -> dict[str, Any] | None:
    if not enabled():
        return None
    url = f"{settings.insights_api_url.rstrip('/')}{path}"
    headers = {
        "Authorization": f"Bearer {settings.insights_ingest_token}",
        "Content-Type": "application/json",
    }
    try:
        resp = httpx.post(url, json=body, headers=headers, timeout=_TIMEOUT_SECONDS)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as exc:
        # Surface the dashboard's response body so request-validation issues
        # are debuggable from the logs alone.
        snippet = exc.response.text[:300] if exc.response.text else "<empty>"
        logger.warning("[insights] POST %s failed: %s — body: %s", path, exc, snippet)
        return None
    except Exception as exc:  # noqa: BLE001 — telemetry must never raise
        logger.warning("[insights] POST %s failed: %s", path, exc)
        return None


def ensure_run(
    *,
    name: str = DEFAULT_RUN_NAME,
    config: dict[str, Any] | None = None,
) -> str | None:
    """Get-or-create the pipeline run for this process; cached for its lifetime.

    The API upserts on ``(name, env, pipeline_repo)`` while the run is open, so
    every Dagster subprocess calling this converges on the same run id.
    """
    global _run_id
    if not enabled():
        return None
    if _run_id is not None:
        return _run_id
    with _run_id_lock:
        if _run_id is not None:
            return _run_id
        body = {
            "name": name,
            "env": resolve_env(),
            "pipeline_repo": PIPELINE_REPO,
            "git_sha": _git_sha(),
            "config": config if config is not None else {"llm_roles": _role_models()},
        }
        result = _post("/api/runs", body)
        if result and "id" in result:
            _run_id = result["id"]
            logger.info("[insights] run_id=%s env=%s", _run_id, body["env"])
        return _run_id


def reset_run_cache() -> None:
    """Forget the cached run id (tests, or after closing a run)."""
    global _run_id
    with _run_id_lock:
        _run_id = None


def record_call(
    *,
    stage: str,
    prompt_version: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    raw_response: str,
    latency_ms: int,
    signal_id: str | None = None,
    event_id: str | None = None,
    parsed_response: dict[str, Any] | None = None,
    parse_error: str | None = None,
    usage: dict[str, int | None] | None = None,
) -> None:
    """Insert one ``llm_call`` row. Fire-and-forget; swallows all errors."""
    if not enabled():
        return
    run_id = ensure_run()
    if not run_id:
        return
    usage = usage or {}
    body = {
        "run_id": run_id,
        "stage": stage,
        "prompt_version": prompt_version,
        "model": model,
        "signal_id": signal_id,
        "event_id": event_id,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        # The API requires a non-empty raw_response; a failed API call has none.
        "raw_response": raw_response or _NO_RESPONSE,
        "parsed_response": parsed_response,
        "parse_error": parse_error,
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "cache_read_tokens": usage.get("cache_read_tokens"),
        "cache_create_tokens": usage.get("cache_create_tokens"),
        "latency_ms": latency_ms,
    }
    _post("/api/calls", body)


# ────────────────────────────────────────────────────────────────────
# Scope — set by callers to label the calls they make
# ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Scope:
    stage: str | None = None
    prompt_version: str | None = None
    signal_id: str | None = None
    event_id: str | None = None


_EMPTY_SCOPE = Scope()
_SCOPE: ContextVar[Scope] = ContextVar("insights_scope", default=_EMPTY_SCOPE)


def current_scope() -> Scope:
    return _SCOPE.get()


@contextlib.contextmanager
def scope(
    *,
    stage: str | None = None,
    prompt_version: str | None = None,
    signal_id: str | None = None,
    event_id: str | None = None,
) -> Iterator[None]:
    """Label every LLM call made inside the block. Nested scopes merge: a field
    passed here overrides the enclosing scope's; an omitted one is inherited.

    Safe to use whether or not telemetry is enabled — it only sets a contextvar.
    """
    outer = _SCOPE.get()
    merged = Scope(
        stage=stage if stage is not None else outer.stage,
        prompt_version=prompt_version if prompt_version is not None else outer.prompt_version,
        signal_id=signal_id if signal_id is not None else outer.signal_id,
        event_id=event_id if event_id is not None else outer.event_id,
    )
    token = _SCOPE.set(merged)
    try:
        yield
    finally:
        _SCOPE.reset(token)


# ────────────────────────────────────────────────────────────────────
# Capture — filled by providers with what the API actually returned
# ────────────────────────────────────────────────────────────────────


@dataclass
class CallCapture:
    model: str | None = None
    raw_response: str = ""
    input_tokens: int | None = None
    output_tokens: int | None = None
    cache_read_tokens: int | None = None
    cache_create_tokens: int | None = None

    def add_usage(self, usage: dict[str, int | None]) -> None:
        # Accumulate across attempts within one logical call (tenacity retries,
        # the OpenAI-compatible JSON repair round-trip, primary → fallback), so
        # the row's tokens reflect what the call really cost.
        for key in ("input_tokens", "output_tokens", "cache_read_tokens", "cache_create_tokens"):
            val = usage.get(key)
            if val is None:
                continue
            setattr(self, key, (getattr(self, key) or 0) + int(val))

    def usage(self) -> dict[str, int | None]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_create_tokens": self.cache_create_tokens,
        }


_CAPTURE: ContextVar[CallCapture | None] = ContextVar("insights_capture", default=None)


def capture(
    *,
    model: str,
    raw_response: str | None,
    usage: dict[str, int | None] | None = None,
) -> None:
    """Called by a provider right after the API returns. No-op unless a
    ``TelemetryProvider`` opened a capture for the current call."""
    cap = _CAPTURE.get()
    if cap is None:
        return
    cap.model = model
    if raw_response:
        cap.raw_response = raw_response
    if usage:
        cap.add_usage(usage)


def usage_from_anthropic(usage: Any) -> dict[str, int | None]:
    return {
        "input_tokens": getattr(usage, "input_tokens", None),
        "output_tokens": getattr(usage, "output_tokens", None),
        "cache_read_tokens": getattr(usage, "cache_read_input_tokens", None),
        "cache_create_tokens": getattr(usage, "cache_creation_input_tokens", None),
    }


def usage_from_openai(usage: Any) -> dict[str, int | None]:
    details = getattr(usage, "prompt_tokens_details", None)
    return {
        "input_tokens": getattr(usage, "prompt_tokens", None),
        "output_tokens": getattr(usage, "completion_tokens", None),
        "cache_read_tokens": getattr(details, "cached_tokens", None) if details else None,
        "cache_create_tokens": None,
    }


# ────────────────────────────────────────────────────────────────────
# Wrapper — applied by make_llm_provider when telemetry is enabled
# ────────────────────────────────────────────────────────────────────


class TelemetryProvider:
    """Duck-types ``LLMProvider``; records one row per call on the inner
    provider, success or failure, then returns/re-raises unchanged."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.role = inner.role
        self.provider_name = inner.provider_name

    @property
    def model(self) -> str:
        # Delegate so a FallbackProvider's "model that served the last call"
        # stays visible to call sites that persist it as provenance.
        return self._inner.model

    def complete_structured(self, **kwargs: Any) -> Any:
        return self._call("complete_structured", kwargs)

    def complete_text(self, **kwargs: Any) -> Any:
        return self._call("complete_text", kwargs)

    def _call(self, method: str, kwargs: dict[str, Any]) -> Any:
        cap = CallCapture()
        token = _CAPTURE.set(cap)
        started = time.monotonic()
        result: Any = None
        error: BaseException | None = None
        try:
            result = getattr(self._inner, method)(**kwargs)
            return result
        except BaseException as exc:
            error = exc
            raise
        finally:
            _CAPTURE.reset(token)
            latency_ms = int((time.monotonic() - started) * 1000)
            try:
                self._record(method, kwargs, cap, result, error, latency_ms)
            except Exception as exc:  # noqa: BLE001 — telemetry must never raise
                logger.warning("[insights] record failed: %s", exc)

    def _record(
        self,
        method: str,
        kwargs: dict[str, Any],
        cap: CallCapture,
        result: Any,
        error: BaseException | None,
        latency_ms: int,
    ) -> None:
        sc = current_scope()
        parsed: dict[str, Any] | None = None
        parse_error: str | None = None
        if error is not None:
            parse_error = f"{type(error).__name__}: {error}"[:_MAX_ERROR_CHARS]
        elif method == "complete_structured" and hasattr(result, "model_dump"):
            parsed = result.model_dump(mode="json")
        raw = cap.raw_response or (result if isinstance(result, str) else "")
        record_call(
            stage=sc.stage or self.role,
            prompt_version=sc.prompt_version or UNVERSIONED,
            model=cap.model or self._inner.model,
            signal_id=sc.signal_id,
            event_id=sc.event_id,
            system_prompt=str(kwargs.get("system", "")),
            user_prompt=str(kwargs.get("user", "")),
            raw_response=raw,
            parsed_response=parsed,
            parse_error=parse_error,
            usage=cap.usage(),
            latency_ms=latency_ms,
        )


def wrap(provider: Any) -> Any:
    """Return ``provider`` wrapped for telemetry, or unchanged when disabled."""
    return TelemetryProvider(provider) if enabled() else provider
