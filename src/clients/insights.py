"""Telemetry client for clear-pipeline-insights dashboard.

Posts run + per-call telemetry to the insights API. All calls are fire-and-forget:
errors are logged and swallowed so telemetry can never break the pipeline.

Repo: github.com/CLEAR-Initiative/clear-pipeline-insights
"""

import getpass
import logging
import subprocess
import threading
from typing import Any

import httpx

from src.config import settings

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 3.0

_run_id: str | None = None
_run_id_lock = threading.Lock()


def _enabled() -> bool:
    return bool(settings.insights_ingest_token and settings.insights_api_url)


def _resolve_env() -> str:
    if settings.pipeline_env:
        return settings.pipeline_env
    try:
        user = getpass.getuser()
    except Exception:
        user = "unknown"
    return f"local-{user}"


def _git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
            check=True,
        )
        return result.stdout.strip() or None
    except Exception:
        return None


def _post(path: str, body: dict) -> dict | None:
    if not _enabled():
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
        # are debuggable without re-running with a packet capture.
        body_snippet = exc.response.text[:300] if exc.response.text else "<empty>"
        logger.warning("[insights] POST %s failed: %s — body: %s", path, exc, body_snippet)
        return None
    except Exception as exc:
        logger.warning("[insights] POST %s failed: %s", path, exc)
        return None


def ensure_run(
    *,
    name: str = "live",
    pipeline_repo: str = "clear-pipeline",
    config: dict[str, Any] | None = None,
) -> str | None:
    """Get-or-create the pipeline run for this process. Cached for the process lifetime.

    The insights API upserts on (name, env, pipeline_repo) while ended_at IS NULL,
    so multiple worker processes calling this concurrently all converge on the
    same run_id.
    """
    global _run_id
    if not _enabled():
        return None
    if _run_id is not None:
        return _run_id
    with _run_id_lock:
        if _run_id is not None:
            return _run_id
        body = {
            "name": name,
            "env": _resolve_env(),
            "pipeline_repo": pipeline_repo,
            "git_sha": _git_sha(),
            "config": config
            or {
                "claude_model": settings.claude_model,
                "relevance_threshold": settings.relevance_threshold,
            },
        }
        result = _post("/api/runs", body)
        if result and "id" in result:
            _run_id = result["id"]
            logger.info("[insights] run_id=%s env=%s", _run_id, body["env"])
        return _run_id


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
    parsed_response: dict | None = None,
    parse_error: str | None = None,
    usage: dict[str, int | None] | None = None,
) -> None:
    """Insert one llm_call row. Fire-and-forget; swallows all errors."""
    if not _enabled():
        return
    run_id = ensure_run()
    if not run_id:
        return
    usage = usage or {}
    # Dashboard requires non-empty raw_response. When the upstream model call
    # itself fails (e.g. Anthropic 400), there's no response to log — send a
    # placeholder so the failure row still lands in the dashboard.
    body = {
        "run_id": run_id,
        "stage": stage,
        "prompt_version": prompt_version,
        "model": model,
        "signal_id": signal_id,
        "event_id": event_id,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "raw_response": raw_response or "(no response — see parse_error)",
        "parsed_response": parsed_response,
        "parse_error": parse_error,
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "cache_read_tokens": usage.get("cache_read_tokens"),
        "cache_create_tokens": usage.get("cache_create_tokens"),
        "latency_ms": latency_ms,
    }
    _post("/api/calls", body)
