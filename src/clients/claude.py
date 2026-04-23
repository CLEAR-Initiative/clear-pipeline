"""Anthropic Claude API wrapper for structured ML operations.

Rate-limit / transient-error handling:
  - The Anthropic SDK auto-retries 408/409/429/5xx with exponential backoff.
    We bump max_retries so the SDK absorbs most bursts without propagating.
  - If the SDK still exhausts retries, `call_claude` catches `RateLimitError`
    and re-raises it as `ClaudeRateLimited`, which carries a suggested
    `retry_after` seconds. Callers (Celery tasks) should use this value when
    scheduling their own retry so they don't re-hit the same ceiling.
"""

import json
import logging
import random
import re
import time

import anthropic

from src.config import settings

logger = logging.getLogger(__name__)

_client: anthropic.Anthropic | None = None


class ClaudeRateLimited(Exception):
    """Raised when Claude returns 429 even after SDK-level retries.
    `retry_after` is the recommended delay (seconds) before retrying."""

    def __init__(self, message: str, retry_after: float) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        # max_retries=5 (default is 2) — the SDK handles 429 + 5xx with
        # exponential backoff per attempt. This absorbs most transient spikes
        # before we ever see them at the call site.
        _client = anthropic.Anthropic(
            api_key=settings.anthropic_api_key,
            max_retries=5,
            timeout=60.0,
        )
    return _client


def _retry_after_from_error(err: Exception) -> float:
    """Extract a retry-after delay from an Anthropic error, with fallback.

    Anthropic 429 responses include a `retry-after` header (seconds). We add
    a small jitter so parallel workers don't all wake up simultaneously.
    """
    default = 30.0
    retry_after = default

    resp = getattr(err, "response", None)
    if resp is not None:
        headers = getattr(resp, "headers", None) or {}
        raw = headers.get("retry-after") or headers.get("Retry-After")
        if raw:
            try:
                retry_after = float(raw)
            except (TypeError, ValueError):
                pass

    # Jitter: ±25% to avoid thundering-herd
    jitter = retry_after * 0.25
    return retry_after + random.uniform(-jitter, jitter)


def _extract_json(text: str) -> str | None:
    """Extract a JSON object from a response that may contain prose or markdown.

    Strategies (in order):
      1. Already pure JSON
      2. JSON inside ```json ... ``` or ``` ... ``` markdown fences
      3. First balanced { ... } object found in the text
    """
    text = text.strip()

    # 1. Pure JSON
    if text.startswith("{") and text.endswith("}"):
        return text

    # 2. Markdown code fence
    fence_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL | re.IGNORECASE)
    if fence_match:
        inner = fence_match.group(1).strip()
        if inner.startswith("{"):
            return inner

    # 3. First balanced { ... } object in the text
    start = text.find("{")
    if start == -1:
        return None

    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if escape:
            escape = False
            continue
        if ch == "\\" and in_string:
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]

    return None


def call_claude(system_prompt: str, user_prompt: str) -> dict:
    """
    Call Claude with a system + user prompt and parse JSON response.

    The system prompt should instruct Claude to respond with valid JSON.

    Raises:
      ClaudeRateLimited: 429 after SDK retries. Callers should reschedule
        the Celery task with `countdown=err.retry_after`.
      anthropic.APIStatusError: any other non-retryable API error.
      json.JSONDecodeError: model output was unparseable even after
        JSON-extraction fallbacks.
    """
    client = _get_client()

    t0 = time.monotonic()
    try:
        response = client.messages.create(
            model=settings.claude_model,
            max_tokens=1024,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
    except anthropic.RateLimitError as err:
        retry_after = _retry_after_from_error(err)
        logger.warning(
            "[CLAUDE] Rate-limited (after SDK retries). Will suggest retry_after=%.1fs. Error: %s",
            retry_after, err,
        )
        raise ClaudeRateLimited(str(err), retry_after=retry_after) from err
    except anthropic.APIStatusError as err:
        # 400/401/403/404 etc. — don't retry, bubble up.
        logger.error("[CLAUDE] API error: status=%s message=%s", err.status_code, err)
        raise

    elapsed_ms = (time.monotonic() - t0) * 1000
    logger.debug("[CLAUDE] API call ok in %.0fms", elapsed_ms)

    text = response.content[0].text.strip()

    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Extract JSON from prose/markdown
    extracted = _extract_json(text)
    if extracted:
        try:
            return json.loads(extracted)
        except json.JSONDecodeError:
            pass

    logger.error(
        "Failed to parse Claude response as JSON. Full response (first 500 chars): %s",
        text[:500],
    )
    raise json.JSONDecodeError("Could not extract valid JSON from Claude response", text, 0)
