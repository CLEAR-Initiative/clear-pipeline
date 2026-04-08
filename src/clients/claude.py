"""Anthropic Claude API wrapper for structured ML operations."""

import json
import logging
import re

import anthropic

from src.config import settings

logger = logging.getLogger(__name__)

_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    return _client


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
    """
    client = _get_client()

    response = client.messages.create(
        model=settings.claude_model,
        max_tokens=1024,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )

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
