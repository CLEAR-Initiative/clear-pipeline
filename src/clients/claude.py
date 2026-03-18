"""Anthropic Claude API wrapper for structured ML operations."""

import json
import logging

import anthropic

from src.config import settings

logger = logging.getLogger(__name__)

_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    return _client


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

    # Extract JSON from response (handle markdown code blocks)
    if text.startswith("```"):
        # Remove ```json and ``` markers
        lines = text.split("\n")
        text = "\n".join(
            line for line in lines if not line.strip().startswith("```")
        )

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.error("Failed to parse Claude response as JSON: %s", text[:200])
        raise
