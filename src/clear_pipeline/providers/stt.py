"""Speech-to-text via OpenAI's Whisper API — hotline voice-note
transcription (defs/ground/transcribe.py).

Dedicated provider, not routed through providers/llm.py's
make_llm_provider: Whisper's API is a multipart audio-file upload that
returns plain text, not a chat/structured-output completion, so the
LLMRole/complete_structured abstraction doesn't fit.
"""

from __future__ import annotations

import openai

from clear_pipeline.signals.config import settings

_client: openai.OpenAI | None = None


def _get_client() -> openai.OpenAI:
    global _client
    if _client is None:
        _client = openai.OpenAI(
            api_key=settings.stt_api_key,
            base_url=settings.stt_base_url or None,
        )
    return _client


def transcribe_audio(audio_bytes: bytes, filename: str) -> str:
    """Transcribe one voice-note attachment. Raises on failure — callers
    isolate it the same way LLM failures are isolated per-item."""
    client = _get_client()
    result = client.audio.transcriptions.create(
        model=settings.stt_model,
        file=(filename, audio_bytes),
    )
    return result.text.strip()
