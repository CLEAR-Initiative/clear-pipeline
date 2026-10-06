"""Tests for the speech-to-text provider (hotline voice-note transcription).

Deterministic — no network. The OpenAI client is monkeypatched; covers the
Whisper call shape, output stripping, and the lazy-singleton client reuse.
"""

from clear_pipeline.providers import stt


def test_transcribe_audio_calls_whisper_and_strips_output(monkeypatch):
    captured = {}

    class _Transcriptions:
        def create(self, *, model, file):
            captured["model"] = model
            captured["file"] = file
            return type("Result", (), {"text": "  we need water  "})()

    class _Audio:
        transcriptions = _Transcriptions()

    class _FakeClient:
        audio = _Audio()

    monkeypatch.setattr(stt, "_client", None)
    monkeypatch.setattr(stt.openai, "OpenAI", lambda **kwargs: _FakeClient())

    result = stt.transcribe_audio(b"raw-audio-bytes", "note.ogg")

    assert result == "we need water"
    assert captured["model"] == stt.settings.stt_model
    assert captured["file"] == ("note.ogg", b"raw-audio-bytes")


def test_get_client_is_a_lazy_singleton(monkeypatch):
    constructed = []

    class _FakeClient:
        pass

    def _fake_openai(**kwargs):
        constructed.append(kwargs)
        return _FakeClient()

    monkeypatch.setattr(stt, "_client", None)
    monkeypatch.setattr(stt.openai, "OpenAI", _fake_openai)

    first = stt._get_client()
    second = stt._get_client()

    assert first is second
    assert len(constructed) == 1
