"""Tests for the vision transcription provider (infographic capture §6 C/D).

Deterministic — no network. Covers the transcription schema defaults, the
prompt builder, and transcribe_figure's best-effort None-on-failure contract
(the LLM provider is monkeypatched).
"""

from clear_context_pipeline.providers import vision
from clear_context_pipeline.providers.vision import (
    FigureGroup,
    FigureRow,
    FigureTranscription,
    _user_prompt,
    transcribe_figure,
)


def test_transcription_schema_defaults():
    t = FigureTranscription(kind="chart", description="A bar chart of PIN by state.")
    assert t.title is None and t.as_of is None and t.unit is None
    assert t.source is None and t.headline is None
    assert t.rows == [] and t.groups == [] and t.callouts == []


def test_row_and_group_round_trip():
    t = FigureTranscription(
        kind="infographic",
        groups=[
            FigureGroup(name="South Darfur", rows=[FigureRow(label="IDPs", value="1,240")]),
        ],
        callouts=["2.5M people in need"],
        description="Composite DTM snapshot.",
    )
    dumped = t.model_dump()
    assert dumped["groups"][0]["name"] == "South Darfur"
    assert dumped["groups"][0]["rows"][0]["value"] == "1,240"
    assert dumped["callouts"] == ["2.5M people in need"]


def test_user_prompt_includes_hint_and_context():
    prompt = _user_prompt("table", "Surrounding page text about displacement.")
    assert "table" in prompt
    assert "displacement" in prompt


def test_user_prompt_without_extras():
    prompt = _user_prompt(None, None)
    assert "Transcribe" in prompt
    assert "guessed" not in prompt  # no hint line


def test_page_context_is_truncated():
    prompt = _user_prompt(None, "Z" * 5000)
    # Only the first 800 chars of context are inlined ('Z' avoids collisions
    # with the prompt template's own letters).
    assert prompt.count("Z") == 800


def test_transcribe_figure_returns_none_on_provider_failure(monkeypatch):
    class _Boom:
        def complete_structured(self, **kwargs):
            raise RuntimeError("vision API down")

    monkeypatch.setattr(vision, "make_llm_provider", lambda role: _Boom())
    assert transcribe_figure(png_bytes=b"\x89PNG", kind_hint="image") is None


def test_transcribe_figure_passes_image_to_provider(monkeypatch):
    captured = {}

    class _Provider:
        def complete_structured(self, *, system, user, schema, max_tokens, images):
            captured["images"] = images
            captured["schema"] = schema
            return FigureTranscription(kind="map", description="A choropleth map.")

    monkeypatch.setattr(vision, "make_llm_provider", lambda role: _Provider())
    result = transcribe_figure(png_bytes=b"PNGBYTES", kind_hint="page")
    assert result is not None and result.kind == "map"
    assert captured["images"] == [("image/png", b"PNGBYTES")]
    assert captured["schema"] is FigureTranscription
