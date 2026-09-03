"""Tests for the pure helpers of the figures asset (infographic capture §6).

Deterministic — no S3, no LLM, no PDF. Covers transcription-text flattening
(fed to the parameter extractor) and the kind fallback.
"""

from clear_pipeline.defs.knowledgebase.figures import (
    _transcription_text,
    fig_kind_from_hint,
)
from clear_pipeline.providers.vision import (
    FigureGroup,
    FigureRow,
    FigureTranscription,
)


def test_transcription_text_flattens_flat_rows():
    t = FigureTranscription(
        kind="table",
        title="PIN by state",
        description="People in need per state.",
        rows=[
            FigureRow(label="Khartoum", value="1.2M"),
            FigureRow(label="South Darfur", value="800K", columns={"trend": "up"}),
        ],
    )
    blob = _transcription_text(t)
    assert "PIN by state" in blob
    assert "Khartoum 1.2M" in blob
    assert "South Darfur 800K up" in blob


def test_transcription_text_flattens_groups_and_callouts():
    t = FigureTranscription(
        kind="infographic",
        description="DTM snapshot.",
        groups=[FigureGroup(name="Nyala", rows=[FigureRow(label="IDPs", value="1,240")])],
        callouts=["2.5M in need"],
    )
    blob = _transcription_text(t)
    assert "Nyala" in blob
    assert "IDPs 1,240" in blob
    assert "2.5M in need" in blob


def test_transcription_text_empty_when_nothing_present():
    t = FigureTranscription(kind="photo", description="")
    assert _transcription_text(t) == ""


def test_fig_kind_from_hint_maps_table_and_defaults_to_infographic():
    assert fig_kind_from_hint({"kind_hint": "table"}) == "table"
    assert fig_kind_from_hint({"kind_hint": "page"}) == "infographic"
    assert fig_kind_from_hint({"kind_hint": "image"}) == "infographic"
    assert fig_kind_from_hint({}) == "infographic"
