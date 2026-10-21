"""Tests for the decoupled figure→KB sync + manual-upload merge (PR #129).

Deterministic: a fake embedder returns fixed vectors and clear-api's upsert is
monkeypatched to capture the call, so no network/DB. Covers the normalized-row
conversion, chunk shaping / high index base, `figures_only=True`, team
attribution passthrough, and the no-embeddable-figures short-circuit.
"""

import clear_pipeline.providers.clear_api as clear_api_mod
from clear_pipeline.defs.knowledgebase.upsert import (
    FIGURE_CHUNK_INDEX_BASE,
    _figure_chunk_rows,
    _normalize_pending_figure,
    merge_report_figures_to_kb,
)


class _Vec:
    embedding = [0.0] * 1024


class _Emb:
    provider_name = "fake"
    model = "fake-1"

    def embed(self, texts, input_type="document"):
        return [_Vec() for _ in texts]


def _pending(**over):
    """A `reportFiguresPendingKbMerge` row (clear-api GraphQL shape)."""
    base = {
        "reportId": "r1",
        "reportTitle": "T",
        "sourceUrl": "https://x",
        "reportS3Key": "pdf/r1.pdf",
        "publishedAt": "2026-01-01",
        "teamId": "team-9",
        "uploadedByUserId": "user-3",
        "figureS3Key": "fig/r1/4-abc.png",
        "figureKind": "chart",
        "pageNumber": 4,
        "title": "PIN by state",
        "transcription": {
            "kind": "chart",
            "title": "PIN by state",
            "description": "People in need per state.",
            "rows": [{"label": "Khartoum", "value": "1.2M"}],
        },
        "locationIds": ["loc-1"],
        "locationPcodes": [],
        "eventTypes": ["FL"],
        "needSectors": ["Food Security"],
        "timeRangeStart": None,
        "timeRangeEnd": None,
    }
    base.update(over)
    return base


def test_normalize_pending_figure_maps_fields():
    n = _normalize_pending_figure(_pending())
    assert n["s3_key"] == "fig/r1/4-abc.png"
    assert n["kind"] == "chart"
    assert n["page"] == 4
    assert n["event_types"] == ["FL"]
    assert n["need_sectors"] == ["Food Security"]
    # A null kind falls back to "infographic" (never empty).
    assert _normalize_pending_figure(_pending(figureKind=None))["kind"] == "infographic"


def test_figure_chunk_rows_shape_and_skip():
    rows = _figure_chunk_rows([_normalize_pending_figure(_pending())])
    assert len(rows) == 1
    assert "[chart figure] PIN by state" in rows[0]["embedded_text"]
    assert "Khartoum 1.2M" in rows[0]["embedded_text"]
    # Missing transcription → skipped (nothing to embed).
    assert _figure_chunk_rows([_normalize_pending_figure(_pending(transcription=None))]) == []


def test_merge_upserts_figures_only_with_attribution(monkeypatch):
    captured: dict = {}

    def _fake_upsert(**kwargs):
        captured.update(kwargs)
        return {
            "reportId": kwargs["report_id"],
            "chunksDeleted": 0,
            "chunksInserted": len(kwargs["chunks"]),
        }

    monkeypatch.setattr(clear_api_mod, "upsert_knowledgebase_chunks", _fake_upsert)

    figs = [
        _normalize_pending_figure(_pending()),
        _normalize_pending_figure(
            _pending(figureS3Key="fig/r1/5-def.png", pageNumber=5, title="Displacement"),
        ),
    ]
    inserted = merge_report_figures_to_kb(
        report_id="r1",
        report_title="T",
        source_url="https://x",
        report_s3_key="pdf/r1.pdf",
        published_at="2026-01-01",
        figures=figs,
        embedder=_Emb(),
        team_id="team-9",
        uploaded_by_user_id="user-3",
    )
    assert inserted == 2
    assert captured["figures_only"] is True
    assert captured["team_id"] == "team-9"
    assert captured["uploaded_by_user_id"] == "user-3"
    chunks = captured["chunks"]
    # High, non-colliding indices; every chunk carries a figure key.
    assert [c["chunkIndex"] for c in chunks] == [FIGURE_CHUNK_INDEX_BASE, FIGURE_CHUNK_INDEX_BASE + 1]
    assert all(c["figureS3Key"] for c in chunks)


def test_merge_no_embeddable_figures_skips_upsert(monkeypatch):
    calls = {"n": 0}

    def _fake_upsert(**kwargs):
        calls["n"] += 1
        return {"reportId": "r1", "chunksDeleted": 0, "chunksInserted": 0}

    monkeypatch.setattr(clear_api_mod, "upsert_knowledgebase_chunks", _fake_upsert)

    inserted = merge_report_figures_to_kb(
        report_id="r1",
        report_title="T",
        source_url="x",
        report_s3_key="p",
        published_at="2026-01-01",
        figures=[_normalize_pending_figure(_pending(transcription=None))],
        embedder=_Emb(),
    )
    assert inserted == 0
    assert calls["n"] == 0  # nothing embeddable → no upsert at all
