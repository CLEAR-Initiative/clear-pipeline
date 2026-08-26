"""Tests for the figure→knowledgebase merge (infographic capture RAG merge).

Deterministic — a fake S3 client returns a figure record JSONL; no network.
Covers row shaping, the high chunk-index base (no collision with text chunks),
and skipping figures with no usable transcription.
"""

import io
import json

from clear_context_pipeline.defs.knowledgebase.upsert import (
    FIGURE_CHUNK_INDEX_BASE,
    _figure_kb_rows,
)


class _Body:
    def __init__(self, data: bytes):
        self._buf = io.BytesIO(data)

    def read(self) -> bytes:
        return self._buf.read()


class _FakeS3:
    def __init__(self, objects: dict[str, bytes]):
        self._objects = objects

    def get_object(self, *, Bucket: str, Key: str):
        if Key not in self._objects:
            raise KeyError(Key)
        return {"Body": _Body(self._objects[Key])}


def _record(*figs: dict) -> bytes:
    return b"\n".join(json.dumps(f).encode("utf-8") for f in figs) + b"\n"


def _fig(**over):
    base = {
        "pageNumber": 4,
        "s3Key": "reliefweb/kb/figures/sdn/situation-report/r1/0004-abc.png",
        "kind": "chart",
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


def test_no_record_key_returns_empty():
    assert _figure_kb_rows(_FakeS3({}), "bucket", None) == []


def test_missing_object_returns_empty():
    assert _figure_kb_rows(_FakeS3({}), "bucket", "reliefweb/kb/figures/sdn/situation-report/r1.jsonl") == []


def test_figure_row_shape_and_tags():
    key = "rec.jsonl"
    s3 = _FakeS3({key: _record(_fig())})
    rows = _figure_kb_rows(s3, "bucket", key)
    assert len(rows) == 1
    row = rows[0]
    assert row["chunk_index"] == FIGURE_CHUNK_INDEX_BASE  # first figure, base offset
    assert row["page_start"] == 4 and row["page_end"] == 4
    assert row["figure_s3_key"] == "reliefweb/kb/figures/sdn/situation-report/r1/0004-abc.png"
    assert row["figure_kind"] == "chart"
    assert row["location_ids"] == ["loc-1"]
    assert row["event_types"] == ["FL"]
    assert row["need_sectors"] == ["Food Security"]
    # embedded_text carries the label prefix + the flattened numbers.
    assert "[chart figure] PIN by state" in row["embedded_text"]
    assert "Khartoum 1.2M" in row["embedded_text"]


def test_chunk_indices_do_not_collide_across_figures():
    s3 = _FakeS3({"k": _record(_fig(), _fig(title="Displacement"))})
    rows = _figure_kb_rows(s3, "bucket", "k")
    idxs = [r["chunk_index"] for r in rows]
    assert idxs == [FIGURE_CHUNK_INDEX_BASE, FIGURE_CHUNK_INDEX_BASE + 1]
    assert all(i >= FIGURE_CHUNK_INDEX_BASE for i in idxs)  # never in text 0..N range


def test_figures_without_transcription_are_skipped():
    s3 = _FakeS3({"k": _record(_fig(transcription=None), _fig())})
    rows = _figure_kb_rows(s3, "bucket", "k")
    assert len(rows) == 1  # only the transcribed one


def test_empty_transcription_text_is_skipped():
    empty = _fig(transcription={"kind": "photo", "description": ""})
    s3 = _FakeS3({"k": _record(empty)})
    assert _figure_kb_rows(s3, "bucket", "k") == []
