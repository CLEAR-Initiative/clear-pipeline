"""The manual push connector + the source-neutral staged-document reader.

`read_latest_staged_documents` is what makes `kb_pdf_text` / `kb_figures`
source-neutral: it unions the latest `.staged.jsonl` batch from every registered
KB source for a country, with S3 mocked here."""

import io
import json

from unittest.mock import MagicMock

from clear_pipeline.providers.kb_sources import get_connector, paths, staging
from clear_pipeline.providers.kb_sources.base import has_fetch
from clear_pipeline.providers.kb_sources.manual import SOURCE, ManualConnector
from clear_pipeline.providers.kb_sources.reliefweb import ReliefWebConnector


class TestManualConnector:
    def test_registered_and_push_only(self):
        c = get_connector("manual")
        assert isinstance(c, ManualConnector)
        assert has_fetch(c) is False  # no fetch → schedulers skip it

    def test_namespaced_and_country_scoped_paths(self):
        c = ManualConnector()
        assert c.reports_prefix("sdn") == paths.reports_prefix(SOURCE, "sdn", "upload")
        assert c.reports_prefix("sdn").startswith("manual/")   # own namespace
        assert "/sdn/" in c.pdf_key("sdn", "manual:x", "a.pdf")  # country folder kept
        assert c.pdf_key("sdn", "manual:x", "a.pdf").startswith("manual/")


def _fake_s3(objs: dict[str, list[str]], bodies: dict[str, bytes]) -> MagicMock:
    s3 = MagicMock()

    def _list(**kw):
        keys = objs.get(kw["Prefix"], [])
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def _get(Bucket, Key):
        return {"Body": io.BytesIO(bodies[Key])}

    s3.list_objects_v2.side_effect = _list
    s3.get_object.side_effect = _get
    return s3


def test_reader_unions_latest_batch_per_source_and_ignores_legacy():
    rw = ReliefWebConnector().reports_prefix("sdn")
    mn = ManualConnector().reports_prefix("sdn")
    objs = {
        rw: [
            rw + "2026-W44.staged.jsonl",
            rw + "2026-W45.staged.jsonl",  # newest → chosen
            rw + "2026-W45.jsonl",         # legacy raw dump → ignored
        ],
        mn: [mn + "2026-11-01T00:00:00Z.staged.jsonl"],
    }
    bodies = {
        rw + "2026-W45.staged.jsonl": json.dumps(
            {"source": "reliefweb", "report_id": "r1", "pdf_s3_key": "k1"},
        ).encode() + b"\n",
        mn + "2026-11-01T00:00:00Z.staged.jsonl": json.dumps(
            {"source": "manual", "report_id": "manual:x", "pdf_s3_key": "k2"},
        ).encode() + b"\n",
    }
    docs = staging.read_latest_staged_documents(_fake_s3(objs, bodies), "bucket", "sdn")
    assert sorted(d["source"] for d in docs) == ["manual", "reliefweb"]
    assert {d["report_id"] for d in docs} == {"r1", "manual:x"}  # W44 not re-read; legacy skipped


def test_reader_dedups_same_doc_within_a_batch():
    rw = ReliefWebConnector().reports_prefix("sdn")
    dup = json.dumps({"source": "reliefweb", "report_id": "r1", "pdf_s3_key": "k1"}).encode()
    objs = {rw: [rw + "2026-W45.staged.jsonl"]}
    bodies = {rw + "2026-W45.staged.jsonl": dup + b"\n" + dup + b"\n"}
    docs = staging.read_latest_staged_documents(_fake_s3(objs, bodies), "bucket", "sdn")
    assert len([d for d in docs if d["report_id"] == "r1"]) == 1


def test_reader_empty_when_no_markers():
    s3 = _fake_s3({}, {})
    assert staging.read_latest_staged_documents(s3, "bucket", "sdn") == []
