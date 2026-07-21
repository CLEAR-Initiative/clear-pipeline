"""Unit tests for the idempotency + fallback helpers in pdf_text.

These cover the pure helpers without touching Dagster/S3:
  - `_existing_page_count`  — the skip-if-exists check that lets a re-run
     reuse a report already extracted by a prior run (instead of re-parsing
     the PDFs and re-OOMing the graphics-dense ones).
  - `_report_summary`       — the downstream record, built identically for
     freshly-extracted and reused reports.
  - `extract_pages_pypdf`   — the lighter fallback extractor.
"""

from unittest.mock import MagicMock

from clear_context_pipeline.defs.knowledgebase import pdf_text as pt
from clear_context_pipeline.defs.knowledgebase._pdf_extract import extract_pages_pypdf

# A minimal one-page PDF with the text "Hello CLEAR".
_SAMPLE_PDF = (
    b"%PDF-1.1\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]/Contents 4 0 R"
    b"/Resources<</Font<</F1 5 0 R>>>>>>endobj\n"
    b"4 0 obj<</Length 44>>stream\nBT /F1 24 Tf 100 700 Td (Hello CLEAR) Tj ET\n"
    b"endstream endobj\n"
    b"5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n"
    b"xref\n0 6\n0000000000 65535 f \ntrailer<</Root 1 0 R/Size 6>>\nstartxref\n0\n%%EOF"
)


def _s3_returning(body: bytes) -> MagicMock:
    s3 = MagicMock()
    obj_body = MagicMock()
    obj_body.read.return_value = body
    s3.get_object.return_value = {"Body": obj_body}
    return s3


class TestExistingPageCount:
    def test_counts_non_empty_lines(self):
        s3 = _s3_returning(b'{"page_num":1}\n{"page_num":2}\n{"page_num":3}\n')
        assert pt._existing_page_count(s3, "bucket", "key") == 3

    def test_none_when_object_missing(self):
        # get_object raises (NoSuchKey / any read failure) -> not cached.
        s3 = MagicMock()
        s3.get_object.side_effect = Exception("NoSuchKey")
        assert pt._existing_page_count(s3, "bucket", "key") is None

    def test_none_when_empty(self):
        # A malformed empty object shouldn't count as a valid extraction.
        s3 = _s3_returning(b"")
        assert pt._existing_page_count(s3, "bucket", "key") is None


class TestReportSummary:
    def test_uses_report_metadata_when_present(self):
        reports_by_id = {
            "123": {"fields": {
                "title": "Sudan Sitrep",
                "url": "https://reliefweb.int/report/123",
                "date": {"original": "2026-06-01", "created": "2026-06-02"},
            }}
        }
        entries = [{"url": "https://pdf", "s3_key": "pdfs/123.pdf", "filename": "a.pdf"}]
        assert pt._report_summary("123", entries, reports_by_id, "text/123.jsonl", 5) == {
            "report_id": "123",
            "report_title": "Sudan Sitrep",
            "source_url": "https://reliefweb.int/report/123",
            "s3_key": "pdfs/123.pdf",
            "published_at": "2026-06-01",  # prefers original over created
            "s3_text_key": "text/123.jsonl",
            "num_pages": 5,
        }

    def test_falls_back_to_filename_and_entry_url_when_metadata_missing(self):
        entries = [{"url": "https://pdf", "s3_key": "pdfs/123.pdf", "filename": "a.pdf"}]
        summary = pt._report_summary("123", entries, {}, "text/123.jsonl", 1)
        assert summary["report_title"] == "a.pdf"
        assert summary["source_url"] == "https://pdf"
        assert summary["published_at"] is None


class TestPypdfFallback:
    def test_extracts_text(self):
        assert extract_pages_pypdf(_SAMPLE_PDF) == [{"page_num": 1, "text": "Hello CLEAR"}]
