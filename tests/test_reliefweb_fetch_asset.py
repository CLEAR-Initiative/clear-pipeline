"""`reliefweb_fetch` — the connector-based fetch asset (ADR-0013 phase-2 step 1).

Additive + inert: it computes the ingest window the same way the legacy
`reliefweb_weekly_reports_in_s3` asset does, delegates staging to
`ReliefWebConnector.fetch`, writes a first-ingest marker under the country's
reports prefix (a distinct `.staged.jsonl` key so it never clobbers the legacy
`<week>.jsonl` while both coexist pre-cutover), and returns the staged docs as
dicts for the (phase-2b) source-neutral downstream.

The connector + S3 are mocked, so these pin the asset's own logic — window
selection + marker — without network or S3.
"""

from dataclasses import asdict
from datetime import datetime
from unittest.mock import MagicMock

import dagster as dg

from clear_pipeline.defs import reliefweb_to_s3 as rw
from clear_pipeline.providers.kb_sources import KbRawDocument
from clear_pipeline.providers.kb_sources.reliefweb import ReliefWebConnector


def _docs(iso3="sdn"):
    return [
        KbRawDocument(
            source="reliefweb",
            report_id="r1",
            pdf_s3_key=f"reliefweb/pdfs/{iso3}/situation-report/r1/a.pdf",
            title="Sitrep",
            published_at="2026-11-01T00:00:00Z",
            country_iso3=iso3,
            filename="a.pdf",
        ),
    ]


def _run(monkeypatch, *, first_ingest, docs, iso3="sdn"):
    s3 = MagicMock()
    s3.list_objects_v2.return_value = {"KeyCount": 0 if first_ingest else 1}
    monkeypatch.setattr(rw, "_s3_client", lambda: s3)
    monkeypatch.setattr(rw, "_require_env", lambda name: "bucket")

    captured: dict = {}

    def fake_fetch(self, *, country_iso3, window_start, window_end):
        captured.update(
            country_iso3=country_iso3, window_start=window_start, window_end=window_end,
        )
        return docs

    monkeypatch.setattr(ReliefWebConnector, "fetch", fake_fetch)
    ctx = dg.build_asset_context(partition_key=iso3)
    result = rw.reliefweb_fetch(ctx)
    return result, s3, captured


def test_returns_staged_docs_as_dicts(monkeypatch):
    docs = _docs()
    result, _, captured = _run(monkeypatch, first_ingest=False, docs=docs)
    assert result == [asdict(d) for d in docs]
    assert captured["country_iso3"] == "sdn"


def test_writes_marker_under_reports_prefix_with_distinct_key(monkeypatch):
    _, s3, _ = _run(monkeypatch, first_ingest=False, docs=_docs())
    s3.put_object.assert_called_once()
    _, kwargs = s3.put_object.call_args
    # Marker lives under THIS country's reports prefix (so _is_first_ingest flips)
    # but under a distinct suffix so it never overwrites the legacy <week>.jsonl.
    assert kwargs["Key"].startswith(rw.reports_prefix("sdn"))
    assert kwargs["Key"].endswith(".staged.jsonl")
    assert kwargs["Bucket"] == "bucket"


def test_first_ingest_uses_initial_lookback(monkeypatch):
    _, _, captured = _run(monkeypatch, first_ingest=True, docs=_docs())
    start = datetime.fromisoformat(captured["window_start"])
    end = datetime.fromisoformat(captured["window_end"])
    assert (end - start).days == rw._DEFAULT_INITIAL_LOOKBACK_DAYS  # 90


def test_established_country_uses_weekly_delta(monkeypatch):
    _, _, captured = _run(monkeypatch, first_ingest=False, docs=_docs())
    start = datetime.fromisoformat(captured["window_start"])
    end = datetime.fromisoformat(captured["window_end"])
    assert (end - start).days == rw._DEFAULT_LOOKBACK_DAYS  # 7


def test_empty_fetch_writes_empty_marker_and_returns_empty(monkeypatch):
    result, s3, _ = _run(monkeypatch, first_ingest=False, docs=[])
    assert result == []
    s3.put_object.assert_called_once()
    _, kwargs = s3.put_object.call_args
    assert kwargs["Body"] == b""  # no records -> empty body, marker still written
