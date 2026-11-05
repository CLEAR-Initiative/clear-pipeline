"""Numbers extraction (PR-7b): pure build_frame_numbers, the mutation client,
and the analysis_numbers_extract asset. clear-api + S3 mocked."""

from unittest.mock import MagicMock, patch

import dagster as dg

from clear_pipeline.defs.analysis import assets_numbers as mod
from clear_pipeline.defs.analysis.numbers import build_frame_numbers
from clear_pipeline.providers import clear_api

_BUCKET = {
    "data": {"displacement": {"value": 10000}},
    "contributingReportIds": ["r1", "r2"],
    "newestSourceAt": "2026-06-01",
    "oldestSourceAt": "2026-01-01",
    "dataQualityScore": 7.5,
    "reportCount": 12,
}


# ── pure ────────────────────────────────────────────────────────────

def test_build_frame_numbers_from_bucket():
    n = build_frame_numbers(_BUCKET)
    assert n["empty"] is False
    assert n["origin"] == "aggregated"
    assert n["data"] == {"displacement": {"value": 10000}}
    assert n["contributing_report_ids"] == ["r1", "r2"]
    assert n["data_quality_score"] == 7.5
    assert n["report_count"] == 12


def test_build_frame_numbers_empty_when_no_bucket():
    n = build_frame_numbers(None)
    assert n["empty"] is True
    assert n["report_count"] == 0
    assert n["data"] == {}
    assert n["origin"] is None


# ── mutation client ─────────────────────────────────────────────────

def test_upsert_analysis_datapoints_builds_mutation():
    with patch.object(clear_api, "_execute",
                      return_value={"upsertAnalysisDatapoints": {"analysisId": "an1", "schemaVersion": "v1", "createdOrReplaced": False}}) as ex:
        res = clear_api.upsert_analysis_datapoints(
            analysis_id="an1", data={"x": 1}, contributing_report_ids=["r1"],
            schema_version="v1", origin="aggregated", report_count=3,
        )
    assert res["analysisId"] == "an1"
    _query, variables = ex.call_args[0]
    payload = variables["input"]
    assert payload["analysisId"] == "an1"
    assert payload["data"] == {"x": 1}
    assert payload["contributingReportIds"] == ["r1"]
    assert payload["origin"] == "aggregated"
    assert payload["reportCount"] == 3
    assert payload["schemaVersion"] == "v1"


# ── asset ───────────────────────────────────────────────────────────

def _run(monkeypatch, *, bucket):
    monkeypatch.setenv("S3_BUCKET", "b")
    saved = {}
    with patch.object(mod.clear_api, "get_pipeline_countries",
                      return_value=[{"name": "Sudan", "iso3": "SDN", "bbox": []}]), \
         patch.object(mod.clear_api, "resolve_country_location_id", return_value="loc-a0"), \
         patch.object(mod.clear_api, "get_aggregated_datapoint", return_value=bucket), \
         patch.object(mod, "_s3", return_value=MagicMock()), \
         patch.object(mod, "save_json", side_effect=lambda s3, bucket, key, payload: saved.update({key: payload})):
        res = mod.analysis_numbers_extract(dg.build_asset_context(partition_key="SDN"))
    return res, saved


def test_numbers_extract_persists_from_bucket(monkeypatch):
    res, saved = _run(monkeypatch, bucket=_BUCKET)
    assert res == {"iso3": "SDN", "report_count": 12, "empty": False}
    (key, payload), = saved.items()
    assert key.startswith("analysis/numbers/SDN/")
    assert payload["origin"] == "aggregated"
    assert payload["report_count"] == 12


def test_numbers_extract_empty_when_no_bucket(monkeypatch):
    res, saved = _run(monkeypatch, bucket=None)
    assert res["empty"] is True and res["report_count"] == 0
    (payload,) = saved.values()
    assert payload["empty"] is True
