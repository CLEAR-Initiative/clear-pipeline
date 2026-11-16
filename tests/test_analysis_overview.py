"""analysis_overview assembly/publish (PR-8b): overview synthesis + the publish
asset. clear-api, S3, and synthesis mocked."""

from unittest.mock import MagicMock, patch

import dagster as dg

from clear_pipeline.defs.analysis import assets_overview as mod
from clear_pipeline.defs.analysis.overview import OverviewLLM, build_overview_prompt
from clear_pipeline.defs.analysis.registry import provenance_stamp
from clear_pipeline.defs.analysis.runtime import NodeOutcome


def _art(name, finding):
    return {
        "node": {"name": name, "task": "Sectors", "level": 1},
        "descriptive": {
            "findings": [{"description": finding, "source_report_ids": ["r1"]}],
            "completeness": 0.7, "questions_not_answered": [], "source_report_ids": ["r1"],
        },
    }


def _empty_art(name):
    return {"node": {"name": name, "task": "Sectors", "level": 1},
            "descriptive": {"findings": [], "completeness": 0.0, "questions_not_answered": [], "source_report_ids": []}}


# ── pure prompt ─────────────────────────────────────────────────────

def test_build_overview_prompt_includes_findings_skips_empty():
    arts = {"sectors/health": _art("Health", "Clinics closed"), "sectors/wash": _empty_art("WASH")}
    prompt = build_overview_prompt(arts)
    assert "### Health" in prompt
    assert "Clinics closed" in prompt
    assert "### WASH" not in prompt  # empty-findings node skipped


# ── asset ───────────────────────────────────────────────────────────

_ARTIFACTS = {"sectors/health": _art("Health", "Clinics closed"), "pillars_1d/displacement": _art("Displacement", "100k displaced")}
_NUMBERS = {"data": {"displacement": {"value": 100000}}, "contributing_report_ids": ["r1"],
            "origin": "aggregated", "report_count": 5, "empty": False}


def _outcome(text="Overview text."):
    return NodeOutcome(
        output=OverviewLLM(overview=text, completeness=0.6), status="ok", turns=1,
        completeness=0.6, questions_not_answered=[], usage={"input_tokens": 1, "output_tokens": 1},
        cost_usd=0.02, provenance=provenance_stamp("deepseek/deepseek-v3.2"),
    )


def _package(kept_keys):
    return {
        "kept_node_keys": list(kept_keys),
        "verdicts": [
            {"node_key": k, "kept": True, "reason": "kept", "completeness": 0.7, "task": "Sectors"}
            for k in kept_keys
        ],
        "by_task": {"Sectors": 0.7}, "mean_completeness": 0.7, "completeness_floor": 0.4,
        "revised_node_keys": [], "web_rescued_node_keys": [],
    }


def _run(monkeypatch, *, numbers, upsert_result, artifacts=None, package=None, gapfills=None):
    monkeypatch.setenv("S3_BUCKET", "b")
    arts = _ARTIFACTS if artifacts is None else artifacts
    pkg = _package(arts.keys()) if package is None else package
    upsert_dp = MagicMock()
    with patch.object(mod.clear_api, "get_pipeline_countries", return_value=[{"name": "Sudan", "iso3": "SDN", "bbox": []}]), \
         patch.object(mod.clear_api, "resolve_country_location_id", return_value="loc-a0"), \
         patch.object(mod.clear_api, "upsert_analysis", return_value=upsert_result) as upsert_an, \
         patch.object(mod.clear_api, "upsert_analysis_datapoints", upsert_dp), \
         patch.object(mod, "_s3", return_value=MagicMock()), \
         patch.object(mod, "make_llm_provider", return_value=MagicMock()), \
         patch.object(mod, "load_final_descriptive", return_value=arts), \
         patch.object(mod, "load_gapfills", return_value=gapfills or {}), \
         patch.object(mod, "load_json", side_effect=[numbers, pkg]), \
         patch.object(mod, "synthesise_overview", return_value=_outcome()):
        res = mod.analysis_overview(dg.build_asset_context(partition_key="SDN"))
    return res, upsert_an, upsert_dp


def test_publishes_v6_payload_and_links_numbers(monkeypatch):
    res, upsert_an, upsert_dp = _run(
        monkeypatch, numbers=_NUMBERS,
        upsert_result={"analysisId": "an1", "skipped": False, "supersededPrevious": True, "reason": None},
    )
    assert res["analysis_id"] == "an1" and res["numbers_linked"] is True
    # analyses row upserted with the hybrid v6+v5 payload at schema_version v6
    kwargs = upsert_an.call_args.kwargs
    assert kwargs["schema_version"] == "v6"
    assert kwargs["location_ids"] == ["loc-a0"]
    data = kwargs["data"]
    assert data["schema_version"] == "v6" and "nodes" in data and data["v5"]["schema_version"] == "v5"
    # numbers linked to the generation
    upsert_dp.assert_called_once()
    assert upsert_dp.call_args.kwargs["analysis_id"] == "an1"


def test_skips_datapoints_when_upsert_skipped(monkeypatch):
    res, _upsert_an, upsert_dp = _run(
        monkeypatch, numbers=_NUMBERS,
        upsert_result={"analysisId": None, "skipped": True, "supersededPrevious": False, "reason": "within 24h floor"},
    )
    assert res["skipped"] is True and res["numbers_linked"] is False
    upsert_dp.assert_not_called()


def test_drops_flagged_nodes_and_attaches_quality_and_web_evidence(monkeypatch):
    arts = {
        "sectors/health": _art("Health", "Clinics closed"),
        "sectors/wash": _art("WASH", "Thin WASH note"),
    }
    # Package keeps only Health; WASH is flagged (dropped). Health has web evidence.
    package = {
        "kept_node_keys": ["sectors/health"],
        "verdicts": [
            {"node_key": "sectors/health", "kept": True, "reason": "kept", "completeness": 0.7, "task": "Sectors"},
            {"node_key": "sectors/wash", "kept": False, "reason": "below_floor", "completeness": 0.2, "task": "Sectors"},
        ],
        "by_task": {"Sectors": 0.45}, "mean_completeness": 0.45, "completeness_floor": 0.4,
        "revised_node_keys": ["sectors/health"], "web_rescued_node_keys": [],
    }
    gapfills = {"sectors/health": {"status": "partial", "confidence": 0.6,
                                   "value": [{"text": "web-sourced detail"}], "sources": [{"url": "x"}],
                                   "source_category_used": 2}}
    _res, upsert_an, _dp = _run(
        monkeypatch, numbers=_NUMBERS, artifacts=arts, package=package, gapfills=gapfills,
        upsert_result={"analysisId": "an1", "skipped": False, "supersededPrevious": True, "reason": None},
    )
    data = upsert_an.call_args.kwargs["data"]
    assert set(data["nodes"].keys()) == {"sectors/health"}  # WASH dropped
    assert data["quality"]["flagged"] == 1 and data["quality"]["kept"] == 1
    assert data["quality"]["dropped"][0]["reason"] == "below_floor"
    assert data["nodes"]["sectors/health"]["web_evidence"]["findings"] == ["web-sourced detail"]


def test_skips_datapoints_when_numbers_empty(monkeypatch):
    res, _upsert_an, upsert_dp = _run(
        monkeypatch, numbers={"empty": True, "data": {}, "report_count": 0},
        upsert_result={"analysisId": "an1", "skipped": False, "supersededPrevious": True, "reason": None},
    )
    assert res["numbers_linked"] is False
    upsert_dp.assert_not_called()
