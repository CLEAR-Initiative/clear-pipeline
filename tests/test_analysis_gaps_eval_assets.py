"""analysis_gaps + analysis_evaluation assets (PR-5). S3 load/save mocked."""

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from clear_pipeline.defs.analysis import assets_gaps as mod
from clear_pipeline.defs.analysis.taxonomy import l1_nodes

_THREE = list(l1_nodes()[:3])


def _art(completeness, *, qna=None, findings=True):
    return {
        "descriptive": {
            "completeness": completeness,
            "questions_not_answered": qna or [],
            "findings": [{"description": "x"}] if findings else [],
        }
    }


def _artifacts():
    a, b, c = _THREE
    return {a.key: _art(0.9), b.key: _art(0.2), c.key: _art(0.8, qna=["gap"])}


def _mocks(stack, saved):
    stack.enter_context(patch.object(mod, "l1_nodes", return_value=_THREE))
    stack.enter_context(patch.object(mod, "_s3", return_value=MagicMock()))
    stack.enter_context(patch.object(mod, "load_descriptive", return_value=_artifacts()))
    stack.enter_context(patch.object(mod, "save_json",
                                     side_effect=lambda s3, bucket, key, payload: saved.update({key: payload})))


def test_analysis_gaps_asset_derives_and_persists(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "b")
    saved = {}
    with ExitStack() as stack:
        _mocks(stack, saved)
        ctx = dg.build_asset_context(partition_key="SDN")
        res = mod.analysis_gaps(ctx)

    assert res["iso3"] == "SDN"
    # node b (0.2 < 0.6) + node c (unanswered) are gaps; node a is not
    assert res["gaps"] == 2
    assert res["artifacts_loaded"] == 3
    (key, payload), = saved.items()
    assert key.startswith("analysis/gaps/SDN/")
    assert len(payload["gaps"]) == 2
    assert {g["reason"] for g in payload["gaps"]} == {"low_completeness", "unanswered_questions"}


def test_analysis_evaluation_asset_filters_and_persists(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "b")
    saved = {}
    with ExitStack() as stack:
        _mocks(stack, saved)
        ctx = dg.build_asset_context(partition_key="SDN")
        res = mod.analysis_evaluation(ctx)

    # floor 0.4: a(0.9) and c(0.8) kept; b(0.2) flagged
    assert res["kept"] == 2
    assert res["flagged"] == 1
    (key, payload), = saved.items()
    assert key.startswith("analysis/evaluation/SDN/")
    assert payload["mean_completeness"] == pytest.approx((0.9 + 0.2 + 0.8) / 3)
    assert len(payload["verdicts"]) == 3


# ── analysis_web_gapfill ────────────────────────────────────────────

from clear_pipeline.defs.analysis.web_gapfill import (  # noqa: E402
    GapFillResult,
    GapSource,
    SourcedFinding,
)

_GAPS_DOC = {
    "gaps": [
        {"node_key": "pillars_1d/casualties", "node_name": "Casualties", "question": "How many killed?", "reason": "low_completeness"},
        {"node_key": "sectors/health", "node_name": "Health", "question": "Health access?", "reason": "unanswered_questions"},
    ]
}


def _fake_fill(*, requirement, provider, budget=None, **kw):
    return GapFillResult(
        requirement=requirement,
        value=[SourcedFinding(text="finding", source_urls=["https://who.int/x"])],
        sources=[GapSource(url="https://who.int/x", title="WHO", domain="who.int", category=1, reliability=4)],
        source_category_used=1, confidence=0.8, status="resolved",
        usage={"input_tokens": 1, "output_tokens": 1}, cost_usd=0.01,
        provenance={"prompt_version": "v1", "model": "m", "schema_version": "v1"},
    )


def _load_json_for(gaps_doc):
    def _f(s3, bucket, key):
        if "/gaps/" in key:
            return gaps_doc
        return None  # pinned-sites config absent
    return _f


def test_web_gapfill_no_gaps_is_noop(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "b")
    with ExitStack() as stack:
        stack.enter_context(patch.object(mod, "_s3", return_value=MagicMock()))
        stack.enter_context(patch.object(mod, "load_json", side_effect=_load_json_for({"gaps": []})))
        ff = stack.enter_context(patch.object(mod, "fill_gap", side_effect=_fake_fill))
        res = mod.analysis_web_gapfill(dg.build_asset_context(partition_key="SDN"))
    assert res["status"] == "no_gaps"
    ff.assert_not_called()


def test_web_gapfill_skips_without_api_key(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "b")
    monkeypatch.delenv("BRAVE_SEARCH_API_KEY", raising=False)
    with ExitStack() as stack:
        stack.enter_context(patch.object(mod, "_s3", return_value=MagicMock()))
        stack.enter_context(patch.object(mod, "load_json", side_effect=_load_json_for(_GAPS_DOC)))
        ff = stack.enter_context(patch.object(mod, "fill_gap", side_effect=_fake_fill))
        res = mod.analysis_web_gapfill(dg.build_asset_context(partition_key="SDN"))
    assert res["status"] == "no_api_key"
    assert res["gaps"] == 2
    ff.assert_not_called()


def test_web_gapfill_runs_for_unmet_gaps(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "b")
    monkeypatch.setenv("BRAVE_SEARCH_API_KEY", "bk")
    saved = {}
    with ExitStack() as stack:
        stack.enter_context(patch.object(mod, "_s3", return_value=MagicMock()))
        stack.enter_context(patch.object(mod, "load_json", side_effect=_load_json_for(_GAPS_DOC)))
        stack.enter_context(patch.object(mod, "make_llm_provider", return_value=MagicMock()))
        stack.enter_context(patch.object(mod, "fill_gap", side_effect=_fake_fill))
        stack.enter_context(patch.object(mod, "save_json",
                                         side_effect=lambda s3, bucket, key, payload: saved.update({key: payload})))
        res = mod.analysis_web_gapfill(dg.build_asset_context(partition_key="SDN"))
    assert res["status"] == "ran"
    assert res["attempted"] == 2 and res["resolved"] == 2
    assert len(saved) == 2
    assert all(k.startswith("analysis/gapfill/SDN/") for k in saved)
