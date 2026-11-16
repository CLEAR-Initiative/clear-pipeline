"""analysis_gaps + analysis_evaluation assets (PR-5). S3 load/save mocked."""

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from clear_pipeline.defs.analysis import assets_gaps as mod
from clear_pipeline.defs.analysis.judge import JudgeVerdict
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


def test_analysis_evaluation_asset_judges_band_and_persists(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "b")
    saved = {}
    a, b, c = _THREE
    # floor 0.4, band 0.2 → ceiling 0.6: a(0.9)/c(0.8) trusted-kept without a judge
    # call; b(0.2) is in the uncertain band → judged. The judge flags it.
    flagged = JudgeVerdict(
        node_key=b.key, score="not_answered", has_content=True, out_of_subject=False,
        out_of_subject_share="none", notes="thin", kept=False, reason="judge_not_answered",
    )
    with ExitStack() as stack:
        _mocks(stack, saved)
        stack.enter_context(patch.object(mod, "make_llm_provider", return_value=MagicMock()))
        jn = stack.enter_context(patch.object(mod, "judge_node", return_value=flagged))
        ctx = dg.build_asset_context(partition_key="SDN")
        res = mod.analysis_evaluation(ctx)

    # only the band node b was judged; a and c kept cheaply
    jn.assert_called_once()
    assert res["nodes_judged"] == 1
    assert res["kept"] == 2 and res["flagged"] == 1
    (key, payload), = saved.items()
    assert key.startswith("analysis/evaluation/SDN/")
    assert payload["mean_completeness"] == pytest.approx((0.9 + 0.2 + 0.8) / 3)
    assert len(payload["verdicts"]) == 3
    # judge verdict stored (for revision re-use) + reason surfaced + recommendation
    assert b.key in payload["judge_verdicts"]
    by_key = {v["node_key"]: v for v in payload["verdicts"]}
    assert by_key[b.key]["reason"] == "judge_not_answered"
    assert by_key[b.key]["judge"]["score"] == "not_answered"
    assert payload["recommendations"][b.key]["action"] == "Add"
    # provenance: which judge model + prompt version produced the verdict
    prov = payload["judge_provenance"][b.key]
    assert prov["prompt_version"] == "v1" and "model" in prov


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
    # default provider is serper → its key must be unset for the skip path
    monkeypatch.delenv("WEB_SEARCH_PROVIDER", raising=False)
    monkeypatch.delenv("SERPER_API_KEY", raising=False)
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
    monkeypatch.setenv("SERPER_API_KEY", "sk")  # default provider is serper
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
