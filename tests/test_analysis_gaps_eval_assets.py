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
