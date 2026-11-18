"""Unit tests for the request-driven node deep-dive generator — frame-addressed
persistence, skip-if-exists reuse, and per-node failure isolation. Mocked S3 +
analyst (no LLM / no boto3)."""

import json
from unittest.mock import MagicMock

import clear_pipeline.defs.analysis.node_deepdive as nd
from clear_pipeline.defs.analysis import taxonomy
from clear_pipeline.defs.analysis.frame import Frame


class _Desc:
    def model_dump(self):
        return {"findings": [], "source_report_ids": ["r1"]}


class _Res:
    descriptive = _Desc()
    status = "ok"
    usage = {"input_tokens": 3, "output_tokens": 1}
    cost_usd = 0.02
    provenance = {"model": "deepseek/deepseek-v3.2"}
    rag_hit_count = 2


_NODE = taxonomy.node_by_key("pillars_1d/casualties")
_FRAME = Frame.build(window_start="2026-01-01", window_end="2026-03-31", location_ids=["x"])


def test_generates_and_persists_a_fresh_node(monkeypatch):
    s3 = MagicMock()
    s3.head_object.side_effect = Exception("404")  # not cached → generate
    monkeypatch.setattr(nd, "_s3_client", lambda: s3)
    monkeypatch.setenv("S3_BUCKET", "bucket")
    monkeypatch.setattr(nd, "make_llm_provider", lambda role: MagicMock())
    monkeypatch.setattr(nd, "analyse_node", lambda **kw: _Res())

    out = nd.generate_nodes_for_frame(
        frame=_FRAME, nodes=(_NODE,), scope_label="Sudan", budget=object(),
    )

    assert len(out) == 1
    art = out[0]
    assert art["reused"] is False
    assert art["node"]["key"] == "pillars_1d/casualties"
    assert art["status"] == "ok"
    assert art["descriptive"]["source_report_ids"] == ["r1"]
    s3.put_object.assert_called_once()
    # Frame-addressed key (not iso3/window_kind) — the on-demand scheme.
    assert _FRAME.digest() in s3.put_object.call_args.kwargs["Key"]


def test_reuses_a_cached_node_without_calling_the_llm(monkeypatch):
    s3 = MagicMock()
    s3.head_object.return_value = {}  # cached
    cached = {"node": {"key": "pillars_1d/casualties"}, "status": "ok", "descriptive": {}}
    body = MagicMock()
    body.read.return_value = json.dumps(cached).encode("utf-8")
    s3.get_object.return_value = {"Body": body}
    monkeypatch.setattr(nd, "_s3_client", lambda: s3)
    monkeypatch.setenv("S3_BUCKET", "bucket")
    monkeypatch.setattr(nd, "make_llm_provider", lambda role: MagicMock())

    def _must_not_generate(**kw):
        raise AssertionError("analyse_node called for a cached node")

    monkeypatch.setattr(nd, "analyse_node", _must_not_generate)

    out = nd.generate_nodes_for_frame(
        frame=_FRAME, nodes=(_NODE,), scope_label="Sudan", budget=object(),
    )

    assert out[0]["reused"] is True
    s3.put_object.assert_not_called()


def test_one_node_failure_is_isolated(monkeypatch):
    s3 = MagicMock()
    s3.head_object.side_effect = Exception("404")
    monkeypatch.setattr(nd, "_s3_client", lambda: s3)
    monkeypatch.setenv("S3_BUCKET", "bucket")
    monkeypatch.setattr(nd, "make_llm_provider", lambda role: MagicMock())

    def _boom(**kw):
        raise RuntimeError("provider exploded")

    monkeypatch.setattr(nd, "analyse_node", _boom)

    out = nd.generate_nodes_for_frame(
        frame=_FRAME, nodes=(_NODE,), scope_label="Sudan", budget=object(),
    )

    assert out[0]["status"] == "failed"
    assert out[0]["node"]["key"] == "pillars_1d/casualties"
