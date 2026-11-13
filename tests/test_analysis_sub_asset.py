"""analysis_sub fan-out asset (PR-4) + the S3 path helper. clear-api, S3, LLM,
and the analyst are all mocked — this tests the asset's orchestration (fan-out,
skip-if-exists, per-node persistence, metadata), not the analyst itself."""

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import dagster as dg

from clear_pipeline.defs.analysis import assets_descriptive as mod
from clear_pipeline.defs.analysis import paths
from clear_pipeline.defs.analysis.descriptive import NodeAnalysis, NodeDescriptive
from clear_pipeline.defs.analysis.taxonomy import l1_nodes


def test_descriptive_key_is_stable_and_nests_by_node():
    k = paths.descriptive_key("SDN", "yearly", "v1", "v1", "pillars_1d/casualties")
    assert k == "analysis/descriptive/SDN/yearly/v1_v1/pillars_1d/casualties.json"
    # version bump re-keys under a fresh prefix
    assert paths.descriptive_prefix("SDN", "yearly", "v2", "v1").endswith("/v2_v1")


def _fake_analyse(*, node, frame, scope_label, provider, aggregated_context=None, budget=None, **kw):
    d = NodeDescriptive(
        node_key=node.key, node_name=node.name, task=node.task, level=node.level,
        findings=[], completeness=0.5, questions_not_answered=[],
        source_report_ids=[], contributing_sources={},
    )
    return NodeAnalysis(
        node_key=node.key, descriptive=d, status="ok",
        usage={"input_tokens": 10, "output_tokens": 2}, cost_usd=0.01,
        provenance={"prompt_version": "v1", "model": "m", "schema_version": "v1"},
        rag_hit_count=3,
    )


def _run(monkeypatch, *, exists: bool):
    """Materialise analysis_sub for one partition with everything mocked.
    Returns (results, s3_mock)."""
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    two_nodes = list(l1_nodes()[:2])

    s3 = MagicMock()
    if exists:
        s3.head_object.return_value = {}  # object present → skip
    else:
        s3.head_object.side_effect = Exception("404")  # absent → compute

    with ExitStack() as stack:
        stack.enter_context(patch.object(mod, "l1_nodes", return_value=two_nodes))
        stack.enter_context(patch.object(mod, "_s3_client", return_value=s3))
        stack.enter_context(patch.object(mod, "make_llm_provider", return_value=MagicMock()))
        stack.enter_context(patch.object(mod, "analyse_node", side_effect=_fake_analyse))
        stack.enter_context(patch.object(mod.clear_api, "get_pipeline_countries",
                                         return_value=[{"name": "Sudan", "iso3": "SDN", "bbox": []}]))
        stack.enter_context(patch.object(mod.clear_api, "resolve_country_location_id",
                                         return_value="loc-sdn-a0"))
        stack.enter_context(patch.object(mod.clear_api, "get_aggregated_datapoint",
                                         return_value=None))
        ctx = dg.build_asset_context(partition_key="SDN")
        results = mod.analysis_sub(ctx)
    return results, s3


def test_fans_out_and_persists_one_artifact_per_node(monkeypatch):
    results, s3 = _run(monkeypatch, exists=False)
    assert len(results) == 2
    assert all(r["status"] == "ok" and not r["reused"] for r in results)
    assert s3.put_object.call_count == 2
    # keys are the per-node descriptive keys
    keys = {c.kwargs["Key"] for c in s3.put_object.call_args_list}
    expected = {
        paths.descriptive_key("SDN", "yearly", "v1", "v1", n.key) for n in l1_nodes()[:2]
    }
    assert keys == expected


def test_skip_if_exists_reuses_and_writes_nothing(monkeypatch):
    results, s3 = _run(monkeypatch, exists=True)
    assert len(results) == 2
    assert all(r["status"] == "cached" and r["reused"] for r in results)
    s3.put_object.assert_not_called()


def test_unknown_partition_fails(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    with patch.object(mod.clear_api, "get_pipeline_countries", return_value=[]):
        ctx = dg.build_asset_context(partition_key="SDN")
        try:
            mod.analysis_sub(ctx)
        except dg.Failure as e:
            assert "pipelineCountries" in str(e.description)
        else:
            raise AssertionError("expected dg.Failure for an unknown partition")
