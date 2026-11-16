"""Request-driven node deep-dive — on-demand generation of SELECTED taxonomy
nodes (L2/L3 leaves), not the country-default L1 fan-out.

This is the generation engine for the on-demand "drill into a node" feature: a
user selects specific taxonomy nodes (``taxonomy.resolve_selection`` reduces a
mixed-level pick to its leaf frontier) and we generate one descriptive finding
per selected leaf for an arbitrary :class:`Frame`.

It reuses the exact single-node machinery the country fan-out uses
(:func:`descriptive.analyse_node` + a shared :class:`RunBudget` + one provider
per concurrent node + bounded in-asset concurrency + skip-if-exists), differing
only in what drives it: a **frame + a resolved node selection** instead of a
country partition, and **frame-addressed** S3 keys (``paths.deepdive_key``)
since an on-demand frame has no single ``(iso3, window_kind)``.

Pure except S3 + the injected provider — the caller (the drain) owns the
clear-api surfacing (per-node rows, decision A) and the request outcome.
"""

from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

from clear_pipeline.defs.analysis import paths
from clear_pipeline.defs.analysis.budget import analysis_run_budget
from clear_pipeline.defs.analysis.descriptive import analyse_node
from clear_pipeline.defs.analysis.registry import (
    ANALYSIS_LLM_ROLE,
    PROMPT_VERSION,
    SCHEMA_VERSION,
)
from clear_pipeline.defs.analysis.runtime import BudgetExhaustedError
from clear_pipeline.providers.llm import make_llm_provider

if TYPE_CHECKING:
    from clear_pipeline.defs.analysis.frame import Frame
    from clear_pipeline.defs.analysis.taxonomy import TaxonomyNode

logger = logging.getLogger(__name__)

# Same bound as the country fan-out: parallel != unbounded spend (doc §7). The
# shared RunBudget is the hard $ ceiling; this just paces provider RPM/TPM.
_MAX_WORKERS = int(os.environ.get("ANALYSIS_FANOUT_WORKERS", "6"))


def _s3_client():
    # Lazy import so boto3 isn't pulled at module import (matches assets_descriptive).
    from clear_pipeline.providers.s3 import s3_client

    return s3_client()


def _exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:  # noqa: BLE001 — absent / unreadable → recompute
        return False


def _load(s3, bucket: str, key: str) -> dict | None:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:  # noqa: BLE001 — a cache read miss just recomputes
        return None


def _artifact(frame: Frame, node: TaxonomyNode, res) -> dict:
    """The per-node artifact persisted to S3 — same shape the country fan-out
    writes, so both paths stay interchangeable for any future consumer."""
    return {
        "frame": frame.upsert_kwargs(),
        "frame_digest": frame.digest(),
        "node": {"key": node.key, "name": node.name, "task": node.task, "level": node.level},
        "descriptive": res.descriptive.model_dump(),
        "status": res.status,
        "usage": res.usage,
        "cost_usd": res.cost_usd,
        "provenance": res.provenance,
        "rag_hit_count": res.rag_hit_count,
    }


def generate_nodes_for_frame(
    *,
    frame: Frame,
    nodes: tuple[TaxonomyNode, ...],
    scope_label: str,
    aggregated_context: str | None = None,
    effective_end: str | None = None,
    budget=None,
    log=None,
) -> list[dict]:
    """Generate one descriptive finding per selected node for ``frame``.

    Returns the per-node artifact dicts (fresh or reused), each carrying
    ``status`` — the caller filters to the ones worth surfacing and owns the
    clear-api upsert. One LLM provider per concurrent node (``total_usage`` would
    race on a shared instance — doc §7); per-node failures are isolated and come
    back as ``status="failed"``/``"budget_skipped"`` rather than sinking the batch.

    RAG retrieval is time-scoped to the frame's window (``effective_end``
    materialises a rolling frame's end), so an on-demand deep-dive over a
    historical window cites only in-window evidence.

    Skip-if-exists is frame-addressed: a re-run of the same frame reuses a node's
    artifact (loaded back from S3 so the caller can still upsert it) instead of
    paying for the LLM again.
    """
    log = log or logger
    budget = budget or analysis_run_budget()
    s3, bucket = _s3_client(), os.environ["S3_BUCKET"]
    fdig = frame.digest()

    def run_one(node: TaxonomyNode) -> dict:
        key = paths.deepdive_key(fdig, PROMPT_VERSION, SCHEMA_VERSION, node.key)
        if _exists(s3, bucket, key):
            cached = _load(s3, bucket, key)
            if cached is not None:
                cached["reused"] = True
                return cached
        provider = make_llm_provider(ANALYSIS_LLM_ROLE)
        res = analyse_node(
            node=node, frame=frame, scope_label=scope_label,
            provider=provider, aggregated_context=aggregated_context, budget=budget,
            include_time_range=True, effective_end=effective_end,
        )
        artifact = _artifact(frame, node, res)
        body = json.dumps(artifact, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
        artifact["reused"] = False
        return artifact

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {pool.submit(run_one, n): n for n in nodes}
        for fut in as_completed(futures):
            node = futures[fut]
            try:
                results.append(fut.result())
            except BudgetExhaustedError:
                log.warning("[node_deepdive] budget exhausted — skipped %s", node.key)
                results.append({"node": {"key": node.key}, "status": "budget_skipped"})
            except Exception:  # noqa: BLE001 — isolate one node's failure (doc §7)
                log.exception("[node_deepdive] node %s failed", node.key)
                results.append({"node": {"key": node.key}, "status": "failed"})

    return results
