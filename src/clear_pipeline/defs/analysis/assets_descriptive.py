"""``analysis_sub`` — descriptive analysis fan-out over the taxonomy (doc §3, §6).

A country-partitioned Dagster asset that, per country, fans out over the
taxonomy nodes **at the configured depth (L1 by default)** and runs the
single-node descriptive analyst (:func:`descriptive.analyse_node`) for each —
with **op-level in-asset concurrency** (a thread pool, not Dagster steps, since
the code location runs the ``in_process`` executor). Each node gets its **own
LLM provider** (so ``total_usage`` is unambiguous — doc §7) and shares one
thread-safe :class:`RunBudget`. Per-node results are persisted to S3, content-
addressed with a **skip-if-exists** guard so a re-run only recomputes missing
nodes.

At depth L1 there is no roll-up — each L1 node artifact *is* the section picture
(doc §6); the roll-up asset arrives when deeper depths are enabled.

NOTE: no ``from __future__ import annotations`` — Dagster introspects the
``context`` parameter annotation at definition load.
"""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import dagster as dg

from clear_pipeline.defs.analysis import paths
from clear_pipeline.defs.analysis.budget import analysis_run_budget
from clear_pipeline.defs.analysis.descriptive import analyse_node
from clear_pipeline.defs.analysis.frame import country_frame
from clear_pipeline.defs.analysis.prompts import format_aggregated_for_prompt
from clear_pipeline.defs.analysis.registry import (
    ANALYSIS_LLM_ROLE,
    PROMPT_VERSION,
    SCHEMA_VERSION,
)
from clear_pipeline.defs.analysis.runtime import BudgetExhaustedError
from clear_pipeline.defs.analysis.taxonomy import l1_nodes
from clear_pipeline.defs.knowledgebase.datapoints_schemas import (
    SCHEMA_VERSION as AGGREGATION_SCHEMA_VERSION,
)
from clear_pipeline.defs.reliefweb_partitions import country_partitions
from clear_pipeline.providers import clear_api
from clear_pipeline.providers.llm import make_llm_provider

logger = logging.getLogger(__name__)

# Bounded concurrency so the fan-out respects provider RPM/TPM, clear-api RAG
# load, and the run budget (doc §7). Parallel != unbounded spend.
_MAX_WORKERS = int(os.environ.get("ANALYSIS_FANOUT_WORKERS", "6"))
_WINDOW_KIND = "yearly"


def _s3_client():
    # Lazy import so boto3 isn't pulled at module import (matches pdf_text.py).
    from clear_pipeline.providers.s3 import s3_client

    return s3_client()


def _exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:  # noqa: BLE001 — absent / unreadable → recompute
        return False


def _calendar_year_window(year: int) -> tuple[str, str]:
    start = datetime(year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    return start.isoformat(), end.isoformat()


@dg.asset(
    group_name="analysis",
    partitions_def=country_partitions,
)
def analysis_sub(context: dg.AssetExecutionContext) -> list[dict]:
    """Descriptive L1 analysis for THIS country partition — one artifact per L1
    node, written to S3 (skip-if-exists). Returns a per-node summary list.

    Root of the analysis subgraph — deliberately NOT a Dagster dependency of the
    weekly knowledge-base pipeline. The analysis module is its OWN automation
    (its own job + schedule); it reads the current KB state via clear-api (RAG)
    at run time rather than being chained to the KB run."""
    iso3 = context.partition_key
    row = {
        (c.get("iso3") or "").lower(): c for c in clear_api.get_pipeline_countries()
    }.get(iso3.lower())
    if row is None:
        raise dg.Failure(
            description=f"partition {iso3!r} is not in clear-api pipelineCountries"
        )
    country_name = row["name"]
    country_id = clear_api.resolve_country_location_id(country_name, pcode=row.get("pcode"))
    if not country_id:
        raise dg.Failure(
            description=(
                f"{country_name} ({iso3}): no A0 location resolved — backfill "
                "country locations first"
            )
        )

    year = datetime.now(timezone.utc).year
    window_start, window_end = _calendar_year_window(year)

    aggregated = None
    try:
        aggregated = clear_api.get_aggregated_datapoint(
            location_id=country_id,
            window_start=window_start,
            window_end=window_end,
            window_kind=_WINDOW_KIND,
            schema_version=AGGREGATION_SCHEMA_VERSION,
        )
    except Exception as exc:  # noqa: BLE001
        context.log.warning("[analysis_sub] %s: aggregated fetch failed (%s)", iso3, exc)
    aggregated_context = format_aggregated_for_prompt(aggregated) if aggregated else None

    frame = country_frame(country_id, window_start=window_start, window_end=window_end)
    budget = analysis_run_budget()
    s3, bucket = _s3_client(), os.environ["S3_BUCKET"]
    nodes = l1_nodes()

    def run_one(node) -> dict:
        key = paths.descriptive_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION, node.key)
        if _exists(s3, bucket, key):
            return {"node": node.key, "status": "cached", "reused": True}
        # ONE provider per node — total_usage would race on a shared instance (§7).
        provider = make_llm_provider(ANALYSIS_LLM_ROLE)
        res = analyse_node(
            node=node, frame=frame, scope_label=country_name,
            provider=provider, aggregated_context=aggregated_context, budget=budget,
        )
        artifact = {
            "iso3": iso3,
            "window_kind": _WINDOW_KIND,
            "frame": frame.upsert_kwargs(),
            "node": {"key": node.key, "name": node.name, "task": node.task, "level": node.level},
            "descriptive": res.descriptive.model_dump(),
            "status": res.status,
            "usage": res.usage,
            "cost_usd": res.cost_usd,
            "provenance": res.provenance,
            "rag_hit_count": res.rag_hit_count,
        }
        body = json.dumps(artifact, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
        return {
            "node": node.key, "status": res.status, "reused": False,
            "cost_usd": res.cost_usd, "completeness": res.descriptive.completeness,
        }

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {pool.submit(run_one, n): n for n in nodes}
        for fut in as_completed(futures):
            node = futures[fut]
            try:
                results.append(fut.result())
            except BudgetExhaustedError:
                context.log.warning("[analysis_sub] budget exhausted — skipped %s", node.key)
                results.append({"node": node.key, "status": "budget_skipped"})
            except Exception:  # noqa: BLE001 — isolate one node's failure (doc §7)
                context.log.exception("[analysis_sub] node %s failed", node.key)
                results.append({"node": node.key, "status": "failed"})

    # context.run.run_id in a real run; fall back for build_asset_context (no run).
    try:
        run_id = context.run.run_id
    except Exception:  # noqa: BLE001
        run_id = getattr(context, "run_id", "unknown")
    budget.log_summary(run_id=run_id)
    created = sum(1 for r in results if not r.get("reused") and r.get("status") == "ok")
    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "nodes": dg.MetadataValue.int(len(nodes)),
        "created": dg.MetadataValue.int(created),
        "reused": dg.MetadataValue.int(sum(1 for r in results if r.get("reused"))),
        "failed": dg.MetadataValue.int(sum(1 for r in results if r.get("status") == "failed")),
        "budget_skipped": dg.MetadataValue.int(sum(1 for r in results if r.get("status") == "budget_skipped")),
        "spend_usd": dg.MetadataValue.float(round(budget.spent, 4)),
        "depth": dg.MetadataValue.text("L1"),
        "schema_version": dg.MetadataValue.text(SCHEMA_VERSION),
    })
    return results
