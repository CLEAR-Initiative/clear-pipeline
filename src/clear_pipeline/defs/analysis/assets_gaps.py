"""``analysis_gaps`` + ``analysis_evaluation`` — the deterministic post-
descriptive stages (doc §8).

Both are country-partitioned, read the per-node descriptive artifacts that
``analysis_sub`` wrote to S3, run the pure logic (:mod:`gaps`, :mod:`evaluation`),
and persist their own per-country artifact. No LLM:

- ``analysis_gaps`` derives the ranked **required-question-unanswered** list —
  what the DB couldn't answer — the (conditional) input to web gap-fill later.
- ``analysis_evaluation`` is the **programmatic filter** over the nodes' own
  completeness self-ratings (no judge model) — kept vs flagged + per-task summary.

NOTE: no ``from __future__ import annotations`` — Dagster introspects the
``context`` parameter annotation at definition load.
"""

import os
from dataclasses import asdict

import dagster as dg

from clear_pipeline.defs.analysis import paths
from clear_pipeline.defs.analysis.budget import analysis_run_budget
from clear_pipeline.defs.analysis.evaluation import (
    DEFAULT_EVAL_COMPLETENESS_FLOOR,
    filter_package,
)
from clear_pipeline.defs.analysis.gaps import (
    DEFAULT_GAP_COMPLETENESS_THRESHOLD,
    derive_gaps,
)
from clear_pipeline.defs.analysis.registry import (
    ANALYSIS_LLM_ROLE,
    PROMPT_VERSION,
    SCHEMA_VERSION,
)
from clear_pipeline.defs.analysis.store import load_descriptive, load_json, save_json
from clear_pipeline.defs.analysis.taxonomy import l1_nodes
from clear_pipeline.defs.analysis.web_gapfill import fill_gap
from clear_pipeline.defs.reliefweb_partitions import country_partitions
from clear_pipeline.providers.brave_search import BraveSearchError
from clear_pipeline.providers.llm import make_llm_provider

_WINDOW_KIND = "yearly"


def _s3():
    from clear_pipeline.providers.s3 import s3_client

    return s3_client()


@dg.asset(
    group_name="analysis",
    deps=["analysis_sub"],
    partitions_def=country_partitions,
)
def analysis_gaps(context: dg.AssetExecutionContext) -> dict:
    """Ranked unmet information gaps for THIS country, from the descriptive
    nodes' completeness self-ratings + questions_not_answered."""
    iso3 = context.partition_key
    s3, bucket = _s3(), os.environ["S3_BUCKET"]
    nodes = l1_nodes()
    artifacts = load_descriptive(s3, bucket, iso3, _WINDOW_KIND, nodes)

    threshold = float(
        os.environ.get("ANALYSIS_GAP_COMPLETENESS_THRESHOLD", DEFAULT_GAP_COMPLETENESS_THRESHOLD)
    )
    found = derive_gaps(nodes, artifacts, threshold=threshold)

    payload = {
        "iso3": iso3,
        "window_kind": _WINDOW_KIND,
        "threshold": threshold,
        "nodes": len(nodes),
        "artifacts_loaded": len(artifacts),
        "gaps": [asdict(g) for g in found],
    }
    save_json(s3, bucket, paths.gaps_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION), payload)

    by_reason: dict[str, int] = {}
    for g in found:
        by_reason[g.reason] = by_reason.get(g.reason, 0) + 1
    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "nodes": dg.MetadataValue.int(len(nodes)),
        "artifacts_loaded": dg.MetadataValue.int(len(artifacts)),
        "gaps": dg.MetadataValue.int(len(found)),
        "by_reason": dg.MetadataValue.json(by_reason),
        "threshold": dg.MetadataValue.float(threshold),
    })
    return {"iso3": iso3, "gaps": len(found), "artifacts_loaded": len(artifacts)}


@dg.asset(
    group_name="analysis",
    deps=["analysis_sub"],
    partitions_def=country_partitions,
)
def analysis_evaluation(context: dg.AssetExecutionContext) -> dict:
    """Programmatic filter over the descriptive nodes' self-ratings — kept vs
    flagged findings + per-task completeness summary. No judge LLM (doc §8)."""
    iso3 = context.partition_key
    s3, bucket = _s3(), os.environ["S3_BUCKET"]
    nodes = l1_nodes()
    artifacts = load_descriptive(s3, bucket, iso3, _WINDOW_KIND, nodes)

    floor = float(
        os.environ.get("ANALYSIS_EVAL_COMPLETENESS_FLOOR", DEFAULT_EVAL_COMPLETENESS_FLOOR)
    )
    package = filter_package(nodes, artifacts, completeness_floor=floor)

    payload = {
        "iso3": iso3,
        "window_kind": _WINDOW_KIND,
        "completeness_floor": floor,
        "mean_completeness": package.mean_completeness,
        "by_task": package.by_task,
        "kept_node_keys": package.kept_node_keys,
        "flagged_node_keys": package.flagged_node_keys,
        "verdicts": [asdict(v) for v in package.verdicts],
    }
    save_json(s3, bucket, paths.evaluation_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION), payload)

    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "nodes": dg.MetadataValue.int(len(nodes)),
        "kept": dg.MetadataValue.int(len(package.kept_node_keys)),
        "flagged": dg.MetadataValue.int(len(package.flagged_node_keys)),
        "mean_completeness": dg.MetadataValue.float(
            round(package.mean_completeness, 4) if package.mean_completeness is not None else 0.0
        ),
        "completeness_floor": dg.MetadataValue.float(floor),
    })
    return {
        "iso3": iso3,
        "kept": len(package.kept_node_keys),
        "flagged": len(package.flagged_node_keys),
    }


@dg.asset(
    group_name="analysis",
    deps=["analysis_gaps"],
    partitions_def=country_partitions,
)
def analysis_web_gapfill(context: dg.AssetExecutionContext) -> dict:
    """CONDITIONAL web gap-fill — runs ONLY for the unmet gaps analysis_gaps
    found, and only when a Brave key is configured (doc §8/§9). When the DB
    already answered the country's questions (no gaps), this is a no-op. Bounded
    by the run budget and ANALYSIS_WEB_GAPFILL_MAX_GAPS; one provider per gap."""
    iso3 = context.partition_key
    s3, bucket = _s3(), os.environ["S3_BUCKET"]

    gaps_doc = load_json(s3, bucket, paths.gaps_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION))
    gaps = (gaps_doc or {}).get("gaps", [])

    if not gaps:
        context.add_output_metadata({
            "country": dg.MetadataValue.text(iso3),
            "gaps": dg.MetadataValue.int(0),
            "status": dg.MetadataValue.text("no_gaps"),
        })
        return {"iso3": iso3, "gaps": 0, "filled": 0, "status": "no_gaps"}

    if not os.environ.get("BRAVE_SEARCH_API_KEY"):
        context.log.warning(
            "[analysis_web_gapfill] %s: %d gaps but BRAVE_SEARCH_API_KEY unset — skipping", iso3, len(gaps)
        )
        context.add_output_metadata({
            "country": dg.MetadataValue.text(iso3),
            "gaps": dg.MetadataValue.int(len(gaps)),
            "status": dg.MetadataValue.text("no_api_key"),
        })
        return {"iso3": iso3, "gaps": len(gaps), "filled": 0, "status": "no_api_key"}

    max_gaps = int(os.environ.get("ANALYSIS_WEB_GAPFILL_MAX_GAPS", "5"))
    pinned = load_json(s3, bucket, paths.pinned_sites_key(iso3)) or {}
    budget = analysis_run_budget()
    domain_cache: dict[str, int] = {}

    results: list[dict] = []
    for gap in gaps[:max_gaps]:  # gaps are pre-ranked worst-first
        if not budget.allow_more_spend():
            break
        node_key = gap.get("node_key")
        requirement = gap.get("question") or gap.get("node_name") or ""
        provider = make_llm_provider(ANALYSIS_LLM_ROLE)
        try:
            gf = fill_gap(
                requirement=requirement, provider=provider, budget=budget,
                freshness="py", pinned_sites=pinned, domain_cache=domain_cache,
            )
        except BraveSearchError as exc:
            context.log.warning("[analysis_web_gapfill] %s %s: search failed (%s)", iso3, node_key, exc)
            results.append({"node": node_key, "status": "search_failed"})
            continue
        except Exception:  # noqa: BLE001 — isolate one gap's failure
            context.log.exception("[analysis_web_gapfill] %s %s failed", iso3, node_key)
            results.append({"node": node_key, "status": "error"})
            continue

        save_json(
            s3, bucket,
            paths.gapfill_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION, node_key),
            {"iso3": iso3, "node_key": node_key, **asdict(gf)},
        )
        results.append({
            "node": node_key, "status": gf.status, "confidence": gf.confidence,
            "source_category_used": gf.source_category_used, "sources": len(gf.sources),
        })

    try:
        run_id = context.run.run_id
    except Exception:  # noqa: BLE001
        run_id = getattr(context, "run_id", "unknown")
    budget.log_summary(run_id=run_id)

    resolved = sum(1 for r in results if r.get("status") == "resolved")
    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "gaps": dg.MetadataValue.int(len(gaps)),
        "attempted": dg.MetadataValue.int(len(results)),
        "resolved": dg.MetadataValue.int(resolved),
        "spend_usd": dg.MetadataValue.float(round(budget.spent, 4)),
    })
    return {
        "iso3": iso3, "gaps": len(gaps), "attempted": len(results),
        "resolved": resolved, "status": "ran",
    }
