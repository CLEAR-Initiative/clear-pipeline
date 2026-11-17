"""``analysis_gaps`` + ``analysis_evaluation`` — the deterministic post-
descriptive stages (doc §8).

Both are country-partitioned and read the per-node descriptive artifacts that
``analysis_sub`` wrote to S3:

- ``analysis_gaps`` derives the ranked **required-question-unanswered** list —
  what the DB couldn't answer — the (conditional) input to web gap-fill later.
  Pure logic (:mod:`gaps`), no LLM.
- ``analysis_evaluation`` runs the **adversarial judge** (the ported ``clear_eval``
  framework, :mod:`judge`) over the uncertain / near-floor nodes: an independent
  model grades each node's findings against its Analytical Question
  (complete/partial/not_answered + off-subject padding detection). The judge is
  primary (:func:`evaluation.filter_package` consumes its verdicts); the cheap
  self-rating only triages which nodes are worth a judge pass.

NOTE: no ``from __future__ import annotations`` — Dagster introspects the
``context`` parameter annotation at definition load.
"""

import os
import time
from dataclasses import asdict

import dagster as dg

from clear_pipeline.defs.analysis import paths
from clear_pipeline.defs.analysis.budget import analysis_run_budget, charge
from clear_pipeline.defs.analysis.evaluation import (
    DEFAULT_EVAL_COMPLETENESS_FLOOR,
    filter_package,
)
from clear_pipeline.defs.analysis.gaps import (
    DEFAULT_GAP_COMPLETENESS_THRESHOLD,
    derive_gaps,
)
from clear_pipeline.defs.analysis.judge import (
    JUDGE_PROMPT_VERSION,
    coverage as judge_coverage,
    format_findings,
    judge_node,
    recommendation_for,
    select_nodes_to_judge,
)
from clear_pipeline.defs.analysis.registry import (
    ANALYSIS_JUDGE_LLM_ROLE,
    ANALYSIS_LLM_ROLE,
    PROMPT_VERSION,
    SCHEMA_VERSION,
)
from clear_pipeline.defs.analysis.store import (
    load_descriptive,
    load_json,
    save_json,
    save_stage_cost,
)
from clear_pipeline.defs.analysis.taxonomy import l1_nodes
from clear_pipeline.defs.analysis.web_gapfill import fill_gap
from clear_pipeline.defs.reliefweb_partitions import country_partitions
from clear_pipeline.providers.llm import make_llm_provider
from clear_pipeline.providers.web_search import WebSearchError, resolve_web_search

_WINDOW_KIND = "yearly"

# Judge triage band: a node whose self-rated completeness is within this much
# ABOVE the floor is still uncertain enough to judge (padding catch); clearly
# above that is trusted and kept without a judge call.
_DEFAULT_JUDGE_NEAR_FLOOR_BAND = 0.2
# Raised max_tokens for the judge call: reasoning ("thinking") tokens plus the
# small JSON verdict must both fit, so the cap is well above the generation cap.
_DEFAULT_JUDGE_MAX_TOKENS = 4000


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
    """Adversarial-judge evaluation (ported ``clear_eval``): judge the uncertain /
    near-floor nodes against their Analytical Question, then build the kept-vs-
    flagged package with the judge as the primary signal.

    The cheap self-rating only *triages* which nodes are worth a judge pass
    (``select_nodes_to_judge``): clearly-strong nodes are kept without judging,
    empty nodes are flagged without judging, and the uncertain band is graded by
    an independent reasoning model (``analysis_judge`` role, thinking on)."""
    iso3 = context.partition_key
    s3, bucket = _s3(), os.environ["S3_BUCKET"]
    nodes = l1_nodes()
    nodes_by_key = {n.key: n for n in nodes}
    artifacts = load_descriptive(s3, bucket, iso3, _WINDOW_KIND, nodes)

    floor = float(
        os.environ.get("ANALYSIS_EVAL_COMPLETENESS_FLOOR", DEFAULT_EVAL_COMPLETENESS_FLOOR)
    )
    band = float(os.environ.get("ANALYSIS_JUDGE_NEAR_FLOOR_BAND", _DEFAULT_JUDGE_NEAR_FLOOR_BAND))
    max_tokens = int(os.environ.get("LLM_ANALYSIS_JUDGE_MAX_TOKENS", _DEFAULT_JUDGE_MAX_TOKENS))
    max_judge = os.environ.get("ANALYSIS_JUDGE_MAX_NODES")
    max_nodes = int(max_judge) if max_judge else None

    to_judge = select_nodes_to_judge(nodes, artifacts, floor=floor, band=band, max_nodes=max_nodes)

    # Judge each uncertain node (budget-gated; one provider per node so
    # provider.last_usage is unambiguous — doc §7 concurrency caveat).
    budget = analysis_run_budget()
    context.log.info(
        "[analysis_evaluation] %s: %d/%d nodes selected for judging (floor=%.2f, band=%.2f)",
        iso3, len(to_judge), len(nodes), floor, band,
    )
    verdicts_by_key = {}
    recommendations = {}
    judge_provenance = {}
    judged = 0
    for node_key in to_judge:
        node = nodes_by_key.get(node_key)
        art = artifacts.get(node_key)
        if node is None or art is None:
            continue
        if not budget.allow_more_spend():
            context.log.warning("[analysis_evaluation] %s: budget exhausted before judging %s", iso3, node_key)
            break
        findings_text = format_findings(art.get("descriptive", {}).get("findings"))
        # Construct INSIDE the try so a judge misconfig (e.g. no creds on the role
        # chain) isolates to this node — it falls back to the self-rating verdict —
        # rather than raising out of the asset and blocking revision + publish.
        provider = None
        try:
            provider = make_llm_provider(ANALYSIS_JUDGE_LLM_ROLE)
            verdict = judge_node(node=node, findings_text=findings_text, provider=provider, max_tokens=max_tokens)
        except Exception:  # noqa: BLE001 — isolate one node's failure; it falls back to self-rating
            context.log.exception("[analysis_evaluation] %s: judging %s failed", iso3, node_key)
            continue
        finally:
            if provider is not None:
                charge(budget, provider)
        verdicts_by_key[node_key] = verdict
        action, rationale = recommendation_for(verdict)
        context.log.info("[analysis_evaluation] %s %s → %s", iso3, node_key, action)
        recommendations[node_key] = {"action": action, "rationale": rationale}
        # Record WHICH judge actually served (the role can fall back mid-run) so a
        # change in kept/dropped sections is explainable.
        judge_provenance[node_key] = {
            "model": getattr(provider, "model", ""),
            "prompt_version": JUDGE_PROMPT_VERSION,
        }
        judged += 1

    # Judge is primary for graded nodes; self-rating triages the rest.
    package = filter_package(
        nodes, artifacts, completeness_floor=floor, judge_verdicts=verdicts_by_key
    )

    payload = {
        "iso3": iso3,
        "window_kind": _WINDOW_KIND,
        "completeness_floor": floor,
        "judge_near_floor_band": band,
        "nodes_judged": judged,
        "judge_coverage": judge_coverage(list(verdicts_by_key.values())),
        "mean_completeness": package.mean_completeness,
        "by_task": package.by_task,
        "kept_node_keys": package.kept_node_keys,
        "flagged_node_keys": package.flagged_node_keys,
        "verdicts": [asdict(v) for v in package.verdicts],
        # Raw judge verdicts (reconstructable as JudgeVerdict) so the final
        # re-evaluation in analysis_revision re-uses them without re-judging.
        "judge_verdicts": {k: asdict(v) for k, v in verdicts_by_key.items()},
        # Which judge model + prompt version produced each verdict (audit trail).
        "judge_provenance": judge_provenance,
        "recommendations": recommendations,
    }
    save_json(s3, bucket, paths.evaluation_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION), payload)

    try:
        run_id = context.run.run_id
    except Exception:  # noqa: BLE001
        run_id = getattr(context, "run_id", "unknown")
    budget.log_summary(run_id=run_id)
    save_stage_cost(s3, bucket, iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION, "evaluation", budget.spent, run_id)

    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "nodes": dg.MetadataValue.int(len(nodes)),
        "nodes_judged": dg.MetadataValue.int(judged),
        "kept": dg.MetadataValue.int(len(package.kept_node_keys)),
        "flagged": dg.MetadataValue.int(len(package.flagged_node_keys)),
        "judge_coverage": dg.MetadataValue.float(
            round(payload["judge_coverage"], 4) if payload["judge_coverage"] is not None else 0.0
        ),
        "mean_completeness": dg.MetadataValue.float(
            round(package.mean_completeness, 4) if package.mean_completeness is not None else 0.0
        ),
        "completeness_floor": dg.MetadataValue.float(floor),
        "spend_usd": dg.MetadataValue.float(round(budget.spent, 4)),
    })
    return {
        "iso3": iso3,
        "nodes_judged": judged,
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
    found, and only when the selected web-search provider's key is configured
    (doc §8/§9). The provider is WEB_SEARCH_PROVIDER (serper default | brave);
    when the DB already answered the country's questions (no gaps), this is a
    no-op. Bounded by the run budget and ANALYSIS_WEB_GAPFILL_MAX_GAPS."""
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

    # Resolve the configured provider (brave | serper); an empty key disables it.
    search, key_env, provider_name = resolve_web_search()
    if not os.environ.get(key_env):
        context.log.warning(
            "[analysis_web_gapfill] %s: %d gaps but %s unset (provider=%s) — skipping",
            iso3, len(gaps), key_env, provider_name,
        )
        context.add_output_metadata({
            "country": dg.MetadataValue.text(iso3),
            "gaps": dg.MetadataValue.int(len(gaps)),
            "provider": dg.MetadataValue.text(provider_name),
            "status": dg.MetadataValue.text("no_api_key"),
        })
        return {"iso3": iso3, "gaps": len(gaps), "filled": 0, "status": "no_api_key"}

    max_gaps = int(os.environ.get("ANALYSIS_WEB_GAPFILL_MAX_GAPS", "5"))
    pinned = load_json(s3, bucket, paths.pinned_sites_key(iso3)) or {}
    budget = analysis_run_budget()
    domain_cache: dict[str, int] = {}

    to_fill = gaps[:max_gaps]  # gaps are pre-ranked worst-first
    context.log.info(
        "[analysis_web_gapfill] %s: %d gaps, filling up to %d via %s",
        iso3, len(gaps), len(to_fill), provider_name,
    )
    results: list[dict] = []
    for i, gap in enumerate(to_fill, 1):
        if not budget.allow_more_spend():
            context.log.warning("[analysis_web_gapfill] %s: budget exhausted after %d gaps", iso3, i - 1)
            break
        node_key = gap.get("node_key")
        requirement = gap.get("question") or gap.get("node_name") or ""
        context.log.info(
            "[analysis_web_gapfill] %s (%d/%d) %s: searching — %s",
            iso3, i, len(to_fill), node_key, requirement[:80],
        )
        provider = make_llm_provider(ANALYSIS_LLM_ROLE)
        t0 = time.monotonic()
        try:
            gf = fill_gap(
                requirement=requirement, provider=provider, budget=budget, search=search,
                freshness="py", pinned_sites=pinned, domain_cache=domain_cache,
            )
        except WebSearchError as exc:
            context.log.warning("[analysis_web_gapfill] %s %s: search failed (%s)", iso3, node_key, exc)
            results.append({"node": node_key, "status": "search_failed"})
            continue
        except Exception:  # noqa: BLE001 — isolate one gap's failure
            context.log.exception("[analysis_web_gapfill] %s %s failed", iso3, node_key)
            results.append({"node": node_key, "status": "error"})
            continue

        context.log.info(
            "[analysis_web_gapfill] %s (%d/%d) %s → %s (confidence=%s, tier=%s, sources=%d, %.1fs)",
            iso3, i, len(to_fill), node_key, gf.status, gf.confidence,
            gf.source_category_used, len(gf.sources), time.monotonic() - t0,
        )
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
    save_stage_cost(s3, bucket, iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION, "web_gapfill", budget.spent, run_id)

    resolved = sum(1 for r in results if r.get("status") == "resolved")
    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "provider": dg.MetadataValue.text(provider_name),
        "gaps": dg.MetadataValue.int(len(gaps)),
        "attempted": dg.MetadataValue.int(len(results)),
        "resolved": dg.MetadataValue.int(resolved),
        "spend_usd": dg.MetadataValue.float(round(budget.spent, 4)),
    })
    return {
        "iso3": iso3, "gaps": len(gaps), "attempted": len(results),
        "resolved": resolved, "status": "ran",
    }
