"""``analysis_revision`` — close the feedback loop before publish (doc §8).

The evaluation stage flags nodes below the completeness floor and the web
gap-fill answers the KB's unmet gaps, but neither fed the published output. This
asset consumes both:

1. **Reflexion** — re-run the descriptive analyst for the worst flagged nodes
   (capped, budget-gated), showing each its prior attempt so it answers what it
   missed. Revised artifacts are written under ``revision_key`` (the original is
   left intact; overview prefers the revised one).
2. **Web rescue** — a flagged node the web gap-fill answered is kept even below
   the floor; its tiered web evidence is attached at publish.
3. **Re-evaluate** — ``filter_package`` is re-run over the revised-or-original
   artifacts (with the rescued set) to produce the FINAL package overview reads.

NOTE: no ``from __future__ import annotations`` — Dagster introspects the
``context`` parameter annotation at definition load.
"""

import os
from dataclasses import asdict
from datetime import datetime, timezone

import dagster as dg

from clear_pipeline.defs.analysis import paths
from clear_pipeline.defs.analysis.budget import analysis_run_budget
from clear_pipeline.defs.analysis.descriptive import NodeDescriptive, analyse_node
from clear_pipeline.defs.analysis.evaluation import (
    DEFAULT_EVAL_COMPLETENESS_FLOOR,
    filter_package,
)
from clear_pipeline.defs.analysis.frame import country_frame
from clear_pipeline.defs.analysis.judge import JudgeVerdict
from clear_pipeline.defs.analysis.registry import ANALYSIS_LLM_ROLE, PROMPT_VERSION, SCHEMA_VERSION
from clear_pipeline.defs.analysis.revision import (
    DEFAULT_WEB_RESCUE_CONFIDENCE,
    select_nodes_to_revise,
    web_rescued_keys,
)
from clear_pipeline.defs.analysis.runtime import BudgetExhaustedError
from clear_pipeline.defs.analysis.store import (
    load_descriptive,
    load_final_descriptive,
    load_gapfills,
    load_json,
    save_json,
    save_stage_cost,
)
from clear_pipeline.defs.analysis.structured import (
    build_structured_artifact,
    compose_l1,
    structured_enabled,
)
from clear_pipeline.defs.analysis.taxonomy import l1_nodes
from clear_pipeline.defs.reliefweb_partitions import country_partitions
from clear_pipeline.providers import clear_api
from clear_pipeline.providers.llm import make_llm_provider

_WINDOW_KIND = "yearly"
_MAX_REVISE = int(os.environ.get("ANALYSIS_REVISION_MAX_NODES", "5"))


def _s3():
    from clear_pipeline.providers.s3 import s3_client

    return s3_client()


def _exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:  # noqa: BLE001 — absent → recompute
        return False


def _calendar_year_window(year: int) -> tuple[str, str]:
    start = datetime(year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    return start.isoformat(), end.isoformat()


def _prior_completeness(art: dict) -> float:
    env = art.get("envelope") or {}
    if env.get("completeness") is not None:
        return float(env["completeness"])
    c = (art.get("descriptive") or {}).get("completeness")
    return float(c) if c is not None else 0.0


def _revise_structured(
    context, s3, bucket, rev_key, iso3, node, art, frame, country_name, budget,
) -> bool:
    """Structured reflexion pass (ADR-0015): regenerate the flagged node's
    envelope and keep it only if completeness improved. Writes the same dual-shape
    artifact as analysis_sub (so load_final_descriptive + assemble_v5 see it).

    First cut: a plain structured regenerate, not a prior-shown reflexion — the
    archetype call has no prior-attempt channel yet. Returns True if a revision
    was written."""
    prior_comp = _prior_completeness(art)
    try:
        env, accts = compose_l1(
            node=node, frame=frame, scope_label=country_name,
            make_provider=lambda: make_llm_provider(ANALYSIS_LLM_ROLE), budget=budget,
        )
    except BudgetExhaustedError:
        context.log.warning("[analysis_revision] %s: budget exhausted revising %s", iso3, node.key)
        return False
    except Exception:  # noqa: BLE001 — isolate one node's failure
        context.log.exception("[analysis_revision] %s: revising %s failed", iso3, node.key)
        return False
    if (env.completeness or 0.0) <= prior_comp:
        context.log.info(
            "[analysis_revision] %s %s: revision did not improve (%.2f→%.2f), dropping",
            iso3, node.key, prior_comp, env.completeness or 0.0,
        )
        return False
    artifact = build_structured_artifact(
        iso3=iso3, window_kind=_WINDOW_KIND, frame=frame, node=node, env=env, accts=accts,
    )
    artifact["revised_from_completeness"] = prior_comp
    save_json(s3, bucket, rev_key, artifact)
    return True


@dg.asset(
    group_name="analysis",
    deps=["analysis_evaluation", "analysis_web_gapfill", "analysis_sub"],
    partitions_def=country_partitions,
)
def analysis_revision(context: dg.AssetExecutionContext) -> dict:
    """Reflexion-rescue the worst flagged nodes + fold in web rescue, then write
    the FINAL evaluated package overview publishes from."""
    iso3 = context.partition_key
    row = {
        (c.get("iso3") or "").lower(): c for c in clear_api.get_pipeline_countries()
    }.get(iso3.lower())
    if row is None:
        raise dg.Failure(description=f"partition {iso3!r} is not in clear-api pipelineCountries")
    country_name = row["name"]
    country_id = clear_api.resolve_country_location_id(row["name"], pcode=row.get("pcode"))
    if not country_id:
        raise dg.Failure(description=f"{country_name} ({iso3}): no A0 location resolved")

    s3, bucket = _s3(), os.environ["S3_BUCKET"]
    nodes = l1_nodes()
    nodes_by_key = {n.key: n for n in nodes}

    evaluation = load_json(
        s3, bucket, paths.evaluation_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION)
    ) or {}
    verdicts = evaluation.get("verdicts", [])
    gapfills = load_gapfills(s3, bucket, iso3, _WINDOW_KIND, nodes)

    confidence_floor = float(
        os.environ.get("ANALYSIS_WEB_RESCUE_CONFIDENCE", DEFAULT_WEB_RESCUE_CONFIDENCE)
    )
    rescued = web_rescued_keys(gapfills, confidence_floor=confidence_floor)
    to_revise = select_nodes_to_revise(verdicts, gapfills, max_nodes=_MAX_REVISE)

    # Reflexion pass over the selected flagged nodes (bounded by the run budget).
    year = datetime.now(timezone.utc).year
    window_start, window_end = _calendar_year_window(year)
    frame = country_frame(country_id, window_start=window_start, window_end=window_end)
    budget = analysis_run_budget()
    originals = load_descriptive(s3, bucket, iso3, _WINDOW_KIND, nodes)
    structured = structured_enabled()

    context.log.info(
        "[analysis_revision] %s: %d flagged nodes selected to revise, %d web-rescued (structured=%s)",
        iso3, len(to_revise), len(rescued), structured,
    )
    revised: list[str] = []
    for node_key in to_revise:
        node = nodes_by_key.get(node_key)
        art = originals.get(node_key)
        if node is None or art is None:
            continue
        rev_key = paths.revision_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION, node_key)
        if _exists(s3, bucket, rev_key):
            revised.append(node_key)
            continue
        if not budget.allow_more_spend():
            context.log.warning("[analysis_revision] %s: budget exhausted before %s", iso3, node_key)
            break
        context.log.info("[analysis_revision] %s %s: revising (reflexion)…", iso3, node_key)
        if structured:
            if _revise_structured(
                context, s3, bucket, rev_key, iso3, node, art, frame, country_name, budget,
            ):
                revised.append(node_key)
            continue
        prior = NodeDescriptive.model_validate(art.get("descriptive", {}))
        provider = make_llm_provider(ANALYSIS_LLM_ROLE)
        try:
            res = analyse_node(
                node=node, frame=frame, scope_label=country_name,
                provider=provider, budget=budget, prior=prior,
            )
        except BudgetExhaustedError:
            context.log.warning("[analysis_revision] %s: budget exhausted revising %s", iso3, node_key)
            break
        except Exception:  # noqa: BLE001 — isolate one node's failure
            context.log.exception("[analysis_revision] %s: revising %s failed", iso3, node_key)
            continue
        # Only keep the revision if it actually improved completeness.
        if res.descriptive.completeness <= prior.completeness:
            context.log.info(
                "[analysis_revision] %s %s: revision did not improve (%.2f→%.2f), dropping",
                iso3, node_key, prior.completeness, res.descriptive.completeness,
            )
            continue
        artifact = {
            "iso3": iso3, "window_kind": _WINDOW_KIND, "frame": frame.upsert_kwargs(),
            "node": {"key": node.key, "name": node.name, "task": node.task, "level": node.level},
            "descriptive": res.descriptive.model_dump(),
            "status": res.status, "usage": res.usage, "cost_usd": res.cost_usd,
            "provenance": res.provenance, "rag_hit_count": res.rag_hit_count,
            "revised_from_completeness": prior.completeness,
        }
        save_json(s3, bucket, rev_key, artifact)
        revised.append(node_key)

    # Re-evaluate over the revised-or-original artifacts, honouring web rescue and
    # the adversarial judge's verdicts (which ran in analysis_evaluation).
    floor = float(os.environ.get("ANALYSIS_EVAL_COMPLETENESS_FLOOR", DEFAULT_EVAL_COMPLETENESS_FLOOR))
    final_artifacts = load_final_descriptive(s3, bucket, iso3, _WINDOW_KIND, nodes)
    # The judge verdict was formed on the ORIGINAL findings, so it's stale for any
    # node whose loaded artifact is a revision — THIS run's or a cached prior run's.
    # Detect every loaded revision by the revision-only `revised_from_completeness`
    # field (not just this run's `revised`, which a lower cap on a rerun would miss)
    # so a stale "judge_not_answered" can't drop an already-improved revision.
    revised_loaded = {k for k, a in final_artifacts.items() if "revised_from_completeness" in a}
    judge_verdicts = {
        k: JudgeVerdict(**d)
        for k, d in (evaluation.get("judge_verdicts") or {}).items()
        if k not in revised_loaded
    }
    package = filter_package(
        nodes, final_artifacts, completeness_floor=floor,
        rescued_node_keys=rescued, judge_verdicts=judge_verdicts,
    )

    final_payload = {
        "iso3": iso3, "window_kind": _WINDOW_KIND, "completeness_floor": floor,
        "mean_completeness": package.mean_completeness, "by_task": package.by_task,
        "kept_node_keys": package.kept_node_keys, "flagged_node_keys": package.flagged_node_keys,
        "verdicts": [asdict(v) for v in package.verdicts],
        "revised_node_keys": revised, "web_rescued_node_keys": sorted(rescued),
    }
    save_json(
        s3, bucket,
        paths.revision_package_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION),
        final_payload,
    )

    try:
        run_id = context.run.run_id
    except Exception:  # noqa: BLE001
        run_id = getattr(context, "run_id", "unknown")
    budget.log_summary(run_id=run_id)
    save_stage_cost(s3, bucket, iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION, "revision", budget.spent)

    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "revised": dg.MetadataValue.int(len(revised)),
        "web_rescued": dg.MetadataValue.int(len(rescued)),
        "kept": dg.MetadataValue.int(len(package.kept_node_keys)),
        "flagged": dg.MetadataValue.int(len(package.flagged_node_keys)),
        "spend_usd": dg.MetadataValue.float(round(budget.spent, 4)),
    })
    return {
        "iso3": iso3, "revised": len(revised), "web_rescued": len(rescued),
        "kept": len(package.kept_node_keys), "flagged": len(package.flagged_node_keys),
    }
