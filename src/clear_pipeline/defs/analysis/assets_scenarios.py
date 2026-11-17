"""``analysis_scenarios`` — forward-looking scenarios for the frame (ADR-0007 §4).

A country-partitioned asset that generates the most-likely / best / worst
trajectories over the frame's evidence (one retrieve-then-generate call) and
writes them to S3, where ``analysis_overview`` reads them into the published
payload's ``scenarios`` field. Independent of the node fan-out — it does its own
RAG retrieval — so it only ``deps`` on ``analysis_sub`` for run ordering.

Only runs under the structured path (``ANALYSIS_STRUCTURED``); otherwise a no-op,
since the legacy v6 assembly doesn't consume it.

NOTE: no ``from __future__ import annotations`` — Dagster introspects the
``context`` parameter annotation at definition load.
"""

import os
from datetime import datetime, timezone

import dagster as dg

from clear_pipeline.defs.analysis import paths
from clear_pipeline.defs.analysis.budget import analysis_run_budget
from clear_pipeline.defs.analysis.frame import country_frame
from clear_pipeline.defs.analysis.registry import ANALYSIS_LLM_ROLE, PROMPT_VERSION, SCHEMA_VERSION
from clear_pipeline.defs.analysis.scenarios import ScenarioResult, generate_scenarios
from clear_pipeline.defs.analysis.store import save_json, save_stage_cost
from clear_pipeline.defs.analysis.structured import structured_enabled
from clear_pipeline.defs.reliefweb_partitions import country_partitions
from clear_pipeline.providers import clear_api
from clear_pipeline.providers.llm import make_llm_provider

_WINDOW_KIND = "yearly"


def _s3():
    from clear_pipeline.providers.s3 import s3_client

    return s3_client()


def _calendar_year_window(year: int) -> tuple[str, str]:
    start = datetime(year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    return start.isoformat(), end.isoformat()


@dg.asset(
    group_name="analysis",
    deps=["analysis_sub"],
    partitions_def=country_partitions,
)
def analysis_scenarios(context: dg.AssetExecutionContext) -> dict:
    """Generate THIS country's forward-looking scenarios and persist them for
    overview to publish under ``data.scenarios`` (v5 backward-compat component)."""
    iso3 = context.partition_key
    if not structured_enabled():
        context.add_output_metadata({"status": dg.MetadataValue.text("skipped (flag off)")})
        return {"iso3": iso3, "status": "skipped"}

    row = {
        (c.get("iso3") or "").lower(): c for c in clear_api.get_pipeline_countries()
    }.get(iso3.lower())
    if row is None:
        raise dg.Failure(description=f"partition {iso3!r} is not in clear-api pipelineCountries")
    country_name = row["name"]
    country_id = clear_api.resolve_country_location_id(country_name, pcode=row.get("pcode"))
    if not country_id:
        raise dg.Failure(description=f"{country_name} ({iso3}): no A0 location resolved")

    year = datetime.now(timezone.utc).year
    window_start, window_end = _calendar_year_window(year)
    frame = country_frame(country_id, window_start=window_start, window_end=window_end)

    budget = analysis_run_budget()
    provider = make_llm_provider(ANALYSIS_LLM_ROLE)
    try:
        result = generate_scenarios(
            provider=provider, frame=frame, scope_label=country_name, budget=budget,
        )
    except Exception:  # noqa: BLE001 — scenarios are optional; overview deps on this
        # asset, so a provider error must NOT block publishing the rest of the
        # analysis. Persist an empty result and record the failure.
        context.log.exception(
            "[analysis_scenarios] %s: generation failed — publishing empty scenarios", iso3
        )
        result = ScenarioResult(
            "", "", "", "", [], "error",
            dict(getattr(provider, "total_usage", {}) or {}), 0.0,
            {"model": getattr(provider, "model", "")},
        )

    s3, bucket = _s3(), os.environ["S3_BUCKET"]
    save_json(
        s3, bucket,
        paths.scenarios_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION),
        {
            "iso3": iso3, "window_kind": _WINDOW_KIND,
            "scenarios": result.to_payload(),
            "status": result.status, "cost_usd": result.cost_usd,
            "provenance": result.provenance,
        },
    )

    try:
        run_id = context.run.run_id
    except Exception:  # noqa: BLE001
        run_id = getattr(context, "run_id", "unknown")
    budget.log_summary(run_id=run_id)
    save_stage_cost(s3, bucket, iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION, "scenarios", budget.spent, run_id)

    has_content = bool(result.most_likely or result.best_case or result.worst_case)
    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "status": dg.MetadataValue.text(result.status),
        "has_content": dg.MetadataValue.bool(has_content),
        "sources": dg.MetadataValue.int(len(result.source_report_ids)),
        "spend_usd": dg.MetadataValue.float(round(budget.spent, 4)),
    })
    return {"iso3": iso3, "status": result.status, "has_content": has_content}
