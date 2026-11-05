"""``analysis_overview`` — assembly / publish (doc §3).

The descriptive path's capstone: JOIN the 22 L1 node artifacts + the numbers
artifact, synthesise the cross-cutting overview, build the hybrid v6+v5 payload,
and **publish** — upsert the frame's ``analyses`` row (schema_version ``v6``),
then link its numbers into ``analysis_datapoints``. This is where the new DAG
first produces a published ``analyses`` row and where the non-null
``analysis_id`` FK gets its row.

NOTE: no ``from __future__ import annotations`` — Dagster introspects the
``context`` parameter annotation at definition load.
"""

import os
from datetime import datetime, timezone

import dagster as dg

from clear_pipeline.defs.analysis import paths
from clear_pipeline.defs.analysis.budget import analysis_run_budget
from clear_pipeline.defs.analysis.overview import synthesise_overview
from clear_pipeline.defs.analysis.payload import (
    PAYLOAD_SCHEMA_VERSION,
    assemble_published_data,
    build_v6_payload,
)
from clear_pipeline.defs.analysis.registry import ANALYSIS_LLM_ROLE, PROMPT_VERSION, SCHEMA_VERSION
from clear_pipeline.defs.analysis.store import load_descriptive, load_json
from clear_pipeline.defs.analysis.taxonomy import l1_nodes
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
    deps=["analysis_sub", "analysis_numbers_extract", "analysis_evaluation"],
    partitions_def=country_partitions,
)
def analysis_overview(context: dg.AssetExecutionContext) -> dict:
    """Assemble + publish THIS country's analysis: overview synthesis → hybrid
    v6+v5 payload → upsert the analyses row → link analysis_datapoints."""
    iso3 = context.partition_key
    row = {
        (c.get("iso3") or "").lower(): c for c in clear_api.get_pipeline_countries()
    }.get(iso3.lower())
    if row is None:
        raise dg.Failure(description=f"partition {iso3!r} is not in clear-api pipelineCountries")
    country_id = clear_api.resolve_country_location_id(row["name"], pcode=row.get("pcode"))
    if not country_id:
        raise dg.Failure(description=f"{row['name']} ({iso3}): no A0 location resolved")

    year = datetime.now(timezone.utc).year
    window_start, window_end = _calendar_year_window(year)

    s3, bucket = _s3(), os.environ["S3_BUCKET"]
    nodes = l1_nodes()
    artifacts = load_descriptive(s3, bucket, iso3, _WINDOW_KIND, nodes)
    numbers = load_json(
        s3, bucket, paths.numbers_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION)
    ) or {}

    # Cross-section overview synthesis (one bounded DeepSeek call).
    budget = analysis_run_budget()
    provider = make_llm_provider(ANALYSIS_LLM_ROLE)
    try:
        outcome = synthesise_overview(provider=provider, node_artifacts=artifacts, budget=budget)
        overview_text = outcome.output.overview
        model = outcome.provenance["model"]
    except Exception:  # noqa: BLE001 — publish the descriptive picture even if synthesis fails
        context.log.exception("[analysis_overview] %s: overview synthesis failed", iso3)
        overview_text, model = "", getattr(provider, "model", "")

    v6 = build_v6_payload(artifacts, numbers, overview_text)
    data = assemble_published_data(v6)
    source_report_ids = sorted(
        {r for n in v6["nodes"].values() for r in (n.get("source_report_ids") or [])}
    )

    # Publish the analyses row (schema_version v6).
    res = clear_api.upsert_analysis(
        location_ids=[country_id],
        event_types=[],
        need_sectors=[],
        window_start=window_start,
        window_end=window_end,
        data=data,
        source_report_ids=source_report_ids,
        generated_by_model=model,
        generation_cost_usd=round(budget.spent, 4),
        schema_version=PAYLOAD_SCHEMA_VERSION,
    )
    analysis_id = res.get("analysisId")

    # Link the extracted numbers to the generation (non-null FK). Skip when the
    # upsert was a no-op (24h floor) or there were no figures.
    linked = False
    if analysis_id and not numbers.get("empty", True):
        clear_api.upsert_analysis_datapoints(
            analysis_id=analysis_id,
            data=numbers.get("data", {}),
            contributing_report_ids=numbers.get("contributing_report_ids", []),
            origin=numbers.get("origin"),
            newest_source_at=numbers.get("newest_source_at"),
            oldest_source_at=numbers.get("oldest_source_at"),
            data_quality_score=numbers.get("data_quality_score"),
            report_count=numbers.get("report_count"),
            schema_version=PAYLOAD_SCHEMA_VERSION,
        )
        linked = True

    try:
        run_id = context.run.run_id
    except Exception:  # noqa: BLE001
        run_id = getattr(context, "run_id", "unknown")
    budget.log_summary(run_id=run_id)

    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "analysis_id": dg.MetadataValue.text(analysis_id or "(skipped)"),
        "skipped": dg.MetadataValue.bool(bool(res.get("skipped"))),
        "nodes_published": dg.MetadataValue.int(len(v6["nodes"])),
        "numbers_linked": dg.MetadataValue.bool(linked),
        "spend_usd": dg.MetadataValue.float(round(budget.spent, 4)),
        "schema_version": dg.MetadataValue.text(PAYLOAD_SCHEMA_VERSION),
    })
    return {
        "iso3": iso3,
        "analysis_id": analysis_id,
        "skipped": bool(res.get("skipped")),
        "numbers_linked": linked,
        "nodes": len(v6["nodes"]),
    }
