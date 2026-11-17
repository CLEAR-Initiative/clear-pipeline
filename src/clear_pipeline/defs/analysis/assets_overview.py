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
from clear_pipeline.defs.analysis.assemble_v5 import assemble_v5
from clear_pipeline.defs.analysis.budget import analysis_run_budget
from clear_pipeline.defs.analysis.numbers import project_v5_datapoints
from clear_pipeline.defs.analysis.output_schemas import NodeEnvelope
from clear_pipeline.defs.analysis.overview import synthesise_overview
from clear_pipeline.defs.analysis.payload import (
    PAYLOAD_SCHEMA_VERSION,
    assemble_published_data,
    build_v6_payload,
)
from clear_pipeline.defs.analysis.registry import ANALYSIS_LLM_ROLE, PROMPT_VERSION, SCHEMA_VERSION
from clear_pipeline.defs.analysis.structured import structured_enabled
from clear_pipeline.defs.analysis.store import (
    load_final_descriptive,
    load_gapfills,
    load_json,
    load_total_cost,
)
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


def _assemble_structured_data(
    v6: dict, kept_artifacts: dict, overview_text: str, numbers: dict, source_report_ids: list,
) -> dict:
    """The v6-as-true-superset ``analyses.data`` (ADR-0015, structured path).

    Merges three views into one blob:
    - the v5 TOP-LEVEL fields from the kept nodes' envelopes (``assemble_v5``) —
      what clear-mvp reads today, so flipping the flag doesn't regress it;
    - the v6 keyed ``nodes`` view + ``overview`` / ``numbers`` / ``quality``;
    - the v5 scalar fields (``ai_summary`` / ``datapoints`` / ``sources``).

    A kept node with no envelope (web-rescue stub) is simply absent from the v5
    top-level assembly — it still appears in the v6 ``nodes`` view."""
    l1_envelopes: dict[str, NodeEnvelope] = {}
    for key, art in kept_artifacts.items():
        dump = art.get("envelope")
        if not dump:
            continue
        try:
            l1_envelopes[key] = NodeEnvelope(**dump)
        except Exception:  # noqa: BLE001 — a malformed envelope just drops from v5 top-level
            continue
    v5_fields = assemble_v5(l1_envelopes)
    return {
        **v6,
        **v5_fields,
        # v5 scalar fields in the exact shapes clear-mvp reads: ai_summary.text,
        # sources.reports[] with titles (clear-api returns data verbatim, so the
        # titles must be embedded here or the UI shows 'Untitled report').
        "ai_summary": {
            "text": overview_text,
            "source_report_ids": source_report_ids,
            "contributing_sources": {},
        },
        "datapoints": project_v5_datapoints(numbers),
        "sources": _build_sources(kept_artifacts, source_report_ids),
    }


def _build_sources(kept_artifacts: dict, source_report_ids: list) -> dict:
    """v5 ``sources.reports`` — each cited report with its title/url/date, lifted
    from the per-node ``report_meta`` (captured from the RAG hits at generation).
    Newest-first; a report with no captured title falls back to its id (so it is
    still citable), matching the legacy situation path."""
    meta: dict[str, dict] = {}
    for art in kept_artifacts.values():
        for rid, m in (art.get("report_meta") or {}).items():
            meta.setdefault(rid, m)
    reports = [
        {
            "report_id": rid,
            "report_title": (meta.get(rid, {}).get("report_title") or rid),
            "source_url": meta.get(rid, {}).get("source_url") or "",
            "published_at": meta.get(rid, {}).get("published_at") or "",
        }
        for rid in source_report_ids
    ]
    reports.sort(key=lambda r: r["published_at"], reverse=True)
    return {"reports": reports}


def _weakness_note(flagged_verdicts: list[dict], names: dict[str, str], by_task: dict) -> str:
    """A short, bounded note on where evidence is thin, for the overview prompt:
    the dropped sections (with why) and the weakest tasks."""
    lines = [
        f"- {names.get(v['node_key'], v['node_key'])} ({v.get('reason')})"
        for v in flagged_verdicts[:10]
    ]
    weak_tasks = sorted((by_task or {}).items(), key=lambda kv: kv[1])[:3]
    if weak_tasks:
        lines.append(
            "weakest sections: " + ", ".join(f"{t} ({c:.2f})" for t, c in weak_tasks)
        )
    return "\n".join(lines)


@dg.asset(
    group_name="analysis",
    deps=[
        "analysis_sub", "analysis_numbers_extract", "analysis_revision",
        "analysis_scenarios", "analysis_changes",
    ],
    partitions_def=country_partitions,
)
def analysis_overview(context: dg.AssetExecutionContext) -> dict:
    """Assemble + publish THIS country's analysis: drop flagged nodes, fold in
    web evidence + the quality summary, synthesise a calibrated overview → hybrid
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
    names = {n.key: n.name for n in nodes}
    nodes_by_key = {n.key: n for n in nodes}
    numbers = load_json(
        s3, bucket, paths.numbers_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION)
    ) or {}

    # The FINAL evaluated package (after reflexion revision + web rescue) decides
    # what we publish: KEPT nodes only — flagged nodes are dropped. Prefer each
    # node's revised artifact over its original.
    final_artifacts = load_final_descriptive(
        s3, bucket, iso3, _WINDOW_KIND, nodes, require_envelope=structured_enabled()
    )
    package = load_json(
        s3, bucket, paths.revision_package_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION)
    ) or {}
    # Defensive: if the package is missing, publish everything generated rather
    # than nothing (revision is a hard dep, so this is an edge case).
    kept_keys = set(package.get("kept_node_keys", final_artifacts.keys()))
    verdicts = package.get("verdicts", [])
    flagged_verdicts = [v for v in verdicts if not v.get("kept")]

    kept_artifacts = {k: a for k, a in final_artifacts.items() if k in kept_keys}
    gapfills = load_gapfills(s3, bucket, iso3, _WINDOW_KIND, nodes)
    web_evidence = {k: gapfills[k] for k in kept_keys if k in gapfills}

    # Web-only rescue: a node whose descriptive call produced no artifact can
    # still be kept (reason web_rescued) when the web gap-fill answered it. It is
    # absent from final_artifacts, so seed a stub from the taxonomy — no KB
    # findings to show, but its web_evidence (attached below) still publishes.
    for key in kept_keys - kept_artifacts.keys():
        node = nodes_by_key.get(key)
        if node is None:
            continue
        kept_artifacts[key] = {
            "node": {"name": node.name, "task": node.task, "level": node.level},
            "descriptive": {
                "findings": [], "completeness": None,
                "questions_not_answered": [], "source_report_ids": [],
            },
        }

    quality = {
        "mean_completeness": package.get("mean_completeness"),
        "by_task": package.get("by_task", {}),
        "completeness_floor": package.get("completeness_floor"),
        "kept": len(kept_artifacts),
        "flagged": len(flagged_verdicts),
        "revised_node_keys": package.get("revised_node_keys", []),
        "web_rescued_node_keys": package.get("web_rescued_node_keys", []),
        "dropped": [
            {"node_key": v["node_key"], "name": names.get(v["node_key"]), "reason": v.get("reason")}
            for v in flagged_verdicts
        ],
    }

    # Cross-section overview synthesis (one bounded DeepSeek call) over KEPT
    # sections, calibrated by where evidence is thin.
    weakness = _weakness_note(flagged_verdicts, names, package.get("by_task", {}))
    budget = analysis_run_budget()
    provider = make_llm_provider(ANALYSIS_LLM_ROLE)
    try:
        outcome = synthesise_overview(
            provider=provider, node_artifacts=kept_artifacts, weakness=weakness, budget=budget
        )
        overview_text = outcome.output.overview
        model = outcome.provenance["model"]
    except Exception:  # noqa: BLE001 — publish the descriptive picture even if synthesis fails
        context.log.exception("[analysis_overview] %s: overview synthesis failed", iso3)
        overview_text, model = "", getattr(provider, "model", "")

    v6 = build_v6_payload(
        kept_artifacts, numbers, overview_text, quality=quality, web_evidence=web_evidence
    )
    source_report_ids = sorted(
        {r for n in v6["nodes"].values() for r in (n.get("source_report_ids") or [])}
    )

    if structured_enabled():
        # v6-as-true-superset (ADR-0015): the overlapping L1s publish to their v5
        # TOP-LEVEL paths (sectors / context_risks / displacement / hazards /
        # access) so clear-mvp reads them unchanged — no regression — and new-only
        # L1s land under data.taxonomy. The v6 keyed `nodes` view + overview /
        # numbers / quality ride alongside.
        data = _assemble_structured_data(
            v6, kept_artifacts, overview_text, numbers, source_report_ids
        )
        # Overlay the forward-looking `scenarios` + temporal `changes` components,
        # each produced by its own upstream asset (analysis_scenarios /
        # analysis_changes) and read here from S3. Absent artifact → the empty
        # skeleton default stays (frontend-stable).
        scenarios = load_json(
            s3, bucket, paths.scenarios_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION)
        )
        if scenarios:
            data["scenarios"] = scenarios.get("scenarios", data.get("scenarios"))
        changes = load_json(
            s3, bucket, paths.changes_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION)
        )
        if changes:
            data["changes"] = changes.get("changes", data.get("changes"))
    else:
        # Legacy hybrid: v6 payload with the (lossy) v5 projection embedded under
        # data["v5"]. Unchanged until the structured flag is flipped.
        data = assemble_published_data(v6)

    # Total generation cost = every upstream stage's recorded spend for THIS run +
    # this asset's own synthesis. Each asset owns a SEPARATE run budget, so
    # `budget.spent` here is only the overview's slice; the per-stage costs are tied
    # to run_id so a stage skipped this run doesn't fold in stale spend.
    try:
        run_id = context.run.run_id
    except Exception:  # noqa: BLE001
        run_id = getattr(context, "run_id", "unknown")
    total_cost = round(
        load_total_cost(s3, bucket, iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION, run_id)
        + budget.spent,
        4,
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
        generation_cost_usd=total_cost,
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

    budget.log_summary(run_id=run_id)  # run_id computed above for the cost sum

    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "analysis_id": dg.MetadataValue.text(analysis_id or "(skipped)"),
        "skipped": dg.MetadataValue.bool(bool(res.get("skipped"))),
        "nodes_published": dg.MetadataValue.int(len(v6["nodes"])),
        "nodes_dropped": dg.MetadataValue.int(len(flagged_verdicts)),
        "nodes_revised": dg.MetadataValue.int(len(quality["revised_node_keys"])),
        "nodes_web_rescued": dg.MetadataValue.int(len(quality["web_rescued_node_keys"])),
        "numbers_linked": dg.MetadataValue.bool(linked),
        "overview_spend_usd": dg.MetadataValue.float(round(budget.spent, 4)),
        "total_generation_cost_usd": dg.MetadataValue.float(total_cost),
        "schema_version": dg.MetadataValue.text(PAYLOAD_SCHEMA_VERSION),
    })
    return {
        "iso3": iso3,
        "analysis_id": analysis_id,
        "skipped": bool(res.get("skipped")),
        "numbers_linked": linked,
        "nodes": len(v6["nodes"]),
    }
