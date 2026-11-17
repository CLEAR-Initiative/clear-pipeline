"""``analysis_changes`` — 'what changed vs the prior generation' (ADR-0007 §8).

A country-partitioned asset that assembles THIS run's v5-shaped analysis from the
final node artifacts, fetches the PRIOR published analysis for the frame, and
makes one LLM call diffing them into per-section change notes. Written to S3,
where ``analysis_overview`` reads it into the published ``changes`` field.

It assembles the new side itself (``assemble_v5`` over the kept envelopes) rather
than waiting on overview, so it is a standalone asset; the diff is section-level,
so the lack of the (overview-generated) ai_summary text on the new side only
costs the ``summary`` note's prose half — figure changes are still caught.

Only runs under the structured path (``ANALYSIS_STRUCTURED``); otherwise a no-op.

NOTE: no ``from __future__ import annotations`` — Dagster introspects the
``context`` parameter annotation at definition load.
"""

import os
from datetime import datetime, timezone

import dagster as dg

from clear_pipeline.defs.analysis import paths
from clear_pipeline.defs.analysis.assemble_v5 import assemble_v5
from clear_pipeline.defs.analysis.budget import analysis_run_budget
from clear_pipeline.defs.analysis.changes import empty_changes, generate_changes
from clear_pipeline.defs.analysis.frame import country_frame
from clear_pipeline.defs.analysis.output_schemas import NodeEnvelope
from clear_pipeline.defs.analysis.payload import PAYLOAD_SCHEMA_VERSION
from clear_pipeline.defs.analysis.registry import ANALYSIS_LLM_ROLE, PROMPT_VERSION, SCHEMA_VERSION
from clear_pipeline.defs.analysis.store import (
    load_final_descriptive,
    load_json,
    save_json,
    save_stage_cost,
)
from clear_pipeline.defs.analysis.structured import structured_enabled
from clear_pipeline.defs.analysis.taxonomy import l1_nodes
from clear_pipeline.defs.reliefweb_partitions import country_partitions
from clear_pipeline.providers import clear_api
from clear_pipeline.providers.llm import make_llm_provider

_WINDOW_KIND = "yearly"
_BASIS = "previous_generation"  # the frame's prior analyses row IS the prior generation


def _s3():
    from clear_pipeline.providers.s3 import s3_client

    return s3_client()


def _calendar_year_window(year: int) -> tuple[str, str]:
    start = datetime(year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    return start.isoformat(), end.isoformat()


def _period_label(window_start: str, window_end: str | None) -> str:
    s = (window_start or "")[:10]
    e = (window_end or "")[:10]
    return f"{s} to {e}" if e else (f"since {s}" if s else "the period")


def _assemble_new_v5(s3, bucket, iso3: str, nodes) -> dict:
    """The new side to diff: the v5-shaped sections from this run's final node
    envelopes + the extracted datapoints (so figure changes are caught)."""
    # Structured mode only (changes asset no-ops otherwise) — ignore envelope-less
    # legacy revisions so the new side reflects the structured originals.
    final_artifacts = load_final_descriptive(
        s3, bucket, iso3, _WINDOW_KIND, nodes, require_envelope=True
    )
    l1_envelopes: dict[str, NodeEnvelope] = {}
    for key, art in final_artifacts.items():
        dump = art.get("envelope")
        if not dump:
            continue
        try:
            l1_envelopes[key] = NodeEnvelope(**dump)
        except Exception:  # noqa: BLE001 — a malformed envelope just drops from the diff
            continue
    new_v5 = assemble_v5(l1_envelopes)
    numbers = load_json(
        s3, bucket, paths.numbers_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION)
    ) or {}
    new_v5["datapoints"] = numbers.get("data", {})
    return new_v5


@dg.asset(
    group_name="analysis",
    deps=["analysis_revision"],
    partitions_def=country_partitions,
)
def analysis_changes(context: dg.AssetExecutionContext) -> dict:
    """Diff THIS run's analysis against the prior published one and persist the
    per-section change notes for overview to publish under ``data.changes``."""
    iso3 = context.partition_key
    if not structured_enabled():
        context.add_output_metadata({"status": dg.MetadataValue.text("skipped (flag off)")})
        return {"iso3": iso3, "status": "skipped"}

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
    frame = country_frame(country_id, window_start=window_start, window_end=window_end)
    period_label = _period_label(window_start, window_end)

    s3, bucket = _s3(), os.environ["S3_BUCKET"]
    out_key = paths.changes_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION)

    # The PRIOR generation = the frame's current analyses row, fetched BEFORE
    # overview's upsert supersedes it.
    try:
        prior = clear_api.get_analysis(
            schema_version=PAYLOAD_SCHEMA_VERSION, **frame.upsert_kwargs()
        )
    except Exception:  # noqa: BLE001 — a fetch hiccup → ship without change notes
        context.log.warning("[analysis_changes] %s: prior-analysis fetch failed", iso3)
        prior = None

    prior_generated_at = (prior or {}).get("generatedAt") or ""
    if not (prior and prior.get("data")):
        context.log.info("[analysis_changes] %s: no prior analysis — empty changes", iso3)
        save_json(s3, bucket, out_key, {
            "iso3": iso3, "window_kind": _WINDOW_KIND,
            "changes": empty_changes(
                basis=_BASIS, prior_generated_at=prior_generated_at,
                compared_to_window_start=window_start, compared_to_label=period_label,
            ),
            "status": "no_prior",
        })
        save_stage_cost(s3, bucket, iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION, "changes", 0.0)
        context.add_output_metadata({
            "country": dg.MetadataValue.text(iso3),
            "status": dg.MetadataValue.text("no_prior"),
            "notes": dg.MetadataValue.int(0),
        })
        return {"iso3": iso3, "status": "no_prior", "notes": 0}

    new_v5 = _assemble_new_v5(s3, bucket, iso3, l1_nodes())
    budget = analysis_run_budget()
    provider = make_llm_provider(ANALYSIS_LLM_ROLE)
    try:
        changes = generate_changes(
            provider=provider, prior_data=prior["data"], new_data=new_v5,
            basis=_BASIS, prior_generated_at=prior_generated_at,
            compared_to_window_start=window_start, compared_to_label=period_label,
            budget=budget,
        )
    except Exception:  # noqa: BLE001 — change notes never block publish
        context.log.exception("[analysis_changes] %s: change-note generation failed", iso3)
        changes = empty_changes(
            basis=_BASIS, prior_generated_at=prior_generated_at,
            compared_to_window_start=window_start, compared_to_label=period_label,
        )

    save_json(s3, bucket, out_key, {
        "iso3": iso3, "window_kind": _WINDOW_KIND, "changes": changes, "status": "ran",
    })

    try:
        run_id = context.run.run_id
    except Exception:  # noqa: BLE001
        run_id = getattr(context, "run_id", "unknown")
    budget.log_summary(run_id=run_id)
    save_stage_cost(s3, bucket, iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION, "changes", budget.spent)

    n_notes = len(changes.get("notes", {}))
    context.log.info("[analysis_changes] %s: %d section(s) changed vs prior", iso3, n_notes)
    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "status": dg.MetadataValue.text("ran"),
        "notes": dg.MetadataValue.int(n_notes),
        "spend_usd": dg.MetadataValue.float(round(budget.spent, 4)),
    })
    return {"iso3": iso3, "status": "ran", "notes": n_notes}
