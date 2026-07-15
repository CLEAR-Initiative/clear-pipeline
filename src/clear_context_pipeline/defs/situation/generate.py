"""Weekly situation-analysis generator.

Phase B scope — deterministic components only. This asset:

  1. Fetches this year's yearly × country aggregated_datapoint bucket
     for every pipeline country (currently: Sudan only).
  2. Hoists the six headline numbers + envelope into `Datapoints`.
  3. Collects the contributing report ids, fetches their titles /
     source_url / published_at via `report_datapoints` lookups, sorts
     chronologically, and packs into `Sources`.
  4. Stubs every LLM-generated component (ai_summary, context_risks,
     hazards_and_vulnerabilities, displacement, sectors) so the JSON
     shape is stable — Phase C / D fill these in.
  5. Upserts one row per country via `upsertSituationAnalysis`
     (bitemporal supersede + insert on the clear-api side).

Cost: zero LLM calls today. Runs downstream of
`reliefweb_weekly_datapoint_aggregations` so the numbers reflect
this week's freshly-recomputed aggregates.
"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dotenv import load_dotenv

from clear_context_pipeline.defs.situation.narrative import (
    generate_ai_summary,
    generate_context_risks,
    generate_displacement_narrative,
    generate_hazards_and_vulnerabilities,
)
from clear_context_pipeline.defs.situation.sectors import generate_all_sectors
from clear_context_pipeline.defs.situation.schemas import (
    SCHEMA_VERSION,
    Datapoints,
    DatapointsEnvelope,
    SituationAnalysisPayload,
    Source,
    Sources,
)
from clear_context_pipeline.providers import clear_api, make_llm_provider

load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

logger = logging.getLogger(__name__)

# Only Sudan for the POC. When we widen, this lookup switches to
# `clear_api.get_pipeline_countries()` and iterates the returned
# list — no code change beyond dropping the hardcoded set.
_POC_COUNTRIES = ("Sudan",)

# Emergency kill-switch — set to "1" / "true" to skip every LLM
# narrative component and ship a deterministic-only row. Same
# semantic as `KB_SKIP_CONTEXTUALIZATION` for the vector pipeline:
# use when the LLM provider is down or the budget is exhausted, so
# the dashboard still gets fresh Datapoints + Sources.
_SKIP_NARRATIVE_ENV = "SITUATION_SKIP_NARRATIVE"


def _skip_narrative() -> bool:
    return os.environ.get(_SKIP_NARRATIVE_ENV, "0").strip().lower() in {"1", "true", "yes", "on"}

# Field labels mirror the FIELD_RULES registry in
# `clear-api/src/services/datapoint-aggregation.ts`. The aggregation
# side owns these keys; the situation-analysis side just consumes.
_LABEL_POPULATION_DISPLACED = "idp_stock"
_LABEL_RETURNEES = "returnees"
_LABEL_FUNDING_REQUIRED = "funding_required_usd"
_LABEL_FUNDING_RECEIVED = "funding_received_usd"
# People in Need. Note `overall_pin` only populates when a report
# headlines a country/appeal-wide figure, so this is driven by HNO /
# HRP / appeal documents and is null for most field reports.
#
# This is deliberately NOT Population Affected — that is the wider
# circle (everyone the crisis touched), nothing extracts it today, and
# it aggregates `Max` rather than `latest_state`. It was previously
# hoisted into a `population_affected` field, which understated it and
# mislabelled it. See docs/adr/0001-affected-extracted-not-sourced-from-events.md.
_LABEL_POPULATION_IN_NEED = "overall_pin"


def _calendar_year_window(year: int) -> tuple[str, str]:
    """Jan 1 → Dec 31 of `year` in UTC, ISO-serialised."""
    start = datetime(year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    return start.isoformat(), end.isoformat()


def _field_value(data: dict[str, Any], label: str) -> float | None:
    """Read the `value` off a QualityEnvelope-shaped field. Returns
    None for missing keys, null fields, or set-union-shaped fields
    (which don't carry a numeric value)."""
    field = data.get(label)
    if not isinstance(field, dict):
        return None
    value = field.get("value")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_datapoints(aggregated: dict[str, Any] | None) -> Datapoints:
    """Hoist the six headline numbers + freshness envelope out of the
    aggregated_datapoint's `data` blob. Missing bucket → all-null
    Datapoints with a zero-report envelope — the dashboard renders
    "no data yet" and moves on."""
    if not aggregated:
        return Datapoints()
    data = aggregated.get("data") or {}

    # "number of events" is derived from the count of contributing
    # reports — Phase C+ will refine this by counting distinct events
    # from the `events` table for the same window; for Phase B this
    # per-report proxy is close enough for the dashboard's headline.
    number_of_events = int(aggregated.get("reportCount") or 0)

    return Datapoints(
        population_displaced=_field_value(data, _LABEL_POPULATION_DISPLACED),
        population_in_need=_field_value(data, _LABEL_POPULATION_IN_NEED),
        returnees=_field_value(data, _LABEL_RETURNEES),
        number_of_events=number_of_events,
        funding_required_usd=_field_value(data, _LABEL_FUNDING_REQUIRED),
        funding_received_usd=_field_value(data, _LABEL_FUNDING_RECEIVED),
        envelope=DatapointsEnvelope(
            quality_score=aggregated.get("dataQualityScore"),
            newest_source_at=aggregated.get("newestSourceAt"),
            oldest_source_at=aggregated.get("oldestSourceAt"),
            report_count=aggregated.get("reportCount"),
        ),
    )


def _build_sources(
    contributing_report_ids: list[str],
    report_meta_by_id: dict[str, dict[str, Any]],
) -> Sources:
    """Chronological (newest first) list of reports that fed this
    analysis. Falls back to the raw report_id when the metadata
    lookup misses (report_datapoints row exists but knowledgebase
    doesn't have a title yet — mostly happens for backfilled rows)."""
    reports: list[Source] = []
    for rid in contributing_report_ids:
        meta = report_meta_by_id.get(rid, {})
        reports.append(Source(
            report_id=rid,
            report_title=meta.get("reportTitle") or rid,
            source_url=meta.get("sourceUrl") or "",
            published_at=meta.get("publishedAt") or "",
        ))
    # Sort by published_at descending — most recent first. Rows with
    # empty publishedAt sort to the bottom naturally because "" < any ISO date.
    reports.sort(key=lambda r: r.published_at, reverse=True)
    return Sources(reports=reports)


def _fetch_report_meta(report_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Look up report metadata one at a time via the existing
    `reportDatapoint(reportId)` query. Sub-optimal for large lists
    but Sudan's yearly bucket has at most ~50 reports today; a
    batched query can land later if the count grows.

    Missing rows (report_id in aggregation but no report_datapoints
    entry) return an empty dict — the source falls back to the raw id."""
    from clear_context_pipeline.providers.clear_api import _execute

    meta: dict[str, dict[str, Any]] = {}
    for rid in report_ids:
        try:
            data = _execute(
                "query ReportMeta($id: String!) { "
                "reportDatapoint(reportId: $id) { "
                "reportTitle sourceUrl publishedAt "
                "} }",
                {"id": rid},
            )
        except Exception as exc:  # noqa: BLE001 — per-report lookup, isolate failures
            logger.warning(
                "[situation] report meta lookup failed for %s: %s", rid, exc,
            )
            continue
        row = data.get("reportDatapoint")
        if row:
            meta[rid] = row
    return meta


def generate_and_upsert_for_country_year(
    *,
    country_name: str,
    year: int,
    log_context=None,
) -> dict | None:
    """Generate and upsert one situation-analysis snapshot for
    (country_name, year). Extracted from the weekly asset so the
    manual-document job can trigger the same regen without cloning
    the asset code path.

    Returns the summary dict the asset historically appended, or
    ``None`` when the country's A0 location can't be resolved (fresh
    env / locations not backfilled yet). Cascades every failure it
    can catch to a returned-None so a caller iterating multiple
    countries doesn't crash on one bad row — direct callers that
    want tight error semantics should wrap.
    """
    log = log_context or logger

    window_start, window_end = _calendar_year_window(year)

    country_id = clear_api.resolve_country_location_id(country_name)
    if not country_id:
        log.warning(
            "[situation] %s: no A0 location resolved — skipping (backfill locations first)",
            country_name,
        )
        return None

    aggregated: dict[str, Any] | None = None
    try:
        aggregated = clear_api.get_aggregated_datapoint(
            location_id=country_id,
            window_start=window_start,
            window_end=window_end,
            window_kind="yearly",
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "[situation] %s: aggregated_datapoint fetch failed (%s) — proceeding with empty datapoints",
            country_name, exc,
        )

    datapoints_component = _build_datapoints(aggregated)
    deterministic_source_ids = (aggregated or {}).get("contributingReportIds") or []
    report_meta = _fetch_report_meta(deterministic_source_ids)
    sources_component = _build_sources(deterministic_source_ids, report_meta)

    skip = _skip_narrative()
    if skip:
        log.warning(
            "[situation] %s: %s set — shipping deterministic-only row",
            country_name, _SKIP_NARRATIVE_ENV,
        )
        ai_summary_component = None
        context_risks_component = None
        hazards_component = None
        displacement_component = None
        sectors_component = None
        generated_by_model = f"deterministic:{SCHEMA_VERSION}"
    else:
        llm = make_llm_provider("extraction")
        cache_key = f"situation:{country_id}:{year}:{SCHEMA_VERSION}"
        ai_summary_component = generate_ai_summary(
            llm, country_name=country_name, year=year,
            aggregated=aggregated, cache_key=cache_key,
        )
        context_risks_component = generate_context_risks(
            llm, country_name=country_name, year=year,
            aggregated=aggregated, cache_key=cache_key,
        )
        hazards_component = generate_hazards_and_vulnerabilities(
            llm, country_name=country_name, year=year,
            aggregated=aggregated, cache_key=cache_key,
        )
        displacement_component = generate_displacement_narrative(
            llm, country_name=country_name, year=year,
            aggregated=aggregated, cache_key=cache_key,
        )
        sectors_component = generate_all_sectors(
            llm, country_name=country_name, year=year,
            aggregated=aggregated, cache_key=cache_key,
        )
        generated_by_model = llm.model

    payload_kwargs: dict[str, Any] = {
        "datapoints": datapoints_component,
        "sources": sources_component,
    }
    if ai_summary_component is not None:
        payload_kwargs["ai_summary"] = ai_summary_component
    if context_risks_component is not None:
        payload_kwargs["context_risks"] = context_risks_component
    if hazards_component is not None:
        payload_kwargs["hazards_and_vulnerabilities"] = hazards_component
    if displacement_component is not None:
        payload_kwargs["displacement"] = displacement_component
    if sectors_component is not None:
        payload_kwargs["sectors"] = sectors_component
    payload = SituationAnalysisPayload(**payload_kwargs)

    sector_source_ids: list[str] = []
    if sectors_component is not None:
        for sector_name in (
            "education", "food_security", "health",
            "shelter", "wash", "protection",
        ):
            sector = getattr(sectors_component, sector_name)
            sector_source_ids.extend(sector.source_report_ids)

    all_source_ids: list[str] = list(dict.fromkeys([
        *deterministic_source_ids,
        *(ai_summary_component.source_report_ids if ai_summary_component else []),
        *(context_risks_component.demographics.source_report_ids if context_risks_component else []),
        *(hazards_component.hazards[0].source_report_ids if hazards_component and hazards_component.hazards else []),
        *(displacement_component.push_factors[0].source_report_ids if displacement_component and displacement_component.push_factors else []),
        *sector_source_ids,
    ]))

    try:
        result = clear_api.upsert_situation_analysis(
            country_location_id=country_id,
            window_start=window_start,
            window_end=window_end,
            data=payload.model_dump(mode="json"),
            source_report_ids=all_source_ids,
            aggregated_datapoint_id=(aggregated or {}).get("id"),
            generated_by_model=generated_by_model,
            generation_cost_usd=None,
            schema_version=SCHEMA_VERSION,
        )
    except clear_api.ClearApiError as exc:
        log.error(
            "[situation] %s: clear-api rejected upsert (non-retryable): %s",
            country_name, exc,
        )
        return None
    except Exception as exc:  # noqa: BLE001
        log.error(
            "[situation] %s: upsert failed after retries: %s",
            country_name, exc,
        )
        return None

    log.info(
        "[situation] %s (%d): wrote analysis %s (superseded=%s, %d deterministic sources, %d total, model=%s)",
        country_name, year, result["situationAnalysisId"],
        result["supersededPrevious"],
        len(deterministic_source_ids), len(all_source_ids), generated_by_model,
    )
    return {
        "country_name": country_name,
        "country_location_id": country_id,
        "year": year,
        "situation_analysis_id": result["situationAnalysisId"],
        "superseded_previous": result["supersededPrevious"],
        "report_count": len(deterministic_source_ids),
        "total_source_count": len(all_source_ids),
        "generated_by_model": generated_by_model,
    }


@dg.asset(
    group_name="reliefweb_kb",
    deps=["reliefweb_weekly_knowledgebase_upsert"],
)
def weekly_situation_analyses(
    context: AssetExecutionContext,
    reliefweb_weekly_datapoint_aggregations: dict,
) -> list[dict]:
    """Generate + upsert one situation-analysis snapshot per pipeline
    country for the current calendar year.

    Two upstream dependencies — the analysis needs BOTH branches of
    this week's ingest to be fresh before it runs:

      - ``reliefweb_weekly_datapoint_aggregations`` (parameter dep):
        the yearly × country aggregated_datapoint bucket must be
        refreshed for the deterministic Datapoints component + as
        the numeric context prompt-cached across every LLM call.

      - ``reliefweb_weekly_knowledgebase_upsert`` (``deps=`` dep):
        the narrative components (AI summary, context risks, hazards,
        displacement, sectors) all run RAG searches over
        `knowledgebase`. Without this dep declared, Dagster might
        run situation-analysis in parallel with the KB upsert and
        the LLM would ground its narrative in last-week's chunks.
        We don't consume its output value — pure ordering constraint,
        hence the `deps=[…]` form rather than a parameter.

    The upstream summary dict is used only to gate on "aggregation
    refresh actually ran" — we re-fetch aggregations from clear-api
    to pick up the freshly-inserted rows.
    """
    del reliefweb_weekly_datapoint_aggregations  # only used to enforce ordering

    now = datetime.now(timezone.utc)
    year = now.year

    summaries: list[dict] = []
    for country_name in _POC_COUNTRIES:
        summary = generate_and_upsert_for_country_year(
            country_name=country_name,
            year=year,
            log_context=context.log,
        )
        if summary is not None:
            summaries.append(summary)

    context.add_output_metadata({
        "countries_processed": dg.MetadataValue.int(len(summaries)),
        "year": dg.MetadataValue.int(year),
        "schema_version": dg.MetadataValue.text(SCHEMA_VERSION),
    })
    return summaries
