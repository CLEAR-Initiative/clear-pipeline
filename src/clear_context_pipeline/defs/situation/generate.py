"""Weekly situation-analysis generator.

Builds one situation-analysis row per pipeline country for the current
calendar year. This asset:

  1. Fetches this year's yearly × country aggregated_datapoint bucket
     for every pipeline country (currently: Sudan only).
  2. Hoists the headline numbers + envelope into `Datapoints`.
  3. Collects the contributing report ids, fetches their titles /
     source_url / published_at via `report_datapoints` lookups, sorts
     chronologically, and packs into `Sources`.
  4. Generates the LLM-backed components - ai_summary, context_risks,
     hazards_and_vulnerabilities, displacement, sectors - each grounded
     in its own RAG search over `knowledgebase`. Set
     `SITUATION_SKIP_NARRATIVE` to ship a deterministic-only row when
     the provider is down or the budget is spent.
  5. Upserts one row per country via `upsertSituationAnalysis`
     (bitemporal supersede + insert on the clear-api side).

Cost: ~10 LLM calls per country-year (4 narrative + 6 sector). Runs
downstream of `reliefweb_weekly_datapoint_aggregations` so the numbers
reflect this week's freshly-recomputed aggregates, and of
`reliefweb_weekly_knowledgebase_upsert` so the narrative is grounded in
this week's chunks rather than last week's.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
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
from clear_context_pipeline.defs.knowledgebase.datapoints_schemas import (
    SCHEMA_VERSION as AGGREGATION_SCHEMA_VERSION,
)
from clear_context_pipeline.defs.situation.changes import generate_changes
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
# list - no code change beyond dropping the hardcoded set.
_POC_COUNTRIES = ("Sudan",)

# Emergency kill-switch - set to "1" / "true" to skip every LLM
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
# The aggregated returnee STOCK (cumulative returned to date) — the meaningful
# single figure for a situation snapshot, mirroring idp_stock. The former
# `returnees` label was split into returnee_stock + new_returns (ADR-0005 §4a).
_LABEL_RETURNEES = "returnee_stock"
_LABEL_FUNDING_REQUIRED = "funding_required_usd"
_LABEL_FUNDING_RECEIVED = "funding_received_usd"
# People in Need. Note `overall_pin` only populates when a report
# headlines a country/appeal-wide figure, so this is driven by HNO /
# HRP / appeal documents and is null for most field reports.
#
# This is deliberately NOT Population Affected - that is the wider
# circle (everyone the crisis touched) and it aggregates `Max` rather
# than `latest_state`. The two are extracted and surfaced side by side;
# do not conflate them. See docs/adr/0001-affected-extracted-not-sourced-from-events.md.
_LABEL_POPULATION_IN_NEED = "overall_pin"
# Population Affected - widest circle of crisis impact. `Max`-aggregated
# and, like PIN, sparse: only populated when a report states an explicit
# affected figure. Distinct from `population_in_need`.
_LABEL_POPULATION_AFFECTED = "overall_affected"


# window_kind + window_start form the clear-api bucket key - (country,
# window_kind, window_start, schema_version) - mirroring
# `aggregated_datapoints`. Month names build the monthly period label
# ("July 2026") the LLM prompts and cache key key off.
_MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)


def _calendar_year_window(year: int) -> tuple[str, str]:
    """Jan 1 → Dec 31 of `year` in UTC, ISO-serialised.

    `window_start` is load-bearing: it keys the bucket. `window_end` is
    stored for display and range work but is never matched on - this
    helper and clear-api's `calendarYearStart` are two independent
    implementations of the same calendar, and an end-of-day that differs
    by a millisecond (23:59:59.000 here vs 23:59:59.999 there) is exactly
    the kind of drift that writes rows no reader can find.
    """
    start = datetime(year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    return start.isoformat(), end.isoformat()


def _calendar_month_window(year: int, month: int) -> tuple[str, str]:
    """1st 00:00:00 → last-day 23:59:59 of (year, month) in UTC, ISO.

    Same load-bearing rule as `_calendar_year_window`: `window_start` keys
    the bucket and is midnight-aligned so it matches clear-api's `monthOf`
    start exactly. `window_end` is display-only (never matched on) - the
    aggregation cascade keys on windowKind + windowStart, not the end.
    """
    start = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
    next_month = (
        datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        if month == 12
        else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    )
    end = next_month - timedelta(seconds=1)
    return start.isoformat(), end.isoformat()


def _previous_window(window_kind: str, window_start: str) -> tuple[str, str] | None:
    """Start (ISO) and period label of the bucket immediately preceding
    `window_start` for the same kind, or None for a kind we cannot step.

    This is what "what changed" should compare against. Diffing a bucket
    against an earlier version of ITSELF answers "what did we learn since
    the last run", which tracks pipeline cadence - regenerate twice in an
    hour and the notes go empty even if the situation is deteriorating.
    Diffing against the period before answers "what changed on the ground",
    which is what the dashboard claims to show.
    """
    start = datetime.fromisoformat(window_start)
    if window_kind == "yearly":
        prev_year = start.year - 1
        return _calendar_year_window(prev_year)[0], str(prev_year)
    if window_kind == "monthly":
        prev_year, prev_month = (
            (start.year - 1, 12) if start.month == 1
            else (start.year, start.month - 1)
        )
        return (
            _calendar_month_window(prev_year, prev_month)[0],
            f"{_MONTH_NAMES[prev_month - 1]} {prev_year}",
        )
    return None


def _resolve_comparison(
    *,
    country_id: str,
    window_kind: str,
    window_start: str,
    period_label: str,
) -> tuple[dict[str, Any], str, str, str] | None:
    """Pick the snapshot to diff the new payload against.

    Returns (prior_row, basis, compared_to_window_start, label), or None
    when there is nothing to compare against at all. Prefers the preceding
    bucket of the same kind; falls back to the prior version of this same
    bucket, which is all that exists for the first period we ever generate.

    The same-bucket read is safe here only because it runs BEFORE the
    upsert - it returns the row this generation is about to supersede.
    """
    prev = _previous_window(window_kind, window_start)
    if prev is not None:
        prev_start, prev_label = prev
        prior = clear_api.get_situation_analysis(
            country_location_id=country_id,
            window_kind=window_kind,
            window_start=prev_start,
            schema_version=SCHEMA_VERSION,
        )
        if prior and prior.get("data"):
            return prior, "previous_period", prev_start, prev_label

    prior = clear_api.get_situation_analysis(
        country_location_id=country_id,
        window_kind=window_kind,
        window_start=window_start,
        schema_version=SCHEMA_VERSION,
    )
    if prior and prior.get("data"):
        return prior, "previous_generation", window_start, period_label
    return None


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
    Datapoints with a zero-report envelope - the dashboard renders
    "no data yet" and moves on."""
    if not aggregated:
        return Datapoints()
    data = aggregated.get("data") or {}

    # KNOWN WRONG: this is the count of contributing reports, not of
    # events - it duplicates `envelope.report_count` exactly, and more
    # reporting on one flood reads as more floods. Ticket #274 replaces
    # it with a count of distinct incident groups from the aggregator,
    # which is blocked on the incident key gaining its Event Type
    # dimension (#270). Not sourced from the `events` table: that is
    # event-driven data over event types that need not correspond to a
    # report's - see docs/adr/0001-affected-extracted-not-sourced-from-events.md.
    number_of_events = int(aggregated.get("reportCount") or 0)

    return Datapoints(
        population_displaced=_field_value(data, _LABEL_POPULATION_DISPLACED),
        population_in_need=_field_value(data, _LABEL_POPULATION_IN_NEED),
        population_affected=_field_value(data, _LABEL_POPULATION_AFFECTED),
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
    doesn't have a title yet - mostly happens for backfilled rows)."""
    reports: list[Source] = []
    for rid in contributing_report_ids:
        meta = report_meta_by_id.get(rid, {})
        reports.append(Source(
            report_id=rid,
            report_title=meta.get("reportTitle") or rid,
            source_url=meta.get("sourceUrl") or "",
            published_at=meta.get("publishedAt") or "",
        ))
    # Sort by published_at descending - most recent first. Rows with
    # empty publishedAt sort to the bottom naturally because "" < any ISO date.
    reports.sort(key=lambda r: r.published_at, reverse=True)
    return Sources(reports=reports)


def _fetch_report_meta(report_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Look up report metadata one at a time via the existing
    `reportDatapoint(reportId)` query. Sub-optimal for large lists
    but Sudan's yearly bucket has at most ~50 reports today; a
    batched query can land later if the count grows.

    Missing rows (report_id in aggregation but no report_datapoints
    entry) return an empty dict - the source falls back to the raw id."""
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
        except Exception as exc:  # noqa: BLE001 - per-report lookup, isolate failures
            logger.warning(
                "[situation] report meta lookup failed for %s: %s", rid, exc,
            )
            continue
        row = data.get("reportDatapoint")
        if row:
            meta[rid] = row
    return meta


def generate_and_upsert_for_country_window(
    *,
    country_name: str,
    window_start: str,
    window_end: str,
    window_kind: str,
    period_label: str,
    log_context=None,
) -> dict | None:
    """Generate and upsert one situation-analysis snapshot for
    (country_name, window_kind, window_start). The wrappers below
    (`generate_and_upsert_for_country_year` / `_month`) supply the calendar
    window + a human `period_label` ("2026" / "July 2026") used in the LLM
    prompts and prompt-cache key.

    Returns the summary dict the asset appends, or ``None`` when the
    country's A0 location can't be resolved (fresh env / locations not
    backfilled yet). Cascades every failure it can catch to a returned-None
    so a caller iterating multiple countries doesn't crash on one bad row.
    """
    log = log_context or logger

    country_id = clear_api.resolve_country_location_id(country_name)
    if not country_id:
        log.warning(
            "[situation] %s: no A0 location resolved - skipping (backfill locations first)",
            country_name,
        )
        return None

    aggregated: dict[str, Any] | None = None
    try:
        aggregated = clear_api.get_aggregated_datapoint(
            location_id=country_id,
            window_start=window_start,
            window_end=window_end,
            window_kind=window_kind,
            # Read the aggregation schema the knowledgebase pipeline
            # writes, not the situation-analysis output schema -
            # otherwise this reads stale buckets of the wrong version.
            schema_version=AGGREGATION_SCHEMA_VERSION,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "[situation] %s: aggregated_datapoint fetch failed (%s) - proceeding with empty datapoints",
            country_name, exc,
        )

    datapoints_component = _build_datapoints(aggregated)
    deterministic_source_ids = (aggregated or {}).get("contributingReportIds") or []
    report_meta = _fetch_report_meta(deterministic_source_ids)
    sources_component = _build_sources(deterministic_source_ids, report_meta)

    skip = _skip_narrative()
    if skip:
        log.warning(
            "[situation] %s: %s set - shipping deterministic-only row",
            country_name, _SKIP_NARRATIVE_ENV,
        )
        ai_summary_component = None
        context_risks_component = None
        hazards_component = None
        displacement_component = None
        sectors_component = None
        generated_by_model = f"deterministic:{SCHEMA_VERSION}"
    else:
        llm = make_llm_provider("narrative")
        cache_key = f"situation:{country_id}:{window_kind}:{period_label}:{SCHEMA_VERSION}"
        ai_summary_component = generate_ai_summary(
            llm, country_name=country_name, period_label=period_label,
            aggregated=aggregated, cache_key=cache_key,
        )
        context_risks_component = generate_context_risks(
            llm, country_name=country_name, period_label=period_label,
            aggregated=aggregated, cache_key=cache_key,
        )
        hazards_component = generate_hazards_and_vulnerabilities(
            llm, country_name=country_name, period_label=period_label,
            aggregated=aggregated, cache_key=cache_key,
        )
        displacement_component = generate_displacement_narrative(
            llm, country_name=country_name, period_label=period_label,
            aggregated=aggregated, cache_key=cache_key,
        )
        sectors_component = generate_all_sectors(
            llm, country_name=country_name, period_label=period_label,
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

    # "What changed" notes, for every bucket kind rather than yearly only -
    # the monthly bucket is the one where a period-over-period diff actually
    # means something. `_resolve_comparison` prefers the preceding bucket and
    # falls back to this bucket's prior version, recording which in `basis`
    # so the dashboard can label the strip honestly.
    #
    # Needs the LLM, so skipped on deterministic-only rows. Best-effort
    # throughout: change notes never block the upsert.
    if not skip:
        try:
            comparison = _resolve_comparison(
                country_id=country_id,
                window_kind=window_kind,
                window_start=window_start,
                period_label=period_label,
            )
            if comparison is not None:
                prior, basis, compared_start, compared_label = comparison
                payload.changes = generate_changes(
                    llm,
                    prior_payload=prior["data"],
                    new_payload=payload.model_dump(mode="json"),
                    basis=basis,
                    prior_generated_at=prior.get("generatedAt") or "",
                    compared_to_window_start=compared_start,
                    compared_to_label=compared_label,
                    cache_key=cache_key,
                )
                log.info(
                    "[situation] %s: change notes vs %s (%s), %d section(s)",
                    country_name, compared_label, basis, len(payload.changes.notes),
                )
        except Exception as exc:  # noqa: BLE001 - change notes never block the upsert
            log.warning(
                "[situation] %s: change-note generation failed (%s); shipping without",
                country_name, exc,
            )

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
            window_kind=window_kind,
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
        "[situation] %s (%s): wrote analysis %s (superseded=%s, %d deterministic sources, %d total, model=%s)",
        country_name, period_label, result["situationAnalysisId"],
        result["supersededPrevious"],
        len(deterministic_source_ids), len(all_source_ids), generated_by_model,
    )
    return {
        "country_name": country_name,
        "country_location_id": country_id,
        "window_kind": window_kind,
        "period": period_label,
        "situation_analysis_id": result["situationAnalysisId"],
        "superseded_previous": result["supersededPrevious"],
        "report_count": len(deterministic_source_ids),
        "total_source_count": len(all_source_ids),
        "generated_by_model": generated_by_model,
    }


def generate_and_upsert_for_country_year(
    *, country_name: str, year: int, log_context=None,
) -> dict | None:
    """Yearly (Jan 1 .. Dec 31) situation snapshot - the original behaviour,
    now a thin wrapper over the window-based core. Kept as a named entry
    point so the manual-document job can trigger a yearly regen."""
    window_start, window_end = _calendar_year_window(year)
    return generate_and_upsert_for_country_window(
        country_name=country_name,
        window_start=window_start,
        window_end=window_end,
        window_kind="yearly",
        period_label=str(year),
        log_context=log_context,
    )


def generate_and_upsert_for_country_month(
    *, country_name: str, year: int, month: int, log_context=None,
) -> dict | None:
    """Monthly (1st .. last day) situation snapshot. Reads the monthly ×
    country aggregated_datapoint bucket (emitted by clear-api's A0 tier) for
    the same window; narrative prompts are framed on the month."""
    window_start, window_end = _calendar_month_window(year, month)
    return generate_and_upsert_for_country_window(
        country_name=country_name,
        window_start=window_start,
        window_end=window_end,
        window_kind="monthly",
        period_label=f"{_MONTH_NAMES[month - 1]} {year}",
        log_context=log_context,
    )


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

    Two upstream dependencies - the analysis needs BOTH branches of
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
        We don't consume its output value - pure ordering constraint,
        hence the `deps=[…]` form rather than a parameter.

    The upstream summary dict is used only to gate on "aggregation
    refresh actually ran" - we re-fetch aggregations from clear-api
    to pick up the freshly-inserted rows.
    """
    del reliefweb_weekly_datapoint_aggregations  # only used to enforce ordering

    now = datetime.now(timezone.utc)
    year = now.year
    month = now.month

    summaries: list[dict] = []
    for country_name in _POC_COUNTRIES:
        # Two snapshots per country: the calendar-year-to-date view and the
        # current month. Each reads its own country-scoped aggregated bucket
        # (yearly-A0 and monthly-A0) for the matching window.
        for summary in (
            generate_and_upsert_for_country_year(
                country_name=country_name, year=year, log_context=context.log,
            ),
            generate_and_upsert_for_country_month(
                country_name=country_name, year=year, month=month,
                log_context=context.log,
            ),
        ):
            if summary is not None:
                summaries.append(summary)

    context.add_output_metadata({
        "countries_processed": dg.MetadataValue.int(len(_POC_COUNTRIES)),
        "snapshots_written": dg.MetadataValue.int(len(summaries)),
        "year": dg.MetadataValue.int(year),
        "month": dg.MetadataValue.int(month),
        "schema_version": dg.MetadataValue.text(SCHEMA_VERSION),
    })
    return summaries
