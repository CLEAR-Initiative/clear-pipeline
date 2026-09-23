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
from typing import Any, Optional

import dagster as dg
from dagster import AssetExecutionContext
from dotenv import load_dotenv

from clear_pipeline.defs.situation.narrative import (
    generate_ai_summary,
    generate_context_risks,
    generate_displacement_narrative,
    generate_hazards_and_vulnerabilities,
    generate_scenarios,
)
from clear_pipeline.defs.knowledgebase.datapoints_schemas import (
    SCHEMA_VERSION as AGGREGATION_SCHEMA_VERSION,
)
from clear_pipeline.defs.situation.changes import generate_changes
from clear_pipeline.defs.situation.frame import (
    Frame,
    build_rag_filters,
    country_frame,
)
from clear_pipeline.defs.situation.sectors import generate_all_sectors
from clear_pipeline.defs.situation.schemas import (
    SCHEMA_VERSION,
    Datapoints,
    DatapointsEnvelope,
    DivergenceSignal,
    RangeFigure,
    SituationAnalysisPayload,
    Source,
    Sources,
    StockFlowEstimate,
)
from clear_pipeline.defs.reliefweb_partitions import country_partitions
from clear_pipeline.providers import clear_api, make_llm_provider
from clear_pipeline.providers.translate import configured_target_locales

load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

logger = logging.getLogger(__name__)

# Country is a Dagster partition: `weekly_situation_analyses` runs once per
# country (its `context.partition_key` iso3), resolving the name from
# `clear_api.get_pipeline_countries()`. The old hardcoded POC set is gone — the
# partition set is what scopes which countries get situation snapshots.

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
# Period-increment FLOWS (additive) — the counterparts to the stocks above.
_LABEL_NEW_DISPLACEMENTS = "new_displacements"
_LABEL_NEW_RETURNS = "new_returns"
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


def _coerce_float(raw: Any) -> float | None:
    """Best-effort float coercion that swallows the untyped-JSONB hazards
    (None, strings, set-union shapes) instead of raising — `_build_datapoints`
    runs outside the fetch try/except, so a stray value must not fail the run."""
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _field_range(data: dict[str, Any], label: str) -> RangeFigure | None:
    """Project a QualityEnvelope-shaped field into a RangeFigure (ADR-0007):
    the point `value` plus its honest error bar `[value_low, value_high]`,
    `range_width`, the projection `bias`, and a `quality_score` confidence.
    Returns None for missing keys, null fields, or set-union-shaped fields
    (which carry no numeric value/band)."""
    field = data.get(label)
    if not isinstance(field, dict):
        return None
    value = _coerce_float(field.get("value"))
    low = _coerce_float(field.get("value_low"))
    high = _coerce_float(field.get("value_high"))
    if value is None and low is None and high is None:
        return None
    bias = field.get("bias")
    return RangeFigure(
        value=value,
        low=low,
        high=high,
        range_width=_coerce_float(field.get("range_width")),
        bias=bias if isinstance(bias, str) else None,
        confidence=_coerce_float(field.get("quality_score")),
    )


def _stock_flow_estimate(raw: Any) -> Optional[StockFlowEstimate]:
    """Map one `estimatedCurrentTotals` metric (ADR-0006 §4) into the payload
    model. `None`/non-dict → `None` (no anchoring stock in scope)."""
    if not isinstance(raw, dict):
        return None
    return StockFlowEstimate(
        total=raw.get("total"),
        stock=raw.get("stock"),
        flows_since=raw.get("flowsSince"),
        t0=raw.get("t0"),
        flow_count=raw.get("flowCount"),
    )


def _collect_divergences(data: dict[str, Any]) -> list[DivergenceSignal]:
    """Scan the aggregated `data` blob for per-field ADR-0006 §7 divergence
    signals (a report figure that lost to the authoritative API figure by more
    than the threshold) and surface them as early-warnings on the snapshot."""
    out: list[DivergenceSignal] = []
    for label, field in data.items():
        if not isinstance(field, dict):
            continue
        div = field.get("divergence")
        if not isinstance(div, dict):
            continue
        report_value = div.get("reportValue")
        api_value = div.get("apiValue")
        pct_diff = div.get("pctDiff")
        if report_value is None or api_value is None or pct_diff is None:
            continue
        # Coerce defensively: `div` is an untyped JSONB blob, so a stray string
        # or a future aggregator shape must NOT raise a ValidationError here —
        # `_build_datapoints` runs outside the fetch try/except and the weekly
        # asset has no per-country guard, so one bad value would fail the run for
        # EVERY country and window. Skip the malformed signal instead (#30),
        # matching `_coerce_float`'s (TypeError, ValueError)-swallowing posture.
        try:
            signal = DivergenceSignal(
                field=label,
                report_value=float(report_value),
                api_value=float(api_value),
                pct_diff=float(pct_diff),
            )
        except (TypeError, ValueError):
            continue
        out.append(signal)
    return out


def _build_datapoints(aggregated: dict[str, Any] | None) -> Datapoints:
    """Hoist the headline numbers + freshness envelope out of the
    aggregated_datapoint's `data` blob, plus the estimated current
    totals and divergence early-warnings (ADR-0006 §4/§7). Missing
    bucket → all-null Datapoints with a zero-report envelope - the
    dashboard renders "no data yet" and moves on."""
    if not aggregated:
        return Datapoints()
    data = aggregated.get("data") or {}
    current_totals = aggregated.get("estimatedCurrentTotals") or {}

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
        population_displaced=_field_range(data, _LABEL_POPULATION_DISPLACED),
        population_in_need=_field_range(data, _LABEL_POPULATION_IN_NEED),
        population_affected=_field_range(data, _LABEL_POPULATION_AFFECTED),
        returnees=_field_range(data, _LABEL_RETURNEES),
        new_displacements=_field_range(data, _LABEL_NEW_DISPLACEMENTS),
        new_returns=_field_range(data, _LABEL_NEW_RETURNS),
        number_of_events=number_of_events,
        funding_required_usd=_field_range(data, _LABEL_FUNDING_REQUIRED),
        funding_received_usd=_field_range(data, _LABEL_FUNDING_RECEIVED),
        estimated_current_displacement=_stock_flow_estimate(current_totals.get("displacement")),
        estimated_current_returns=_stock_flow_estimate(current_totals.get("returns")),
        divergences=_collect_divergences(data),
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
    from clear_pipeline.providers.clear_api import _execute

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


_CR_DOMAINS = (
    "demographics", "political", "economy", "socio_culture",
    "security", "legal_policy", "infrastructure", "environment",
)
_SECTOR_CONTENT_FIELDS = (
    "impact", "humanitarian_conditions", "vulnerable_sections",
    "top_needs", "priority_interventions",
)


def _log_component_summary(
    log, country_name: str, period_label: str, *,
    ai_summary, context_risks, hazards, displacement, sectors,
) -> bool:
    """Emit a single INFO line stating which narrative/sector components ended up
    populated vs empty — the fast signal for an all-null generation run (the exact
    symptom when RAG returns nothing because clear-api's EMBEDDING_* is unset).

    Returns ``all_empty`` — True when every narrative/sector component came back
    empty. The caller uses it to SKIP the upsert on a normal run, so a failed
    generation never supersedes the previous (good) row."""
    ai = bool(ai_summary and getattr(ai_summary, "text", ""))
    cr = bool(context_risks and any(
        getattr(getattr(context_risks, d, None), "bullets", None) for d in _CR_DOMAINS
    ))
    hz = bool(hazards and (hazards.hazards or hazards.vulnerabilities))
    dp = bool(displacement and (displacement.push_factors or displacement.return_intention))
    sec_total = sec_pop = 0
    if sectors is not None:
        for name in type(sectors).model_fields:
            s = getattr(sectors, name, None)
            if s is None or not hasattr(s, "severity"):
                continue
            sec_total += 1
            if s.severity is not None or any(getattr(s, f, None) for f in _SECTOR_CONTENT_FIELDS):
                sec_pop += 1
    all_empty = not (ai or cr or hz or dp or sec_pop)
    log.log(
        logging.WARNING if all_empty else logging.INFO,
        "[situation] %s (%s) component summary%s: ai_summary=%s context_risks=%s "
        "hazards=%s displacement=%s sectors=%d/%d",
        country_name, period_label,
        " — ALL EMPTY (likely RAG returned no hits; check clear-api EMBEDDING_* + logs)" if all_empty else "",
        "ok" if ai else "empty", "ok" if cr else "empty",
        "ok" if hz else "empty", "ok" if dp else "empty", sec_pop, sec_total,
    )
    return all_empty


def generate_and_upsert_for_frame(
    *,
    frame: Frame,
    scope_label: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    rag_filters: dict[str, Any] | None,
    log_context=None,
) -> dict | None:
    """Generate + upsert one unified analysis snapshot for a FRAME (ADR-0007) —
    the generalisation of the situation generator. Instead of a (country,
    calendar window) it takes a ``Frame`` plus the pre-resolved retrieval scope
    (``rag_filters``) and deterministic numbers (``aggregated`` — a matching
    aggregated_datapoints bucket, or None → narrative-only, decision #2).
    ``scope_label`` is the human label the LLM prompts frame on (a country name
    for the default; a derived label for a custom frame).

    Writes to the ``analyses`` table via ``upsertAnalysis`` (bitemporal
    supersede-then-insert). Returns a summary dict, or None on unrecoverable
    failure / an all-empty normal run (which must not supersede a good row).
    """
    log = log_context or logger

    log.info(
        "[analysis] %s (%s): starting generation (locs=%d events=%d sectors=%d window=%s..%s)",
        scope_label, period_label,
        len(frame.location_ids), len(frame.event_types), len(frame.need_sectors),
        frame.window_start, frame.window_end or "present",
    )

    datapoints_component = _build_datapoints(aggregated)
    deterministic_source_ids = (aggregated or {}).get("contributingReportIds") or []
    report_meta = _fetch_report_meta(deterministic_source_ids)
    sources_component = _build_sources(deterministic_source_ids, report_meta)

    # Stable frame key for the Anthropic prompt cache — byte-identical across a
    # run's component calls so calls 2..N read from cache.
    frame_key = ":".join([
        "|".join(frame.location_ids), "|".join(frame.event_types),
        "|".join(frame.need_sectors), frame.window_start, frame.window_end or "",
    ])
    cache_key = f"analysis:{frame_key}:{SCHEMA_VERSION}"

    skip = _skip_narrative()
    # Stays False for a deliberate deterministic-only (skip) run — that IS a valid
    # row and is allowed to supersede. Only a normal run that produces nothing sets
    # it True, which gates out the upsert below.
    all_empty = False
    if skip:
        log.warning(
            "[analysis] %s: %s set - shipping deterministic-only row",
            scope_label, _SKIP_NARRATIVE_ENV,
        )
        ai_summary_component = None
        context_risks_component = None
        hazards_component = None
        displacement_component = None
        sectors_component = None
        scenarios_component = None
        generated_by_model = f"deterministic:{SCHEMA_VERSION}"
    else:
        llm = make_llm_provider("narrative")
        log.info(
            "[analysis] %s (%s): generating LLM components (provider=%s model=%s)",
            scope_label, period_label, llm.provider_name, llm.model,
        )
        ai_summary_component = generate_ai_summary(
            llm, country_name=scope_label, period_label=period_label,
            aggregated=aggregated, cache_key=cache_key, rag_filters=rag_filters,
        )
        context_risks_component = generate_context_risks(
            llm, country_name=scope_label, period_label=period_label,
            aggregated=aggregated, cache_key=cache_key, rag_filters=rag_filters,
        )
        hazards_component = generate_hazards_and_vulnerabilities(
            llm, country_name=scope_label, period_label=period_label,
            aggregated=aggregated, cache_key=cache_key, rag_filters=rag_filters,
        )
        displacement_component = generate_displacement_narrative(
            llm, country_name=scope_label, period_label=period_label,
            aggregated=aggregated, cache_key=cache_key, rag_filters=rag_filters,
        )
        sectors_component = generate_all_sectors(
            llm, country_name=scope_label, period_label=period_label,
            aggregated=aggregated, cache_key=cache_key, rag_filters=rag_filters,
        )
        scenarios_component = generate_scenarios(
            llm, country_name=scope_label, period_label=period_label,
            aggregated=aggregated, cache_key=cache_key, rag_filters=rag_filters,
        )
        generated_by_model = llm.model
        # Populated-vs-empty summary at INFO — makes an all-null run (e.g. RAG
        # returning nothing because clear-api's EMBEDDING_* is misconfigured)
        # obvious at a glance instead of a silent bad row.
        all_empty = _log_component_summary(
            log, scope_label, period_label,
            ai_summary=ai_summary_component,
            context_risks=context_risks_component,
            hazards=hazards_component,
            displacement=displacement_component,
            sectors=sectors_component,
        )

    # Don't let a failed generation supersede the previous good row. A normal run
    # whose narrative + sectors ALL came back empty is treated as unsuccessful:
    # skip the upsert so clear-api never stamps validTo on the previous current
    # row. (A deliberate SITUATION_SKIP_NARRATIVE run keeps all_empty=False and
    # still writes its intentional deterministic-only row.)
    if all_empty:
        log.warning(
            "[analysis] %s (%s): all narrative/sector components empty — SKIPPING upsert "
            "(NOT superseding the previous row). Likely RAG returned no hits; check "
            "clear-api EMBEDDING_* + logs.",
            scope_label, period_label,
        )
        return None

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
    if scenarios_component is not None:
        payload_kwargs["scenarios"] = scenarios_component
    payload = SituationAnalysisPayload(**payload_kwargs)

    # "What changed" vs the frame's PRIOR GENERATION (ADR-0007 §8): the current
    # `analyses` row for this frame IS the prior generation — read here, before
    # the upsert below supersedes it. Needs the LLM, so skipped on
    # deterministic-only rows. Best-effort: change notes never block the upsert.
    if not skip:
        try:
            prior = clear_api.get_analysis(
                schema_version=SCHEMA_VERSION, **frame.upsert_kwargs(),
            )
            if prior and prior.get("data"):
                payload.changes = generate_changes(
                    llm,
                    prior_payload=prior["data"],
                    new_payload=payload.model_dump(mode="json"),
                    basis="previous_generation",
                    prior_generated_at=prior.get("generatedAt") or "",
                    compared_to_window_start=frame.window_start,
                    compared_to_label=period_label,
                    cache_key=cache_key,
                )
                log.info(
                    "[analysis] %s: change notes vs prior generation, %d section(s)",
                    scope_label, len(payload.changes.notes),
                )
        except Exception as exc:  # noqa: BLE001 - change notes never block the upsert
            log.warning(
                "[analysis] %s: change-note generation failed (%s); shipping without",
                scope_label, exc,
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
        *(scenarios_component.source_report_ids if scenarios_component else []),
    ]))

    try:
        result = clear_api.upsert_analysis(
            data=payload.model_dump(mode="json"),
            source_report_ids=all_source_ids,
            generated_by_model=generated_by_model,
            generation_cost_usd=None,
            schema_version=SCHEMA_VERSION,
            **frame.upsert_kwargs(),
        )
    except clear_api.ClearApiError as exc:
        # Re-raise (don't swallow to None): a None return means "empty, nothing to
        # supersede", so a caller can't distinguish it from a failure. Raising lets
        # the on-demand drain mark the request FAILED and the automation scheduler
        # leave the frame DUE (retry) instead of advancing its cadence past a blip.
        log.error("[analysis] %s: clear-api rejected upsert (non-retryable): %s", scope_label, exc)
        raise
    except Exception as exc:  # noqa: BLE001
        log.error("[analysis] %s: upsert failed after retries: %s", scope_label, exc)
        raise

    analysis_id = result["analysisId"]
    log.info(
        "[analysis] %s (%s): wrote analysis %s (superseded=%s, %d deterministic sources, %d total, model=%s)",
        scope_label, period_label, analysis_id, result["supersededPrevious"],
        len(deterministic_source_ids), len(all_source_ids), generated_by_model,
    )

    # Enqueue the freshly-written analysis for translation at every configured
    # locale. Best-effort — the analysis still ships in English on failure, and
    # the read-miss path re-enqueues on the next non-English read.
    for locale in configured_target_locales():
        try:
            clear_api.enqueue_translation("analysis", analysis_id, locale)
        except Exception:  # noqa: BLE001 — translation enqueue must not fail generation
            log.warning(
                "[analysis] %s: enqueue_translation failed for analysis %s (%s) — skipped",
                scope_label, analysis_id, locale, exc_info=True,
            )

    return {
        "scope_label": scope_label,
        "location_ids": list(frame.location_ids),
        "period": period_label,
        "analysis_id": analysis_id,
        "superseded_previous": result["supersededPrevious"],
        "report_count": len(deterministic_source_ids),
        "total_source_count": len(all_source_ids),
        "generated_by_model": generated_by_model,
    }


def generate_and_upsert_for_country_window(
    *,
    country_name: str,
    country_pcode: str | None = None,
    window_start: str,
    window_end: str,
    window_kind: str,
    period_label: str,
    log_context=None,
) -> dict | None:
    """Country-default analysis: resolve the A0, read its (yearly/monthly ×
    country) aggregated_datapoint bucket, and generate over a country FRAME.
    Retrieval stays country-scoped (countryLocationId subtree, no time filter —
    decision #1); the write goes to the unified `analyses` table (decision #3).
    Thin wrapper over `generate_and_upsert_for_frame`.

    Returns the summary dict (augmented with the country identity the weekly
    asset + callers key on), or None when the A0 can't be resolved.
    """
    log = log_context or logger

    log.info(
        "[analysis] %s (%s): country generation (window_kind=%s window_start=%s)",
        country_name, period_label, window_kind, window_start,
    )

    country_id = clear_api.resolve_country_location_id(country_name, pcode=country_pcode)
    if not country_id:
        log.warning(
            "[analysis] %s (pcode=%s): no A0 location resolved - skipping (backfill locations first)",
            country_name, country_pcode or "-",
        )
        return None
    log.debug("[analysis] %s: resolved country_id=%s", country_name, country_id)

    aggregated: dict[str, Any] | None = None
    try:
        aggregated = clear_api.get_aggregated_datapoint(
            location_id=country_id,
            window_start=window_start,
            window_end=window_end,
            window_kind=window_kind,
            # Read the aggregation schema the knowledgebase pipeline writes, not
            # the analysis output schema — otherwise this reads stale buckets.
            schema_version=AGGREGATION_SCHEMA_VERSION,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "[analysis] %s: aggregated_datapoint fetch failed (%s) - proceeding with empty datapoints",
            country_name, exc,
        )

    frame = country_frame(country_id, window_start=window_start, window_end=window_end)
    rag_filters = build_rag_filters(frame, country_scope_id=country_id, include_time_range=False)
    try:
        result = generate_and_upsert_for_frame(
            frame=frame,
            scope_label=country_name,
            period_label=period_label,
            aggregated=aggregated,
            rag_filters=rag_filters,
            log_context=log_context,
        )
    except Exception as exc:  # noqa: BLE001 — the weekly asset iterates countries; one country's failure must not crash the run
        log.error("[analysis] %s: generation failed (%s) — skipping this country", country_name, exc)
        return None
    if result is None:
        return None
    # Augment with the country identity the weekly asset + existing callers key on.
    result["country_name"] = country_name
    result["country_location_id"] = country_id
    result["window_kind"] = window_kind
    return result


def generate_and_upsert_for_country_year(
    *, country_name: str, country_pcode: str | None = None, year: int, log_context=None,
) -> dict | None:
    """Yearly (Jan 1 .. Dec 31) situation snapshot - the original behaviour,
    now a thin wrapper over the window-based core. Kept as a named entry
    point so the manual-document job can trigger a yearly regen."""
    window_start, window_end = _calendar_year_window(year)
    return generate_and_upsert_for_country_window(
        country_name=country_name,
        country_pcode=country_pcode,
        window_start=window_start,
        window_end=window_end,
        window_kind="yearly",
        period_label=str(year),
        log_context=log_context,
    )


def generate_and_upsert_for_country_month(
    *, country_name: str, country_pcode: str | None = None, year: int, month: int, log_context=None,
) -> dict | None:
    """Monthly (1st .. last day) situation snapshot. Reads the monthly ×
    country aggregated_datapoint bucket (emitted by clear-api's A0 tier) for
    the same window; narrative prompts are framed on the month."""
    window_start, window_end = _calendar_month_window(year, month)
    return generate_and_upsert_for_country_window(
        country_name=country_name,
        country_pcode=country_pcode,
        window_start=window_start,
        window_end=window_end,
        window_kind="monthly",
        period_label=f"{_MONTH_NAMES[month - 1]} {year}",
        log_context=log_context,
    )


@dg.asset(
    group_name="reliefweb_kb",
    deps=["reliefweb_weekly_knowledgebase_upsert"],
    partitions_def=country_partitions,
)
def weekly_situation_analyses(
    context: AssetExecutionContext,
    reliefweb_weekly_datapoint_aggregations: dict,
) -> list[dict]:
    """Generate + upsert the situation-analysis snapshots for THIS country
    partition (calendar-year-to-date + current month) for the current year.

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

    iso3 = context.partition_key
    # Map the partition iso3 back to the country's name + pcode. The generators
    # resolve the A0 location by PCODE (name-independent), falling back to name.
    row_by_iso3 = {
        (c.get("iso3") or "").lower(): c for c in clear_api.get_pipeline_countries()
    }
    country = row_by_iso3.get(iso3.lower())
    if country is None:
        raise dg.Failure(
            description=(
                f"partition {iso3!r} is not in clear-api pipelineCountries — cannot "
                "resolve a country for situation generation"
            ),
        )
    country_name = country["name"]
    country_pcode = country.get("pcode")

    now = datetime.now(timezone.utc)
    year = now.year
    month = now.month

    # Two snapshots for this country: the calendar-year-to-date view and the
    # current month. Each reads its own country-scoped aggregated bucket
    # (yearly-A0 and monthly-A0) for the matching window.
    summaries: list[dict] = []
    for summary in (
        generate_and_upsert_for_country_year(
            country_name=country_name, country_pcode=country_pcode,
            year=year, log_context=context.log,
        ),
        generate_and_upsert_for_country_month(
            country_name=country_name, country_pcode=country_pcode,
            year=year, month=month, log_context=context.log,
        ),
    ):
        if summary is not None:
            summaries.append(summary)

    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "snapshots_written": dg.MetadataValue.int(len(summaries)),
        "year": dg.MetadataValue.int(year),
        "month": dg.MetadataValue.int(month),
        "schema_version": dg.MetadataValue.text(SCHEMA_VERSION),
    })
    return summaries
