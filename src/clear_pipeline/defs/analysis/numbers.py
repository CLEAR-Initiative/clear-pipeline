"""Numbers extraction (doc §8, §10) — deterministic.

Hoists the frame's KB-aggregated figures (the ``aggregated_datapoints`` bucket
clear-api already computes, QualityEnvelope leaves + provenance) into the
analysis module's numbers artifact. Runs **before** the gap decision so the
pipeline knows what the DB already answers (web gap-fill is the fallback for
what it doesn't).

Pure: no I/O. The asset fetches the bucket and persists the payload; the DB
write to ``analysis_datapoints`` happens later in the assembly/publish stage
(which creates the ``analyses`` row the non-null ``analysis_id`` FK needs).

Web-sourced numbers are intentionally not merged here — web gap-fill emits prose
findings, not structured figures; numeric reconciliation against web evidence is
a later concern (doc §10).
"""

from __future__ import annotations

from typing import Any

from clear_pipeline.defs.analysis.schemas import (
    Datapoints,
    DatapointsEnvelope,
    DivergenceSignal,
    RangeFigure,
    StockFlowEstimate,
)

# Origin tag for figures hoisted from the KB aggregation.
ORIGIN_AGGREGATED = "aggregated"

# Aggregation label → v5 Datapoints field (the aggregation side owns these keys;
# mirrors clear-api's datapoint-aggregation FIELD_RULES).
_LABEL_POPULATION_DISPLACED = "idp_stock"
_LABEL_RETURNEES = "returnee_stock"
_LABEL_NEW_DISPLACEMENTS = "new_displacements"
_LABEL_NEW_RETURNS = "new_returns"
_LABEL_FUNDING_REQUIRED = "funding_required_usd"
_LABEL_FUNDING_RECEIVED = "funding_received_usd"
_LABEL_POPULATION_IN_NEED = "overall_pin"
_LABEL_POPULATION_AFFECTED = "overall_affected"


def build_frame_numbers(aggregated: dict[str, Any] | None) -> dict[str, Any]:
    """Repackage an ``aggregated_datapoints`` bucket into the analysis numbers
    payload. Returns an ``empty`` payload when there is no bucket (no figures
    the DB could offer for this frame/window yet).

    ``aggregated`` is clear-api's ``AggregatedDatapoint`` shape (camelCase):
    ``data``, ``contributingReportIds``, ``newestSourceAt``, ``oldestSourceAt``,
    ``dataQualityScore``, ``reportCount``.
    """
    if not aggregated:
        return {
            "data": {},
            "contributing_report_ids": [],
            "origin": None,
            "newest_source_at": None,
            "oldest_source_at": None,
            "data_quality_score": None,
            "report_count": 0,
            "empty": True,
        }
    return {
        "data": aggregated.get("data") or {},
        "estimated_current_totals": aggregated.get("estimatedCurrentTotals") or {},
        "contributing_report_ids": list(aggregated.get("contributingReportIds") or []),
        "origin": ORIGIN_AGGREGATED,
        "newest_source_at": aggregated.get("newestSourceAt"),
        "oldest_source_at": aggregated.get("oldestSourceAt"),
        "data_quality_score": aggregated.get("dataQualityScore"),
        "report_count": aggregated.get("reportCount") or 0,
        "empty": False,
    }


def _coerce_float(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _field_range(data: dict[str, Any], label: str) -> RangeFigure | None:
    """Project a QualityEnvelope-shaped aggregation field into a v5 RangeFigure
    (ADR-0007): point value + honest band + bias + confidence. None for missing /
    null / set-union-shaped fields."""
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
        value=value, low=low, high=high,
        range_width=_coerce_float(field.get("range_width")),
        bias=bias if isinstance(bias, str) else None,
        confidence=_coerce_float(field.get("quality_score")),
    )


def _stock_flow_estimate(raw: Any) -> StockFlowEstimate | None:
    if not isinstance(raw, dict):
        return None
    return StockFlowEstimate(
        total=raw.get("total"), stock=raw.get("stock"), flows_since=raw.get("flowsSince"),
        t0=raw.get("t0"), flow_count=raw.get("flowCount"),
    )


def _collect_divergences(data: dict[str, Any]) -> list[DivergenceSignal]:
    out: list[DivergenceSignal] = []
    for label, field in data.items():
        if not isinstance(field, dict):
            continue
        div = field.get("divergence")
        if not isinstance(div, dict):
            continue
        rv, av, pd = div.get("reportValue"), div.get("apiValue"), div.get("pctDiff")
        if rv is None or av is None or pd is None:
            continue
        try:
            out.append(DivergenceSignal(
                field=label, report_value=float(rv), api_value=float(av), pct_diff=float(pd),
            ))
        except (TypeError, ValueError):
            continue
    return out


def project_v5_datapoints(numbers: dict[str, Any] | None) -> dict[str, Any]:
    """Project the numbers payload into the **v5 Datapoints** shape clear-mvp reads
    (``population_displaced`` / ``population_in_need`` / ``returnees`` / funding as
    RangeFigures + the freshness envelope), instead of the raw aggregation label
    map (``idp_stock`` / ``overall_pin`` / …) — so the headline figures survive and
    the change diff compares the figures the frontend shows."""
    numbers = numbers or {}
    data = numbers.get("data") or {}
    current = numbers.get("estimated_current_totals") or {}
    return Datapoints(
        population_displaced=_field_range(data, _LABEL_POPULATION_DISPLACED),
        population_in_need=_field_range(data, _LABEL_POPULATION_IN_NEED),
        population_affected=_field_range(data, _LABEL_POPULATION_AFFECTED),
        returnees=_field_range(data, _LABEL_RETURNEES),
        new_displacements=_field_range(data, _LABEL_NEW_DISPLACEMENTS),
        new_returns=_field_range(data, _LABEL_NEW_RETURNS),
        number_of_events=int(numbers.get("report_count") or 0),
        funding_required_usd=_field_range(data, _LABEL_FUNDING_REQUIRED),
        funding_received_usd=_field_range(data, _LABEL_FUNDING_RECEIVED),
        estimated_current_displacement=_stock_flow_estimate(current.get("displacement")),
        estimated_current_returns=_stock_flow_estimate(current.get("returns")),
        divergences=_collect_divergences(data),
        envelope=DatapointsEnvelope(
            quality_score=numbers.get("data_quality_score"),
            newest_source_at=numbers.get("newest_source_at"),
            oldest_source_at=numbers.get("oldest_source_at"),
            report_count=int(numbers.get("report_count") or 0),
        ),
    ).model_dump()
