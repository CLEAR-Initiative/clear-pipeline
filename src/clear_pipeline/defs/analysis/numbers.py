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

# Origin tag for figures hoisted from the KB aggregation.
ORIGIN_AGGREGATED = "aggregated"


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
        "contributing_report_ids": list(aggregated.get("contributingReportIds") or []),
        "origin": ORIGIN_AGGREGATED,
        "newest_source_at": aggregated.get("newestSourceAt"),
        "oldest_source_at": aggregated.get("oldestSourceAt"),
        "data_quality_score": aggregated.get("dataQualityScore"),
        "report_count": aggregated.get("reportCount") or 0,
        "empty": False,
    }
