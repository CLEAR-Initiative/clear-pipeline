"""Multi-location datapoint roll-up for a frame (ADR-0007, decision #2).

``clear_api.get_aggregated_datapoint`` returns ONE bucket per location (its
subtree roll-up over ``report_datapoints`` in the window). A frame with several
locations therefore fetches one bucket per location and combines them here.

Two steps, both pure (no I/O — the caller injects the parent map and the
buckets) so they're unit-testable without a live clear-api:

  1. :func:`dedupe_nested_locations` drops any location that is a descendant of
     another location the frame also lists. Each bucket is already a SUBTREE
     roll-up, so summing an ancestor and its descendant would double-count the
     descendant. Siblings / disjoint locations are kept and summed.
  2. :func:`combine_aggregated_buckets` sums the de-nested buckets into one
     synthetic aggregated dict shaped exactly like a single bucket — only the
     keys the situation generator consumes (``data`` figures, envelope,
     ``estimatedCurrentTotals``, ``reportCount``, ``contributingReportIds``).

Combine semantics (documented because a few are judgement calls, not pure sums):

  * Point figures ``value`` / ``value_low`` / ``value_high`` — summed across the
    buckets that carry the field (a location contributing no figure adds
    nothing). ``range_width`` is re-derived as ``high - low``.
  * ``bias`` — dropped (``None``): a per-location projection direction has no
    meaning once summed across locations.
  * ``quality_score`` / ``dataQualityScore`` — report-count-weighted mean across
    contributing buckets (a bigger location's confidence weighs more).
  * ``divergence`` — dropped: a single-bucket "report figure vs API figure"
    early-warning isn't reconstructable for a roll-up; we don't fabricate one.
  * Envelope dates — ``newestSourceAt`` = max, ``oldestSourceAt`` = min.
  * ``estimatedCurrentTotals`` (displacement / returns) — ``total`` / ``stock`` /
    ``flowsSince`` / ``flowCount`` summed; ``t0`` = min.
  * ``reportCount`` summed; ``contributingReportIds`` unioned (order-preserving).

A single-location frame never reaches here — the caller returns its one bucket
untouched, so the country/default path is byte-for-byte unchanged.
"""

from __future__ import annotations

from typing import Any

# The numeric figure fields carried per label in a bucket's ``data`` blob.
_FIGURE_KEYS = ("value", "value_low", "value_high")


def dedupe_nested_locations(
    location_ids: list[str], parent_of: dict[str, str | None]
) -> list[str]:
    """Return ``location_ids`` with any descendant of another listed id removed.

    ``parent_of`` maps id → immediate parent id (``None`` at the root); an id
    absent from the map is treated as a root (kept). Order is preserved. Cycle-
    and self-parent-safe (a bounded walk that stops on a repeat)."""
    selected = set(location_ids)
    kept: list[str] = []
    for loc in location_ids:
        # Walk ancestors; drop `loc` if any ancestor is also selected.
        seen: set[str] = {loc}
        cur = parent_of.get(loc)
        is_nested = False
        while cur is not None and cur not in seen:
            if cur in selected:
                is_nested = True
                break
            seen.add(cur)
            cur = parent_of.get(cur)
        if not is_nested:
            kept.append(loc)
    return kept


def _num(raw: Any) -> float | None:
    if raw is None or isinstance(raw, bool):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _sum_present(values: list[float | None]) -> float | None:
    present = [v for v in values if v is not None]
    return sum(present) if present else None


def _weighted_mean(pairs: list[tuple[float | None, float]]) -> float | None:
    """Mean of ``(score, weight)`` pairs, skipping None scores. Falls back to a
    simple mean when every weight is 0/absent."""
    scored = [(s, w) for s, w in pairs if s is not None]
    if not scored:
        return None
    total_w = sum(w for _, w in scored)
    if total_w > 0:
        return sum(s * w for s, w in scored) / total_w
    return sum(s for s, _ in scored) / len(scored)


def _combine_current_total(buckets: list[dict], metric: str) -> dict[str, Any] | None:
    """Sum one ``estimatedCurrentTotals`` metric (``displacement`` / ``returns``)
    across buckets; ``t0`` = earliest anchoring instant. None if no bucket has it."""
    metrics = [
        m for b in buckets
        if isinstance((m := (b.get("estimatedCurrentTotals") or {}).get(metric)), dict)
    ]
    if not metrics:
        return None
    t0s = [m.get("t0") for m in metrics if m.get("t0")]
    return {
        "total": _sum_present([_num(m.get("total")) for m in metrics]),
        "stock": _sum_present([_num(m.get("stock")) for m in metrics]),
        "flowsSince": _sum_present([_num(m.get("flowsSince")) for m in metrics]),
        "flowCount": _sum_present([_num(m.get("flowCount")) for m in metrics]),
        "t0": min(t0s) if t0s else None,
    }


def _combine_field(buckets: list[dict], label: str) -> dict[str, Any] | None:
    """Combine one figure label (e.g. ``population_displaced``) across the buckets
    that carry it. Returns None when no bucket has a numeric value for it."""
    fields = [
        (f, _num(b.get("reportCount")) or 0.0)
        for b in buckets
        if isinstance((f := (b.get("data") or {}).get(label)), dict)
    ]
    if not fields:
        return None
    combined = {k: _sum_present([_num(f.get(k)) for f, _ in fields]) for k in _FIGURE_KEYS}
    if all(combined[k] is None for k in _FIGURE_KEYS):
        return None
    low, high = combined["value_low"], combined["value_high"]
    combined["range_width"] = (high - low) if (low is not None and high is not None) else None
    combined["bias"] = None            # not meaningful once summed across locations
    combined["quality_score"] = _weighted_mean(
        [(_num(f.get("quality_score")), w) for f, w in fields]
    )
    # `divergence` intentionally omitted — a single-bucket concept.
    return combined


def combine_aggregated_buckets(buckets: list[dict]) -> dict[str, Any] | None:
    """Sum already-de-nested per-location buckets into one aggregated dict, or
    None if there's nothing to combine. Shaped like a single bucket over the
    keys the situation generator reads."""
    buckets = [b for b in buckets if b]
    if not buckets:
        return None
    if len(buckets) == 1:
        return buckets[0]

    labels: list[str] = []
    for b in buckets:
        for label in (b.get("data") or {}):
            if label not in labels:
                labels.append(label)
    data = {label: field for label in labels if (field := _combine_field(buckets, label))}

    report_count = int(sum(int(_num(b.get("reportCount")) or 0) for b in buckets))
    contributing: list[str] = []
    seen: set[str] = set()
    for b in buckets:
        for rid in b.get("contributingReportIds") or []:
            if rid and rid not in seen:
                seen.add(rid)
                contributing.append(rid)

    newest = [b.get("newestSourceAt") for b in buckets if b.get("newestSourceAt")]
    oldest = [b.get("oldestSourceAt") for b in buckets if b.get("oldestSourceAt")]

    return {
        "data": data,
        "estimatedCurrentTotals": {
            "displacement": _combine_current_total(buckets, "displacement"),
            "returns": _combine_current_total(buckets, "returns"),
        },
        "reportCount": report_count,
        "contributingReportIds": contributing,
        "dataQualityScore": _weighted_mean(
            [(_num(b.get("dataQualityScore")), _num(b.get("reportCount")) or 0.0) for b in buckets]
        ),
        "newestSourceAt": max(newest) if newest else None,
        "oldestSourceAt": min(oldest) if oldest else None,
    }
