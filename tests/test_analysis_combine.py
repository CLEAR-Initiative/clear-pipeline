"""Unit tests for the multi-location datapoint roll-up (ADR-0007, decision #2):
subtree de-nesting + summing per-location aggregated buckets. Pure logic."""

from clear_pipeline.defs.analysis.combine import (
    combine_aggregated_buckets,
    dedupe_nested_locations,
)


class TestDedupeNestedLocations:
    # sudan(a0) → khartoum(a1) → omdurman(a2); darfur(a1) is a sibling of khartoum.
    PARENTS = {
        "sudan": None,
        "khartoum": "sudan",
        "omdurman": "khartoum",
        "darfur": "sudan",
    }

    def test_drops_descendant_of_listed_ancestor(self):
        # khartoum is inside sudan → its subtree bucket is already in sudan's.
        assert dedupe_nested_locations(["sudan", "khartoum"], self.PARENTS) == ["sudan"]

    def test_drops_deep_descendant(self):
        assert dedupe_nested_locations(["sudan", "omdurman"], self.PARENTS) == ["sudan"]

    def test_keeps_siblings(self):
        assert dedupe_nested_locations(["khartoum", "darfur"], self.PARENTS) == ["khartoum", "darfur"]

    def test_preserves_order_and_keeps_unknown_as_root(self):
        assert dedupe_nested_locations(["darfur", "mystery"], self.PARENTS) == ["darfur", "mystery"]

    def test_cycle_terminates_safely(self):
        # A real location hierarchy has no cycles; the guarantee here is that a
        # malformed one terminates rather than looping forever. In a mutual cycle
        # each sees the other as an ancestor, so both drop out (→ KB-only), which
        # is a safe degradation.
        parents = {"a": "b", "b": "a"}
        assert dedupe_nested_locations(["a", "b"], parents) == []


def _bucket(**kw):
    """A minimal aggregated bucket; only the combined keys matter."""
    return {
        "data": kw.get("data", {}),
        "estimatedCurrentTotals": kw.get("current", {}),
        "reportCount": kw.get("report_count", 0),
        "contributingReportIds": kw.get("ids", []),
        "dataQualityScore": kw.get("quality"),
        "newestSourceAt": kw.get("newest"),
        "oldestSourceAt": kw.get("oldest"),
    }


class TestCombineBuckets:
    def test_empty_is_none(self):
        assert combine_aggregated_buckets([]) is None
        assert combine_aggregated_buckets([None]) is None

    def test_single_bucket_returned_unchanged(self):
        b = _bucket(report_count=3, data={"x": {"value": 1}})
        assert combine_aggregated_buckets([b]) is b

    def test_sums_point_figures_and_rederives_range_width(self):
        a = _bucket(
            report_count=2, ids=["r1", "r2"],
            data={"population_displaced": {
                "value": 100, "value_low": 90, "value_high": 110,
                "bias": "underreport", "quality_score": 8,
                "divergence": {"reportValue": 1, "apiValue": 2, "pctDiff": 50},
            }},
        )
        b = _bucket(
            report_count=1, ids=["r2", "r3"],
            data={"population_displaced": {
                "value": 40, "value_low": 30, "value_high": 55,
                "bias": "overreport", "quality_score": 5,
            }},
        )
        out = combine_aggregated_buckets([a, b])
        fig = out["data"]["population_displaced"]
        assert fig["value"] == 140
        assert fig["value_low"] == 120
        assert fig["value_high"] == 165
        assert fig["range_width"] == 45          # high - low, re-derived
        assert fig["bias"] is None               # not combinable
        assert "divergence" not in fig           # single-bucket concept, dropped
        # report-count-weighted mean: (8*2 + 5*1) / 3 = 7.0
        assert fig["quality_score"] == 7.0

    def test_field_present_in_only_one_bucket(self):
        a = _bucket(report_count=1, data={"returnees": {"value": 10}})
        b = _bucket(report_count=1, data={"population_in_need": {"value": 5}})
        out = combine_aggregated_buckets([a, b])
        assert out["data"]["returnees"]["value"] == 10
        assert out["data"]["population_in_need"]["value"] == 5

    def test_envelope_counts_ids_and_dates(self):
        a = _bucket(report_count=2, ids=["r1", "r2"], newest="2026-03-01", oldest="2026-01-01")
        b = _bucket(report_count=3, ids=["r2", "r3"], newest="2026-04-01", oldest="2026-02-01")
        out = combine_aggregated_buckets([a, b])
        assert out["reportCount"] == 5
        assert out["contributingReportIds"] == ["r1", "r2", "r3"]   # unioned, ordered
        assert out["newestSourceAt"] == "2026-04-01"                # max
        assert out["oldestSourceAt"] == "2026-01-01"                # min

    def test_current_totals_summed_with_earliest_t0(self):
        a = _bucket(current={"displacement": {"total": 100, "stock": 80, "flowsSince": 20, "flowCount": 2, "t0": "2026-02-01"}})
        b = _bucket(current={"displacement": {"total": 50, "stock": 40, "flowsSince": 10, "flowCount": 1, "t0": "2026-01-01"}})
        out = combine_aggregated_buckets([a, b])
        disp = out["estimatedCurrentTotals"]["displacement"]
        assert disp["total"] == 150 and disp["stock"] == 120
        assert disp["flowsSince"] == 30 and disp["flowCount"] == 3
        assert disp["t0"] == "2026-01-01"                          # earliest anchor
        assert out["estimatedCurrentTotals"]["returns"] is None    # no bucket had it

    def test_output_has_exactly_the_consumed_keys(self):
        # Guards the contract with generate._build_datapoints / the prompt formatters.
        out = combine_aggregated_buckets([_bucket(report_count=1), _bucket(report_count=1)])
        assert set(out) == {
            "data", "estimatedCurrentTotals", "reportCount",
            "contributingReportIds", "dataQualityScore", "newestSourceAt", "oldestSourceAt",
        }
