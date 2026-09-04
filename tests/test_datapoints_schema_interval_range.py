"""Phase 1 (Capture) of the interval-and-range model (ADR-0007).

NumericField gains value_low/value_high (magnitude range), qualifier, measure_type
and an optional figure-level basis_period. `value` stays the headline point; the
range defaults to it for an exact figure so downstream is unaffected.
"""

from clear_pipeline.defs.knowledgebase.datapoints_schemas import (
    SCHEMA_VERSION,
    NumericField,
)


def _nf(**kw) -> NumericField:
    base = {"value": 800.0, "unit": "people", "confidence": "reported",
            "source_quote": "800 people were killed."}
    return NumericField(**{**base, **kw})


def test_schema_version_is_v4():
    assert SCHEMA_VERSION == "v4"


def test_exact_figure_defaults_range_to_point():
    f = _nf()
    assert f.value_low == 800.0 and f.value_high == 800.0
    assert f.qualifier == "exact"
    assert f.measure_type is None
    assert f.basis_period_start is None and f.basis_period_end is None


def test_stated_range_is_preserved():
    f = _nf(value=600.0, value_low=500.0, value_high=700.0, qualifier="approx")
    assert (f.value_low, f.value, f.value_high) == (500.0, 600.0, 700.0)
    assert f.qualifier == "approx"


def test_at_least_degenerate_opens_finite_band_not_a_point():
    # D: if the extractor emits only the floor for 'at least 500', the high must
    # NOT collapse to the point (that reads as 'exactly 500'). A modest finite
    # ceiling opens above the firm floor so the figure stays directionally honest.
    f = _nf(value=500.0, value_low=500.0, qualifier="at_least")
    assert f.value_low == 500.0            # the firm floor stays pinned
    assert f.value_high > 500.0            # a finite ceiling, not a degenerate point
    assert f.value_high == 500.0 * 1.15
    assert f.qualifier == "at_least"


def test_at_most_degenerate_opens_finite_floor_not_a_point():
    # D, mirror: only the ceiling given for 'up to 700' → a finite floor opens
    # below it rather than collapsing to [700, 700].
    f = _nf(value=700.0, value_high=700.0, qualifier="at_most")
    assert f.value_high == 700.0           # the firm ceiling stays pinned
    assert f.value_low < 700.0
    assert f.value_low == 700.0 * 0.85
    assert f.qualifier == "at_most"


def test_at_most_band_cannot_sit_above_its_ceiling():
    # E: 'up to 700' with a band that (after any swap) reaches above 700 is
    # contradictory — the ceiling is the firm bound, so value_high is pulled to
    # it and the band stays at or below the ceiling.
    f = _nf(value=700.0, value_low=600.0, value_high=900.0, qualifier="at_most")
    assert f.value_high == 700.0
    assert f.value_low == 600.0            # a legitimate floor below the ceiling is kept
    assert f.value_low <= f.value <= f.value_high


def test_at_least_carries_finite_ceiling_not_open():
    # "at least 500" → floor 500 + a PLAUSIBLE finite ceiling (never infinity).
    f = _nf(value=500.0, value_low=500.0, value_high=800.0, qualifier="at_least")
    assert f.value_low == 500.0 and f.value_high == 800.0
    assert f.qualifier == "at_least"


def test_at_most_carries_finite_floor_not_open():
    # "up to 700" → ceiling 700 + a plausible finite floor (never 0).
    f = _nf(value=700.0, value_low=500.0, value_high=700.0, qualifier="at_most")
    assert f.value_low == 500.0 and f.value_high == 700.0
    assert f.qualifier == "at_most"


def test_approx_carries_symmetric_band():
    # "around 600" → a modest symmetric band, not a degenerate point.
    f = _nf(value=600.0, value_low=570.0, value_high=630.0, qualifier="approx")
    assert f.value_low == 570.0 and f.value_high == 630.0
    assert f.qualifier == "approx"


def test_inverted_range_is_normalised():
    f = _nf(value=600.0, value_low=700.0, value_high=500.0)
    assert f.value_low == 500.0 and f.value_high == 700.0


def test_measure_type_and_basis_period_carry_through():
    f = _nf(
        measure_type="period_flow",
        basis_period_start="2026-04-02",
        basis_period_end="2026-04-10",
    )
    assert f.measure_type == "period_flow"
    assert f.basis_period_start == "2026-04-02"
    assert f.basis_period_end == "2026-04-10"


def test_value_stays_authoritative_headline():
    # clear-api still reads `value`; it must always be populated as the point.
    f = _nf(value=650.0, value_low=500.0, value_high=800.0)
    assert f.value == 650.0


# ── B: value is always inside [value_low, value_high] ────────────────────────

def test_point_above_band_widens_high_not_clamps_value():
    # "900" with a sloppy [500,700] band → keep the headline, widen the ceiling.
    f = _nf(value=900.0, value_low=500.0, value_high=700.0)
    assert f.value == 900.0
    assert f.value_low == 500.0 and f.value_high == 900.0


def test_point_below_band_widens_low_not_clamps_value():
    f = _nf(value=300.0, value_low=500.0, value_high=700.0)
    assert f.value == 300.0
    assert f.value_low == 300.0 and f.value_high == 700.0


def test_qualifier_blind_swap_no_longer_strands_the_point():
    # E's stranded-point case: at_most, value=700 (the ceiling), inverted
    # [900,700]. Inversion → [700,900]; then the at_most direction rule pins the
    # ceiling to 700 and opens a finite floor below it → [595,700], value at the
    # ceiling. The point is never left stranded outside its own band.
    f = _nf(value=700.0, value_low=900.0, value_high=700.0, qualifier="at_most")
    assert f.value == 700.0
    assert f.value_low <= f.value <= f.value_high
    assert f.value_low == 700.0 * 0.85 and f.value_high == 700.0


# ── F: out-of-enum qualifier/measure_type coerce, never raise ────────────────

def test_unknown_qualifier_coerces_to_exact_not_raises():
    f = _nf(qualifier="minimum")          # off-taxonomy synonym
    assert f.qualifier == "at_least"      # mapped, not nulled
    f2 = _nf(qualifier="totally-made-up")
    assert f2.qualifier == "exact"        # unknown → safe default


def test_qualifier_synonyms_map_to_canonical():
    assert _nf(qualifier="MORE_THAN").qualifier == "at_least"
    assert _nf(qualifier="up to").qualifier == "at_most"
    assert _nf(qualifier="approximately").qualifier == "approx"


def test_unknown_measure_type_coerces_to_none_not_raises():
    f = _nf(measure_type="flow")               # synonym
    assert f.measure_type == "period_flow"
    f2 = _nf(measure_type="not-a-measure")     # unknown → None (indeterminate)
    assert f2.measure_type is None


def test_measure_type_synonyms_map_to_canonical():
    assert _nf(measure_type="stock").measure_type == "stock_as_of"
    assert _nf(measure_type="cumulative").measure_type == "cumulative_to_date"
