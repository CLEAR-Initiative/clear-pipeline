"""Phase 1 (Capture) of the interval-and-range model (ADR-0007).

NumericField gains value_low/value_high (magnitude range), qualifier, measure_type
and an optional figure-level basis_period. `value` stays the headline point; the
range defaults to it for an exact figure so downstream is unaffected.
"""

from clear_context_pipeline.defs.knowledgebase.datapoints_schemas import (
    SCHEMA_VERSION,
    NumericField,
)


def _nf(**kw) -> NumericField:
    base = {"value": 800.0, "unit": "people", "confidence": "reported",
            "source_quote": "800 people were killed."}
    return NumericField(**{**base, **kw})


def test_schema_version_is_v3():
    assert SCHEMA_VERSION == "v3"


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


def test_only_low_bound_given_fills_high_from_point():
    # Fallback only: if the extractor emits just the floor, high defaults to the
    # point. The prompt now asks for a finite ceiling too (see tests below).
    f = _nf(value=500.0, value_low=500.0, qualifier="at_least")
    assert f.value_low == 500.0 and f.value_high == 500.0
    assert f.qualifier == "at_least"


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
