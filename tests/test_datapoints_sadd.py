"""Sex/age disaggregation (SADD, ADR-0008) — schema + scope propagation.

Covers the v4 additions: the `Disaggregation` cells + `DisaggregatedNumericField`
carrying a `breakdown`, and the post-extraction step that propagates the parent
figure's resolved scope/source/basis-period into each cell (without which the
aggregator would drop the cells as unscoped).
"""

import json

from clear_context_pipeline.defs.knowledgebase.datapoints_extract import (
    _propagate_breakdown_scope,
)
from clear_context_pipeline.defs.knowledgebase.datapoints_schemas import (
    DisaggregatedNumericField,
    Disaggregation,
    Displacement,
    NumericField,
)


def _nf(value: float) -> NumericField:
    return NumericField(value=value, unit="people", confidence="reported", source_quote="q")


# ── schema ────────────────────────────────────────────────────────────────

class TestSchema:
    def test_disaggregated_field_carries_breakdown_cells(self):
        f = DisaggregatedNumericField(
            value=1000, unit="people", confidence="reported", source_quote="q",
            breakdown=Disaggregation(female=_nf(520), children_0_17=_nf(400)),
        )
        assert f.value == 1000
        assert f.breakdown.female.value == 520
        assert f.breakdown.children_0_17.value == 400
        # unstated cells stay null (not zero)
        assert f.breakdown.male is None
        assert f.breakdown.elderly_60plus is None

    def test_cells_get_the_full_interval_envelope(self):
        # A cell is a NumericField, so the range validator fills value_low/high.
        f = DisaggregatedNumericField(
            value=1000, unit="people", confidence="reported", source_quote="q",
            breakdown=Disaggregation(female=_nf(520)),
        )
        assert f.breakdown.female.value_low == 520
        assert f.breakdown.female.value_high == 520
        assert f.breakdown.female.qualifier == "exact"

    def test_breakdown_defaults_null(self):
        f = DisaggregatedNumericField(
            value=1, unit="people", confidence="reported", source_quote="q",
        )
        assert f.breakdown is None

    def test_breakdown_tolerates_stringified_json(self):
        # Claude tool_use sometimes returns a JSON-encoded string for a nested
        # object; the tolerate validator decodes it before Pydantic's type check.
        f = DisaggregatedNumericField(
            value=1, unit="people", confidence="reported", source_quote="q",
            breakdown=json.dumps(
                {"female": {"value": 5, "unit": "people", "confidence": "reported", "source_quote": "q"}},
            ),
        )
        assert f.breakdown.female.value == 5


# ── scope/source propagation ────────────────────────────────────────────────

class TestPropagation:
    def _merged_with_resolved_parent(self):
        idp = DisaggregatedNumericField(
            value=1000, unit="people", confidence="reported", source_quote="q",
            scope_location_name="Kassala",
            breakdown=Disaggregation(female=_nf(520), children_0_17=_nf(400)),
        )
        merged = {"displacement": {"idp_stock": idp.model_dump(mode="json")}}
        parent = merged["displacement"]["idp_stock"]
        # simulate the scope + source resolvers having run on the parent
        parent["scope_location_id"] = "loc-kassala"
        parent["source_id"] = "src-iom"
        parent["basis_period_start"] = "2026-01-01"
        return merged, parent

    def test_fills_non_null_cells_from_parent(self):
        merged, parent = self._merged_with_resolved_parent()
        n = _propagate_breakdown_scope(merged)
        assert n == 2  # female + children_0_17; the four null cells are skipped
        female = parent["breakdown"]["female"]
        assert female["scope_location_id"] == "loc-kassala"
        assert female["source_id"] == "src-iom"
        assert female["basis_period_start"] == "2026-01-01"
        assert parent["breakdown"]["male"] is None  # untouched

    def test_does_not_clobber_a_cell_that_already_has_scope(self):
        merged, parent = self._merged_with_resolved_parent()
        parent["breakdown"]["female"]["scope_location_id"] = "own-scope"
        _propagate_breakdown_scope(merged)
        assert parent["breakdown"]["female"]["scope_location_id"] == "own-scope"

    def test_noop_when_no_breakdown(self):
        merged = {
            "displacement": {
                "idp_stock": {
                    "value": 1, "unit": "people", "confidence": "reported",
                    "source_quote": "q", "scope_location_name": "P",
                    "scope_location_id": "loc",
                },
            },
        }
        assert _propagate_breakdown_scope(merged) == 0


# ── robustness: deep nesting must not null the domain ────────────────────────

class TestRobustness:
    """The breakdown adds a level where Claude's tool_use is most error-prone.
    Because complete_structured validates the WHOLE domain at once, a malformed
    nested cell must degrade gracefully (drop to null) rather than raise and null
    the entire displacement / needs_and_funding domain."""

    def _base(self):
        return {"value": 5, "unit": "people", "confidence": "reported", "source_quote": "q"}

    def test_stringified_cell_decodes(self):
        f = DisaggregatedNumericField(
            value=1, unit="people", confidence="reported", source_quote="q",
            breakdown={"female": json.dumps({**self._base(), "value": 7})},
        )
        assert f.breakdown.female.value == 7

    def test_malformed_cell_drops_to_none_keeps_the_rest(self):
        f = DisaggregatedNumericField(
            value=1000, unit="people", confidence="reported", source_quote="q",
            breakdown={"female": {**self._base(), "value": 520}, "male": "not-a-figure"},
        )
        assert f.breakdown.female.value == 520  # good cell survives
        assert f.breakdown.male is None          # bad cell isolated, not raised

    def test_non_dict_breakdown_drops_to_none_keeps_figure(self):
        f = DisaggregatedNumericField(
            value=1000, unit="people", confidence="reported", source_quote="q",
            breakdown=42,  # garbage
        )
        assert f.breakdown is None
        assert f.value == 1000  # the figure itself survives

    def test_bad_cell_does_not_null_the_whole_domain(self):
        # The blast-radius guard, at domain granularity: one malformed cell in
        # idp_stock's breakdown must not sink the entire Displacement domain.
        d = Displacement.model_validate({
            "idp_stock": {
                **self._base(), "value": 1000,
                "breakdown": {
                    "female": {**self._base(), "value": 520},
                    "male": "garbage",
                },
            },
            "new_displacements": {**self._base(), "value": 42},
        })
        assert d.idp_stock.value == 1000
        assert d.idp_stock.breakdown.female.value == 520
        assert d.idp_stock.breakdown.male is None
        assert d.new_displacements.value == 42  # sibling figure intact
