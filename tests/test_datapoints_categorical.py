"""Closed-vocab categorical disaggregation (ADR-0009) — schema + propagation.

Covers the housing pilot: `HousingCount` carrying `by_<axis>` maps
(`dict[<AxisEnum>, NumericField]`), the key coercer that snake-cases and folds
off-taxonomy keys to ``other`` (never dropping the cell), and the generalised
post-extraction step that propagates the parent figure's resolved scope/source
into every `by_*` cell — the same inheritance SADD cells get.
"""

import json

from clear_pipeline.defs.knowledgebase.datapoints_extract import (
    _propagate_breakdown_scope,
)
from clear_pipeline.defs.knowledgebase.datapoints_schemas import (
    AccessAndIncidents,
    AccessConstrainedField,
    Displacement,
    DisplacementNumericField,
    FamilySeparationField,
    HousingCount,
    HousingDamage,
    MovementFigure,
    NumericField,
    SectorNeeds,
    SiteCountFigure,
)


def _nf(value: float) -> NumericField:
    return NumericField(value=value, unit="dwellings", confidence="reported", source_quote="q")


def _housing_dict(value: float, by_dwelling_type: dict) -> dict:
    return {
        "value": value, "unit": "dwellings", "confidence": "reported",
        "source_quote": "q", "by_dwelling_type": by_dwelling_type,
    }


# ── schema ──────────────────────────────────────────────────────────────────

class TestSchema:
    def test_housing_count_carries_categorical_maps(self):
        h = HousingCount(
            value=250, unit="dwellings", confidence="reported", source_quote="q",
            by_dwelling_type={"house": _nf(200), "apartment": _nf(50)},
        )
        assert h.value == 250
        assert h.by_dwelling_type["house"].value == 200
        assert h.by_dwelling_type["apartment"].value == 50
        assert h.by_severity is None  # unstated axis stays null

    def test_cells_get_the_full_interval_envelope(self):
        # A cell is a NumericField, so the range validator fills value_low/high.
        h = HousingCount(
            value=250, unit="dwellings", confidence="reported", source_quote="q",
            by_dwelling_type={"house": _nf(200)},
        )
        cell = h.by_dwelling_type["house"]
        assert cell.value_low == 200 and cell.value_high == 200
        assert cell.qualifier == "exact"

    def test_off_taxonomy_key_folds_to_other_never_dropped(self):
        # "shanty" is not in DwellingType → folds to "other", keeping the count.
        h = HousingCount.model_validate(_housing_dict(250, {
            "House": {"value": 200, "unit": "dwellings", "confidence": "reported", "source_quote": "q"},
            "shanty": {"value": 50, "unit": "dwellings", "confidence": "reported", "source_quote": "q"},
        }))
        assert sorted(h.by_dwelling_type) == ["house", "other"]  # "House"→"house"
        assert h.by_dwelling_type["other"].value == 50

    def test_key_collision_keeps_first_cell(self):
        # Two source labels normalizing onto the same key: keep the first.
        h = HousingCount.model_validate(_housing_dict(90, {
            "hut": {"value": 60, "unit": "dwellings", "confidence": "reported", "source_quote": "first"},
            "shack": {"value": 30, "unit": "dwellings", "confidence": "reported", "source_quote": "second"},
        }))
        assert list(h.by_dwelling_type) == ["other"]
        assert h.by_dwelling_type["other"].value == 60  # first wins

    def test_maps_default_null(self):
        h = HousingCount(value=1, unit="dwellings", confidence="reported", source_quote="q")
        assert h.by_dwelling_type is None and h.by_severity is None

    def test_tolerates_stringified_json_map(self):
        h = HousingCount.model_validate(_housing_dict(
            5, json.dumps({"house": {"value": 5, "unit": "dwellings", "confidence": "reported", "source_quote": "q"}}),
        ))
        assert h.by_dwelling_type["house"].value == 5

    def test_housing_wired_into_access_and_incidents(self):
        ai = AccessAndIncidents.model_validate({
            "housing": {"destroyed": _housing_dict(250, {"house": {
                "value": 250, "unit": "dwellings", "confidence": "reported", "source_quote": "q"}})},
        })
        assert ai.housing.destroyed.by_dwelling_type["house"].value == 250
        assert isinstance(ai.housing, HousingDamage)


# ── scope/source propagation ────────────────────────────────────────────────

class TestPropagation:
    def _merged_with_resolved_parent(self):
        h = HousingCount(
            value=250, unit="dwellings", confidence="reported", source_quote="q",
            scope_location_name="Kassala",
            by_dwelling_type={"house": _nf(200), "other": _nf(50)},
        )
        merged = {"access_and_incidents": {"housing": {"destroyed": h.model_dump(mode="json")}}}
        parent = merged["access_and_incidents"]["housing"]["destroyed"]
        parent["scope_location_id"] = "loc-kassala"
        parent["source_id"] = "src-ocha"
        parent["basis_period_start"] = "2026-01-01"
        return merged, parent

    def test_fills_categorical_cells_from_parent(self):
        merged, parent = self._merged_with_resolved_parent()
        n = _propagate_breakdown_scope(merged)
        assert n == 2  # house + other
        house = parent["by_dwelling_type"]["house"]
        assert house["scope_location_id"] == "loc-kassala"
        assert house["source_id"] == "src-ocha"
        assert house["basis_period_start"] == "2026-01-01"
        assert house["scope_location_name"] == "Kassala"  # overwritten to parent

    def test_unresolved_parent_leaves_cell_unscoped(self):
        h = HousingCount(
            value=250, unit="dwellings", confidence="reported", source_quote="q",
            scope_location_name="Nowhere",  # never resolves → id stays null
            by_dwelling_type={"house": _nf(200)},
        )
        merged = {"access_and_incidents": {"housing": {"destroyed": h.model_dump(mode="json")}}}
        _propagate_breakdown_scope(merged)
        cell = merged["access_and_incidents"]["housing"]["destroyed"]["by_dwelling_type"]["house"]
        assert cell["scope_location_id"] is None  # dropped by aggregator, like its parent

    def test_noop_when_no_categorical_map(self):
        merged = {"access_and_incidents": {"housing": {"destroyed": {
            "value": 1, "unit": "dwellings", "confidence": "reported", "source_quote": "q",
            "scope_location_name": "P", "scope_location_id": "loc",
        }}}}
        assert _propagate_breakdown_scope(merged) == 0


# ── displacement axes (ADR-0009 §3: category-unit rows are breakdowns) ───────

class TestDisplacementAxes:
    def _dnf(self, value, **axes):
        return {
            "value": value, "unit": "people", "confidence": "reported",
            "source_quote": "q", "scope_location_name": "Kordofan", **axes,
        }

    def test_idp_stock_carries_accommodation_and_intention(self):
        # #8 accommodation + #10 intentions are by_<axis> maps on the IDP stock,
        # not standalone figures — the counts belong to the displacement total.
        d = Displacement.model_validate({"idp_stock": self._dnf(
            8000,
            by_accommodation_type={
                "collective_centre": {"value": 5000, "unit": "people", "confidence": "reported", "source_quote": "q"},
                "host family": {"value": 3000, "unit": "people", "confidence": "reported", "source_quote": "q"},
            },
            by_intention={"return": {"value": 6000, "unit": "people", "confidence": "reported", "source_quote": "q"}},
        )})
        assert isinstance(d.idp_stock, DisplacementNumericField)
        assert sorted(d.idp_stock.by_accommodation_type) == ["collective_centre", "host_family"]
        assert d.idp_stock.by_intention["return"].value == 6000

    def test_new_displacements_carries_cause_with_other_fold(self):
        d = Displacement.model_validate({"new_displacements": self._dnf(
            1000,
            by_cause={
                "conflict": {"value": 800, "unit": "people", "confidence": "reported", "source_quote": "q"},
                "flooding": {"value": 200, "unit": "people", "confidence": "reported", "source_quote": "q"},
            },
        )})
        assert sorted(d.new_displacements.by_cause) == ["conflict", "other"]  # flooding → other

    def test_new_count_figures_movement_and_sites(self):
        d = Displacement.model_validate({
            "movement": {"value": 500, "unit": "people", "confidence": "reported", "source_quote": "q",
                         "by_movement_type": {"evacuation": {"value": 500, "unit": "people", "confidence": "reported", "source_quote": "q"}}},
            "displacement_sites": {"value": 12, "unit": "locations", "confidence": "reported", "source_quote": "q",
                                   "by_site_type": {"camp": {"value": 12, "unit": "locations", "confidence": "reported", "source_quote": "q"}}},
        })
        assert isinstance(d.movement, MovementFigure)
        assert d.movement.by_movement_type["evacuation"].value == 500
        assert isinstance(d.displacement_sites, SiteCountFigure)
        assert d.displacement_sites.value == 12  # a count of LOCATIONS, not people

    def test_displacement_cells_inherit_parent_scope(self):
        d = Displacement.model_validate({"idp_stock": self._dnf(
            8000,
            by_accommodation_type={"collective_centre": {"value": 5000, "unit": "people", "confidence": "reported", "source_quote": "q"}},
        )})
        merged = {"displacement": {"idp_stock": d.idp_stock.model_dump(mode="json")}}
        merged["displacement"]["idp_stock"]["scope_location_id"] = "loc-kordofan"
        merged["displacement"]["idp_stock"]["source_id"] = "src-iom"
        n = _propagate_breakdown_scope(merged)
        assert n == 1
        cell = merged["displacement"]["idp_stock"]["by_accommodation_type"]["collective_centre"]
        assert cell["scope_location_id"] == "loc-kordofan"
        assert cell["source_id"] == "src-iom"


# ── access / protection / service group (ADR-0009) ──────────────────────────

class TestAccessProtectionService:
    def _nfp(self, value):
        return {"value": value, "unit": "people", "confidence": "reported", "source_quote": "q"}

    def test_access_constrained_carries_classification_and_barrier(self):
        f = AccessConstrainedField.model_validate({
            **self._nfp(50000), "scope_location_name": "Darfur",
            "by_access_classification": {"besieged": self._nfp(30000)},
            "by_barrier": {"checkpoints": self._nfp(20000), "some weird barrier": self._nfp(5000)},
        })
        assert f.by_access_classification["besieged"].value == 30000
        assert sorted(f.by_barrier) == ["checkpoints", "other"]  # weird → other

    def test_access_barriers_list_folds_and_dedupes(self):
        ai = AccessAndIncidents.model_validate({
            "access_barriers": ["Insecurity", "road damage", "insecurity", "made-up"],
        })
        # snake-cased, off-taxonomy→other, order-preserving de-dupe
        assert ai.access_barriers == ["insecurity", "road_damage", "other"]

    def test_family_separation_is_sadd_splittable(self):
        f = FamilySeparationField.model_validate({
            **self._nfp(300),
            "by_separation_category": {"unaccompanied": self._nfp(120)},
            "breakdown": {"children_0_17": self._nfp(300)},
        })
        assert f.by_separation_category["unaccompanied"].value == 120
        assert f.breakdown.children_0_17.value == 300  # both breakdown kinds coexist

    def test_response_gap_field(self):
        s = SectorNeeds.model_validate({"people_not_reached": self._nfp(8000)})
        assert s.people_not_reached.value == 8000

    def test_propagation_fills_access_constrained_cells(self):
        f = AccessConstrainedField.model_validate({
            **self._nfp(50000), "scope_location_name": "Darfur",
            "by_barrier": {"checkpoints": self._nfp(20000)},
        })
        merged = {"access_and_incidents": {"access_constrained_population": f.model_dump(mode="json")}}
        parent = merged["access_and_incidents"]["access_constrained_population"]
        parent["scope_location_id"] = "loc-darfur"
        parent["source_id"] = "src-ocha"
        _propagate_breakdown_scope(merged)
        cell = parent["by_barrier"]["checkpoints"]
        assert cell["scope_location_id"] == "loc-darfur"
        assert cell["source_id"] == "src-ocha"


# ── casualties status + facilities (ADR-0009 deferred items) ────────────────

class TestCasualtiesAndFacilities:
    def _nfp(self, value):
        return {"value": value, "unit": "people", "confidence": "reported", "source_quote": "q"}

    def test_killed_total_carries_status_with_other_fold(self):
        from clear_pipeline.defs.knowledgebase.datapoints_schemas import Casualties
        c = Casualties.model_validate({"killed": {"total": {
            **self._nfp(100),
            "by_casualty_status": {"confirmed": self._nfp(80), "rumoured": self._nfp(20)},
        }}})
        assert sorted(c.killed.total.by_casualty_status) == ["confirmed", "other"]  # rumoured→other

    def test_missing_total_carries_case_status(self):
        from clear_pipeline.defs.knowledgebase.datapoints_schemas import Casualties
        c = Casualties.model_validate({"missing": {"total": {
            **self._nfp(30), "by_case_status": {"active": self._nfp(30)},
        }}})
        assert c.missing.total.by_case_status["active"].value == 30

    def test_killed_status_cells_inherit_scope(self):
        from clear_pipeline.defs.knowledgebase.datapoints_schemas import KilledTotal
        t = KilledTotal.model_validate({
            **self._nfp(100), "scope_location_name": "Gaza",
            "by_casualty_status": {"confirmed": self._nfp(80)},
        })
        merged = {"casualties": {"killed": {"total": t.model_dump(mode="json")}}}
        merged["casualties"]["killed"]["total"]["scope_location_id"] = "loc-gaza"
        _propagate_breakdown_scope(merged)
        cell = merged["casualties"]["killed"]["total"]["by_casualty_status"]["confirmed"]
        assert cell["scope_location_id"] == "loc-gaza"

    def test_power_and_comms_facilities_close_the_gap(self):
        ai = AccessAndIncidents.model_validate({
            "power_facilities": {"destroyed": {"value": 3, "unit": "facilities", "confidence": "reported", "source_quote": "q"}},
            "communication_facilities": {"damaged": {"value": 2, "unit": "facilities", "confidence": "reported", "source_quote": "q"}},
        })
        assert ai.power_facilities.destroyed.value == 3
        assert ai.communication_facilities.damaged.value == 2
