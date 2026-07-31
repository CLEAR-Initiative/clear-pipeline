"""Unit tests for the ported IOM DTM aggregation (no network)."""

from clear_context_pipeline.providers import iom_dtm


def _rec(pcode, round_no, ind, *, origin=None, origin_name=None, atype="BA", name="Dest"):
    r = {
        "admin2Pcode": pcode,
        "admin2Name": name,
        "roundNumber": round_no,
        "reportingDate": f"2026-0{round_no}-01",
        "numPresentIdpInd": ind,
        "assessmentType": atype,
        "operation": "Op",
    }
    if origin:
        r["idpOriginAdmin1Pcode"] = origin
        r["idpOriginAdmin1Name"] = origin_name
    return r


def test_extract_displacement_value_prefers_individuals():
    assert iom_dtm.extract_displacement_value({"numPresentIdpInd": "1200"}) == 1200
    assert iom_dtm.extract_displacement_value({"numPresentIdp": 5}) == 5
    assert iom_dtm.extract_displacement_value({"nope": 1}) is None
    assert iom_dtm.extract_displacement_value({"numPresentIdpInd": None, "numIdp": 7}) == 7


def test_aggregate_sums_across_origins_same_destination_round():
    # One destination, one round, two origin admin1s → summed to 30.
    records = [
        _rec("SD0101", 7, 20, origin="SD01", origin_name="Khartoum"),
        _rec("SD0101", 7, 10, origin="SD02", origin_name="Red Sea"),
    ]
    out = iom_dtm.aggregate_displacement_by_destination(records, admin_level=2, assessment_type_filter="BA")
    assert out["SD0101"]["population_displaced"] == 30
    # Origin breakdown sorted desc by count.
    ob = out["SD0101"]["origin_breakdown"]
    assert [o["origin_admin1_pcode"] for o in ob] == ["SD01", "SD02"]
    assert ob[0]["count"] == 20


def test_aggregate_latest_round_wins():
    records = [
        _rec("SD0101", 6, 999, origin="SD01"),
        _rec("SD0101", 8, 42, origin="SD01"),
    ]
    out = iom_dtm.aggregate_displacement_by_destination(records, admin_level=2, assessment_type_filter="BA")
    assert out["SD0101"]["round_number"] == 8
    assert out["SD0101"]["population_displaced"] == 42  # headline = latest round


def test_aggregate_keeps_recent_rounds_history():
    # The monthly DTM job must not lose intermediate rounds: every round's total
    # is retained in recent_rounds (newest first), with the latest as the head.
    records = [
        _rec("SD0101", 6, 999, origin="SD01"),
        _rec("SD0101", 8, 42, origin="SD01"),
        _rec("SD0101", 7, 300, origin="SD01"),
    ]
    out = iom_dtm.aggregate_displacement_by_destination(records, admin_level=2, assessment_type_filter="BA")
    rounds = out["SD0101"]["recent_rounds"]
    assert [r["round_number"] for r in rounds] == [8, 7, 6]  # newest first, none lost
    assert rounds[0]["population_displaced"] == 42
    assert {r["round_number"]: r["population_displaced"] for r in rounds} == {8: 42, 7: 300, 6: 999}


def test_aggregate_assessment_priority_fills_gaps_only():
    # BA covers SD0101; only FM covers SD0202. Priority [BA, FM] keeps BA's
    # SD0101 and fills SD0202 from FM — no double-count.
    records = [
        _rec("SD0101", 7, 100, origin="SD01", atype="BA"),
        _rec("SD0101", 7, 55, origin="SD01", atype="FM"),   # ignored — BA already filled it
        _rec("SD0202", 7, 30, origin="SD02", atype="FM"),
    ]
    out = iom_dtm.aggregate_displacement_by_destination(
        records, admin_level=2, assessment_type_filter=["BA", "FM"],
    )
    assert out["SD0101"]["population_displaced"] == 100
    assert out["SD0101"]["assessment_type"] == "BA"
    assert out["SD0202"]["population_displaced"] == 30
    assert out["SD0202"]["assessment_type"] == "FM"


def test_aggregate_skips_zero_and_missing_values():
    records = [_rec("SD0101", 7, 0, origin="SD01"), _rec("SD0303", 7, 5, origin="SD03")]
    out = iom_dtm.aggregate_displacement_by_destination(records, admin_level=2, assessment_type_filter=None)
    assert "SD0101" not in out
    assert out["SD0303"]["population_displaced"] == 5


def test_record_pcode_camel_and_pascal():
    assert iom_dtm.record_pcode({"admin1Pcode": "SD01"}, 1) == "SD01"
    assert iom_dtm.record_pcode({"Admin2Pcode": "SD0101"}, 2) == "SD0101"
    assert iom_dtm.record_pcode({}, 0) is None
