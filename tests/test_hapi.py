"""Unit tests for the HAPI blob builders (no network)."""

import pytest

from clear_context_pipeline.providers import hapi


def test_app_identifier_required_and_trimmed(monkeypatch):
    # No committed default: empty/whitespace must fail clearly, not send an
    # empty auth header. A real value is stripped.
    monkeypatch.setenv("HAPI_APP_IDENTIFIER", "")
    with pytest.raises(RuntimeError):
        hapi._app_identifier()
    monkeypatch.setenv("HAPI_APP_IDENTIFIER", "  token123  ")
    assert hapi._app_identifier() == "token123"


def test_build_blobs_groups_by_key_pcode_and_keeps_records():
    spec = hapi.EndpointSpec("hapi_humanitarian_needs", "affected-people/humanitarian-needs",
                             "ocha_hpc_hapi_v2", 2)
    rows = [
        {"admin2_code": "SD05002", "admin2_name": "Nyala Janoub", "admin1_code": "SD05",
         "sector_code": "PRO", "population": 148000,
         "reference_period_start": "2026-01-01", "reference_period_end": "2026-12-31"},
        {"admin2_code": "SD05002", "admin2_name": "Nyala Janoub", "admin1_code": "SD05",
         "sector_code": "WSH", "population": 90000,
         "reference_period_start": "2026-02-01", "reference_period_end": "2026-11-30"},
        {"admin1_code": "SD05", "sector_code": "PRO", "population": 5},  # no admin2 → dropped
    ]
    blobs = hapi.build_blobs(rows, spec)
    assert set(blobs) == {"SD05002"}
    b = blobs["SD05002"]
    assert b["source"] == "ocha_hpc_hapi_v2"
    assert b["admin_level"] == 2
    assert b["admin_pcode"] == "SD05002"
    assert b["admin_name"] == "Nyala Janoub"
    assert b["record_count"] == 2
    # Widest reference window across the two rows.
    assert b["reference_period_start"] == "2026-01-01"
    assert b["reference_period_end"] == "2026-12-31"
    # Admin-boilerplate columns are lifted out of each record.
    assert all("admin2_code" not in r for r in b["records"])
    assert {r["sector_code"] for r in b["records"]} == {"PRO", "WSH"}


def test_build_blobs_origin_keyed_refugees():
    # Refugees/returnees have no location_code — keyed on origin_location_code,
    # asylum is a per-record dimension.
    spec = hapi.HAPI_ENDPOINTS[0]  # hapi_refugees
    assert spec.type_ == "hapi_refugees"
    assert spec.location_filter == "origin_location_code"
    rows = [
        {"origin_location_code": "SDN", "origin_location_name": "Sudan",
         "asylum_location_code": "TCD", "asylum_location_name": "Chad",
         "population_group": "REF", "gender": "all", "age_range": "all",
         "min_age": None, "max_age": None, "population": 312450,
         "reference_period_start": "2026-01-01", "reference_period_end": "2026-06-30"},
        {"origin_location_code": "SDN", "origin_location_name": "Sudan",
         "asylum_location_code": "EGY", "asylum_location_name": "Egypt",
         "population_group": "REF", "gender": "all", "age_range": "all",
         "min_age": None, "max_age": None, "population": 100000,
         "reference_period_start": "2026-01-01", "reference_period_end": "2026-06-30"},
    ]
    blobs = hapi.build_blobs(rows, spec)
    assert set(blobs) == {"SDN"}  # one blob, keyed on origin
    b = blobs["SDN"]
    assert b["admin_pcode"] == "SDN"
    assert b["admin_name"] == "Sudan"
    # Two asylum series preserved; origin key stripped from records, asylum kept.
    assert {r["asylum_location_code"] for r in b["records"]} == {"TCD", "EGY"}
    assert all("origin_location_code" not in r for r in b["records"])


def test_build_blobs_per_level_captures_national_and_avoids_bleed():
    # HAPI returns admin0/1/2 rows in one response, each carrying its parent
    # pcodes. Building per level must route each row to its FINEST level only —
    # so the national row is captured (not discarded) and admin2 rows don't fold
    # into the national blob.
    spec = hapi.EndpointSpec(
        "hapi_humanitarian_needs", "affected-people/humanitarian-needs",
        "ocha_hpc_hapi_v2", 2, series_key=(),
    )
    rows = [
        {"location_code": "SDN", "location_name": "Sudan", "population": 100,
         "reference_period_end": "2026-12-31"},
        {"location_code": "SDN", "admin1_code": "SD05", "admin1_name": "South Darfur",
         "population": 50, "reference_period_end": "2026-12-31"},
        {"location_code": "SDN", "admin1_code": "SD05", "admin2_code": "SD05002",
         "admin2_name": "Nyala", "population": 20, "reference_period_end": "2026-12-31"},
    ]
    l0 = hapi.build_blobs(rows, spec, level=0)
    l1 = hapi.build_blobs(rows, spec, level=1)
    l2 = hapi.build_blobs(rows, spec, level=2)
    assert set(l0) == {"SDN"} and l0["SDN"]["admin_level"] == 0
    assert l0["SDN"]["record_count"] == 1  # only the national row, not all 3
    assert set(l1) == {"SD05"} and l1["SD05"]["admin_level"] == 1
    assert set(l2) == {"SD05002"} and l2["SD05002"]["admin_level"] == 2


def test_build_blobs_admin0_keying():
    spec = hapi.EndpointSpec("hapi_funding", "coordination-context/funding", "ocha_fts_hapi_v2", 0)
    rows = [{"location_code": "SDN", "location_name": "Sudan", "requirements_usd": 4.2e9,
             "reference_period_start": "2026-01-01", "reference_period_end": "2026-12-31"}]
    blobs = hapi.build_blobs(rows, spec)
    assert set(blobs) == {"SDN"}
    assert blobs["SDN"]["admin_level"] == 0
    assert blobs["SDN"]["records"][0]["requirements_usd"] == 4.2e9


def test_build_blobs_collapses_time_series_to_latest_per_series():
    spec = hapi.EndpointSpec(
        "hapi_food_prices", "food-security-nutrition-poverty/food-prices-market-monitor",
        "wfp_vam_hapi_v2", 2, series_key=("market_code", "commodity_code"),
    )
    rows = [
        {"admin2_code": "SD0601", "admin2_name": "Kassala", "market_code": "1489",
         "commodity_code": "401", "price": 380,
         "reference_period_start": "2026-05-01", "reference_period_end": "2026-05-31"},
        {"admin2_code": "SD0601", "admin2_name": "Kassala", "market_code": "1489",
         "commodity_code": "401", "price": 400,  # newer → wins for (1489, 401)
         "reference_period_start": "2026-06-01", "reference_period_end": "2026-06-30"},
        {"admin2_code": "SD0601", "admin2_name": "Kassala", "market_code": "1489",
         "commodity_code": "402", "price": 210,  # a different commodity series
         "reference_period_start": "2026-06-01", "reference_period_end": "2026-06-30"},
    ]
    recs = hapi.build_blobs(rows, spec)["SD0601"]["records"]
    assert len(recs) == 2  # two series, latest of each
    prices = {r["commodity_code"]: r["price"] for r in recs}
    assert prices["401"] == 400  # the May 380 was dropped
    # Collapsed records keep their own period (self-describing) …
    assert all("reference_period_end" in r for r in recs)
    # … but location columns are still stripped.
    assert all("admin2_code" not in r for r in recs)


def test_build_blobs_empty_series_key_keeps_single_latest():
    spec = hapi.EndpointSpec(
        "hapi_poverty_rate", "food-security-nutrition-poverty/poverty-rate",
        "ophi_mpi_hapi_v2", 1, series_key=(),
    )
    rows = [
        {"admin1_code": "SD01", "admin1_name": "Khartoum", "mpi": 0.15,
         "reference_period_start": "2014-01-01", "reference_period_end": "2014-12-31"},
        {"admin1_code": "SD01", "admin1_name": "Khartoum", "mpi": 0.12,  # latest
         "reference_period_start": "2020-01-01", "reference_period_end": "2020-12-31"},
    ]
    recs = hapi.build_blobs(rows, spec)["SD01"]["records"]
    assert len(recs) == 1
    assert recs[0]["mpi"] == 0.12


def test_operational_presence_rollup_dedupes_orgs_and_counts_types():
    rows = [
        {"admin2_code": "SD0101", "admin2_name": "Jebel Aulia", "sector_code": "PRO",
         "sector_name": "Protection", "org_acronym": "UNHCR", "org_name": "UN Refugee Agency",
         "org_type_description": "UN", "reference_period_start": "2026-06-01T00:00:00",
         "reference_period_end": "2026-06-30T00:00:00"},
        # Same org repeated in the same sector → counted once.
        {"admin2_code": "SD0101", "admin2_name": "Jebel Aulia", "sector_code": "PRO",
         "sector_name": "Protection", "org_acronym": "UNHCR", "org_name": "UN Refugee Agency",
         "org_type_description": "UN"},
        {"admin2_code": "SD0101", "admin2_name": "Jebel Aulia", "sector_code": "PRO",
         "sector_name": "Protection", "org_acronym": "IRC", "org_name": "Intl Rescue Committee",
         "org_type_description": "NGO"},
        {"admin2_code": "SD0101", "admin2_name": "Jebel Aulia", "sector_code": "WSH",
         "sector_name": "WASH", "org_acronym": "UNICEF", "org_name": "UNICEF",
         "org_type_description": "UN"},
    ]
    blobs = hapi.build_operational_presence_blobs(rows)
    assert set(blobs) == {"SD0101"}
    b = blobs["SD0101"]
    assert b["admin_level"] == 2
    assert b["active_sector_count"] == 2
    assert b["total_org_count"] == 3  # UNHCR, IRC, UNICEF (UNHCR not double-counted)
    assert b["as_of"] == "2026-06-30"  # date part only
    pro = next(s for s in b["sectors"] if s["code"] == "PRO")
    assert pro["org_count"] == 2
    assert pro["by_type"] == {"UN": 1, "NGO": 1}


def test_operational_presence_skips_rows_without_admin2():
    blobs = hapi.build_operational_presence_blobs([{"sector_code": "PRO", "org_acronym": "X"}])
    assert blobs == {}
