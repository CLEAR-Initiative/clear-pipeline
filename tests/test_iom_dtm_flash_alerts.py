"""Unit tests for IOM DTM Flash Alerts' external-id parsing (HTML-entity
handling) and the bronze/silver separation between the S3 lake blob and
Postgres's rawData. Plain dict fixtures, no network/DB — mirrors
test_idmc_split.py's style.
"""

import json
from unittest.mock import patch

import pytest

from clear_pipeline.defs.gx_pipeline.sources import IOMDTMFlashAlertsGXSource
from clear_pipeline.defs.signals.connectors import IOMDTMFlashAlertsConnector
from clear_pipeline.providers.iom_dtm_flash_alerts import (
    _extract_external_id_from_report_file,
    build_iom_dtm_flash_alert_signal_input,
    build_signal_content_update,
    parse_record,
)


def _raw(**overrides) -> dict:
    row = {
        "field_country1": "Sudan",
        "field_report_file": (
            "https://dtm.iom.int/dtm_download_track/100646"
            "?file=1&amp;type=node&amp;id=65946"
        ),
        "field_published_date": "2026-06-30T12:00:00",
        "title": "DTM Sudan Focused Flash Alert: Kordofan Region (6)",
        "field_summary": "An estimated 219,319 IDPs&nbsp;displaced from Kordofan.",
        "field_from": "2025-10-25T12:00:00",
        "field_to": "2026-06-23T12:00:00",
    }
    row.update(overrides)
    return row


# ─── _extract_external_id_from_report_file ──────────────────────────────────


def test_extract_external_id_does_not_decode_entities_itself():
    """Decoding is parse_record's job (it decodes the whole record before
    calling this) — this documents that calling it directly on undecoded
    "&amp;"-style input fails, so that responsibility never gets assumed here."""
    url = "https://dtm.iom.int/dtm_download_track/100646?file=1&amp;type=node&amp;id=65946"
    assert _extract_external_id_from_report_file(url) is None


def test_extract_external_id_plain_ampersand_still_works():
    url = "https://dtm.iom.int/dtm_download_track/100646?file=1&type=node&id=65946"
    assert _extract_external_id_from_report_file(url) == "iom_dtm_flash_alert:65946"


def test_extract_external_id_none_url_returns_none():
    assert _extract_external_id_from_report_file(None) is None


def test_extract_external_id_missing_id_param_returns_none():
    assert (
        _extract_external_id_from_report_file("https://dtm.iom.int/no-id-here") is None
    )


# ─── parse_record ────────────────────────────────────────────────────────────


def test_parse_record_attaches_external_id():
    parsed = parse_record(_raw())
    assert parsed["external_id"] == "iom_dtm_flash_alert:65946"


def test_parse_record_decodes_html_entities_in_top_level_fields():
    parsed = parse_record(_raw())
    assert "&nbsp;" not in parsed["field_summary"]
    assert "&amp;" not in parsed["field_report_file"]


def test_parse_record_keeps_raw_untouched():
    raw = _raw()
    parsed = parse_record(raw)
    assert parsed["raw"] == raw
    assert "&amp;" in parsed["raw"]["field_report_file"]


def test_parse_record_returns_none_when_id_unparseable():
    raw = _raw(field_report_file="https://dtm.iom.int/no-id-here")
    assert parse_record(raw) is None


# ─── bronze (S3) / silver (Postgres rawData) separation ─────────────────────


def test_raw_bytes_serializes_only_the_pristine_original():
    raw = _raw()
    parsed = parse_record(raw)
    blob = IOMDTMFlashAlertsGXSource().raw_bytes(parsed)
    assert json.loads(blob) == raw


def test_gx_parse_round_trips_the_bronze_blob():
    source = IOMDTMFlashAlertsGXSource()
    parsed = parse_record(_raw())
    assert source.parse(source.raw_bytes(parsed)) == parsed


def test_signal_input_raw_data_is_decoded_and_excludes_housekeeping_keys():
    parsed = parse_record(_raw())
    signal_input = build_iom_dtm_flash_alert_signal_input(parsed, "source-1", None)
    raw_data = signal_input["rawData"]
    assert "raw" not in raw_data
    assert "external_id" not in raw_data
    assert "content_hash" not in raw_data
    assert "&nbsp;" not in raw_data["field_summary"]


def test_project_rehydrated_from_pristine_blob_still_decodes():
    raw = _raw()
    parsed = parse_record(raw)
    connector = IOMDTMFlashAlertsConnector()
    blob = IOMDTMFlashAlertsGXSource().raw_bytes(parsed)
    rehydrated = connector.parse(blob)
    assert rehydrated == raw  # confirms the round-trip is truly pristine

    view = connector.project(rehydrated, {"publishedAt": None})
    assert view.external_id == "iom_dtm_flash_alert:65946"
    assert "&amp;" not in view.url
    assert "&nbsp;" not in (view.description or "")


def test_project_without_blob_is_an_anomaly():
    with pytest.raises(ValueError, match="no rawS3Key"):
        IOMDTMFlashAlertsConnector().project(None, {"id": "sig-1"})


# ─── structured fields ──────────────────────────────────────────────────────


def test_signal_input_carries_reporting_period_as_day_dates():
    signal_input = build_iom_dtm_flash_alert_signal_input(parse_record(_raw()), "source-1", None)
    assert signal_input["startDate"] == "2025-10-25"
    assert signal_input["startDateAccuracy"] == "DAY"
    assert signal_input["endDate"] == "2026-06-23"
    assert signal_input["endDateAccuracy"] == "DAY"


def test_signal_input_omits_a_missing_period_bound():
    signal_input = build_iom_dtm_flash_alert_signal_input(
        parse_record(_raw(field_from="")), "source-1", None
    )
    assert "startDate" not in signal_input
    assert "startDateAccuracy" not in signal_input
    assert signal_input["endDate"] == "2026-06-23"


def test_signal_input_country_is_location_and_countries():
    signal_input = build_iom_dtm_flash_alert_signal_input(parse_record(_raw()), "source-1", "loc-sdn")
    assert signal_input["locationId"] == "loc-sdn"
    assert signal_input["countryIds"] == ["loc-sdn"]


def test_signal_input_without_country_sends_neither():
    signal_input = build_iom_dtm_flash_alert_signal_input(parse_record(_raw()), "source-1", None)
    assert "locationId" not in signal_input
    assert "countryIds" not in signal_input


def test_content_hash_tracks_the_pristine_record():
    base = parse_record(_raw())["content_hash"]
    assert parse_record(_raw())["content_hash"] == base
    assert parse_record(_raw(field_summary="revised"))["content_hash"] != base


def test_content_update_mirrors_the_create_input():
    signal_input = build_iom_dtm_flash_alert_signal_input(parse_record(_raw()), "source-1", "loc-sdn")
    signal_input["rawS3Key"] = "raw/iom_dtm_flash_alerts/k.json"
    update = build_signal_content_update(signal_input, retracted=False)
    for key in (
        "sourceId", "externalId", "contentHash", "rawData", "startDate", "endDate",
        "countryIds", "locationId", "rawS3Key",
    ):
        assert update[key] == signal_input[key]
    assert update["retracted"] is False
    assert "publishedAt" not in update  # not an updateSignalContent field


def test_content_update_leaves_absent_fields_out():
    signal_input = build_iom_dtm_flash_alert_signal_input(
        parse_record(_raw(field_from="")), "source-1", None
    )
    update = build_signal_content_update(signal_input)
    assert "startDate" not in update
    assert "countryIds" not in update
    assert "retracted" not in update


def test_gx_source_resolves_country_once():
    source = IOMDTMFlashAlertsGXSource()
    with patch(
        "clear_pipeline.defs.gx_pipeline.sources.get_locations_by_level",
        return_value=[{"id": "loc-sdn", "name": "Sudan"}],
    ) as lookup:
        first = source.to_silver_input(parse_record(_raw()), "source-1")
        second = source.to_silver_input(parse_record(_raw()), "source-1")
    assert first["countryIds"] == second["countryIds"] == ["loc-sdn"]
    lookup.assert_called_once_with(0)
