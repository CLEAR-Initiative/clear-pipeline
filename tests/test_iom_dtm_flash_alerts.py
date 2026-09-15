"""Unit tests for IOM DTM Flash Alerts' external-id parsing (HTML-entity
handling) and the bronze/silver separation between the S3 lake blob and
Postgres's rawData. Plain dict fixtures, no network/DB — mirrors
test_idmc_split.py's style.
"""

import json

from clear_pipeline.defs.signals.connectors import DTMFlashAlertConnector
from clear_pipeline.providers.iom_dtm_flash_alerts import (
    _extract_external_id_from_report_file,
    build_iom_dtm_flash_alert_signal_input,
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
    assert _extract_external_id_from_report_file(url) == "iomdtm:65946"


def test_extract_external_id_none_url_returns_none():
    assert _extract_external_id_from_report_file(None) is None


def test_extract_external_id_missing_id_param_returns_none():
    assert (
        _extract_external_id_from_report_file("https://dtm.iom.int/no-id-here") is None
    )


# ─── parse_record ────────────────────────────────────────────────────────────


def test_parse_record_attaches_external_id():
    parsed = parse_record(_raw())
    assert parsed["external_id"] == "iomdtm:65946"


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
    blob = DTMFlashAlertConnector().raw_bytes(parsed)
    assert json.loads(blob) == raw


def test_signal_input_raw_data_is_decoded_and_excludes_housekeeping_keys():
    parsed = parse_record(_raw())
    signal_input = build_iom_dtm_flash_alert_signal_input(parsed, "source-1", None)
    raw_data = signal_input["rawData"]
    assert "raw" not in raw_data
    assert "external_id" not in raw_data
    assert "&nbsp;" not in raw_data["field_summary"]


def test_project_rehydrated_from_pristine_blob_still_decodes():
    raw = _raw()
    parsed = parse_record(raw)
    connector = DTMFlashAlertConnector()
    blob = connector.raw_bytes(parsed)
    rehydrated = connector.parse(blob)
    assert rehydrated == raw  # confirms the round-trip is truly pristine

    view = connector.project(rehydrated, {"publishedAt": None})
    assert view.external_id == "iomdtm:65946"
    assert "&amp;" not in view.url
    assert "&nbsp;" not in (view.description or "")
