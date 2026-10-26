"""INSO report parser (seam 2): ``inso.parse(raw_bytes)`` against a hand-written
golden ``*.report.json`` per fixture, compared field by field so a parsing
regression names the field that broke. Fixtures: tests/golden/inso/."""

import json
from dataclasses import asdict
from pathlib import Path

import pytest

from clear_pipeline.providers import inso
from clear_pipeline.providers.inso import InsoParseError

GOLDEN = Path(__file__).parent / "golden" / "inso"
REPORT_FIXTURES = sorted(p.name.removesuffix(".report.json") for p in GOLDEN.glob("*.report.json"))


def _eml(name: str) -> bytes:
    return (GOLDEN / f"{name}.eml").read_bytes()


def _replace(raw: bytes, old: str, new: str) -> bytes:
    """Edit the decoded body of every text part, re-encoded: the fixtures are base64."""
    from email import policy
    from email.parser import BytesParser

    msg = BytesParser(policy=policy.default).parsebytes(raw)
    for part in msg.walk():
        if part.get_content_maintype() == "text":
            part.set_content(part.get_content().replace(old, new), subtype=part.get_content_subtype(), cte="base64")
    return msg.as_bytes()


@pytest.mark.parametrize("name", REPORT_FIXTURES)
@pytest.mark.parametrize("field", list(json.loads((GOLDEN / "01_multi_incident.report.json").read_text())))
def test_parse_matches_the_golden_report(name, field):
    expected = json.loads((GOLDEN / f"{name}.report.json").read_text())
    assert asdict(inso.parse(_eml(name)))[field] == expected[field]


def test_every_golden_report_has_the_same_fields():
    fields = {name: set(json.loads((GOLDEN / f"{name}.report.json").read_text())) for name in REPORT_FIXTURES}
    assert len(set(map(frozenset, fields.values()))) == 1
    assert set(fields[REPORT_FIXTURES[0]]) == {f for f in asdict(inso.parse(_eml(REPORT_FIXTURES[0])))}


def test_html_and_plain_parts_parse_alike():
    """The HTML part wins when both exist; the plain part alone gives the same report."""
    from email import policy
    from email.parser import BytesParser

    msg = BytesParser(policy=policy.default).parsebytes(_eml("01_multi_incident"))
    plain_only = msg.get_body(("plain",))
    for header in ("From", "Subject", "Date", "Message-ID"):
        plain_only[header] = msg[header]
    assert inso.parse(plain_only.as_bytes()) == inso.parse(_eml("01_multi_incident"))


def test_missing_serial_raises():
    with pytest.raises(InsoParseError, match="SERIAL"):
        inso.parse(_eml("05_missing_serial"))


def test_missing_label_raises():
    with pytest.raises(InsoParseError, match="ANALYSIS"):
        inso.parse(_replace(_eml("02_single_incident"), "ANALYSIS", "COMMENTARY"))


def test_report_datetime_is_day_month_year_only():
    # 10/14/2026 is a month/day order: month 14 doesn't exist.
    with pytest.raises(InsoParseError, match="DATE & TIME"):
        inso.parse(_replace(_eml("02_single_incident"), "14/10/2026 16:05", "10/14/2026 16:05"))


def test_missing_date_header_raises():
    raw = _eml("02_single_incident").replace(b"Date: Wed, 14 Oct 2026 16:05:00 +0000\n", b"")
    with pytest.raises(InsoParseError, match="Date"):
        inso.parse(raw)


def test_unknown_charset_raises():
    raw = _eml("02_single_incident").replace(b'charset="utf-8"', b'charset="x-bogus"')
    with pytest.raises(InsoParseError, match="x-bogus"):
        inso.parse(raw)


def test_serial_is_kept_verbatim():
    raw = _replace(_eml("02_single_incident"), "SDN/Khartoum/IR/20261410/R1", "sdn / khartoum-IR 7")
    assert inso.parse(raw).serial == "sdn / khartoum-IR 7"
