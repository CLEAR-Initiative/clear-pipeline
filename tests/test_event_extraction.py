"""Unit tests for the unified event extractor (providers/event_extraction.py)
and the event-level aggregation it feeds (providers/event.py)."""

from __future__ import annotations

from clear_pipeline.providers import event_extraction as ee
from clear_pipeline.providers.event import _aggregate_signal_event_fields
from clear_pipeline.providers.event_extraction import (
    ExtractedEvent,
    extract_event,
    extracted_signal_fields,
)


def _event(**kw) -> ExtractedEvent:
    base = {
        "event_name": "Strike on al-Fashir",
        "event_type": "air_strike",
        "relevance": "RELEVANT",
        "severity": 4,
    }
    base.update(kw)
    return ExtractedEvent(**base)


class _FakeProvider:
    def __init__(self, event: ExtractedEvent) -> None:
        self._event = event

    def complete_structured(self, **_kw):
        return self._event


def _patch_llm(monkeypatch, event: ExtractedEvent) -> None:
    monkeypatch.setattr(ee, "make_llm_provider", lambda _role: _FakeProvider(event))


def test_extract_event_returns_the_event(monkeypatch):
    # The general-case prompt returns ONE object (no splitting).
    _patch_llm(monkeypatch, _event(event_name="al-Fashir strike"))
    got = extract_event(title="t", description="d")
    assert got is not None
    assert got.event_name == "al-Fashir strike"


def test_extract_event_none_on_failure(monkeypatch):
    class _Boom:
        def complete_structured(self, **_kw):
            raise RuntimeError("model down")

    monkeypatch.setattr(ee, "make_llm_provider", lambda _role: _Boom())
    # Best-effort: a model blip must never raise out of ingest.
    assert extract_event(title="t", description="d") is None


def test_extracted_signal_fields_maps_accuracy_and_resolves_countries(monkeypatch):
    monkeypatch.setattr(
        ee,
        "resolve_country_location_id",
        lambda name: {"Sudan": "loc-sd", "Chad": "loc-td"}.get(name),
    )
    ev = _event(
        start_date="2026-10-01",
        start_date_accuracy="Day",
        end_date="2026-10",
        end_date_accuracy="Month",
        event_magnitude=6.4,
        magnitude_unit="Mw",
        event_actors=["RSF", "SAF"],
        countries=["Sudan", "Chad", "Atlantis"],  # unresolved name is dropped
    )
    out = extracted_signal_fields(ev)
    assert out["severity"] == 4
    assert out["startDate"] == "2026-10-01"
    assert out["startDateAccuracy"] == "DAY"
    assert out["endDate"] == "2026-10"
    assert out["endDateAccuracy"] == "MONTH"
    assert out["magnitude"] == 6.4
    assert out["magnitudeUnit"] == "Mw"
    assert out["actors"] == ["RSF", "SAF"]
    assert out["countryIds"] == ["loc-sd", "loc-td"]


def test_extracted_signal_fields_omits_absent(monkeypatch):
    monkeypatch.setattr(ee, "resolve_country_location_id", lambda _name: None)
    out = extracted_signal_fields(_event(severity=None, countries=[]))
    assert out == {}


def test_aggregate_event_fields_rolls_up_members():
    members = [
        {
            "startDate": "2026-10-05",
            "startDateAccuracy": "DAY",
            "endDate": "2026-10-08",
            "endDateAccuracy": "DAY",
            "actors": ["RSF"],
            "countries": [{"id": "loc-sd"}],
        },
        {
            # Earlier start (its accuracy wins), later end (its accuracy wins).
            "startDate": "2026-10-01",
            "startDateAccuracy": "MONTH",
            "endDate": "2026-10-12",
            "endDateAccuracy": "WEEK",
            "magnitude": 5.9,
            "magnitudeUnit": "Mw",
            "actors": ["SAF", "RSF"],
            "countries": [{"id": "loc-td"}, {"id": "loc-sd"}],
        },
    ]
    out = _aggregate_signal_event_fields(members)
    assert out["startedAtAccuracy"] == "MONTH"  # from the earliest start
    assert out["endedAt"] == "2026-10-12"  # latest end
    assert out["endedAtAccuracy"] == "WEEK"
    assert out["magnitude"] == 5.9
    assert out["magnitudeUnit"] == "Mw"
    assert out["actors"] == ["RSF", "SAF"]  # union, first-seen order
    assert out["countryIds"] == ["loc-sd", "loc-td"]  # union, de-duplicated


def test_aggregate_event_fields_empty_when_members_lack_fields():
    # Additive (grouping): members with no extracted fields contribute nothing.
    assert _aggregate_signal_event_fields([{"severity": 3}, {}]) == {}


def test_aggregate_absolute_clears_unsupported_fields():
    # Recompute: unsupported fields are explicitly cleared so a retraction drops
    # facts no live member still carries.
    assert _aggregate_signal_event_fields([{"severity": 3}, {}], absolute=True) == {
        "startedAtAccuracy": None,
        "endedAt": None,
        "endedAtAccuracy": None,
        "magnitude": None,
        "magnitudeUnit": None,
        "actors": [],
        "countryIds": [],
    }
