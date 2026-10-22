"""Unit tests for alert relevance + severity LLM classification."""

from unittest.mock import MagicMock, patch

from clear_pipeline.providers.alert_severity import (
    AlertSeverityClassification,
    _payload,
    classify_alert_severity,
    llm_severity_or_none,
)


def test_payload_empty_when_fields_absent():
    assert _payload() == {
        "country": "",
        "event_name": "",
        "event_description": "",
        "additional_data": {},
    }


def test_payload_includes_only_present_casualty_fields():
    assert _payload(people_dead=12)["additional_data"] == {"people_dead": 12}
    assert _payload(people_injured=3)["additional_data"] == {"people_injured": 3}
    assert _payload(people_displaced=500)["additional_data"] == {"people_displaced": 500}
    assert _payload(people_affected=9000)["additional_data"] == {"people_affected": 9000}
    assert _payload(
        people_dead=1, people_injured=2, people_displaced=100, people_affected=50,
    )["additional_data"] == {
        "people_dead": 1,
        "people_injured": 2,
        "people_displaced": 100,
        "people_affected": 50,
    }


def test_classify_alert_severity_calls_structured_llm():
    fake = AlertSeverityClassification(relevance="RELEVANT", severity=4)
    llm = MagicMock()
    llm.complete_structured.return_value = fake

    with patch(
        "clear_pipeline.providers.alert_severity.make_llm_provider",
        return_value=llm,
    ) as make:
        result = classify_alert_severity(
            country="Sudan",
            event_name="Clashes in El Fasher",
            event_description="Shelling killed 12 civilians",
            people_dead=12,
        )

    make.assert_called_once_with("alert")
    assert result == fake
    kwargs = llm.complete_structured.call_args.kwargs
    assert kwargs["schema"] is AlertSeverityClassification
    assert '"country": "Sudan"' in kwargs["user"]
    assert '"people_dead": 12' in kwargs["user"]
    assert '"people_injured"' not in kwargs["user"]


def test_llm_severity_or_none_returns_severity():
    fake = AlertSeverityClassification(relevance="NOT_RELEVANT", severity=1)
    with patch(
        "clear_pipeline.providers.alert_severity.classify_alert_severity",
        return_value=fake,
    ):
        assert llm_severity_or_none(event_name="Debt talks") == 1


def test_llm_severity_or_none_swallows_failure():
    with patch(
        "clear_pipeline.providers.alert_severity.classify_alert_severity",
        side_effect=RuntimeError("boom"),
    ):
        assert llm_severity_or_none(event_name="x") is None
