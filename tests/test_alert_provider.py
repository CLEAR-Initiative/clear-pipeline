"""Unit tests for providers/alert.escalate_to_alert — in particular that an
NRC-irrelevant event (clear-api returns a null alert) is a logged skip, NOT an
exception. Dereferencing a null alert would surface as a GlitchTip error on a
perfectly normal "not alert-worthy" event, which is what we must avoid."""

from unittest.mock import patch

from clear_pipeline.providers.alert import escalate_to_alert


def test_returns_the_alert_when_one_is_created():
    with patch(
        "clear_pipeline.providers.alert.create_alert",
        return_value={"id": "a1", "status": "published"},
    ):
        assert escalate_to_alert({"id": "e1"}) == {"id": "a1", "status": "published"}


def test_null_alert_is_a_logged_skip_not_an_error():
    # clear-api's createAlert returns null when the event's types are all
    # NRC-irrelevant. escalate_to_alert must return None without raising (no
    # `alert["id"]` dereference) so the alert drain doesn't log an exception.
    with patch("clear_pipeline.providers.alert.create_alert", return_value=None):
        assert escalate_to_alert({"id": "e1"}) is None
