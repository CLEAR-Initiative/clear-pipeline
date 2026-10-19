"""Alert escalation — create a published alert for an alert-worthy event.

Alerts are decided at the EVENT level (not per-signal): the alert stage drains
``eventsPendingAlert`` (events at/above the severity threshold with no alert yet)
and escalates each one directly. There is no LLM assess gate — severity is the
gate, applied by the ``eventsPendingAlert`` query. A staleness check suppresses
alerts for backdated events so a replayed/backfilled event doesn't fan out emails.
"""

import logging
from datetime import UTC, datetime

from clear_pipeline.providers.clear_api import create_alert
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)


def is_stale_signal(published_at: str | None) -> bool:
    """True iff ``published_at`` is older than the configured staleness threshold.
    Unparseable or missing timestamps are NOT treated as stale — we'd rather fire
    a possibly-late alert than swallow it when the source omitted a timestamp.
    """
    max_age_hours = settings.alert_max_signal_age_hours
    if max_age_hours <= 0 or not published_at:
        return False
    try:
        ts = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False
    age_hours = (datetime.now(UTC) - ts).total_seconds() / 3600.0
    return age_hours > max_age_hours


def escalate_to_alert(event: dict, status: str = "published") -> dict | None:
    """Create a published alert for an event. ``createAlert`` is idempotent per
    eventId, so re-escalating an already-alerted event returns the existing alert.

    Returns ``None`` (not an error) when the event is NOT raised: clear-api's
    ``createAlert`` returns null when the event's types are all NRC-irrelevant.
    That is an expected skip — we LOG it and move on rather than dereferencing a
    null alert, which would otherwise surface as a GlitchTip error on a perfectly
    normal "this event isn't alert-worthy" outcome.
    """
    logger.info("[ALERT] Escalating event %s (status=%s)", event["id"], status)
    alert = create_alert({"eventId": event["id"], "status": status})
    if alert is None:
        logger.info(
            "[ALERT] Event %s not raised — its types are not relevant to NRC", event["id"]
        )
        return None
    logger.info("[ALERT] Created alert id=%s for event %s", alert["id"], event["id"])
    return alert
