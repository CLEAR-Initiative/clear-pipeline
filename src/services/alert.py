"""Alert escalation service: assess events for alert-worthiness using Claude."""

import logging

from src.clients.claude import call_claude
from src.clients.graphql import create_alert
from src.config import settings
from src.models.clear import AlertAssessment
from src.prompts.assess import ASSESS_PROMPT_VERSION, SYSTEM_PROMPT, build_assess_prompt

logger = logging.getLogger(__name__)


def escalate_to_alert(event: dict, status: str = "published") -> dict:
    """Direct alert creation — no Claude gate. Used by v2 mode because the
    caller has already decided (via severity thresholds) that the event is
    alert-worthy, and we want to conserve Claude tokens.

    `createAlert` on the API is idempotent per eventId (Wave 1), so calling
    this on an already-alerted event just returns the existing alert.
    """
    logger.info("[ALERT] Escalating event %s directly (status=%s, v2 mode)", event["id"], status)
    alert = create_alert({"eventId": event["id"], "status": status})
    logger.info("[ALERT] Created alert id=%s for event %s", alert["id"], event["id"])
    return alert


def maybe_escalate(
    event: dict,
    signal_summaries: list[str],
    max_severity: int,
) -> dict | None:
    """Dispatcher: v2 skips the Claude gate (severity threshold alone), v1
    still uses `assess_and_escalate` for the Claude alert-worthiness check.
    """
    if settings.grouping_algo == "v2":
        return escalate_to_alert(event)
    return assess_and_escalate(
        event=event,
        signal_summaries=signal_summaries,
        max_severity=max_severity,
    )


def assess_and_escalate(
    event: dict,
    signal_summaries: list[str],
    max_severity: int,
) -> dict | None:
    """
    Use Claude to assess if an event warrants an alert.
    If yes, creates an alert via GraphQL (always as draft).

    Returns the created alert dict or None.
    """
    # Extract location name from event
    location_name = None
    for key in ("originLocation", "destinationLocation", "generalLocation"):
        loc = event.get(key)
        if loc and loc.get("name"):
            location_name = loc["name"]
            break

    prompt = build_assess_prompt(
        title=event.get("title"),
        description=event.get("description"),
        types=event.get("types", []),
        location_name=location_name,
        signal_count=len(signal_summaries),
        max_severity=max_severity,
        valid_from=event.get("validFrom", ""),
        valid_to=event.get("validTo", ""),
        signal_summaries=signal_summaries,
    )

    result_data = call_claude(
        SYSTEM_PROMPT,
        prompt,
        stage="assess",
        prompt_version=ASSESS_PROMPT_VERSION,
        event_id=event.get("id"),
    )
    assessment = AlertAssessment.model_validate(result_data)

    if not assessment.should_alert:
        logger.info("Event %s does not warrant an alert", event["id"])
        return None

    logger.info("Escalating event %s to alert (status=published)", event["id"])
    alert = create_alert({
        "eventId": event["id"],
        "status": "published",
    })
    logger.info("Created alert id=%s for event %s", alert["id"], event["id"])
    return alert
