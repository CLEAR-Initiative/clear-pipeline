"""Alert escalation service: assess events for alert-worthiness using Claude."""

import logging

from src.clients.claude import call_claude
from src.clients.graphql import create_alert
from src.models.clear import AlertAssessment
from src.prompts.assess import SYSTEM_PROMPT, build_assess_prompt

logger = logging.getLogger(__name__)


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

    result_data = call_claude(SYSTEM_PROMPT, prompt)
    assessment = AlertAssessment.model_validate(result_data)

    if not assessment.should_alert:
        logger.info("Event %s does not warrant an alert", event["id"])
        return None

    logger.info("Escalating event %s to alert (status=draft)", event["id"])
    alert = create_alert({
        "eventId": event["id"],
        "status": "draft",  # Always draft for human review in v1
    })
    logger.info("Created alert id=%s for event %s", alert["id"], event["id"])
    return alert
