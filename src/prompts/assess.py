"""Alert assessment prompt: decide if an event should become an alert."""

# Bump whenever the prompt text changes (see CLASSIFY_PROMPT_VERSION for rationale).
ASSESS_PROMPT_VERSION = "assess-v1"

SYSTEM_PROMPT = """\
You are a humanitarian intelligence analyst for the CLEAR early warning system.

You assess whether an event is severe enough to warrant an alert notification to humanitarian workers and NGOs in Sudan.

You MUST respond with valid JSON only — no markdown, no explanation."""

USER_PROMPT_TEMPLATE = """\
Assess whether this event warrants an alert:

Event Title: {title}
Event Description: {description}
Event Types: {types}
Location: {location_name}
Signal Count: {signal_count}
Max Severity: {max_severity}
Time Range: {valid_from} → {valid_to}

Signal summaries:
{signal_summaries}

Respond with this exact JSON structure:
{{
  "should_alert": <true or false>,
  "status": "draft",
  "reasoning": "<one-line explanation>"
}}

Rules:
- Alert if: mass casualties, large-scale displacement, imminent natural disaster, active conflict affecting civilians
- Do NOT alert for: minor incidents, unverified rumors, routine reports
- All alerts start as "draft" for human review
- Consider the number of corroborating signals (more signals = higher confidence)
- Consider severity level (4-5 = likely alert, 1-2 = unlikely)"""


def build_assess_prompt(
    title: str | None,
    description: str | None,
    types: list[str],
    location_name: str | None,
    signal_count: int,
    max_severity: int,
    valid_from: str,
    valid_to: str,
    signal_summaries: list[str],
) -> str:
    """Build the user prompt for alert assessment."""
    summaries_text = "\n".join(
        f"  - {s}" for s in signal_summaries
    ) if signal_summaries else "  (no summaries available)"

    return USER_PROMPT_TEMPLATE.format(
        title=title or "(untitled event)",
        description=description or "(no description)",
        types=", ".join(types) if types else "(none)",
        location_name=location_name or "(unknown)",
        signal_count=signal_count,
        max_severity=max_severity,
        valid_from=valid_from,
        valid_to=valid_to,
        signal_summaries=summaries_text,
    )
