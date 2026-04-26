"""Event grouping prompt: decide if a signal joins an existing event or creates a new one."""

# Bump whenever the prompt text changes (see CLASSIFY_PROMPT_VERSION for rationale).
GROUP_PROMPT_VERSION = "group-v1"

SYSTEM_PROMPT = """\
You are a humanitarian intelligence analyst for the CLEAR early warning system.

You decide whether a new signal belongs to an existing active event or warrants creating a new event. Events group related signals that describe the same real-world situation.

You MUST respond with valid JSON only — no markdown, no explanation."""

USER_PROMPT_TEMPLATE = """\
New signal to group:

Title: {title}
Description: {description}
Location: {location_name}
Types: {disaster_types}
Severity: {severity}
Summary: {summary}
Timestamp: {timestamp}

Active events (from the last 7 days):
{active_events_list}

Respond with this exact JSON structure:

If the signal belongs to an existing event:
{{
  "action": "add_to_existing",
  "event_id": "<id of the matching event>",
  "title": "<updated event title incorporating the new signal information>",
  "description": "<updated 2-3 sentence event description incorporating new information from this signal>",
  "population_affected": <number or null>
}}

If the signal represents a new situation:
{{
  "action": "create_new",
  "title": "<event title>",
  "description": "<2-3 sentence event description>",
  "types": ["<disaster type strings>"],
  "population_affected": <number or null>
}}

Rules:
- Every signal MUST be assigned to an event — either existing or new
- Group signals about the SAME real-world incident/situation
- Signals about the same conflict, same flood, same displacement wave = same event
- Signals about different incidents even if same type = different events
- Consider geographic proximity and temporal proximity (within the last 7 days)
- When adding to an existing event, update the title and description to reflect the latest developments from the new signal
- If no active events exist or none match, always create_new
- Prefer adding to an existing event over creating a new one if the situation is the same
- Extract population_affected from the signal text if mentioned (e.g., "9,000 IDPs", "12,000 displaced", "8000 people affected"). Use the highest credible number. Set to null if no population data is mentioned"""


def build_group_prompt(
    title: str | None,
    description: str | None,
    location_name: str | None,
    disaster_types: list[str],
    severity: int,
    summary: str,
    timestamp: str | None,
    active_events: list[dict],
) -> str:
    """Build the user prompt for event grouping."""
    if active_events:
        events_lines = "\n".join(
            f"  - ID: {e['id']}\n"
            f"    Title: {e.get('title', '(untitled)')}\n"
            f"    Description: {e.get('description', '(none)')}\n"
            f"    Types: {e.get('types', [])}\n"
            f"    Location: {_event_location_name(e)}\n"
            f"    Severity: {e.get('severity', 'unknown')}\n"
            f"    Valid: {e.get('validFrom', '?')} → {e.get('validTo', '?')}\n"
            f"    Has alert: {'yes' if e.get('alerts') else 'no'}"
            for e in active_events
        )
    else:
        events_lines = "  (no active events)"

    return USER_PROMPT_TEMPLATE.format(
        title=title or "(no title)",
        description=description or "(no description)",
        location_name=location_name or "(unknown)",
        disaster_types=", ".join(disaster_types) if disaster_types else "(none)",
        severity=severity,
        summary=summary,
        timestamp=timestamp or "(unknown)",
        active_events_list=events_lines,
    )


def _event_location_name(event: dict) -> str:
    """Extract a readable location name from an event dict."""
    for key in ("originLocation", "destinationLocation", "generalLocation"):
        loc = event.get(key)
        if loc and loc.get("name"):
            return loc["name"]
    return "(unknown)"
