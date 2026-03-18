"""Signal classification prompt: determine disaster type, relevance, severity."""

SYSTEM_PROMPT = """\
You are a humanitarian intelligence analyst for the CLEAR early warning system focused on Sudan.

You classify incoming signals (news alerts, social media reports, sensor data) into disaster types, assess their relevance to the Sudan humanitarian context, and rate severity.

You MUST respond with valid JSON only — no markdown, no explanation."""

USER_PROMPT_TEMPLATE = """\
Classify this signal:

Title: {title}
Description: {description}
Location: {location_name}
Source URL: {url}
Timestamp: {timestamp}

Additional context from raw data:
{raw_context}

Available disaster types (use glide_number codes):
{disaster_types_list}

Respond with this exact JSON structure:
{{
  "disaster_types": ["<glide_number>", ...],
  "relevance": <float 0.0-1.0>,
  "severity": <int 1-5>,
  "summary": "<one-line summary>"
}}

Rules:
- disaster_types: array of glide_number codes from the list above (can be multiple)
- relevance: how relevant this is to Sudan humanitarian monitoring (0.0 = irrelevant, 1.0 = critical)
- severity: 1=minimal, 2=low, 3=moderate, 4=high, 5=critical
- summary: concise one-line description of the event for analysts"""


def build_classify_prompt(
    title: str | None,
    description: str | None,
    location_name: str | None,
    url: str | None,
    timestamp: str,
    raw_context: str,
    disaster_types: list[dict],
) -> str:
    """Build the user prompt for signal classification."""
    dt_lines = "\n".join(
        f"  {dt['glideNumber']}: {dt['disasterType']}"
        + (f" ({dt['disasterClass']})" if dt.get("disasterClass") else "")
        for dt in disaster_types
    )

    return USER_PROMPT_TEMPLATE.format(
        title=title or "(no title)",
        description=description or "(no description)",
        location_name=location_name or "(unknown location)",
        url=url or "(no URL)",
        timestamp=timestamp,
        raw_context=raw_context[:2000],  # Cap raw context length
        disaster_types_list=dt_lines,
    )
