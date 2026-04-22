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

Available disaster types — each code belongs to a level_1 category and a level_2 group:
{disaster_types_list}

Respond with this exact JSON structure:
{{
  "disaster_types": ["<code>", ...],
  "relevance": <float 0.0-1.0>,
  "severity": <int 1-5>,
  "summary": "<one-line summary>"
}}

Rules:
- disaster_types: array of codes from the list above. Pick the most specific level_3 code that applies.
- IMPORTANT: if you return multiple codes, they MUST all belong to the same level_1 category. A single signal describes one humanitarian situation (e.g. a conflict OR a flood, not both). If the event mixes level_1 categories, return only the dominant level_1's codes.
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
    # Group codes by level_1 → level_2 so the LLM sees the hierarchy clearly.
    # Falls back to flat listing if the new level1/level2 fields aren't set.
    by_l1: dict[str, dict[str, list[dict]]] = {}
    for dt in disaster_types:
        l1 = dt.get("level1") or dt.get("disasterClass") or "other"
        l2 = dt.get("level2") or dt.get("disasterType") or "other"
        by_l1.setdefault(l1, {}).setdefault(l2, []).append(dt)

    lines: list[str] = []
    for l1_name, groups in sorted(by_l1.items()):
        lines.append(f"\n[{l1_name}]")
        for l2_name, rows in sorted(groups.items()):
            for dt in rows:
                code = dt.get("glideNumber", "")
                label = dt.get("disasterType", "")
                lines.append(f"  {code}: {label}  (level_2: {l2_name})")
    dt_lines = "\n".join(lines)

    return USER_PROMPT_TEMPLATE.format(
        title=title or "(no title)",
        description=description or "(no description)",
        location_name=location_name or "(unknown location)",
        url=url or "(no URL)",
        timestamp=timestamp,
        raw_context=raw_context[:2000],  # Cap raw context length
        disaster_types_list=dt_lines,
    )
