"""Event title/description/severity/displacement rewrite prompt.

Used by the new district+type grouping algorithm. When a new signal is added
to an existing event (or a new event is formed), Claude polishes the event's
human-facing text and — as fallbacks — estimates severity and population
displaced from the signal text. The pipeline ignores these fallback values
when structured source data is available.

Claude no longer makes clustering decisions.
"""

# Bump whenever the prompt text changes.
REWRITE_PROMPT_VERSION = "rewrite-v2"

SYSTEM_PROMPT = """\
You are a humanitarian intelligence analyst for the CLEAR early warning system.

Given a set of signals (news / reports / posts) that have ALREADY been grouped
into a single event, produce a concise title + description and — when the text
supports it — extract severity and population displaced.

You MUST respond with valid JSON only — no markdown, no explanation."""


USER_PROMPT_TEMPLATE = """\
Analyse this event.

Location: {location_name}
Event type: {level_2_type}
Signal count: {signal_count}

Signals:
{signals_block}

Guidelines:
- title: <=80 chars, human-readable, no emojis, no quotes/brackets. Lead with
  the event type and location (e.g. "Armed clash in East Darfur").
- description: 1-2 sentences. What happened, where, scale if known, key
  humanitarian implication.
- severity (integer 1-5 or null):
    1=minimal, 2=low, 3=moderate, 4=high, 5=critical.
    Base on the WHOLE event (all signals), not the worst single word. Return
    null only if you truly can't judge.
- population_displaced (integer or null):
    The maximum number of people FORCED TO FLEE / displaced mentioned across
    any of the signals. Include refugees, IDPs, evacuees. Do NOT count deaths,
    injuries, or "people affected" — those are different. Return null if no
    signal mentions a displacement count.

Respond with this exact JSON:
{{
  "title": "<short descriptive title>",
  "description": "<1-2 sentence summary>",
  "severity": <integer 1-5 or null>,
  "population_displaced": <integer or null>
}}
"""


def build_rewrite_prompt(
    location_name: str | None,
    level_2_type: str | None,
    signals: list[dict],
) -> str:
    lines: list[str] = []
    for i, s in enumerate(signals, 1):
        title = (s.get("title") or "").strip() or "(untitled)"
        desc = (s.get("description") or "").strip()
        src = (s.get("source") or {}).get("name") or ""
        published = s.get("publishedAt") or ""
        header = f"{i}. [{src}] {published}" if src else f"{i}. {published}"
        lines.append(f"{header}\n   title: {title[:200]}")
        if desc:
            lines.append(f"   desc:  {desc[:300]}")
    signals_block = "\n".join(lines) if lines else "(no signals)"

    return USER_PROMPT_TEMPLATE.format(
        location_name=location_name or "(unknown location)",
        level_2_type=level_2_type or "(unknown type)",
        signal_count=len(signals),
        signals_block=signals_block,
    )
