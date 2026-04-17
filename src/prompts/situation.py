"""Situation narrative prompt: generate a coherent title + summary across events."""

SYSTEM_PROMPT = """\
You are a humanitarian intelligence analyst for the CLEAR early warning system.

You write concise, actionable narratives for humanitarian workers and NGOs
operating in crisis zones. Your summaries connect multiple events into a
single coherent situation so responders can act quickly.

You MUST respond with valid JSON only — no markdown, no explanation before or after."""


USER_PROMPT_TEMPLATE = """\
Generate a title and summary for a humanitarian situation linking the events below.

Events ({event_count}):
{events_block}

Locations affected: {locations}

Guidelines:
- Title: <=70 chars, human-readable, no emojis, no brackets/quotes. Lead with the
  dominant disaster type(s) and location (e.g. "Floods in North Darfur and Kassala").
- Summary: 2-3 sentences. Describe what is happening, where, scale (population
  affected if known), and the humanitarian implication (displacement, food security,
  health risk, etc.). Avoid generic filler.

Respond with this exact JSON structure:
{{
  "title": "<short descriptive title>",
  "summary": "<2-3 sentence narrative>"
}}
"""


def build_situation_prompt(events: list[dict], locations: list[str]) -> str:
    lines = []
    for i, e in enumerate(events, 1):
        title = e.get("title") or "(untitled)"
        desc = (e.get("description") or "").strip()
        types = ", ".join(e.get("types") or []) or "unknown"
        severity = e.get("severity") if e.get("severity") is not None else "?"
        pop = e.get("populationAffected")
        pop_str = f" pop_affected={pop}" if pop else ""
        lines.append(
            f"{i}. [{types}] severity={severity}{pop_str}\n"
            f"   title: {title}\n"
            f"   description: {desc[:300]}"
        )
    events_block = "\n".join(lines) if lines else "(no events)"
    locations_str = ", ".join(locations) if locations else "unknown"

    return USER_PROMPT_TEMPLATE.format(
        event_count=len(events),
        events_block=events_block,
        locations=locations_str,
    )
