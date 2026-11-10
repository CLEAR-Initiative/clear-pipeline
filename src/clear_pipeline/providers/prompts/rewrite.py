"""Event title/description/severity/displacement rewrite prompt.

Used by the new district+type grouping algorithm. When a new signal is added
to an existing event (or a new event is formed), Claude polishes the event's
human-facing text and — as fallbacks — estimates severity and population
displaced from the signal text. The pipeline ignores these fallback values
when structured source data is available.

Claude no longer makes clustering decisions.
"""

import re

# Bump whenever the prompt text changes.
# v3: added `start_date` (event onset) — the LLM fallback for events.startedAt.
# v4: the title names the places the signals name (with their Arabic spelling),
#     not only the admin area the event was geocoded to.
REWRITE_PROMPT_VERSION = "rewrite-v4"

# The most places a title names before Location. An event with more is in
# many places: its title is the area alone.
TITLE_MAX_PLACES = 3

# The longest title kept with its Arabic spellings. The prompt asks for 80
# characters of English; the spellings in parentheses come on top of that.
TITLE_MAX_CHARS = 100

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
- title: human-readable, no emojis, no quotes. Lead with the event type, then
  the place. The last place in the title is the area on the Location line
  above. At most 80 characters, not counting Arabic spellings in parentheses.
    * The area on the Location line is where the whole event is (a district,
      a state or a country). Write its name as it is given. Do not leave it
      out, and do not write a wider area (the state, the country) in its
      place or after it. Never write the word "Location" in the title.
    * When the signals name a place more specific than that area (a town, a
      village, a neighbourhood, a camp, a facility), name that place, then
      the area: "Clashes in Tawila (طويلة), El Fasher" and "Battles in
      Mazbud, Aruri and Kamo, North Darfur".
    * Use Location alone when the signals name nothing more specific:
      "Armed clash in East Darfur".
    * With several signals, compare them before you write. The title is
      about the whole event: name the places of ALL the signals (three
      places at most), or name none and use Location alone. Never name the
      place of one signal and leave out the place of another. With one
      village in signal 1 and another in signal 2, write "Battles in Fata
      Borno and Kassab, Kutum" or "Battles in Kutum", never "Battles in
      Kassab, Kutum".
    * Name only places that are written in the signals. Never add a place,
      and never correct or complete a place name.
    * When the signals write a place in Arabic script, ALWAYS put that
      Arabic spelling in parentheses after its English spelling, copied
      letter for letter from the signal text, for each place you name:
      "Clashes in Al-Kanabi (الكنابي), Northern" and "Battles in Mazbud
      (مزبد) and Kamo (كامو), North Darfur". A reader checks your English
      spelling against it. Use no parentheses for a place the signals write
      only in Latin script, and none for Location.
    * If the signals name more than three places, or the places do not
      fit, use Location alone.
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
- start_date (YYYY-MM-DD or null):
    When the real-world event actually STARTED (its onset) — the EARLIEST date
    any signal indicates it began, NOT when the signal was published/collected.
    Resolve relative phrases ("yesterday", "three days ago") against that
    signal's date shown above. Use only what the text states or clearly implies;
    return null if no signal indicates a start date — do NOT guess or default to
    the publish date.

Respond with this exact JSON:
{{
  "title": "<short descriptive title>",
  "description": "<1-2 sentence summary>",
  "severity": <integer 1-5 or null>,
  "population_displaced": <integer or null>,
  "start_date": "<YYYY-MM-DD or null>"
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


# ── The title, checked in code after the model call ──────────────────────────

_ARABIC_LETTER = re.compile(r"[\u0621-\u064A]")
# A parenthetical that holds Arabic script: "(الكنابي)".
_ARABIC_PARENS = re.compile(r"\s*\(([^()]*[\u0621-\u064A][^()]*)\)")
# Marks that change the direction of the text around them. A title is stored
# as plain text; the page that shows it handles direction.
_BIDI_CONTROLS = re.compile(r"[\u061C\u200E\u200F\u202A-\u202E\u2066-\u2069]")
# Vowel marks and tatweel: a post may write them, a model may drop them.
_ARABIC_MARKS = re.compile(r"[\u064B-\u065F\u0670\u0640]")


def _plain_arabic(text: str) -> str:
    return _ARABIC_MARKS.sub("", text)


def _area_alone_when_too_many_places(title: str, location: str) -> str:
    """"Battles in A, B, C and D, Um Rawaba" → "Battles in Um Rawaba".

    The prompt asks for three places at most; a model lists more. Read only a
    title of the shape the prompt asks for: "<what> in <places>, <Location>".
    """
    match = re.match(
        rf"^(?P<what>.+)\s+in\s+(?P<places>.+?),\s*{re.escape(location)}\s*$", title, re.IGNORECASE,
    )
    if not match:
        return title
    places = re.split(r"\s*,\s*|\s+and\s+", _ARABIC_PARENS.sub("", match.group("places")))
    if len([p for p in places if p.strip()]) <= TITLE_MAX_PLACES:
        return title
    return f"{match.group('what')} in {location}"


def clean_event_title(title: str, signals: list[dict], location_name: str | None = None) -> str:
    """The model's title, with what must not be stored taken out:

    - an Arabic spelling in parentheses after `location_name` at the end of
      the title. Location is CLEAR's own name for the area, not a spelling to
      check. Only at the end, where the prompt puts Location: a town with the
      name of its district keeps its spelling;

    - the places, when the title names more than `TITLE_MAX_PLACES` before
      `location_name`: the event is in many places, the area alone says so;
    - an Arabic spelling in parentheses that occurs in no signal's text. The
      model copies a place's spelling from the post; one it made up, or
      "corrected", must not reach a reader as if the source had written it;
    - every Arabic spelling, when the title is longer than `TITLE_MAX_CHARS`
      with them: the English title is what a list has room for;
    - invisible direction marks.

    English place names are not checked here: a transliteration cannot be
    matched to the text by code. The prompt forbids added places, and the
    eval cases in `evals/event_title/` check it.
    """
    title = _BIDI_CONTROLS.sub("", title or "")
    text = _plain_arabic(" ".join(
        f"{s.get('title') or ''} {s.get('description') or ''}" for s in signals
    ))

    def keep_if_written(match: re.Match) -> str:
        spelling = _plain_arabic(match.group(1)).strip()
        return match.group(0) if spelling and spelling in text else ""

    # The prompt's own label, copied into the title: "…, Um Rawaba Location".
    # Removed first: the two checks below read the end of the title.
    title = re.sub(r"[\s,]+Location\s*$", "", title)
    if location_name and location_name.strip():
        after_location = re.compile(
            rf"(?<![\w-])({re.escape(location_name.strip())})\s*\([^()]*[\u0621-\u064A][^()]*\)\s*$",
            re.IGNORECASE,
        )
        title = after_location.sub(r"\1", title)
        title = _area_alone_when_too_many_places(title, location_name.strip())
    title = _ARABIC_PARENS.sub(keep_if_written, title)
    if len(title) > TITLE_MAX_CHARS:
        title = _ARABIC_PARENS.sub("", title)
    # Tidy what a removal leaves behind: "Al-Kanabi , Northern" → "Al-Kanabi, Northern".
    title = re.sub(r"\s+([,.;:])", r"\1", title)
    return re.sub(r"\s{2,}", " ", title).strip()
