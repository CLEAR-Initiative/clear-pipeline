"""Signal creation: map Dataminr payload → CLEAR signal and persist via GraphQL."""

import calendar
import logging
import re
from datetime import UTC, datetime, timedelta

from clear_pipeline.providers.clear_api import (
    create_signal,
    find_or_create_landmark_l4,
)
from clear_pipeline.providers.dataminr import DataminrSignal
from clear_pipeline.providers.geoparser import (
    GeoparseResult,
    country_from_coords,
    extract_top_candidate,
    geoparse_signal,
)
from clear_pipeline.providers.location import resolve_signal_location

logger = logging.getLogger(__name__)

# Map Dataminr alertType.name to severity 1-5
DATAMINR_SEVERITY_MAP: dict[str, int] = {
    "flash": 5,
    "urgent": 4,
    "alert": 3,
    "watch": 2,
}


def _estimate_severity_from_dataminr(signal: DataminrSignal) -> int | None:
    """Extract severity from Dataminr alertType, or return None if absent."""
    if signal.alertType and signal.alertType.name:
        name = signal.alertType.name.lower().strip()
        return DATAMINR_SEVERITY_MAP.get(name)
    return None


# ─── Casualty extraction ─────────────────────────────────────────────────
#
# Match common phrasings of fatality counts in news/alert text:
#   "12 killed", "at least 5 dead", "3 fatalities", "killed 8 people",
#   "death toll of 14", "leaving 6 dead". Also handles written-out
#   numbers: "fourteen killed", "twenty-three dead", "one hundred killed".
#
# We deliberately stay narrow on the verb list to avoid false positives
# ("injured", "displaced" are tracked separately and don't belong here).

# Word→int building blocks. Covers 0-999 with hyphenated or space-separated
# compounds ("twenty-one", "twenty one", "one hundred and fifty"). Beyond
# ~99 written-out is uncommon, but "one hundred" / "two hundred" do appear.
_ONES: dict[str, int] = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9,
}
_TEENS: dict[str, int] = {
    "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
    "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19,
}
_TENS_MULTIPLES: dict[str, int] = {
    "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50,
    "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
}
_SCALES: dict[str, int] = {"hundred": 100, "thousand": 1000}
_NUMBER_WORDS: dict[str, int] = {**_ONES, **_TEENS, **_TENS_MULTIPLES, **_SCALES}

# Regex alternation of every supported number word. Longest-first to keep
# the engine from picking "ten" out of "tenth"-prefixed greedy matches.
_NUMBER_WORD_TOKEN_RE = "|".join(
    re.escape(w) for w in sorted(_NUMBER_WORDS.keys(), key=len, reverse=True)
)
# A "phrase" is one or more number-word tokens joined by space/hyphen,
# optionally with "and" between groups ("two hundred and fifty"). The whole
# phrase is one captured group from the caller's perspective.
_NUM_WORD_PHRASE = (
    rf"(?:{_NUMBER_WORD_TOKEN_RE})"
    rf"(?:[-\s]+(?:and[-\s]+)?(?:{_NUMBER_WORD_TOKEN_RE}))*"
)
# Combined capture: either a digit run or a number-word phrase.
_NUM = rf"(\d{{1,5}}|{_NUM_WORD_PHRASE})"


def _parse_number_phrase(raw: str) -> int | None:
    """Parse a captured number — digits or English words — into an int.

    Returns None for inputs we can't interpret (unknown tokens, empty
    string, etc.) so the caller can skip the match.
    """
    if not raw:
        return None
    raw = raw.strip()
    # Digit form: cheap path.
    if raw.isdigit():
        try:
            return int(raw)
        except ValueError:
            return None
    # Word form: split on whitespace/hyphen, drop fillers ("and"), sum up.
    tokens = [
        t for t in raw.lower().replace("-", " ").split()
        if t and t != "and"
    ]
    if not tokens:
        return None
    total = 0
    current = 0
    for tok in tokens:
        if tok not in _NUMBER_WORDS:
            return None  # unknown token — refuse to guess
        value = _NUMBER_WORDS[tok]
        if value == 100:
            current = (current or 1) * 100
        elif value == 1000:
            current = (current or 1) * 1000
            total += current
            current = 0
        else:
            current += value
    return total + current


_CASUALTY_PATTERNS: list[re.Pattern[str]] = [
    re.compile(rf"\b(?:at least|over|more than|nearly|around|about)?\s*{_NUM}\s+(?:people\s+)?(?:were\s+|are\s+)?(?:killed|dead|deceased|fatalities)\b", re.IGNORECASE),
    re.compile(rf"\b(?:killed|leaving|left)\s+(?:at least\s+|over\s+|more than\s+|nearly\s+)?{_NUM}\s+(?:people|dead|civilians|soldiers)?\b", re.IGNORECASE),
    re.compile(rf"\bdeath toll\s+(?:of|at|reaches?|rose to|climbed to|stands at)\s+{_NUM}\b", re.IGNORECASE),
    re.compile(rf"\b{_NUM}\s+(?:civilians?|soldiers?|militants?|protesters?)\s+(?:were\s+)?killed\b", re.IGNORECASE),
]


def extract_casualties_from_text(*texts: str | None) -> int | None:
    """Best-effort fatality count parsed from free-text headlines/descriptions.

    Accepts both digit forms ("12 killed") and English number words
    ("fourteen killed", "twenty-three dead", "one hundred killed").

    Returns the maximum number found across all matched patterns (multiple
    sources sometimes mention different running totals; the upper bound is
    the most useful for severity assessment). Returns None if no pattern
    matches.
    """
    best: int | None = None
    for text in texts:
        if not text:
            continue
        for pat in _CASUALTY_PATTERNS:
            for m in pat.finditer(text):
                val = _parse_number_phrase(m.group(1))
                if val is None:
                    continue
                if val < 0 or val > 100_000:
                    continue
                if best is None or val > best:
                    best = val
    return best


# Match common phrasings of population-affected counts in news/alert text:
#   "10,000 displaced", "5000 evacuated", "3 million affected",
#   "displacing 12k people", "leaving 8000 homeless".
# Includes scale modifiers (k/thousand/million) so we capture rough orders
# of magnitude when sources don't give exact counts.
_POP_NUM = r"(\d{1,3}(?:[,\s]\d{3})*|\d+(?:\.\d+)?)\s*(k|thousand|m|million|mln)?"
_POPULATION_PATTERNS: list[re.Pattern[str]] = [
    re.compile(
        rf"\b(?:at least|over|more than|nearly|around|about|approximately)?\s*{_POP_NUM}\s+(?:people\s+)?(?:were\s+|are\s+|have been\s+)?(?:displaced|evacuated|affected|homeless|forced to flee|fled their homes)\b",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\b(?:displacing|evacuating|affecting|leaving)\s+(?:at least\s+|over\s+|more than\s+|nearly\s+|about\s+|approximately\s+)?{_POP_NUM}\s+(?:people|residents|civilians|families)?\b",
        re.IGNORECASE,
    ),
]


def _parse_pop_match(num_str: str, scale: str | None) -> int | None:
    """Resolve a (number, scale) regex capture into an absolute integer."""
    cleaned = num_str.replace(",", "").replace(" ", "").strip()
    try:
        base = float(cleaned)
    except ValueError:
        return None
    if scale:
        s = scale.lower()
        if s in ("k", "thousand"):
            base *= 1_000
        elif s in ("m", "million", "mln"):
            base *= 1_000_000
    if base < 1 or base > 100_000_000:
        return None
    return int(base)


def extract_population_affected_from_text(*texts: str | None) -> int | None:
    """Best-effort affected-population count parsed from free text.

    Returns the maximum across all matches. Recognises common phrasings
    ("10,000 displaced", "3 million affected", "evacuating 5000 people").
    Returns None if no pattern matches.
    """
    best: int | None = None
    for text in texts:
        if not text:
            continue
        for pat in _POPULATION_PATTERNS:
            for m in pat.finditer(text):
                try:
                    val = _parse_pop_match(m.group(1), m.group(2))
                except (IndexError, ValueError):
                    continue
                if val is None:
                    continue
                if best is None or val > best:
                    best = val
    return best


# ── Event onset (startedAt) extraction ───────────────────────────────────────
# The event's real-world start — distinct from when the signal was collected.
# Parsed best-effort from headline/description text, anchored to the signal's own
# timestamp so relative phrases ("yesterday", "3 days ago") resolve. Returns an
# ISO-8601 date (YYYY-MM-DD) or None; the LLM rewrite pass is the fallback when
# this returns None (see providers/event.py).

# A mention older than this before the reference is treated as background
# ("since the 2019 war"), not the current event's onset.
_MAX_ONSET_LOOKBACK_DAYS = 180

# Month name / abbreviation → month number, from the C locale (stable, no env
# dependency). calendar.month_name[0] / month_abbr[0] are empty strings, skipped.
_MONTHS: dict[str, int] = {}
for _mi in range(1, 13):
    _MONTHS[calendar.month_name[_mi].lower()] = _mi
    _MONTHS[calendar.month_abbr[_mi].lower()] = _mi

_ISO_DATE_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
_DAYS_AGO_RE = re.compile(r"\b(\d{1,3})\s+days?\s+ago\b", re.IGNORECASE)
_WEEKS_AGO_RE = re.compile(r"\b(\d{1,2})\s+weeks?\s+ago\b", re.IGNORECASE)
_YESTERDAY_RE = re.compile(r"\byesterday\b", re.IGNORECASE)
_LAST_WEEK_RE = re.compile(r"\b(?:last week|a week ago)\b", re.IGNORECASE)
# "3 March 2026", "3rd March", "03 March"
_DAY_MONTH_RE = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})(?:\s+(\d{4}))?\b", re.IGNORECASE,
)
# "March 3, 2026", "March 3rd", "March 3". The (?!\d) stops the day capture from
# eating the first digits of a bare year ("March 2026" must NOT parse as day=20).
_MONTH_DAY_RE = re.compile(
    r"\b([A-Za-z]{3,9})\s+(\d{1,2})(?:st|nd|rd|th)?(?!\d)(?:,?\s+(\d{4}))?\b", re.IGNORECASE,
)


def _onset_reference_dt(reference_ts: str | None) -> datetime:
    """The signal timestamp as an aware UTC datetime; falls back to now()."""
    if reference_ts:
        try:
            dt = datetime.fromisoformat(reference_ts.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except (ValueError, AttributeError):
            pass
    return datetime.now(UTC)


def _ymd(year: int, month: int, day: int) -> datetime | None:
    try:
        return datetime(year, month, day, tzinfo=UTC)
    except ValueError:  # e.g. 31 February
        return None


def _month_day_candidate(
    month_word: str, day: int, year_str: str | None, ref: datetime,
) -> datetime | None:
    """Resolve a (month-name, day, optional-year) triple. With no year, assume
    the reference year, rolling back a year if that lands in the future (a report
    can't predate the onset it describes)."""
    month = _MONTHS.get(month_word.lower())
    if not month:
        return None
    if year_str:
        return _ymd(int(year_str), month, day)
    cand = _ymd(ref.year, month, day)
    if cand and cand.date() > ref.date():
        cand = _ymd(ref.year - 1, month, day)
    return cand


def extract_event_start_from_text(
    *texts: str | None, reference_ts: str | None = None,
) -> str | None:
    """Best-effort event-onset date parsed from free text.

    Handles ISO dates, day/month(/year) and month/day(/year) phrasings, and
    relative expressions ("yesterday", "N days/weeks ago", "last week") anchored
    to ``reference_ts`` (the signal's collection time). Returns the EARLIEST
    plausible onset as an ISO-8601 date (``YYYY-MM-DD``), or None when nothing
    parses.

    A candidate must fall within ``[reference - _MAX_ONSET_LOOKBACK_DAYS,
    reference]`` — future dates (an onset can't post-date its report) and
    long-past background mentions are dropped.
    """
    ref = _onset_reference_dt(reference_ts)
    earliest_ok = ref - timedelta(days=_MAX_ONSET_LOOKBACK_DAYS)
    candidates: list[datetime] = []

    for text in texts:
        if not text:
            continue
        for m in _ISO_DATE_RE.finditer(text):
            c = _ymd(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            if c:
                candidates.append(c)
        for m in _DAYS_AGO_RE.finditer(text):
            candidates.append(ref - timedelta(days=int(m.group(1))))
        for m in _WEEKS_AGO_RE.finditer(text):
            candidates.append(ref - timedelta(weeks=int(m.group(1))))
        if _YESTERDAY_RE.search(text):
            candidates.append(ref - timedelta(days=1))
        if _LAST_WEEK_RE.search(text):
            candidates.append(ref - timedelta(days=7))
        for m in _DAY_MONTH_RE.finditer(text):
            c = _month_day_candidate(m.group(2), int(m.group(1)), m.group(3), ref)
            if c:
                candidates.append(c)
        for m in _MONTH_DAY_RE.finditer(text):
            c = _month_day_candidate(m.group(1), int(m.group(2)), m.group(3), ref)
            if c:
                candidates.append(c)

    # Earliest plausible candidate = "when it started".
    valid = [c for c in candidates if earliest_ok <= c <= ref]
    if not valid:
        return None
    return min(valid).date().isoformat()


def earliest_onset_iso(*values: str | None) -> str | None:
    """The earliest of several ISO-8601 date/datetime strings (lexicographic
    order is chronological for zero-padded ISO-8601), ignoring None. Used to keep
    an event's ``startedAt`` at the earliest onset across its signals."""
    present = [v for v in values if v]
    return min(present) if present else None


def geoparse_to_dict(result: GeoparseResult) -> dict:
    """Shape a GeoparseResult for storage in signals.geoparsed_data.

    Matches the JSONB shape documented on the Prisma model. We deliberately
    drop the raw Nominatim payload — callers comparing against source coords
    only need the resolved fields, and keeping the raw payload bloats the row.
    """
    return {
        "candidate": result.candidate,
        "kind": result.kind,
        "field": result.field,
        "lat": result.lat,
        "lng": result.lng,
        "country_code": result.country_code,
        "osm_class": result.osm_class,
        "osm_type": result.osm_type,
        "importance": result.importance,
        "display_name": result.display_name,
    }


def enrich_with_geoparser(
    input_data: dict,
    *,
    title: str | None,
    description: str | None,
    extra_body_text: str | None = None,
    promote: bool = True,
    log_tag: str = "signal",
) -> GeoparseResult | None:
    """Run the geoparser on title+description and mutate `input_data`.

    Shared between Dataminr/ACLED/GDACS pre-create flows. On success the
    function sets:
      - `geoparsedData`: structured dict shaped for the JSONB column
      - `locationId` (only when `promote=True` and the L4 promotion clears
        the same-A2 safety check) — passing this skips clear-api's default
        "signal-title L4" branch in createPointLocation

    `extra_body_text` lets the caller feed additional text to the geoparser
    *without* changing the signal row's stored description — e.g. deeper
    body/sub-fields that mention a precise landmark ("Nyala Airport") which
    never makes it into the user-visible description. No current source passes
    it, but it's kept for sources whose payload carries such extra prose.

    Best-effort. Any failure (no candidate, Nominatim down, circuit open,
    L4 promotion error) is swallowed; the caller continues with source coords.

    Source coords for the same-A2 check are read from
    `input_data["lat"]`/`input_data["lng"]`, so callers must set those
    before invoking this helper.

    Returns the GeoparseResult (or None) so callers that need the candidate
    name for logging don't have to re-parse the dict.
    """
    logger.info("[%s] Running geoparser", log_tag)
    geoparser_body = description
    if extra_body_text:
        geoparser_body = (
            f"{description}\n{extra_body_text}" if description else extra_body_text
        )
    # Scope the geocode to the country the signal's source coordinates fall
    # in. Without this, geoparse_signal searches all supported countries at
    # once and a same-named place in another POC country can outrank the
    # correct one (Sudanese OSM entries score near-zero importance). Falls
    # back to all configured countries when coords are missing or land
    # outside every box.
    scoped_country = country_from_coords(input_data.get("lat"), input_data.get("lng"))
    expected = {scoped_country} if scoped_country else None
    try:
        geo_result = geoparse_signal(
            title, geoparser_body, expected_country_codes=expected
        )
    except Exception as exc:  # noqa: BLE001 — best-effort
        logger.warning("[%s] Geoparser failed (continuing without enrichment): %s", log_tag, exc)
        return None

    if geo_result is None:
        # No usable candidate. The geoparser itself logs the specific reason
        # (no candidates extracted / disqualified / Nominatim empty / below
        # importance floor) at INFO — look for the adjacent `[geoparser] ...`
        # line in the log to see which gate fired.
        logger.info("[%s] Geoparser produced no result — falling back to source coords", log_tag)
        # We still ran the extraction stages even when Nominatim failed
        # the resolution. Pass the unresolved candidate name to clear-api
        # so the L4 row created from source coords gets a meaningful
        # label ("al-Obeid (unresolved)") instead of falling through to
        # the signal title — Dataminr titles in particular are full
        # paragraphs and pollute the locations table. When no candidate
        # is extractable at all, clear-api's coord-based fallback kicks
        # in ("Point 15.6280, 30.2156").
        unresolved = extract_top_candidate(title, geoparser_body)
        if unresolved:
            input_data["pointName"] = f"{unresolved} (unresolved)"
        return None

    input_data["geoparsedData"] = geoparse_to_dict(geo_result)
    logger.info(
        "[%s] Geoparsed: candidate=%r kind=%s field=%s importance=%.2f",
        log_tag, geo_result.candidate, geo_result.kind, geo_result.field, geo_result.importance,
    )

    if not promote:
        return geo_result

    try:
        promo = find_or_create_landmark_l4(
            name=geo_result.candidate,
            lat=geo_result.lat,
            lng=geo_result.lng,
            kind=geo_result.kind,
            source_lat=input_data.get("lat"),
            source_lng=input_data.get("lng"),
        )
        if promo.get("abortedReason"):
            logger.info(
                "[%s] L4 promotion aborted (%s) — keeping source coords",
                log_tag, promo["abortedReason"],
            )
        elif promo.get("locationId"):
            input_data["locationId"] = promo["locationId"]
            logger.info(
                "[%s] Promoted to L4 %s (reused=%s, point_type=%s)",
                log_tag, promo["locationId"], promo.get("reused"), promo.get("pointType"),
            )
    except Exception as exc:  # noqa: BLE001 — promotion is best-effort
        logger.warning("[%s] L4 promotion failed: %s", log_tag, exc)

    return geo_result


def build_signal_input(signal: DataminrSignal, source_id: str) -> dict:
    """Map a Dataminr signal to a CLEAR CreateSignalInput dict."""
    # Description comes from the structured `subHeadline` fields. (Dataminr no
    # longer sends the `liveBrief` / `intelAgents` prose we used to fall back to,
    # so an alert with a null subHeadline simply has no description.)
    description_parts: list[str] = []
    if signal.subHeadline:
        if signal.subHeadline.title:
            description_parts.append(signal.subHeadline.title)
        if signal.subHeadline.subHeadlines:
            description_parts.append(signal.subHeadline.subHeadlines)
    description = " — ".join(description_parts) if description_parts else None

    # URL from publicPost
    url = None
    if signal.publicPost and signal.publicPost.href:
        url = signal.publicPost.href

    # Full raw payload as JSON
    raw_data = signal.model_dump(mode="json")

    # Estimate severity from Dataminr alertType (1-5 or None)
    severity = _estimate_severity_from_dataminr(signal)

    input_data: dict = {
        "sourceId": source_id,
        # Idempotent ingestion key — the clear-api upsert behaviour keys on
        # (sourceId, externalId), so re-ingesting the same Dataminr alert
        # returns the existing row instead of creating a duplicate.
        "externalId": f"dataminr:{signal.alertId}",
        "rawData": raw_data,
        "publishedAt": signal.alertTimestamp,
        "url": url,
        "title": signal.headline,
        "description": description,
    }
    if severity is not None:
        input_data["severity"] = severity

    # Dataminr has no structured casualties field; parse it from the headline
    # and description text. Best-effort — only set when a match is found.
    casualties = extract_casualties_from_text(signal.headline, description)
    if casualties is not None:
        input_data["casualties"] = casualties

    # Check if Dataminr provides coordinates. We do this BEFORE the geoparser
    # so the L4-promotion step can run a same-A2 safety check between the
    # candidate's location and the source's coords.
    has_coords = False
    dataminr_location_name = None
    if signal.estimatedEventLocation:
        dataminr_location_name = signal.estimatedEventLocation.name
        if signal.estimatedEventLocation.coordinates:
            coords = signal.estimatedEventLocation.coordinates
            if len(coords) >= 2:
                input_data["lat"] = coords[0]
                input_data["lng"] = coords[1]
                has_coords = True

    # Text-based geoparser: additive enrichment + opportunistic L4 promotion.
    # Source coords stay on `rawData` (full Dataminr dump above), so the
    # original is always recoverable. The geoparser sees the headline (title)
    # and the subHeadline-derived description — Dataminr no longer sends the
    # liveBrief / intelAgents body text we used to feed it as extra context.
    enrich_with_geoparser(
        input_data,
        title=signal.headline,
        description=description,
        log_tag=f"dataminr:{signal.alertId}",
    )

    if input_data.get("locationId"):
        # Geoparser promoted the signal to a precise L4 — clear-api will use
        # that locationId verbatim and skip its own createPointLocation path.
        logger.info("Signal location resolved via geoparser: %s", input_data["locationId"])
    elif has_coords:
        # Source coords only — let the API's PostGIS geo-resolution handle it.
        # Skip the Claude displacement check: origin/destination aren't used
        # downstream yet, so the LLM call is wasted credits.
        logger.info("Signal has coords: using lat/lng for PostGIS resolution")
    else:
        # No coordinates — use Claude to resolve location from text
        loc_result = resolve_signal_location(
            title=signal.headline,
            description=description,
            dataminr_location_name=dataminr_location_name,
        )
        if loc_result["location_type"] == "displacement":
            if loc_result["origin_id"]:
                input_data["originId"] = loc_result["origin_id"]
            if loc_result["destination_id"]:
                input_data["destinationId"] = loc_result["destination_id"]
            logger.info(
                "Displacement signal (no coords): origin=%s destination=%s",
                loc_result["origin_id"],
                loc_result["destination_id"],
            )
        else:
            if loc_result["location_id"]:
                input_data["locationId"] = loc_result["location_id"]
            logger.info("General signal (no coords): locationId=%s", loc_result["location_id"])

    return input_data


def ingest_signal(signal: DataminrSignal, source_id: str) -> dict:
    """Build and persist a CLEAR signal from a Dataminr payload. Returns the created signal."""
    input_data = build_signal_input(signal, source_id)
    result = create_signal(input_data)
    logger.info(
        "Created signal id=%s title=%s location=%s",
        result["id"],
        result.get("title", "")[:60],
        result.get("generalLocation", {}).get("name") if result.get("generalLocation") else
        result.get("originLocation", {}).get("name") if result.get("originLocation") else "none",
    )
    return result
