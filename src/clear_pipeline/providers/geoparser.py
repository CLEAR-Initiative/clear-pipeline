"""Signal geoparser — extract event location from title + description.

The geoparser is **best-effort enrichment**, not a critical path. Every
failure mode returns None and the caller falls back to source-supplied
coordinates. Designed for ~100 signals/day with one outbound Nominatim
call per signal (or zero, if the cache is warm).

Pipeline:
  1. Extract candidate place names from title and (optionally) body
     using a small set of regex patterns. Title hits dominate body hits.
  2. Filter out organisation / generic stopwords ("Sudanese Armed Forces",
     "Reports", etc.) so we don't ship those to the geocoder.
  3. Classify each candidate as **landmark** (ends with a known suffix
     like "Airport", "Hospital") or **admin** (default).
  4. Detect disqualifying context — candidates immediately preceded by
     "transferred to", "fled toward", etc. get demoted.
  5. Rank: landmarks beat admin; title beats body; earlier beats later;
     disqualified candidates drop to the bottom.
  6. Send the single top candidate to Nominatim. Pick the best result
     (importance >= 0.3, within expected country codes).
  7. Return a structured `GeoparseResult`, or None if anything failed.

What this is NOT:
  - A general-purpose NER system. No spaCy, no transformers, no ML.
  - A noise-cleaning miracle. Garbage in, garbage out — but at least
    "garbage" returns None rather than misattributing a signal.
  - A precision-recovery engine. If both text and coords are at district
    granularity, no algorithm here makes that any finer.
"""

from __future__ import annotations

import difflib
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

#: Caller's test on a candidate point (lat, lng) — e.g. "inside the admin area
#: the text was matched to". A rejected hit counts as a miss, so the next tier
#: (or the next result) is tried instead.
AcceptPoint = Callable[[float, float], bool]

from clear_pipeline.providers import clear_api as graphql, nominatim
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)


# ─── Stopwords ────────────────────────────────────────────────────────────
# Phrases that the extraction regexes will pick up but that aren't actually
# place names — we drop them before they ever reach the geocoder.

# Organisations + actors that appear capitalised in alert text.
ORG_STOPWORDS: frozenset[str] = frozenset(
    s.lower() for s in [
        "Sudan", "Sudanese",  # nationality adjectives, not the country signal
        "Armed Forces", "Sudanese Armed Forces", "SAF",
        "Rapid Support Forces", "RSF",
        "SPLM-N", "SPLA", "JEM", "SLA",
        "United Nations", "UN", "UNAMID", "UNHCR", "UNICEF", "WFP",
        "OCHA", "ICRC", "MSF", "Doctors Without Borders",
        "Red Cross", "Red Crescent",
        "World Food Programme", "World Health Organisation", "World Health Organization", "WHO",
        "International Organization for Migration", "IOM",
        "African Union", "AU", "European Union", "EU",
        "International Criminal Court", "ICC",
    ]
)

# Generic words that look like place names but aren't.
GENERIC_STOPWORDS: frozenset[str] = frozenset(
    s.lower() for s in [
        "Reports", "Sources", "According", "Source", "Reuters",
        "Twitter", "Facebook", "Telegram", "Instagram", "YouTube", "X",
        "BBC", "CNN", "Al Jazeera", "AFP", "AP",
        "Major News Outlet", "Local News Outlet", "News Outlet",
        "Government", "Ministry", "Council",
        "Sovereign Council", "Sovereignty Council",
        "President", "Prime Minister", "General", "Lieutenant", "Major", "Colonel",
        "North", "South", "East", "West",  # cardinal-only is too ambiguous
        "Northern", "Southern", "Eastern", "Western",
        # Days + months — these get picked up after "on" ("on Monday", "on May 19").
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
)

STOPWORDS: frozenset[str] = ORG_STOPWORDS | GENERIC_STOPWORDS


# ─── Landmark suffixes ────────────────────────────────────────────────────
# A candidate ending with any of these is classified as a landmark and
# wins precedence over admin-unit candidates. Compared case-insensitively
# with a leading-whitespace boundary so "Airport" matches "Nyala Airport"
# but not "Airportside" or "Khairport".

LANDMARK_SUFFIXES: frozenset[str] = frozenset(
    s.lower() for s in [
        "Airport", "Airfield", "Aerodrome", "Heliport",
        "Hospital", "Clinic", "Medical Centre", "Health Centre",
        "Bridge", "Crossing", "Border Crossing",
        "School", "University", "College", "Institute",
        "Market", "Souq", "Bazaar",
        "Mosque", "Church", "Cathedral", "Synagogue", "Temple",
        "Camp", "IDP Camp", "Refugee Camp",
        "Stadium", "Arena", "Sports Complex",
        "Refinery", "Power Station", "Power Plant", "Substation",
        "Port", "Harbour", "Harbor", "Dock", "Wharf",
        "Station", "Bus Station", "Railway Station", "Train Station",
        "Barracks", "Compound", "Base", "Headquarters", "HQ",
        "Prison", "Detention Centre",
        "Palace", "Embassy", "Consulate",
        "Dam", "Reservoir", "Lake",
        "Bank",
    ]
)


# ─── Disqualifying phrases ────────────────────────────────────────────────
# When a candidate is immediately preceded by one of these phrases (within
# ~3 words), it's flagged as incidental — typically a destination or
# departure point, not the event location.
# Matched case-insensitively against the text immediately before the
# candidate's start position.

DISQUALIFYING_PHRASES: frozenset[str] = frozenset(
    s.lower() for s in [
        "transferred to", "evacuated to", "fled toward", "fled to",
        "headed to", "headed toward", "en route to", "bound for",
        "destined for", "originating in", "originating from",
        "moved to", "relocated to", "transported to",
        "via",
        "from",
        "reports from", "according to sources in", "filed from",
        "broadcast from", "reported from",
    ]
)


# ─── Extraction patterns ──────────────────────────────────────────────────
# A small set of regex patterns designed to be conservative — better to
# miss a candidate than to ship a candidate that's a person's name or an
# organisation. The patterns each emit (name, position) pairs.

# Arabic definite-article prefixes commonly seen in Sudanese / pan-Arab
# transliteration. `[Aa]l`, `[Ee]l`, etc. each match either case at the
# leading letter so we capture both "al-Obeid" (Dataminr-style) and
# "Al-Obeid" (when the source headline-cased the article). The separator
# is "-" or whitespace ("al-Nahud", "Al Nahud" both match). Without this
# allowance, the regex below requires an uppercase first letter and ends
# up capturing the post-prefix fragment alone ("Obeid", "Nahud") which
# never matches Nominatim.
_ARTICLE = (
    r"(?:[Aa]l|[Ee]l|[Aa]s|[Aa]n|[Aa]sh|[Aa]d|[Aa]t|[Aa]r|[Aa]z|[Ii]l)[- ]"
)

# Each token of the captured name: optional Arabic article + uppercase
# letter + word characters / hyphens / apostrophes. Reused below so both
# the prep pattern and the comma pattern stay symmetric.
_NAME_TOKEN = rf"(?:{_ARTICLE})?[A-Z][\w\-']*"

# Place names appearing after location-indicating prepositions:
#   "explosion in Al Fasher, Sudan"  →  "Al Fasher"
#   "near Markib"                    →  "Markib"
#   "drone strike on Nyala Airport"  →  "Nyala Airport"
#   "around al-Obeid, Sudan"         →  "al-Obeid"
# "on" is included because attack-style headlines and body text routinely
# use "strike/attack ON <place>". Days-of-week + months are filtered as
# stopwords above so "on Monday" / "on May 19" don't produce candidates.
_PREP_PATTERN = re.compile(
    r"\b(in|at|near|around|outside|on)\s+"
    rf"({_NAME_TOKEN}(?:\s+{_NAME_TOKEN}){{0,4}})",
)

# Relative / movement phrasing, as field reports put it:
#   "the road out of Mukjar"        →  "Mukjar"
#   "villages east of Um Dukhun"    →  "Um Dukhun"
#   "families arriving from Kutum"  →  "Kutum"
# Opt-in (`relative_phrases=True`): hotline text reads like this, while news
# headlines rarely do, and "from"/"to" + a capitalised word is often not a
# place in wire copy ("statement from UNICEF", "to Reuters"). The phrase is
# matched case-insensitively; the name still has to be capitalised.
_RELATIVE_PATTERN = re.compile(
    r"\b((?i:(?:north|south|east|west)(?:[- ]?(?:east|west))?\s+of|out\s+of|"
    r"from|towards?|into|to))\s+"
    rf"({_NAME_TOKEN}(?:\s+{_NAME_TOKEN}){{0,4}})",
)

# Comma-separated hierarchical patterns:
#   "Al Fasher, North Darfur, Sudan"  →  emits "Al Fasher" (deepest part)
#   "al-Obeid, Sudan"                 →  emits "al-Obeid"
# Multi-segment splits handled later by re-running the matcher recursively.
_COMMA_PATTERN = re.compile(
    rf"\b({_NAME_TOKEN}(?:\s+{_NAME_TOKEN}){{0,4}}),\s+"
    rf"{_NAME_TOKEN}(?:\s+{_NAME_TOKEN}){{0,4}},?",
)


@dataclass
class Candidate:
    """One extracted place-name candidate from a signal's text."""
    name: str
    field: Literal["title", "body"]
    position: int                        # char index inside the field
    kind: Literal["landmark", "admin"] = "admin"
    disqualified: bool = False
    extraction_reason: str = ""          # debug hint: 'after_in', 'comma_split', etc.
    score: float = 0.0


@dataclass
class GeoparseResult:
    """Final output of the geoparser when resolution succeeds."""
    candidate: str
    kind: Literal["landmark", "admin"]
    field: Literal["title", "body"]
    lat: float
    lng: float
    country_code: str | None
    osm_class: str | None                # e.g. 'place', 'aeroway', 'amenity'
    osm_type: str | None                 # e.g. 'city', 'aerodrome', 'hospital'
    importance: float                    # Nominatim's importance score
    display_name: str                    # full address-style label from Nominatim
    raw: dict = field(default_factory=dict)  # the raw Nominatim result for callers that want more
    #: The CLEAR location of a place-alias hit (clear-api ADR-0019). Set only
    #: when the gazetteer answered from CLEAR's own aliases: use it as the
    #: signal's location directly, with no landmark promote. `lat` / `lng`
    #: are then that location's point.
    location_id: str | None = None


@dataclass
class GeocodeTrace:
    """What a `geocode_place_name` call met on the way to a miss, for a caller
    that records misses (the unresolved place-name queue)."""
    #: A lookup found a candidate, and the caller's `accept` refused it.
    outside: bool = False
    #: A lookup tier failed (clear-api or LocationIQ), so the miss proves nothing.
    failed: bool = False
    #: A lookup knows the name as an area no point stands for, and as nothing
    #: else: the gazetteer matched it exactly to a state, a region or a
    #: mountain range (no later tier is then asked), or LocationIQ answered
    #: with whole states and no lookup found a place. A caller that records
    #: misses records none.
    area: bool = False
    #: LocationIQ gave a whole state or country, and it was refused. Not yet
    #: "the name is an area": a place may turn up for another spelling, or
    #: outside the caller's area (`outside`), and that one is a real miss.
    whole_state: bool = False


# ─── Extraction ───────────────────────────────────────────────────────────


def _is_stopword(name: str) -> bool:
    """True if the candidate is in either stopword list."""
    return name.lower().strip() in STOPWORDS


def _extract_from_text(
    text: str,
    field: Literal["title", "body"],
    *,
    relative_phrases: bool = False,
) -> list[Candidate]:
    """Run the extraction patterns over `text` and return surviving candidates.
    `relative_phrases` adds `_RELATIVE_PATTERN` (hotline text; see there).

    Stopwords are filtered here so they never enter the ranking stage.
    Duplicates (same lowercase name in same field) are deduplicated, keeping
    the earliest position.
    """
    if not text:
        return []

    raw: list[Candidate] = []

    # Pass 1: post-preposition candidates
    for match in _PREP_PATTERN.finditer(text):
        prep = match.group(1).lower()
        name = match.group(2).strip().rstrip(".,;:!?")
        if _is_stopword(name):
            continue
        raw.append(Candidate(
            name=name,
            field=field,
            position=match.start(2),
            extraction_reason=f"after_{prep}",
        ))

    # Pass 1b (opt-in): relative / movement phrasing
    if relative_phrases:
        for match in _RELATIVE_PATTERN.finditer(text):
            name = match.group(2).strip().rstrip(".,;:!?")
            if _is_stopword(name):
                continue
            raw.append(Candidate(
                name=name,
                field=field,
                position=match.start(2),
                extraction_reason="after_" + re.sub(r"\W+", "_", match.group(1).lower()),
            ))

    # Pass 2: comma-separated hierarchical patterns — take the leading part
    for match in _COMMA_PATTERN.finditer(text):
        name = match.group(1).strip().rstrip(".,;:!?")
        if _is_stopword(name):
            continue
        raw.append(Candidate(
            name=name,
            field=field,
            position=match.start(1),
            extraction_reason="comma_lead",
        ))

    # Deduplicate by lowercased name within the same field — keep earliest position
    by_key: dict[str, Candidate] = {}
    for c in raw:
        key = c.name.lower()
        if key not in by_key or c.position < by_key[key].position:
            by_key[key] = c
    return list(by_key.values())


# ─── Classification ───────────────────────────────────────────────────────


def _is_landmark(name: str) -> bool:
    """True iff the candidate ends with a known landmark suffix."""
    lower = name.lower()
    for suffix in LANDMARK_SUFFIXES:
        if lower.endswith(" " + suffix) or lower == suffix:
            return True
    return False


def _is_disqualified(candidate: Candidate, text: str) -> bool:
    """True iff the text immediately before the candidate matches a
    disqualifying phrase. Checks up to 30 chars back from the candidate's
    start position — enough to catch "transferred to" but not so much that
    distant mentions interfere.
    """
    if candidate.position == 0:
        return False
    look_back = max(0, candidate.position - 30)
    preceding = text[look_back:candidate.position].lower().rstrip()
    for phrase in DISQUALIFYING_PHRASES:
        # Tolerate trailing punctuation/whitespace before the candidate.
        if preceding.endswith(phrase):
            return True
    return False


# ─── Ranking ──────────────────────────────────────────────────────────────


def _score(c: Candidate) -> float:
    """Multi-factor score, used WITHIN a ranking tier (see `_rank`):
       - title hits weighted 3× over body hits
       - earlier positions weighted higher via 1/(pos+1)
       - landmark candidates get a 2× bonus over admin

    Disqualification is handled outside the score so disqualified candidates
    drop below all clean ones regardless of how high they'd otherwise rank.
    """
    pos_score = 1.0 / (c.position + 1)
    field_bonus = 3.0 if c.field == "title" else 1.0
    kind_bonus = 2.0 if c.kind == "landmark" else 1.0
    return pos_score * field_bonus * kind_bonus


def _rank(candidates: list[Candidate]) -> list[Candidate]:
    """Three-tier ranking so the design intent "landmark beats admin" holds
    even when admin appears in the title and landmark only in the body
    (e.g. headline says "in Nyala", the description says
    "on Nyala Airport"):

      Tier 0: clean landmarks
      Tier 1: clean admins
      Tier 2: anything disqualified

    Inside each tier, `_score` orders by title-vs-body, position, and kind.
    """
    for c in candidates:
        c.score = _score(c)

    def tier_key(c: Candidate) -> tuple[int, float]:
        if c.disqualified:
            tier = 2
        elif c.kind == "landmark":
            tier = 0
        else:
            tier = 1
        return (tier, -c.score)

    return sorted(candidates, key=tier_key)


# ─── Nominatim result selection ───────────────────────────────────────────


# Classes Nominatim returns that are NOT valid point locations for an L4.
# These are linear or zoned features rather than discrete places — promoting
# them to an A4 point would attribute signals to an arbitrary point on a road
# or a stretch of river. Everything else (place, boundary, amenity, aeroway,
# building, leisure, tourism, landuse, natural, man_made, historic, ...)
# passes through.
_REJECTED_CLASSES: frozenset[str] = frozenset({"highway", "railway", "waterway"})


# GeoNames analogue of `_REJECTED_CLASSES` for the gazetteer tier. Feature
# classes that are linear or zonal rather than discrete points: R=road/
# railroad, H=hydrographic (streams, wadis, lakes), V=vegetation (forest,
# grassland). Same rationale — promoting one to an A4 point would attribute a
# signal to an arbitrary spot on a road or a stretch of river.
_REJECTED_FEATURE_CLASSES: frozenset[str] = frozenset({"R", "H", "V"})

# GeoNames feature codes of an area that no single point stands for: a
# country, a state (first-order division, present or historical), a region, a
# tribal area, a mountain range, a desert, a plain. GeoNames gives each a
# centre point; a signal promoted to it lands in whatever district holds that
# centre. "Darfur" (RGN) is at 13.0, 25.0, "Khartoum" (ADM1, the state) at
# 15.8, 33.0, "Jibāl an Nūbah" (MTS) at 12.0, 30.75. A district (ADM2) is not
# listed: its point lies inside the district it names.
_AREA_FEATURE_CODES: frozenset[str] = frozenset({
    "PCL", "PCLD", "PCLF", "PCLH", "PCLI", "PCLIX", "PCLS",
    "ADM1", "ADM1H",
    "RGN", "RGNE", "RGNH", "RGNL", "AREA", "TRB", "CONT",
    "MTS", "DSRT", "PLN", "PLAT", "UPLD",
})


def _names_an_area(hit: dict) -> bool:
    """True when a GeoNames hit is an area too large for one point (see
    `_AREA_FEATURE_CODES`)."""
    return (hit.get("featureCode") or "").upper() in _AREA_FEATURE_CODES


def _not_a_point(hit: dict, trace: "GeocodeTrace | None" = None) -> bool:
    """True when a GeoNames hit must not become a point: a road, a river or
    a forest (`_REJECTED_FEATURE_CLASSES`), or an area (`_names_an_area`).
    An exact match on an area is noted on ``trace``: the name is that area's
    own, so asking another tier for a point would only find its capital or
    a village that sounds like it."""
    if _names_an_area(hit):
        if trace is not None and hit.get("exact"):
            trace.area = True
        return True
    return (hit.get("featureClass") or "").upper() in _REJECTED_FEATURE_CLASSES


# The parts of a Nominatim address that name no more than a state or a
# country. A result whose address names only these is that state or country.
_STATE_OR_COUNTRY_PARTS: frozenset[str] = frozenset({"country", "state", "region", "ISO3166-2-lvl4"})


def _is_state_or_country(result: dict) -> bool:
    """True when a Nominatim result is a whole state or country: its address
    names one, and nothing finer. LocationIQ answers "Darfur" with the five
    Darfur states, each at its centre point. A result with no address, or
    with a country code alone, is kept: there is nothing to judge it by."""
    address = result.get("address")
    if not isinstance(address, dict):
        return False
    named = set(address) - {"country_code"}
    return bool(named) and named <= _STATE_OR_COUNTRY_PARTS


def _pick_best_nominatim_result(
    results: list[dict],
    expected_country_codes: set[str],
    accept: AcceptPoint | None = None,
    trace: GeocodeTrace | None = None,
) -> dict | None:
    """Pick the highest-importance Nominatim result for a candidate, after
    filtering for country and class.

    No importance floor: LocationIQ / Nominatim importance scores for places
    outside the OSM-dense Anglosphere can be vanishingly small (e.g., 0.0001
    for "El Obeid Teaching Hospital", a real and well-known facility).
    Filtering on importance silently dropped the bulk of valid Sudan matches
    even though they were already cached. Nominatim returns results in
    relevance order, so picking the highest-importance after class+country
    filtering is sufficient on its own.

    `accept`, when given, drops results the caller rejects (e.g. outside its
    admin area) before ranking — Sudanese importance scores are near zero, so
    a same-named place elsewhere can outrank the right one.
    """
    if not results:
        return None

    best: dict | None = None
    best_importance = -1.0  # accept 0-importance hits (real places in low-density OSM regions)
    for r in results:
        try:
            importance = float(r.get("importance", 0))
        except (TypeError, ValueError):
            continue
        cls = r.get("class")
        if isinstance(cls, str) and cls.lower() in _REJECTED_CLASSES:
            continue
        if _is_state_or_country(r):
            if trace is not None:
                trace.whole_state = True
            continue
        country = _extract_country_code(r)
        if expected_country_codes and country and country not in expected_country_codes:
            continue
        if accept is not None:
            try:
                inside = accept(float(r["lat"]), float(r["lon"]))
            except (KeyError, TypeError, ValueError):
                continue
            if not inside:
                if trace is not None:
                    trace.outside = True
                continue
        if importance > best_importance:
            best_importance = importance
            best = r
    return best


def _extract_country_code(nominatim_result: dict) -> str | None:
    """Pull the ISO country code from a Nominatim result if present."""
    addr = nominatim_result.get("address") or {}
    code = addr.get("country_code")
    return code.lower() if isinstance(code, str) else None


# ─── Transliteration-tolerant Nominatim queries ───────────────────────────


# Captures Arabic article + separator + the rest of the token, used to
# generate alternate transliterations of the same place name (e.g.
# "al-Obeid" → "Al Obeid" → "El Obeid" → "Obeid"). Mirrors `_ARTICLE`
# above; kept here so the extraction layer and the query layer share a
# single allow-list.
_ARTICLE_HEAD_RE = re.compile(
    r"^([Aa]l|[Ee]l|[Aa]s|[Aa]n|[Aa]sh|[Aa]d|[Aa]t|[Aa]r|[Aa]z|[Ii]l)[- ](.+)$"
)


def _query_variants(name: str) -> list[str]:
    """Generate ordered Nominatim query variants for a candidate name.

    Sudanese / pan-Arab place names appear in OSM under multiple
    transliterations — "al-Obeid" is more commonly indexed as "El Obeid"
    or "Al Ubayyid". Trying the original first then fanning out into
    space-separated and El/Al variants raises the true-positive rate
    without changing the disqualification semantics; the stripped form
    is tried last so we degrade to the pre-fix behaviour rather than
    miss entirely.

    Each call adds at most ~3 extra Nominatim queries. Cached on the
    second occurrence; benign as a one-shot lookup cost otherwise.
    """
    variants: list[str] = [name]

    m = _ARTICLE_HEAD_RE.match(name)
    if m:
        article_raw, rest = m.group(1), m.group(2)
        # Normalise: capitalise the article and use a space separator.
        cap_article = article_raw.capitalize()
        variants.append(f"{cap_article} {rest}")
        # Swap Al/El — both transliterate ‫الـ‬ depending on convention.
        if cap_article == "Al":
            variants.append(f"El {rest}")
        elif cap_article == "El":
            variants.append(f"Al {rest}")
        # Last resort: drop the article entirely (current pre-fix
        # behaviour). Keeps backward compatibility for names that only
        # match Nominatim under the bare form.
        variants.append(rest)

    # Dedupe while preserving order.
    return list(dict.fromkeys(variants))


# ─── Public entry point ───────────────────────────────────────────────────


def extract_top_candidate(
    title: str | None,
    description: str | None = None,
) -> str | None:
    """Run only the extraction + classification + disqualification +
    ranking stages and return the top candidate's name.

    Used by `geoparse_signal` internally; also exposed so the pipeline
    can recover a sensible human label when the full geoparse fails (the
    Nominatim lookup misses) — that label is then used as the L4
    location's name instead of the signal title bleeding through.

    Returns None when no usable candidate exists (no candidates
    extracted, or the top is disqualified).
    """
    candidates: list[Candidate] = []
    candidates.extend(_extract_from_text(title or "", "title"))
    candidates.extend(_extract_from_text(description or "", "body"))

    if not candidates:
        return None

    for c in candidates:
        c.kind = "landmark" if _is_landmark(c.name) else "admin"
        source_text = title if c.field == "title" else (description or "")
        c.disqualified = _is_disqualified(c, source_text or "")

    ranked = _rank(candidates)
    top = ranked[0]
    if top.disqualified:
        return None
    return top.name


def _configured_country_codes() -> set[str]:
    """ISO-3166-1 alpha-2 codes the geocoder may resolve into — the
    pipeline's supported countries, from `settings.geoparser_country_codes`.
    Falls back to Sudan only if the setting is somehow empty."""
    codes = {
        c.strip().lower()
        for c in settings.geoparser_country_codes.split(",")
        if c.strip()
    }
    return codes or {"sd"}


# Bounding boxes (min_lat, max_lat, min_lng, max_lng) for the POC countries,
# used only to pick which configured country a signal's source coordinates
# fall in. The three are on separate continents, so a coarse box is
# unambiguous — this is never used for precise containment. Add a box here
# when adding a country to `geoparser_country_codes`.
_COUNTRY_BBOXES: dict[str, tuple[float, float, float, float]] = {
    "sd": (8.0, 23.5, 21.0, 39.5),    # Sudan
    "ve": (0.0, 13.0, -74.0, -59.0),  # Venezuela
    "af": (29.0, 39.5, 60.0, 75.5),   # Afghanistan
}


def country_from_coords(lat: float | None, lng: float | None) -> str | None:
    """Return the configured-country ISO2 whose bounding box contains the
    point, or None when the coordinates are missing or fall outside every
    configured country.

    Callers use this to scope a signal's geocode to the one country it's
    actually in. Without it, `geoparse_signal` searches all supported
    countries at once, and because Sudanese OSM entries carry near-zero
    importance a same-named place in another POC country outranks the correct
    one (and the gazetteer's country pin goes unused)."""
    if lat is None or lng is None:
        return None
    try:
        lat_f = float(lat)
        lng_f = float(lng)
    except (TypeError, ValueError):
        return None
    for cc in _configured_country_codes():
        box = _COUNTRY_BBOXES.get(cc)
        if box and box[0] <= lat_f <= box[1] and box[2] <= lng_f <= box[3]:
            return cc
    return None


def _alias_location_id(hit: dict) -> str | None:
    """The CLEAR location of a gazetteer hit that came from a place alias,
    else None (a GeoNames hit, or a clear-api without the alias fields)."""
    if hit.get("source") == "ALIAS" and hit.get("locationId"):
        return str(hit["locationId"])
    return None


def _resolve_via_gazetteer(
    top: Candidate,
    expected: set[str],
    within_location_id: str | None = None,
    trace: GeocodeTrace | None = None,
    alias_only: bool = False,
) -> GeoparseResult | None:
    """Hybrid tier 1: look the top candidate up in clear-api's gazetteer:
    CLEAR's place aliases first, then the offline GeoNames copy. Returns a
    GeoparseResult on a confident hit, else None so the caller falls back to
    LocationIQ. An alias hit carries `location_id`.

    `alias_only` keeps an alias hit and drops a GeoNames hit: for landmark
    names, which GeoNames answers with the wrong place (see
    `_resolve_candidate`), but which a person may have aliased.

    Country scope: pin the lookup to the one expected country when
    unambiguous; otherwise search every loaded country and drop a hit
    outside the expected set. A wrong-country hit that slips through is
    still caught by clear-api's same-A2 check when the L4 is promoted."""
    cc = next(iter(expected)).upper() if len(expected) == 1 else None
    try:
        hit = graphql.resolve_gazetteer_location(
            top.name,
            country_code=cc,
            min_similarity=settings.geoparser_gazetteer_min_similarity,
            within_location_id=within_location_id,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort, fall back to LocationIQ
        logger.warning(
            "[geoparser] gazetteer lookup failed for %r (%s) — trying LocationIQ",
            top.name, exc,
        )
        if trace is not None:
            trace.failed = True
        return None

    if not hit:
        return None
    alias_location_id = _alias_location_id(hit)
    if alias_only and not alias_location_id:
        return None

    # Country guard. Reject a hit outside the expected set — and, when an
    # expected set is given, reject one whose countryCode is missing entirely:
    # an unverifiable hit must not bypass the guardrail, so fall back to
    # LocationIQ instead of accepting it.
    country = (hit.get("countryCode") or "").lower()
    if expected and country not in expected:
        logger.info(
            "[geoparser] gazetteer hit %r resolved to %r (missing/outside expected %s) — skipping",
            hit.get("name"), country or None, sorted(expected),
        )
        return None

    # Feature-class guard, mirroring `_REJECTED_CLASSES` on the LocationIQ
    # tier: reject linear/zonal GeoNames classes that can't be meaningfully
    # promoted to a point L4. The field is already selected in the query.
    # An area (a state, a region, a mountain range) is refused the same way.
    # A place alias is a person's answer: it needs neither check.
    if not alias_location_id and _not_a_point(hit, trace):
        logger.info(
            "[geoparser] gazetteer hit %r is feature %s/%s (a line or an area, not a point) — skipping",
            hit.get("name"), hit.get("featureClass"), hit.get("featureCode"),
        )
        return None

    try:
        lat = float(hit["latitude"])
        lng = float(hit["longitude"])
    except (KeyError, TypeError, ValueError):
        return None

    logger.info(
        "[geoparser] gazetteer resolved %r -> %r (%s, score=%.2f, exact=%s)",
        top.name, hit.get("name"), country, float(hit.get("score") or 0), hit.get("exact"),
    )
    return GeoparseResult(
        candidate=top.name,
        kind=top.kind,
        field=top.field,
        lat=lat,
        lng=lng,
        country_code=country or None,
        osm_class=None,
        osm_type=f"geonames:{hit.get('featureCode') or ''}",
        importance=float(hit.get("score") or 0.0),
        display_name=hit.get("name") or top.name,
        raw=hit,
        location_id=alias_location_id,
    )


def geoparse_signal(
    title: str | None,
    description: str | None = None,
    *,
    expected_country_codes: set[str] | None = None,
    relative_phrases: bool = False,
) -> GeoparseResult | None:
    """Parse a signal's title + description into a single resolved location.

    `relative_phrases` also extracts places after "east of", "out of",
    "from", "to" etc. (see `_RELATIVE_PATTERN`). Off for signals; the
    hotline turns it on.

    Returns None for any failure path:
      - empty inputs
      - no extractable candidates (all stopwords / regex didn't match)
      - top candidate has no Nominatim result above the importance floor
      - Nominatim is unhealthy (circuit open) or unreachable

    The caller is expected to handle None as "no geoparser enrichment" and
    fall back to whatever coord-based resolution it does today. When that
    coord-based fallback needs a human label for the resulting L4
    location, call `extract_top_candidate(title, description)` separately
    — the extraction stages succeed even when Nominatim doesn't.
    """
    expected = expected_country_codes or _configured_country_codes()

    candidates: list[Candidate] = []
    candidates.extend(_extract_from_text(title or "", "title", relative_phrases=relative_phrases))
    candidates.extend(_extract_from_text(description or "", "body", relative_phrases=relative_phrases))

    if not candidates:
        logger.info("[geoparser] no candidates extracted from text")
        return None

    # Classify + disqualify
    for c in candidates:
        c.kind = "landmark" if _is_landmark(c.name) else "admin"
        source_text = title if c.field == "title" else (description or "")
        c.disqualified = _is_disqualified(c, source_text or "")

    ranked = _rank(candidates)
    top = ranked[0]
    logger.info(
        "[geoparser] top candidate %r (kind=%s, field=%s, score=%.3f, "
        "disqualified=%s, %d total candidates)",
        top.name, top.kind, top.field, top.score, top.disqualified, len(ranked),
    )

    # Hard fail: if even the top candidate is disqualified, there's nothing to do
    if top.disqualified:
        logger.info("[geoparser] top candidate %r is disqualified — bailing", top.name)
        return None

    return _resolve_candidate(top, expected)


def latin_name_key(name: str) -> str:
    """Spelling-tolerant key for comparing two Latin spellings of one place:
    drop a leading article and a trailing "state", fold q/g ("Qadarif" /
    "Gedaref"), then keep consonants only ("Sinnar" / "Sennar")."""
    key = re.sub(r"^(al|el|ad|ed|an|as|at|ar|az|ash)[\s-]+", "", name.casefold().strip())
    key = re.sub(r"\s+state$", "", key).replace("q", "g")
    key = re.sub(r"[^a-z]", "", key)
    return re.sub(r"(.)\1+", r"\1", re.sub(r"[aeiouy]", "", key))


def _spellings_agree(a: str, b: str) -> bool:
    """True when two Latin spellings plausibly name the same place."""
    ka, kb = latin_name_key(a), latin_name_key(b)
    return bool(ka and kb) and (
        ka == kb or difflib.SequenceMatcher(None, ka, kb).ratio() >= 0.8
    )


def _resolve_arabic(
    arabic_name: str,
    latin_name: str,
    expected: set[str],
    accept: AcceptPoint | None,
    within_location_id: str | None = None,
    trace: GeocodeTrace | None = None,
) -> GeoparseResult | None:
    """Exact gazetteer match on a place's Arabic spelling (GeoNames' Arabic
    alternate names), tried before the Latin tiers: it skips transliteration,
    where "Tendalti" fuzzy-matched El Fasher. A hit counts only when its Latin
    name agrees with `latin_name` — GeoNames is volunteer-edited, and Kosti
    carries Rabak's Arabic name (ربك) — and `accept` takes it. Also tries the
    name without a leading ال, which posts and GeoNames attach inconsistently.

    A place-alias hit (a person mapped this Arabic spelling to a CLEAR
    location) skips the Latin-name and feature-class checks, which guard
    against GeoNames' own errors; `accept` still applies. For a landmark name
    only an alias hit counts: GeoNames holds few facilities."""
    alias_only = _is_landmark(latin_name)
    cc = next(iter(expected)).upper() if len(expected) == 1 else None
    queries = [arabic_name]
    if arabic_name.startswith("ال") and len(arabic_name) > 4:
        queries.append(arabic_name[2:])
    for query in queries:
        try:
            hit = graphql.resolve_gazetteer_location(
                query, country_code=cc, within_location_id=within_location_id
            )
        except Exception as exc:  # noqa: BLE001 — best-effort, the Latin tiers follow
            logger.warning("[geoparser] Arabic gazetteer lookup failed for %r (%s)", query, exc)
            if trace is not None:
                trace.failed = True
            return None
        if not hit:
            continue
        country = (hit.get("countryCode") or "").lower()
        if expected and country not in expected:
            continue
        alias_location_id = _alias_location_id(hit)
        if alias_only and not alias_location_id:
            continue
        # Same guard as the Latin gazetteer tier: a river or road ("النيل
        # الأبيض") would become a point at an arbitrary spot along it.
        # An area is refused too: "الخرطوم" matches Khartoum state, and its
        # centre point is not where the post is.
        if not alias_location_id and _not_a_point(hit, trace):
            logger.info(
                "[geoparser] Arabic %r matched %r, feature %s/%s (a line or an area, not a point) — skipping",
                query, hit.get("name"), hit.get("featureClass"), hit.get("featureCode"),
            )
            if trace is not None and trace.area:
                # The name is the area's own. The same name without its
                # article would find the area's capital.
                return None
            continue
        if not alias_location_id and not _spellings_agree(hit.get("name") or "", latin_name):
            logger.info(
                "[geoparser] Arabic %r matched %r, which disagrees with %r — skipping",
                query, hit.get("name"), latin_name,
            )
            continue
        try:
            lat, lng = float(hit["latitude"]), float(hit["longitude"])
        except (KeyError, TypeError, ValueError):
            continue
        if accept is not None and not accept(lat, lng):
            logger.info("[geoparser] Arabic %r matched %r outside the caller's area", query, hit.get("name"))
            if trace is not None:
                trace.outside = True
            continue
        logger.info("[geoparser] Arabic %r resolved to %r (%s)", query, hit.get("name"), country)
        return GeoparseResult(
            candidate=latin_name,
            kind="admin",
            field="body",
            lat=lat,
            lng=lng,
            country_code=country or None,
            osm_class=None,
            osm_type=f"geonames:{hit.get('featureCode') or ''}",
            importance=float(hit.get("score") or 0.0),
            display_name=hit.get("name") or latin_name,
            raw=hit,
            location_id=alias_location_id,
        )
    return None


def arabic_lookup_applies(name: str) -> bool:
    """Whether `geocode_place_name` would try an Arabic spelling of `name`:
    only with the gazetteer on. Landmark names are tried too: GeoNames holds
    few facilities, but a place alias can map a facility's Arabic name
    ("محطة أم دباكر") to its CLEAR location. Callers check this before paying
    to obtain the spelling."""
    return bool(name.strip()) and settings.geoparser_use_gazetteer


def geocode_place_name(
    name: str,
    *,
    expected_country_codes: set[str] | None = None,
    accept: AcceptPoint | None = None,
    arabic_name: str | None = None,
    within_location_id: str | None = None,
    trace: GeocodeTrace | None = None,
) -> GeoparseResult | None:
    """Geocode a place name that was already extracted elsewhere — e.g. by
    Claude reading an Arabic X post, which the regex extraction stages above
    can't parse. Skips extraction/ranking and runs the same two resolution
    tiers as `geoparse_signal` (gazetteer, then LocationIQ). None on a miss,
    an empty name, or a geocoder outage.

    `accept` filters candidate points in both tiers: a gazetteer hit it
    rejects falls through to LocationIQ, and LocationIQ picks the best result
    it accepts rather than the most important one.

    `arabic_name`, the same place as the text writes it in Arabic, is tried
    first as an exact gazetteer match (see `_resolve_arabic`).

    `within_location_id` (a CLEAR location id, usually the area `accept`
    tests) lets the gazetteer itself choose among same-named places by area,
    instead of returning the most populous one; `accept` stays as the check.

    `trace`, when given, is filled in on the way: a candidate that `accept`
    refused, or a lookup tier that failed. It tells a real miss from an
    outage, and "no candidate" from "a candidate outside the area"."""
    if not name or not name.strip():
        return None
    name = name.strip()
    top = Candidate(
        name=name,
        field="body",
        position=0,
        kind="landmark" if _is_landmark(name) else "admin",
        extraction_reason="given",
    )
    expected = expected_country_codes or _configured_country_codes()
    if arabic_name and arabic_name.strip() and arabic_lookup_applies(name):
        # A trace of our own when the caller has none: `area` must be read here.
        arabic_trace = trace if trace is not None else GeocodeTrace()
        hit = _resolve_arabic(arabic_name.strip(), name, expected, accept, within_location_id, arabic_trace)
        if hit is not None:
            return hit
        if arabic_trace.area:
            return None
    return _resolve_candidate(top, expected, accept, within_location_id, trace)


def gazetteer_knows(name: str, country_code: str, *, arabic_name: str | None = None) -> bool:
    """True when the gazetteer holds an exact match for the place anywhere in
    the country: for `name`, or for `arabic_name`, the same place as the text
    writes it in Arabic. Asked after a miss inside an area: the gazetteer
    filters by area on the server, so a same-named place elsewhere is
    invisible to the area lookup.

    A hit counts under the rules the lookups themselves apply. Exact matches
    only: a fuzzy hit elsewhere is usually another place. Not a river or a
    road. An Arabic hit must agree with `name` in Latin script: GeoNames
    gives Kosti the Arabic name of Rabak. False when the gazetteer is off or
    fails."""
    if not settings.geoparser_use_gazetteer:
        return False
    for query, is_arabic in ((arabic_name, True), (name, False)):
        if not query or not query.strip():
            continue
        try:
            hit = graphql.resolve_gazetteer_location(query.strip(), country_code=country_code.upper())
        except Exception as exc:  # noqa: BLE001 — only a hint for the review queue
            logger.info("[geoparser] gazetteer check for %r failed: %s", query, exc)
            return False
        if not hit or not hit.get("exact"):
            continue
        # A place alias is a person's answer for this spelling: it needs none
        # of the checks below, which guard against GeoNames' own errors.
        if _alias_location_id(hit):
            return True
        if _not_a_point(hit):
            continue
        if is_arabic and not _spellings_agree(hit.get("name") or "", name):
            continue
        return True
    return False


def gazetteer_names_area(name: str, country_code: str, *, arabic_name: str | None = None) -> bool:
    """True when the gazetteer's exact match for the name, anywhere in the
    country, is an area no point stands for (`_names_an_area`): for `name`,
    or for `arabic_name`, the same name as the text writes it in Arabic.

    Asked after a miss inside an area. The lookup there is filtered by area
    on the server, and a state's centre point can lie outside the district
    that was searched: "الخرطوم" inside Bahri finds nothing, and would be
    recorded as a place nobody could find. False when the gazetteer is off
    or fails."""
    if not settings.geoparser_use_gazetteer:
        return False
    for query in (arabic_name, name):
        if not query or not query.strip():
            continue
        try:
            hit = graphql.resolve_gazetteer_location(query.strip(), country_code=country_code.upper())
        except Exception as exc:  # noqa: BLE001 — only keeps a name out of the review queue
            logger.info("[geoparser] gazetteer area check for %r failed: %s", query, exc)
            return False
        if hit and hit.get("exact") and not _alias_location_id(hit) and _names_an_area(hit):
            return True
    return False


def exact_gazetteer_match(
    name: str,
    country_code: str,
    *,
    arabic_name: str | None = None,
    within_location_id: str,
    min_population: int | None = None,
    feature_codes: frozenset[str] = frozenset(),
) -> GeoparseResult | None:
    """The gazetteer's exact match for a place inside one CLEAR location, or
    None. For a caller that looks for a place outside the area a lookup was
    made in, one location at a time.

    With ``arabic_name`` (the place as the text writes it in Arabic) only
    that spelling is asked, with and without a leading ال; else ``name``.
    A ``name`` that is itself in Arabic script (the model copied it from the
    post) is asked the same way, and has no Latin name to agree with.
    The hit counts under the rules of the lookups themselves: exact only, in
    the country, not a river or a road, and an Arabic hit must agree with
    ``name`` in Latin script. A place alias needs none of these.

    ``min_population`` limits a match to a place of that many people, or one
    whose feature code is in ``feature_codes``. None: no limit.

    A lookup that fails raises. The caller counts the locations that hold the
    name: "no match here" and "no answer" must not look the same, or a failed
    request would hide a second match and make the first look unique."""
    if not settings.geoparser_use_gazetteer or not name or not name.strip():
        return None
    name = name.strip()
    arabic = (arabic_name or "").strip()
    latin = latin_name_key(name)
    if not arabic and not latin:
        arabic = name
    if arabic:
        queries = [arabic] + ([arabic[2:]] if arabic.startswith("ال") and len(arabic) > 4 else [])
    else:
        queries = [name]
    for query in queries:
        hit = graphql.resolve_gazetteer_location(
            query, country_code=country_code.upper(), within_location_id=within_location_id
        )
        if not hit or not hit.get("exact"):
            continue
        if (hit.get("countryCode") or "").lower() != country_code.lower():
            continue
        alias_location_id = _alias_location_id(hit)
        if not alias_location_id:
            # GeoNames answers a facility's name with the wrong place (see
            # `_resolve_candidate`): for a landmark only an alias counts.
            if _is_landmark(name):
                continue
            if _not_a_point(hit):
                continue
            if arabic and latin and not _spellings_agree(hit.get("name") or "", name):
                continue
            if min_population is not None:
                listed = (hit.get("featureCode") or "").upper() in feature_codes
                if not listed and float(hit.get("population") or 0) < min_population:
                    continue
        try:
            lat, lng = float(hit["latitude"]), float(hit["longitude"])
        except (KeyError, TypeError, ValueError):
            continue
        return GeoparseResult(
            candidate=name,
            kind="landmark" if _is_landmark(name) else "admin",
            field="body",
            lat=lat,
            lng=lng,
            country_code=country_code.lower(),
            osm_class=None,
            osm_type=f"geonames:{hit.get('featureCode') or ''}",
            importance=float(hit.get("score") or 0.0),
            display_name=hit.get("name") or name,
            raw=hit,
            location_id=alias_location_id,
        )
    return None


def _resolve_candidate(
    top: Candidate,
    expected: set[str],
    accept: AcceptPoint | None = None,
    within_location_id: str | None = None,
    trace: GeocodeTrace | None = None,
) -> GeoparseResult | None:
    """Resolve one ranked candidate to coordinates: the offline gazetteer
    first, then LocationIQ with transliteration variants. `accept` (see
    `geocode_place_name`) applies to both tiers."""
    # ── Hybrid tier 1: offline GeoNames gazetteer (clear-api) ─────────
    # Transliteration-tolerant and quota-free; resolves the bulk of place
    # names. LocationIQ below only handles what the gazetteer misses.
    #
    # Landmarks skip the gazetteer: GeoNames covers admin units and populated
    # places densely but carries few POIs, so a fuzzy match on "Nyala Airport"
    # lands on the populated place "Nyala" — a city centroid the same-A2 check
    # can't reject (it's in the same district as the airport), which would pin
    # a permanently wrong landmark L4. Landmarks go straight to LocationIQ
    # tier 2, which does resolve aerodromes / hospitals / camps.
    #
    # A landmark still asks for a place alias (`alias_only`): a person may have
    # mapped the facility's name to its CLEAR location, and that answer is
    # exact, unlike a fuzzy GeoNames match.
    if settings.geoparser_use_gazetteer:
        # A trace of our own when the caller has none: `area` must be read here.
        gazetteer_trace = trace if trace is not None else GeocodeTrace()
        gaz = _resolve_via_gazetteer(
            top, expected, within_location_id, gazetteer_trace, alias_only=top.kind == "landmark"
        )
        if gaz is None and gazetteer_trace.area:
            # The name is an area's own (a state, a region). LocationIQ would
            # answer with the area's capital, or with the area at its centre.
            logger.info("[geoparser] %r names an area, not a place — no point", top.name)
            return None
        if gaz is not None:
            if accept is None or accept(gaz.lat, gaz.lng):
                return gaz
            if trace is not None:
                trace.outside = True
            # The gazetteer returns one row (best score / most populous), so a
            # same-named place elsewhere hides the right one: ask LocationIQ.
            logger.info(
                "[geoparser] gazetteer hit %r for %r rejected by caller — trying LocationIQ",
                gaz.display_name, top.name,
            )

    # ── Hybrid tier 2: LocationIQ / Nominatim (landmarks & POIs) ──────
    # Resolve the top candidate via Nominatim, trying transliteration
    # variants. Variants are dedup'd and returned in priority order; the
    # first one with a usable result wins.
    country_codes_param = ",".join(sorted(expected))
    variants = _query_variants(top.name)
    best: dict | None = None
    matched_variant: str | None = None
    # Per-variant tallies so the "no usable result" log distinguishes
    # "Nominatim returned zero rows" from "Nominatim returned rows we
    # filtered out". Without this, the two cases collapse into the same
    # message and every failed lookup looks like an OSM data gap — even
    # when the real cause is our class-filter (highway/railway/waterway).
    per_variant_counts: list[tuple[str, int, int]] = []  # (variant, raw, kept)
    # Only a caller that traces asks the search to report a failure.
    outcome: dict = {}
    search_extra = {"outcome": outcome} if trace is not None else {}
    for variant in variants:
        results = nominatim.search(variant, country_codes=country_codes_param, limit=5, **search_extra)
        if trace is not None and outcome.get("failed"):
            trace.failed = True
        raw = len(results or [])
        if not results:
            per_variant_counts.append((variant, 0, 0))
            continue
        candidate_best = _pick_best_nominatim_result(results, expected, accept, trace)
        # kept ≈ 1 if the picker returned anything post-filter, else 0.
        # (`raw - kept` is the count filtered out — usually class-rejected.)
        kept = 1 if candidate_best else 0
        per_variant_counts.append((variant, raw, kept))
        if candidate_best:
            best = candidate_best
            matched_variant = variant
            break

    if not best:
        # Format each variant as "name=raw→kept" so a caller can spot
        # whether the miss was empty-response (raw=0) or filter-driven
        # (raw>0, kept=0) at a glance.
        summary = ", ".join(
            f"{v}={r}→{k}" for (v, r, k) in per_variant_counts
        )
        logger.info(
            "[geoparser] nominatim returned no usable result for %r "
            "(tried %d variant(s): %s)",
            top.name, len(variants), summary,
        )
        if trace is not None and trace.whole_state and not trace.outside:
            # Over every spelling and both tiers, the only answer was a whole
            # state: the name is an area's ("الخرطوم" passes the name check,
            # and with the gazetteer off nothing else says so). A place that
            # was found outside the caller's area is a real miss, and stays one.
            trace.area = True
        return None

    if matched_variant and matched_variant != top.name:
        logger.info(
            "[geoparser] matched %r via transliteration variant %r",
            top.name, matched_variant,
        )

    try:
        lat = float(best["lat"])
        lng = float(best["lon"])
    except (KeyError, TypeError, ValueError):
        logger.warning("[geoparser] nominatim result missing lat/lon: %r", best)
        return None

    return GeoparseResult(
        candidate=top.name,
        kind=top.kind,
        field=top.field,
        lat=lat,
        lng=lng,
        country_code=_extract_country_code(best),
        osm_class=best.get("class"),
        osm_type=best.get("type"),
        importance=float(best.get("importance", 0)),
        display_name=str(best.get("display_name") or ""),
        raw=best,
    )
