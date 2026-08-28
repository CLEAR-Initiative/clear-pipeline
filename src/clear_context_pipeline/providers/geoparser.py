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

import logging
import re
from dataclasses import dataclass, field
from typing import Literal

from clear_context_pipeline.providers import clear_api as graphql, nominatim
from clear_context_pipeline.signals.config import settings

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


# ─── Extraction ───────────────────────────────────────────────────────────


def _is_stopword(name: str) -> bool:
    """True if the candidate is in either stopword list."""
    return name.lower().strip() in STOPWORDS


def _extract_from_text(text: str, field: Literal["title", "body"]) -> list[Candidate]:
    """Run both extraction patterns over `text` and return surviving candidates.

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


def _pick_best_nominatim_result(
    results: list[dict],
    expected_country_codes: set[str],
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
        country = _extract_country_code(r)
        if expected_country_codes and country and country not in expected_country_codes:
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


def _resolve_via_gazetteer(top: Candidate, expected: set[str]) -> GeoparseResult | None:
    """Hybrid tier 1: look the top candidate up in clear-api's offline
    GeoNames gazetteer. Returns a GeoparseResult on a confident hit, else
    None so the caller falls back to LocationIQ.

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
        )
    except Exception as exc:  # noqa: BLE001 — best-effort, fall back to LocationIQ
        logger.warning(
            "[geoparser] gazetteer lookup failed for %r (%s) — trying LocationIQ",
            top.name, exc,
        )
        return None

    if not hit:
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
    feature_class = (hit.get("featureClass") or "").upper()
    if feature_class in _REJECTED_FEATURE_CLASSES:
        logger.info(
            "[geoparser] gazetteer hit %r is feature class %s (linear/zonal) — skipping",
            hit.get("name"), feature_class,
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
    )


def geoparse_signal(
    title: str | None,
    description: str | None = None,
    *,
    expected_country_codes: set[str] | None = None,
) -> GeoparseResult | None:
    """Parse a signal's title + description into a single resolved location.

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
    candidates.extend(_extract_from_text(title or "", "title"))
    candidates.extend(_extract_from_text(description or "", "body"))

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
    if settings.geoparser_use_gazetteer and top.kind != "landmark":
        gaz = _resolve_via_gazetteer(top, expected)
        if gaz is not None:
            return gaz

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
    for variant in variants:
        results = nominatim.search(variant, country_codes=country_codes_param, limit=5)
        raw = len(results or [])
        if not results:
            per_variant_counts.append((variant, 0, 0))
            continue
        candidate_best = _pick_best_nominatim_result(results, expected)
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
