"""Per-crisis enrichment: gather events → RAG-ground → generate → write back.

The three LLM generators (narrative / scenarios / needs) each run their own
knowledgebase RAG search scoped to the crisis's country + event types, then ask
the ``narrative`` LLM role for a structured output. Failures are isolated: one
generator raising leaves the others intact and simply skips its field on the
write-back — the crisis still flips to ENRICHED so it leaves the queue.
"""

import json
import logging

from clear_context_pipeline.defs.crisis.population import compute_population_in_area
from clear_context_pipeline.defs.crisis.prompts import (
    NARRATIVE_SYSTEM_PROMPT,
    NEEDS_ANALYSIS_SYSTEM_PROMPT,
    SCENARIOS_SYSTEM_PROMPT,
    build_narrative_prompt,
    build_needs_analysis_prompt,
    build_scenarios_prompt,
)
from clear_context_pipeline.defs.crisis.schemas import (
    NEEDS_SECTORS,
    CrisisNarrative,
    CrisisNeedsAnalysis,
    CrisisScenarios,
)
from clear_context_pipeline.defs.situation.rag_helper import fetch_rag_context
from clear_context_pipeline.providers import clear_api, make_llm_provider
from clear_context_pipeline.providers.llm import TRANSIENT_LLM_ERRORS, LLMProvider

logger = logging.getLogger(__name__)

# Needs-analysis emits 6 full sector entries + a 4-bullet summary — it needs
# real headroom or the JSON is truncated mid-sector. Narrative + scenarios are
# short.
_NARRATIVE_MAX_TOKENS = 1500
_SCENARIOS_MAX_TOKENS = 1500
_NEEDS_MAX_TOKENS = 4096


# ─── Context gathering ────────────────────────────────────────────────────

_LOCATION_KEYS = ("originLocation", "generalLocation", "destinationLocation")


def gather_events(event_ids: list[str]) -> list[dict]:
    """Fetch full event detail (title / types / severity / location metadata)
    for each linked event. Skips events that fail to resolve — a missing event
    shouldn't sink the whole crisis."""
    events: list[dict] = []
    for eid in event_ids:
        try:
            event = clear_api.get_event_for_crisis(eid)
        except Exception:  # noqa: BLE001 — one bad event must not sink the crisis
            logger.warning("[crisis:enrich] event %s fetch failed — skipping", eid, exc_info=True)
            continue
        if event:
            events.append(event)
    return events


def collect_location_names(events: list[dict]) -> list[str]:
    """Distinct origin/destination/general location names across the events —
    the human-readable list the prompts embed."""
    names: list[str] = []
    seen: set[str] = set()
    for event in events:
        for key in _LOCATION_KEYS:
            loc = event.get(key) or {}
            name = loc.get("name")
            if name and name not in seen:
                seen.add(name)
                names.append(name)
    return names


def collect_event_types(events: list[dict]) -> list[str]:
    """Distinct hazard types across the events — the RAG ``eventTypes`` filter."""
    types: list[str] = []
    seen: set[str] = set()
    for event in events:
        for t in event.get("types") or []:
            if t and t not in seen:
                seen.add(t)
                types.append(t)
    return types


def collect_district_ids(events: list[dict]) -> list[str]:
    """Distinct location ids across the events — the population computation's
    input (the admin areas the crisis touches)."""
    ids: list[str] = []
    seen: set[str] = set()
    for event in events:
        for key in _LOCATION_KEYS:
            loc = event.get(key) or {}
            lid = loc.get("id")
            if lid and lid not in seen:
                seen.add(lid)
                ids.append(lid)
    return ids


def compute_time_range(events: list[dict]) -> dict | None:
    """The crisis's temporal window from its events — [min(validFrom),
    max(validTo)] — used as the RAG ``timeRange`` filter so the knowledgebase
    search prefers reports covering the crisis period, not the country's whole
    history. ``validFrom``/``validTo`` come from the GraphQL ``DateTime`` scalar,
    which serialises uniformly as ISO-8601 UTC (``…Z``), so these strings sort
    lexicographically and plain min/max is correct without parsing. Returns None
    when no event carries usable bounds (search then stays time-unscoped)."""
    froms = [e["validFrom"] for e in events if e.get("validFrom")]
    tos = [e["validTo"] for e in events if e.get("validTo")]
    if not froms and not tos:
        return None
    time_range: dict = {}
    if froms:
        time_range["from"] = min(froms)
    if tos:
        time_range["to"] = max(tos)
    return time_range or None


def resolve_country_id(crisis: dict, events: list[dict], a0_ids: set[str]) -> str | None:
    """Find the crisis's country admin-0 ``locations.id`` for RAG country
    scoping. Scans the crisis's general location and every event location's id +
    ancestorIds for the first id that is a known A0. Returns None when no A0 is
    in reach (RAG then falls back to event-type-only scoping)."""
    def _candidates():
        gl = crisis.get("generalLocation") or {}
        if gl.get("id"):
            yield gl["id"]
        yield from gl.get("ancestorIds") or []
        for event in events:
            for key in _LOCATION_KEYS:
                loc = event.get(key) or {}
                if loc.get("id"):
                    yield loc["id"]
                yield from loc.get("ancestorIds") or []

    matched: list[str] = []
    for cid in _candidates():
        if cid in a0_ids and cid not in matched:
            matched.append(cid)
    if not matched:
        return None
    if len(matched) > 1:
        # `countryLocationId` is single-valued, so a cross-border crisis can only
        # be scoped to ONE country — the others' reports won't be retrieved. Log
        # it so the gap is visible (a per-country union search is the real fix).
        logger.warning(
            "[crisis:enrich] crisis %s spans %d countries %s — RAG scopes to the first (%s) only",
            crisis.get("id"), len(matched), matched, matched[0],
        )
    return matched[0]


# ─── RAG-grounded generators ──────────────────────────────────────────────

def _rag_evidence(query: str, filters: dict | None, country_id: str | None, limit: int) -> str:
    """Run one knowledgebase search under the given filters (country is merged in
    by ``fetch_rag_context``) and return the formatted evidence block (empty
    string on no hits — the prompt then notes the absence and the LLM leans on
    the events alone)."""
    rag = fetch_rag_context(
        query=query, limit=limit, filters=filters or None, country_id=country_id,
    )
    return "" if rag.is_empty else rag.formatted_for_prompt


def generate_narrative(
    llm: LLMProvider, *, events, locations, event_types, filters, country_id, cache_key,
) -> CrisisNarrative | None:
    evidence = _rag_evidence(
        query=f"{' '.join(locations)} {' '.join(event_types)} humanitarian crisis "
        "displacement needs response scale",
        filters=filters, country_id=country_id, limit=10,
    )
    user = build_narrative_prompt(events, locations, evidence)
    try:
        return llm.complete_structured(
            system=NARRATIVE_SYSTEM_PROMPT, user=user, schema=CrisisNarrative,
            max_tokens=_NARRATIVE_MAX_TOKENS, cache_key=cache_key,
        )
    except TRANSIENT_LLM_ERRORS:
        # Transient provider outage (survived the per-call retry) — let it
        # propagate so the drain's attempt-bounded retry re-runs the whole
        # crisis, instead of permanently blanking this field.
        raise
    except Exception:  # noqa: BLE001 — component-level isolation
        logger.exception("[crisis:enrich] narrative generation failed — skipping field")
        return None


def generate_scenarios(
    llm: LLMProvider, *, events, locations, event_types, filters, country_id, cache_key,
) -> CrisisScenarios | None:
    evidence = _rag_evidence(
        query=f"{' '.join(locations)} {' '.join(event_types)} outlook trajectory "
        "scenario risk humanitarian access response capacity",
        filters=filters, country_id=country_id, limit=10,
    )
    user = build_scenarios_prompt(events, locations, evidence)
    try:
        return llm.complete_structured(
            system=SCENARIOS_SYSTEM_PROMPT, user=user, schema=CrisisScenarios,
            max_tokens=_SCENARIOS_MAX_TOKENS, cache_key=cache_key,
        )
    except TRANSIENT_LLM_ERRORS:
        raise  # transient — let the drain retry the whole crisis (see generate_narrative)
    except Exception:  # noqa: BLE001
        logger.exception("[crisis:enrich] scenarios generation failed — skipping field")
        return None


def generate_needs_analysis(
    llm: LLMProvider, *, events, locations, event_types, filters, country_id, cache_key,
) -> CrisisNeedsAnalysis | None:
    # The needs analysis is sector-scoped by definition, so narrow the search to
    # the six NRC SAF sectors on top of the shared country/type/time filters.
    needs_filters = {**(filters or {}), "needSectors": list(NEEDS_SECTORS)}
    evidence = _rag_evidence(
        query=f"{' '.join(locations)} {' '.join(event_types)} humanitarian needs "
        "shelter WASH protection health food security education sector severity",
        filters=needs_filters, country_id=country_id, limit=15,
    )
    user = build_needs_analysis_prompt(events, locations, evidence)
    try:
        return llm.complete_structured(
            system=NEEDS_ANALYSIS_SYSTEM_PROMPT, user=user, schema=CrisisNeedsAnalysis,
            max_tokens=_NEEDS_MAX_TOKENS, cache_key=cache_key,
        )
    except TRANSIENT_LLM_ERRORS:
        raise  # transient — let the drain retry the whole crisis (see generate_narrative)
    except Exception:  # noqa: BLE001
        logger.exception("[crisis:enrich] needs-analysis generation failed — skipping field")
        return None


# ─── Orchestration ────────────────────────────────────────────────────────

# Sentinel outcomes the drain uses to classify a crisis's processing.
ENRICHED = "enriched"      # generated + wrote back + flipped to ENRICHED
EMPTY = "empty"            # no resolvable events — flipped to ENRICHED to leave the queue


def enrich_one_crisis(crisis: dict, *, a0_ids: set[str]) -> str:
    """Enrich a single PENDING crisis end-to-end and flip it to ENRICHED.

    Always marks the crisis ENRICHED at the end (even when a generator failed or
    the crisis has no resolvable events) so a persistently-bad crisis leaves the
    oldest-first queue instead of poisoning its head. Raises only on an
    infrastructural failure (clear-api down) — the drain then retries it."""
    crisis_id = crisis["id"]
    event_ids = [e["id"] for e in (crisis.get("events") or []) if e.get("id")]
    events = gather_events(event_ids)

    if not events:
        logger.warning("[crisis:enrich] crisis %s has no resolvable events — marking ENRICHED", crisis_id)
        clear_api.mark_crisis_enriched(crisis_id)
        return EMPTY

    locations = collect_location_names(events)
    event_types = collect_event_types(events)
    district_ids = collect_district_ids(events)
    country_id = resolve_country_id(crisis, events, a0_ids)
    cache_key = f"crisis-enrich:{crisis_id}"

    # Shared knowledgebase filters for all three generators: the crisis's hazard
    # types + its temporal window (country is merged per-call from country_id).
    # The needs generator narrows further by NRC sectors.
    base_filters: dict = {}
    if event_types:
        base_filters["eventTypes"] = event_types
    time_range = compute_time_range(events)
    if time_range:
        base_filters["timeRange"] = time_range

    llm = make_llm_provider("narrative")
    narrative = generate_narrative(
        llm, events=events, locations=locations, event_types=event_types,
        filters=base_filters, country_id=country_id, cache_key=cache_key,
    )
    scenarios = generate_scenarios(
        llm, events=events, locations=locations, event_types=event_types,
        filters=base_filters, country_id=country_id, cache_key=cache_key,
    )
    needs = generate_needs_analysis(
        llm, events=events, locations=locations, event_types=event_types,
        filters=base_filters, country_id=country_id, cache_key=cache_key,
    )

    # populationInArea is best-effort — a raster/geometry failure returns None
    # and simply leaves the existing value untouched.
    try:
        population_in_area = compute_population_in_area(district_ids)
    except Exception:  # noqa: BLE001 — population must never block enrichment
        logger.exception("[crisis:enrich] population computation failed for %s", crisis_id)
        population_in_area = None

    # Write narrative (title + stringified summary) + scenarios + population in
    # one mutation; only the fields we actually generated are sent (None fields
    # leave the DB value untouched).
    title = narrative.title if narrative else None
    summary = (
        json.dumps({"description": narrative.description, "tldr": narrative.tldr})
        if narrative else None
    )
    scenarios_payload = scenarios.model_dump() if scenarios else None
    if any(v is not None for v in (title, summary, scenarios_payload, population_in_area)):
        clear_api.update_crisis_population(
            crisis_id,
            population_in_area=population_in_area,
            title=title,
            summary=summary,
            scenarios=scenarios_payload,
        )

    if needs:
        clear_api.set_crisis_needs_analysis(
            crisis_id,
            general_summary=needs.generalSummary,
            sector={k: v.model_dump() for k, v in needs.sector.items()},
        )

    clear_api.mark_crisis_enriched(crisis_id)
    logger.info(
        "[crisis:enrich] crisis %s enriched (narrative=%s scenarios=%s needs=%s pop=%s country=%s)",
        crisis_id, narrative is not None, scenarios is not None, needs is not None,
        population_in_area, country_id,
    )
    return ENRICHED
