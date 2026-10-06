"""The ``event.impact_prior`` handler (clear-api ADR-0010): what has typically
happened before for this Event's hazard type in this country.

Cases come from CLEAR first — its own Events of the same GLIDE type under the
Event's country, then knowledge-base passages — and a model decides which
candidates are genuine prior occurrences (one case per distinct prior event,
same hazard, same country, inside the horizon, citable), labelled by scope.
The model's usage is reported on the Task. Without a configured model the
handler falls back to the rule-based CLEAR-Events-only selection, so the
drain still produces evidenced priors (or ``no_prior_found``) rather than
failing every Task.

The web is deliberately not searched here: the Dagster Worker has no web
tool; the routine Worker (clear-mcp's ``clear-impact-prior`` skill) does.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

from clear_pipeline.defs.tasks.worker import TaskOutcome, register_handler
from clear_pipeline.providers import clear_api
from clear_pipeline.providers.llm import LLMProvider, make_llm_provider

logger = logging.getLogger(__name__)

KIND = "event.impact_prior"
METHOD_VERSION = "clear-pipeline-impact-prior@0.2.0"
DEFAULT_HORIZON_YEARS = 10
_PAGE = 25
_MAX_PAGES = 8
_KB_LIMIT = 15
_MAX_CANDIDATES = 40
_LLM_ROLE = "narrative"

# USD per million tokens (input, output), by model-id prefix. The Worker
# computes cost from its own table, as clear-api expects; an unknown model
# reports 0 with a note in the result rather than a guess.
_PRICE_PER_MTOK: list[tuple[str, float, float]] = [
    ("claude-opus-5", 15.0, 75.0),
    ("claude-sonnet-5", 3.0, 15.0),
    ("claude-sonnet-4", 3.0, 15.0),
    ("claude-haiku-4", 1.0, 5.0),
    ("claude-3-5-haiku", 0.8, 4.0),
]


def primary_location(event: dict[str, Any]) -> dict[str, Any] | None:
    """general → origin → destination, the order clear-api resolves the Event's country."""
    return event.get("generalLocation") or event.get("originLocation") or event.get("destinationLocation")


def resolve_country_id(event: dict[str, Any], country_ids: set[str]) -> str | None:
    """The level-0 ancestor of the Event's primary location (or the location itself at level 0)."""
    loc = primary_location(event)
    if not loc:
        return None
    if loc.get("level") == 0:
        return loc["id"]
    for ancestor in loc.get("ancestorIds") or []:
        if ancestor in country_ids:
            return ancestor
    return None


def district_id(event: dict[str, Any]) -> str | None:
    """The Event's level-2 location, when its primary location is one."""
    loc = primary_location(event)
    return loc["id"] if loc and loc.get("level") == 2 else None


def event_date(event: dict[str, Any]) -> str | None:
    return event.get("startedAt") or event.get("firstSignalCreatedAt")


# ── candidates ─────────────────────────────────────────────────────────────


def case_from_event(prior: dict[str, Any], *, input_district: str | None) -> dict[str, Any]:
    loc = primary_location(prior)
    scope = "district" if input_district and district_id(prior) == input_district else "country"
    title = (prior.get("title") or "").strip()
    description = (prior.get("description") or "").strip()
    return {
        "tier": "clear",
        "eventId": prior["id"],
        "occurredAt": event_date(prior),
        "locationLabel": loc.get("name") if loc else None,
        "scope": scope,
        "quote": (title or description)[:300],
    }


def case_from_passage(hit: dict[str, Any]) -> dict[str, Any]:
    text = (hit.get("chunkText") or "").strip().replace("\n", " ")
    return {
        "tier": "clear",
        "reportId": hit.get("reportId"),
        "sourceUrl": hit.get("sourceUrl"),
        "occurredAt": hit.get("publishedAt"),
        "locationLabel": hit.get("reportTitle"),
        "scope": "country",
        "quote": text[:300],
    }


def clear_candidates(*, hazard: str, country_id: str, since: str, until: str) -> list[dict[str, Any]]:
    """CLEAR Events of the hazard under the country inside the horizon."""
    items: list[dict[str, Any]] = []
    for page in range(_MAX_PAGES):
        res = clear_api.worker_events_page({
            "eventTypes": [hazard],
            "locationId": country_id,
            "from": since,
            "to": until,
            "includeDummy": False,
            "limit": _PAGE,
            "offset": page * _PAGE,
            "orderBy": "CREATED_DESC",
        })
        items.extend(res.get("items") or [])
        if not res.get("hasMore"):
            break
    return items


def kb_candidates(*, hazard: str, hazard_label: str, country_id: str, country_name: str, since: str, until: str) -> list[dict[str, Any]]:
    """Knowledge-base passages about the hazard in the country inside the horizon."""
    return clear_api.worker_search_knowledgebase(
        query=f"{hazard_label} in {country_name}: past impact, displacement, casualties, damage",
        filters={"countryLocationId": country_id, "eventTypes": [hazard], "timeRange": {"from": since, "to": until}},
        limit=_KB_LIMIT,
    )


# ── model selection ────────────────────────────────────────────────────────


class SelectedCase(BaseModel):
    candidate: int = Field(description="The candidate number from the list.")
    scope: Literal["district", "country"]
    occurred_at: str | None = Field(default=None, description="ISO-8601 date of the prior occurrence, if the candidate states one.")
    note: str | None = Field(default=None, description="Seasonality or context worth an analyst's attention. Never a reason to exclude.")


class ExcludedCandidate(BaseModel):
    candidate: int
    reason: str


class ImpactPriorSelection(BaseModel):
    cases: list[SelectedCase] = Field(description="One entry per distinct prior occurrence. Empty when nothing qualifies.")
    excluded: list[ExcludedCandidate] = Field(default_factory=list)
    reasoning: str = Field(description="Two or three sentences on how the line was drawn.")


_SYSTEM = """You are a humanitarian analyst's assistant building the evidence basis of an ImpactPrior:
what has typically happened before given a hazard type and a country. You are given the input Event and a
numbered list of candidate prior occurrences drawn from CLEAR's own Events and knowledge base.

A case is one DISTINCT prior occurrence of the SAME hazard type in the SAME country, inside the horizon,
that the candidate text actually describes (a date, a place, an impact). Rules:
- Several candidates describing the same occurrence are ONE case: keep the most specific, exclude the rest.
- A candidate that is the input Event itself, or its earlier phase (same place, continuous dates), is not a case.
- A general overview with no specific occurrence is background, not a case.
- Another hazard or another country is never a case, however relevant it looks.
- Season, climate drivers, conflict context and severity are notes on a case, never reasons to exclude.
- scope is "district" only when the candidate is in the input Event's district (it is marked), else "country".
Return every decision: cases to keep, candidates excluded with the rule that excluded each."""


def _candidate_lines(candidates: list[dict[str, Any]]) -> str:
    lines = []
    for i, c in enumerate(candidates, 1):
        kind = "CLEAR Event" if c.get("eventId") else "Report passage"
        where = c.get("locationLabel") or "unknown place"
        district = " [IN THE INPUT EVENT'S DISTRICT]" if c.get("scope") == "district" else ""
        when = c.get("occurredAt") or "date unknown"
        lines.append(f"{i}. ({kind}, {where}{district}, {when}) {c.get('quote') or ''}")
    return "\n".join(lines)


def _llm_provider() -> LLMProvider | None:
    try:
        return make_llm_provider(_LLM_ROLE)
    except RuntimeError as exc:
        logger.warning("[impact_prior] no LLM configured for role %s — rule-based selection: %s", _LLM_ROLE, exc)
        return None


def cost_usd(model: str, usage: dict[str, int] | None) -> float | None:
    """From the Worker's own price table; None when the model is unknown."""
    if not usage:
        return None
    for prefix, in_price, out_price in _PRICE_PER_MTOK:
        if model.startswith(prefix):
            return round((usage.get("input_tokens", 0) * in_price + usage.get("output_tokens", 0) * out_price) / 1_000_000, 6)
    return None


def usage_for_task(llm: LLMProvider) -> dict[str, Any] | None:
    usage = llm.last_usage
    if not usage:
        return None
    return {
        "model": llm.model,
        "inputTokens": usage.get("input_tokens", 0),
        "outputTokens": usage.get("output_tokens", 0),
        "costUsd": cost_usd(llm.model, usage) or 0.0,
    }


def select_cases(
    llm: LLMProvider, *, event: dict[str, Any], hazard: str, country_name: str, horizon: int, candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Ask the model which candidates are cases. Returns the basis and a
    record of the decision for the Task's result."""
    user = (
        f"Input Event: {event.get('title') or '(untitled)'} — hazard {hazard}, {country_name}, "
        f"started {event_date(event) or 'unknown'}. Horizon: {horizon} years.\n"
        f"{(event.get('description') or '')[:600]}\n\nCandidates:\n{_candidate_lines(candidates)}"
    )
    selection = llm.complete_structured(system=_SYSTEM, user=user, schema=ImpactPriorSelection, max_tokens=2048)
    basis: list[dict[str, Any]] = []
    seen: set[int] = set()
    for chosen in selection.cases:
        idx = chosen.candidate - 1
        if idx < 0 or idx >= len(candidates) or idx in seen:
            continue
        seen.add(idx)
        case = dict(candidates[idx])
        case["scope"] = chosen.scope
        if chosen.occurred_at:
            case["occurredAt"] = chosen.occurred_at
        if chosen.note:
            case["note"] = chosen.note
        basis.append(case)
    decision = {
        "model": llm.model,
        "reasoning": selection.reasoning,
        "excluded": [{"candidate": e.candidate, "reason": e.reason} for e in selection.excluded],
    }
    return basis, decision


# ── the handler ────────────────────────────────────────────────────────────


@register_handler(KIND)
def handle_impact_prior(context, task: dict[str, Any]) -> TaskOutcome:
    event_id = task["subjectId"]
    horizon = int((task.get("payload") or {}).get("horizonYears") or DEFAULT_HORIZON_YEARS)

    event = clear_api.worker_get_event(event_id)
    if not event:
        raise RuntimeError(f"Event {event_id} not found")
    types = event.get("types") or []
    if not types:
        return TaskOutcome(result={"searched": [], "candidates": 0, "reason": "event has no hazard type"})
    hazard = types[0]

    countries = {loc["id"]: loc for loc in clear_api.worker_locations_by_level(0)}
    country_id = resolve_country_id(event, set(countries))
    if not country_id:
        return TaskOutcome(result={"searched": [], "candidates": 0, "reason": "event has no resolvable country"})
    country_name = countries[country_id].get("name") or country_id

    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=365 * horizon)).isoformat()
    until = event_date(event) or now.isoformat()
    input_district = district_id(event)

    # 1. CLEAR Events, then 2. knowledge-base passages. Never the web here.
    events = [e for e in clear_candidates(hazard=hazard, country_id=country_id, since=since, until=until) if e["id"] != event_id]
    event_cases = [case_from_event(e, input_district=input_district) for e in events]
    try:
        passages = kb_candidates(
            hazard=hazard, hazard_label=hazard, country_id=country_id, country_name=country_name, since=since, until=until,
        )
    except Exception:  # noqa: BLE001 — the KB is the second source; its outage is not the Task's failure
        context.log.warning("[impact_prior] knowledge-base search failed for %s — Events only", event_id, exc_info=True)
        passages = []
    passage_cases = [case_from_passage(h) for h in passages if h.get("sourceUrl") or h.get("reportId")]
    candidates = (event_cases + passage_cases)[:_MAX_CANDIDATES]

    searched = [
        {"tool": "eventsPage", "eventTypes": [hazard], "locationId": country_id, "from": since, "to": until, "hits": len(events)},
        {"tool": "searchKnowledgebase", "countryLocationId": country_id, "eventTypes": [hazard], "from": since, "to": until, "hits": len(passages)},
    ]
    result: dict[str, Any] = {"searched": searched, "candidates": len(candidates), "method": METHOD_VERSION}
    if not candidates:
        result["cases"] = 0
        return TaskOutcome(result=result)

    # 3. The model draws the line; without one, CLEAR Events are the cases.
    usage: dict[str, Any] | None = None
    llm = _llm_provider()
    if llm is not None:
        basis, decision = select_cases(
            llm, event=event, hazard=hazard, country_name=country_name, horizon=horizon, candidates=candidates,
        )
        usage = usage_for_task(llm)
        result["selection"] = decision
        if usage and cost_usd(llm.model, llm.last_usage) is None:
            result["note"] = f"no price known for {llm.model}; costUsd reported as 0"
    else:
        basis = event_cases
        result["selection"] = {"model": None, "reasoning": "no model configured: CLEAR Events of the hazard in the country are the cases"}

    result["cases"] = len(basis)
    if not basis:
        return TaskOutcome(result=result, usage=usage)

    return TaskOutcome(
        result=result,
        usage=usage,
        impact_prior={
            "hazardType": hazard,
            "countryLocationId": country_id,
            "geographicScope": "district" if all(c["scope"] == "district" for c in basis) else "country",
            "horizonYears": horizon,
            "numberOfCases": len(basis),
            "basis": basis,
            "methodVersion": METHOD_VERSION,
        },
    )
