"""LLM alert relevance + severity classification (OpenRouter / DeepSeek).

Replaces per-source heuristic severity ladders for every signal source except
GDACS (which keeps its alert-level map). Relevance is returned on the
structured result for a later DB write; callers today only persist ``severity``.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Literal

from pydantic import BaseModel

from clear_pipeline.providers.llm import make_llm_provider

logger = logging.getLogger(__name__)

ALERT_SEVERITY_PROMPT_VERSION = "alert-severity-v1"
_LLM_MAX_TOKENS = 256

SYSTEM_PROMPT = """\
You are a humanitarian early-warning analyst. Classify each alert for operational relevance and humanitarian severity.

Return ONLY valid JSON (no markdown):
{
  "relevance": "RELEVANT" | "NOT_RELEVANT",
  "severity": 1 | 2 | 3 | 4 | 5,
}

============================================================
RELEVANCE
============================================================
Goal: keep alerts useful for active conflict dynamics, civilian harm, displacement, and immediate humanitarian access risks. Drop diplomatic noise, routine governance, crime/economy, and miscategorized non-events.

RELEVANT if any of:
- Active hostilities: clashes, shelling, drone/air strikes, bombings, battles, seizure of territory/strategic sites
- Civilian harm from conflict: deaths, injuries, attacks on civilians/camps/hospitals/aid workers or convoys
- Acute humanitarian consequences: new/large displacement; water/power/health infrastructure hit or shut down in conflict-affected areas; blocked or launched emergency aid for IDPs
- Immediate security shifts that change the operating picture (major redeployment against infiltration; control of strategic assets)
- Acute epidemic / famine / natural-hazard shocks with clear humanitarian impact

NOT_RELEVANT if mainly:
- Diplomatic meetings/visits, leader inspections, routine political statements
- Condemnations / statements of concern (ministry, AU, Arab League, etc.) without a new operational fact
- Retrospective investigation calls on past events with no new incident
- Admin/governance: appointments, decrees, port clearance, hospital rehab schedules
- Macroeconomy / natural resources (debt, gold production) unless tied to an active humanitarian shock
- Ordinary crime, smuggling, traffic accidents, labor strikes, teacher salary disputes
- Generic advocacy quotes with no new incident
- Court sentencing / legal process about past cooperation
- Protests/sit-ins with no clear link to active conflict or humanitarian access (exception: protests in active war zones like al-Fashir can be RELEVANT)
- Test/placeholder/gibberish; clearly wrong-type noise

Borderline statement-only items (condemnations, Amnesty calls on past attacks, brief secondary effects with no casualties) → NOT_RELEVANT unless the same text also contains a new combat/humanitarian fact.
When unsure between “nice to know” and “actionable now” → NOT_RELEVANT.

============================================================
SEVERITY (1–5) — humanitarian impact
============================================================
Score the humanitarian impact of the event itself (not media volume). If NOT_RELEVANT, still assign best-fit severity from content (often 1–2).

1 Low — Limited impact; no life threat; services OK; local capacity enough → record & monitor
2 Moderate — Limited but noteworthy; manageable locally; minor disruption → verify & notify
3 High — Significant impact / deterioration risk; services strained; external support needed → alert & assess
4 Very High — Severe harm / major unmet needs / critical services down; capacity overwhelmed → urgent mobilize
5 Critical — Imminent/ongoing life threat or catastrophic escalation → immediate life-saving action

Heuristics:
- Statements, admin, crime, economy, small protests → usually 1
- Localized clash / intercept / limited disruption, few/no casualties → 2
- Multiple casualties, hospital/aid convoy hit, clear displacement hundreds+, service disruption → 3
- Mass casualties, large displacement (thousands+), critical infrastructure destroyed, unmet life-saving needs → 4
- Catastrophic / rapidly escalating mass-casualty or imminent life-threat at scale → 5

When unsure between two adjacent severity scores, choose the higher one — better to over-flag than miss something operationally important.

============================================================
FEW-SHOT ANCHORS
============================================================
RELEVANT:
- drone/air strikes with casualties; hospital destroyed; power station/fuel depot/airbase hit
- mass killings in Kordofan/Darfur; frontline clashes
- displacement of hundreds–tens of thousands; aid convoy attacked; water crisis in al-Fashir
- cholera deaths / acute epidemic with humanitarian impact

NOT_RELEVANT:
- gold production boom; World Bank debt; port clearance; hospital rehab inauguration
- diplomatic meetings; smuggling seizure; Berber sit-in / Kassala labor strike
- UNHCR generic advocacy; teacher reopen threats
- cargo aircraft crash without combat link
- ministry/AU/Arab League condemnations alone

============================================================
INPUT
============================================================
Classify the alert below.
"""


class AlertSeverityClassification(BaseModel):
    """Structured LLM output for one alert. ``relevance`` is not persisted yet."""

    relevance: Literal["RELEVANT", "NOT_RELEVANT"]
    severity: Literal[1, 2, 3, 4, 5]


def _payload(
    *,
    country: str | None = None,
    event_name: str | None = None,
    event_description: str | None = None,
    people_dead: int | None = None,
    people_injured: int | None = None,
) -> dict[str, Any]:
    """Build the classifier input dict. Absent fields are empty (``""`` / ``{}``)."""
    additional: dict[str, int] = {}
    if people_dead is not None:
        additional["people_dead"] = people_dead
    if people_injured is not None:
        additional["people_injured"] = people_injured
    return {
        "country": country or "",
        "event_name": event_name or "",
        "event_description": event_description or "",
        "additional_data": additional,
    }


def classify_alert_severity(
    *,
    country: str | None = None,
    event_name: str | None = None,
    event_description: str | None = None,
    people_dead: int | None = None,
    people_injured: int | None = None,
) -> AlertSeverityClassification:
    """Run the DeepSeek (OpenRouter) classifier; raises on provider/parse failure."""
    user = json.dumps(
        _payload(
            country=country,
            event_name=event_name,
            event_description=event_description,
            people_dead=people_dead,
            people_injured=people_injured,
        ),
        ensure_ascii=False,
    )
    return make_llm_provider("alert").complete_structured(
        system=SYSTEM_PROMPT,
        user=user,
        schema=AlertSeverityClassification,
        max_tokens=_LLM_MAX_TOKENS,
        cache_key=ALERT_SEVERITY_PROMPT_VERSION,
    )


def llm_severity_or_none(
    *,
    country: str | None = None,
    event_name: str | None = None,
    event_description: str | None = None,
    people_dead: int | None = None,
    people_injured: int | None = None,
    log_tag: str = "alert",
) -> int | None:
    """Best-effort severity for ingest builders. Returns None on LLM failure
    (null stays honest — never invent a floor). Relevance is discarded until
    the DB write lands."""
    try:
        result = classify_alert_severity(
            country=country,
            event_name=event_name,
            event_description=event_description,
            people_dead=people_dead,
            people_injured=people_injured,
        )
    except Exception:  # noqa: BLE001 — ingest must not die on a model blip
        logger.warning(
            "[%s] alert severity LLM failed — leaving severity null",
            log_tag,
            exc_info=True,
        )
        return None
    logger.info(
        "[%s] alert severity LLM: relevance=%s severity=%s",
        log_tag,
        result.relevance,
        result.severity,
    )
    return result.severity
