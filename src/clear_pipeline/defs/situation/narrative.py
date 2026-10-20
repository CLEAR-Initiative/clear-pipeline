"""LLM-generated narrative components for the situation analysis.

Components:
  2. AI summary                  (2–4 paragraph prose)
  3. context_risks               (9 sub-domains × bullets)
  4. hazards_and_vulnerabilities (bullets each)
  5. displacement                (push_factors + return_intention)
  5b. humanitarian_access        (barriers + constraints)

Component 6 (per-sector analysis) lives in `sectors.py` — it needs its
own prompt discipline per sector.

Each generator runs one RAG search and asks the LLM to cite the
retrieved evidence with inline `[Rn]` markers; `situation/citations.py`
resolves those into a per-line `report_id -> [lines]` map
(`contributing_sources`), with the search's `contributing_report_ids`
union exposed as the component-level `source_report_ids` fallback.

Prompt-cache strategy: every generator's system prompt is identical
across a single country-year generation cycle. The shared prefix
(instructions + aggregated-datapoints context) is cached by
Anthropic's prompt cache; only the per-component RAG hits + output
schema vary per call. Sequential invocation stays inside the 5-minute
cache TTL by design.

Failure isolation: any generator that raises returns its component's
empty default. One bad LLM call doesn't drop the other three.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel, Field

from clear_pipeline.defs.situation.citations import (
    _SENTENCE_RE,
    merge_contributing,
    resolve_prose,
)
from clear_pipeline.defs.situation.generate_component import (
    BulletComponentSpec,
    GuidedCoverageLLM,
    run_bullet_component,
    run_structured,
    sourced_bullets,
)
from clear_pipeline.defs.situation.prompts import (
    build_system_prompt,
    format_aggregated_for_prompt,
)
from clear_pipeline.defs.situation.questions import (
    CONTEXT_DOMAIN_KEYS,
    rag_query_for,
    situation_questions,
)
from clear_pipeline.defs.situation.rag_helper import (
    RAGContext,
    fetch_rag_context,
    merge_rag_contexts,
)
from clear_pipeline.defs.situation.schemas import (
    AISummary,
    ContextRisks,
    DisplacementNarrative,
    HazardsAndVulnerabilities,
    HumanitarianAccess,
    RiskDomain,
    Scenarios,
    SourcedBullet,
)
from clear_pipeline.providers.llm import LLMProvider

logger = logging.getLogger(__name__)

# Re-exports for tests / callers that imported the old private names.
_CONTEXT_DOMAIN_KEYS = CONTEXT_DOMAIN_KEYS
_sourced_bullets = sourced_bullets


# ────────────────────────────────────────────────────────────────────
# LLM output schemas — decoupled from the DB-written schemas.
#
# The DB shapes (AISummary, RiskDomain, SourcedBullet …) carry
# `source_report_ids` + `contributing_sources`. We do NOT ask the LLM
# to emit report ids directly (it can't reason about which id supported
# which bullet), and a nested {text, sources} output made the cheap
# models return a JSON-encoded string and blank the whole component.
# Instead the LLM appends inline [Rn] markers referring to the numbered
# RETRIEVED EVIDENCE, and citations.py resolves those deterministically
# to report ids (per-line map + component union). So the output schemas
# below stay FLAT (list[str] / prose) — the markers ride inside the
# strings and are stripped after resolution.
# ────────────────────────────────────────────────────────────────────


class _AISummaryLLM(BaseModel):
    """Executive summary + key findings, in the shape of NRC's incident and
    flash reports."""
    text: str = Field(
        description=(
            "Executive summary: 3 short sentences, at most 70 words, one "
            "paragraph, prose only. What is happening now, where it is "
            "concentrated, the main driver, and whether the situation is "
            "worsening, stable or improving. Do not restate the headline "
            "displaced, people-in-need or funding figures; the dashboard "
            "shows them."
        ),
    )
    key_findings: list[str] = Field(
        default_factory=list,
        description=(
            "4 to 6 key findings. Order: findings about the last 30 days "
            "first (newest first), then the most important background. Each item is "
            "'Subject: finding', where Subject is a 2–5 word label for the "
            "pattern (e.g. 'Drone strikes on civilian sites'). One sentence "
            "each, at most 30 words, one fact (no semicolon lists), with "
            "places and dates as evidence."
        ),
    )


class _GuidedAnswerLLM(GuidedCoverageLLM):
    """Structured answer for one context-pillar analysis question.

    ``answer`` integrates into ``RiskDomain.bullets``; completeness /
    questions_not_answered are additive coverage metadata.
    """

    answer: list[str] = Field(
        default_factory=list,
        description=(
            "Concise bulleted answer addressing the analysis question. "
            "Each bullet a terse fragment (max 15 words), figure or fact "
            "first. Prefer 3–6 bullets. Empty list only when no evidence "
            "supports any claim."
        ),
    )


class _HumanitarianAccessLLM(GuidedCoverageLLM):
    barriers: list[str] = Field(
        default_factory=list,
        description="Access barriers for actors and affected populations. Terse bullets.",
    )
    constraints: list[str] = Field(
        default_factory=list,
        description="Physical, seasonal, and operational access constraints. Terse bullets.",
    )


class _HazardsVulnerabilitiesLLM(GuidedCoverageLLM):
    hazards: list[str] = Field(
        default_factory=list,
        description="Natural / man-made shocks. Terse bullets.",
    )
    vulnerabilities: list[str] = Field(
        default_factory=list,
        description="Pre-crisis structural vulnerabilities. Terse bullets.",
    )


class _DisplacementLLM(GuidedCoverageLLM):
    push_factors: list[str] = Field(
        default_factory=list,
        description="Drivers of displacement. Terse bullets.",
    )
    return_intention: list[str] = Field(
        default_factory=list,
        description="Return intent, blockers, and conditions. Terse bullets.",
    )


class _ScenariosLLM(BaseModel):
    """Forward-looking trajectories. Prose only — no bullets, no markers."""
    most_likely: str = Field(
        default="",
        description="The most likely trajectory over the coming weeks/months, 2–4 sentences, grounded in the evidence.",
    )
    best_case: str = Field(
        default="",
        description="A plausible best-case trajectory if drivers ease / response scales, 2–4 sentences.",
    )
    worst_case: str = Field(
        default="",
        description="A plausible worst-case trajectory if drivers intensify / access collapses, 2–4 sentences.",
    )
    description: str = Field(
        default="",
        description="The key variables / assumptions the scenarios hinge on (access, funding, escalation, seasonality).",
    )


# ────────────────────────────────────────────────────────────────────
# Bullet-component specs (hazards / displacement / access / context)
# ────────────────────────────────────────────────────────────────────

_HAZARDS_SPEC = BulletComponentSpec(
    schema_key="hazards_and_vulnerabilities",
    log_label="hazards_and_vulnerabilities",
    llm_schema=_HazardsVulnerabilitiesLLM,
    result_cls=HazardsAndVulnerabilities,
    bullet_fields=("hazards", "vulnerabilities"),
    map_instruction="Map the answer into `hazards` and `vulnerabilities`.",
    fallback_topic="Shock-Event",
)

_DISPLACEMENT_SPEC = BulletComponentSpec(
    schema_key="displacement",
    log_label="displacement",
    llm_schema=_DisplacementLLM,
    result_cls=DisplacementNarrative,
    bullet_fields=("push_factors", "return_intention"),
    map_instruction="Map the answer into `push_factors` and `return_intention`.",
    fallback_topic="Displacement",
)

_ACCESS_SPEC = BulletComponentSpec(
    schema_key="humanitarian_access",
    log_label="humanitarian_access",
    llm_schema=_HumanitarianAccessLLM,
    result_cls=HumanitarianAccess,
    bullet_fields=("barriers", "constraints"),
    map_instruction="Map the answer into `barriers` and `constraints`.",
    fallback_topic="Humanitarian Access",
)

_CONTEXT_DOMAIN_SPEC = BulletComponentSpec(
    schema_key="",  # set per domain call
    log_label="context_risks",
    llm_schema=_GuidedAnswerLLM,
    result_cls=RiskDomain,
    bullet_fields=("answer",),
    map_instruction="Emit `answer` (3–6 bullets).",
    sourced=False,
    field_map={"answer": "bullets"},
    max_tokens=1500,
)


# ────────────────────────────────────────────────────────────────────
# Component 2 — AI Summary
# ────────────────────────────────────────────────────────────────────

# The window the summary leads with: developments this recent come first.
RECENT_DAYS = 30
# Evidence breadth: the recent FRAME band, then one RAG search per
# situation-analysis CSV question so the summary draws on the same
# framework questions as the narrative components (not hand-rolled themes).
RECENT_LIMIT = 20
THEME_LIMIT = 8
MAX_CHUNKS_PER_REPORT = 2
# Deterministic guardrails on the output.
SUMMARY_MAX_WORDS = 80
SUMMARY_MAX_SENTENCES = 3
MIN_CITED_FINDINGS = 4
FINDING_MAX_WORDS = 35


def _parse_iso(value: Any) -> datetime | None:
    """ISO-8601 string (a trailing `Z` included) to an aware datetime, else None."""
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _recent_window(rag_filters: dict[str, Any] | None, *, days: int) -> tuple[datetime, datetime]:
    """The last `days` of the ANALYSIS window, not of today: ends at the
    frame's `timeRange.to` (a fixed past window stays in its period) or now
    when the frame is open-ended or ends in the future (a calendar-year frame
    runs to 31 Dec), and never starts before `timeRange.from`."""
    window = (rag_filters or {}).get("timeRange") or {}
    now = datetime.now(timezone.utc)
    end = min(_parse_iso(window.get("to")) or now, now)
    start = end - timedelta(days=days)
    window_start = _parse_iso(window.get("from"))
    if window_start and window_start > start:
        start = window_start
    return start, end


def _recent_filters(rag_filters: dict[str, Any] | None, *, days: int) -> dict[str, Any]:
    """The scope's retrieval filters narrowed to the recent window."""
    start, end = _recent_window(rag_filters, days=days)
    return {**(rag_filters or {}), "timeRange": {"from": start.isoformat(), "to": end.isoformat()}}


def _summary_evidence(scope_label: str, rag_filters: dict[str, Any] | None) -> RAGContext:
    recent = fetch_rag_context(
        query="",
        limit=RECENT_LIMIT,
        filters=_recent_filters(rag_filters, days=RECENT_DAYS),
        mode="FRAME",
    )
    themed = [
        fetch_rag_context(
            query=rag_query_for(scope_label, q),
            limit=THEME_LIMIT,
            filters=rag_filters,
        )
        for q in situation_questions()
    ]
    return merge_rag_contexts(recent, *themed, max_per_report=MAX_CHUNKS_PER_REPORT)


def _cited_count(findings: list[SourcedBullet]) -> int:
    return sum(1 for f in findings if f.source_report_ids)


def _summary_problems(text: str, findings: list[SourcedBullet]) -> list[str]:
    problems = []
    cited_findings = _cited_count(findings)
    long_findings = [f.description for f in findings if len(f.description.split()) > FINDING_MAX_WORDS]
    words = len(text.split())
    if words > SUMMARY_MAX_WORDS:
        problems.append(
            f"The executive summary has {words} words; it must be at most "
            f"{SUMMARY_MAX_WORDS - 10} words in {SUMMARY_MAX_SENTENCES} short sentences."
        )
    if cited_findings < MIN_CITED_FINDINGS:
        problems.append(
            f"Only {cited_findings} key findings carry an [Rn] citation; give at "
            f"least {MIN_CITED_FINDINGS}, each ending with its [Rn] marker(s)."
        )
    if long_findings:
        problems.append(
            f"{len(long_findings)} key findings exceed {FINDING_MAX_WORDS} words; each must be "
            f"one sentence of at most {FINDING_MAX_WORDS - 5} words, one fact, no semicolon lists."
        )
    return problems


def _trim_sentences(text: str, contributing: dict[str, list[str]], keep: int) -> tuple[str, dict[str, list[str]]]:
    """Keep the first `keep` sentences, and drop citation lines for the rest."""
    sentences = [s for s in _SENTENCE_RE.split(text.strip()) if s]
    if len(sentences) <= keep:
        return text, contributing
    kept = sentences[:keep]
    kept_set = set(kept)
    trimmed = {rid: [ln for ln in lines if ln in kept_set] for rid, lines in contributing.items()}
    return " ".join(kept), {rid: lines for rid, lines in trimmed.items() if lines}


def generate_ai_summary(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
    rag_filters: dict[str, Any] | None = None,
) -> AISummary:
    """Executive summary + key findings, NRC flash-report style.

    Evidence: the scope's last `RECENT_DAYS` in FRAME mode (recency-ordered
    reports plus a guaranteed incident band, ADR-0006) first, then one topical
    search per theme, merged with at most `MAX_CHUNKS_PER_REPORT` chunks per
    report. The aggregated-figures block is deliberately NOT given to this
    component: the page shows those figures, and uncited numbers from it leaked
    into findings. Output rules the prompt can't guarantee are enforced here:
    findings without a citation are dropped, and one retry is made when the
    summary is too long or too few findings are cited."""
    del aggregated  # see docstring
    rag = _summary_evidence(country_name, rag_filters)
    _, recent_end = _recent_window(rag_filters, days=RECENT_DAYS)
    if rag.is_empty:
        logger.info("[situation:ai_summary] no RAG hits — returning empty summary")
        return AISummary()

    system = build_system_prompt(
        country_name, period_label,
        "(not provided for this component: use only RETRIEVED EVIDENCE)",
    )
    user = (
        f"AI Summary for {country_name}, {period_label}: 3 short sentences "
        f"(≤70 words) plus 4–6 key findings ('Subject: finding', ≤30 words each).\n"
        f"Lead with the {RECENT_DAYS} days up to {recent_end:%d %B %Y}; older "
        "evidence is background only. No cumulative multi-year totals. "
        "Every finding must end with [Rn] from RETRIEVED EVIDENCE.\n"
        "\n"
        "RETRIEVED EVIDENCE:\n"
        f"{rag.formatted_for_prompt}"
    )

    def run(prompt: str) -> tuple[str, dict[str, list[str]], list[SourcedBullet], dict[str, list[str]]]:
        result = run_structured(
            llm, system_prompt=system, user_prompt=prompt,
            schema=_AISummaryLLM, cache_key=cache_key, max_tokens=1500,
        )
        text, contributing = resolve_prose(result.text, rag.hit_report_ids)
        findings, findings_contributing = sourced_bullets(result.key_findings, rag)
        return text, contributing, findings, findings_contributing

    try:
        first = run(user)
    except Exception:  # noqa: BLE001 — component-level isolation
        logger.exception("[situation:ai_summary] LLM call failed — returning empty component")
        return AISummary()
    text, contributing, findings, findings_contributing = first
    problems = _summary_problems(text, findings)
    if problems:
        logger.info("[situation:ai_summary] retrying once: %s", " ".join(problems))
        try:
            second = run(
                user + "\n\nYOUR PREVIOUS ANSWER HAD THESE PROBLEMS, FIX THEM:\n- " + "\n- ".join(problems)
            )
        except Exception:  # noqa: BLE001 — a failed retry must not discard a usable first answer
            logger.warning("[situation:ai_summary] retry failed — keeping the first answer", exc_info=True)
        else:
            # Keep the retry only if it is no worse: no more guardrail problems
            # and no fewer cited findings than the first answer.
            if len(_summary_problems(second[0], second[2])) <= len(problems) and (
                _cited_count(second[2]) >= _cited_count(findings)
            ):
                text, contributing, findings, findings_contributing = second
            else:
                logger.info("[situation:ai_summary] retry was worse — keeping the first answer")

    if len(text.split()) > SUMMARY_MAX_WORDS:
        text, contributing = _trim_sentences(text, contributing, SUMMARY_MAX_SENTENCES)
    cited = [f for f in findings if f.source_report_ids]
    cited_lines = {f.description for f in cited}
    findings_contributing = {
        rid: [ln for ln in lines if ln in cited_lines] for rid, lines in findings_contributing.items()
    }
    contributing = merge_contributing(contributing, {r: ls for r, ls in findings_contributing.items() if ls})
    return AISummary(
        text=text,
        key_findings=cited,
        source_report_ids=list(contributing) or rag.contributing_report_ids,
        contributing_sources=contributing,
    )


# ────────────────────────────────────────────────────────────────────
# Component 3 — Context Risks (one LLM call per context-pillar name)
# ────────────────────────────────────────────────────────────────────


def _generate_one_context_domain(
    llm: LLMProvider,
    *,
    domain_key: str,
    country_name: str,
    period_label: str,
    aggregated_context: str,
    cache_key: str,
    rag_filters: dict[str, Any] | None = None,
) -> RiskDomain:
    """One RAG search + one LLM call for a single context pillar."""
    spec = BulletComponentSpec(
        schema_key=domain_key,
        log_label=f"context_risks:{domain_key}",
        llm_schema=_CONTEXT_DOMAIN_SPEC.llm_schema,
        result_cls=_CONTEXT_DOMAIN_SPEC.result_cls,
        bullet_fields=_CONTEXT_DOMAIN_SPEC.bullet_fields,
        map_instruction=_CONTEXT_DOMAIN_SPEC.map_instruction,
        rag_limit=_CONTEXT_DOMAIN_SPEC.rag_limit,
        max_tokens=_CONTEXT_DOMAIN_SPEC.max_tokens,
        sourced=_CONTEXT_DOMAIN_SPEC.sourced,
        field_map=_CONTEXT_DOMAIN_SPEC.field_map,
    )
    return run_bullet_component(
        llm,
        spec,
        country_name=country_name,
        period_label=period_label,
        aggregated=None,
        cache_key=cache_key,
        rag_filters=rag_filters,
        aggregated_context=aggregated_context,
    )


def generate_context_risks(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
    rag_filters: dict[str, Any] | None = None,
) -> ContextRisks:
    """Nine context pillars — one RAG + LLM call each, keyed by CSV name.

    Sequential so Anthropic prompt caching can reuse the shared system
    prefix across domains within the 5-minute TTL.
    """
    aggregated_context = format_aggregated_for_prompt(aggregated)
    outputs: dict[str, RiskDomain] = {}
    for domain_key in CONTEXT_DOMAIN_KEYS:
        outputs[domain_key] = _generate_one_context_domain(
            llm,
            domain_key=domain_key,
            country_name=country_name,
            period_label=period_label,
            aggregated_context=aggregated_context,
            cache_key=cache_key,
            rag_filters=rag_filters,
        )
    return ContextRisks(**outputs)


# ────────────────────────────────────────────────────────────────────
# Components 4 / 5 / 5b — hazards, displacement, humanitarian access
# ────────────────────────────────────────────────────────────────────


def generate_hazards_and_vulnerabilities(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
    rag_filters: dict[str, Any] | None = None,
) -> HazardsAndVulnerabilities:
    return run_bullet_component(
        llm, _HAZARDS_SPEC,
        country_name=country_name, period_label=period_label,
        aggregated=aggregated, cache_key=cache_key, rag_filters=rag_filters,
    )


def generate_displacement_narrative(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
    rag_filters: dict[str, Any] | None = None,
) -> DisplacementNarrative:
    return run_bullet_component(
        llm, _DISPLACEMENT_SPEC,
        country_name=country_name, period_label=period_label,
        aggregated=aggregated, cache_key=cache_key, rag_filters=rag_filters,
    )


def generate_humanitarian_access(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
    rag_filters: dict[str, Any] | None = None,
) -> HumanitarianAccess:
    return run_bullet_component(
        llm, _ACCESS_SPEC,
        country_name=country_name, period_label=period_label,
        aggregated=aggregated, cache_key=cache_key, rag_filters=rag_filters,
    )


# ────────────────────────────────────────────────────────────────────
# Component 8 — Scenarios (forward-looking, ADR-0007 §4)
# ────────────────────────────────────────────────────────────────────


def generate_scenarios(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
    rag_filters: dict[str, Any] | None = None,
) -> Scenarios:
    """Forward-looking most-likely / best / worst trajectories for the frame.
    Prose-only; coarse provenance (the RAG report union) since projections are
    not per-line citations. Empty component when there's no evidence.

    RAG is grounded in the situation-analysis CSV questions (no separate
    scenario question in the first-level framework)."""
    sit_qs = situation_questions()
    rag = merge_rag_contexts(
        *[
            fetch_rag_context(
                query=rag_query_for(country_name, q),
                limit=4,
                filters=rag_filters,
            )
            for q in sit_qs
        ],
        max_per_report=2,
    ) if sit_qs else fetch_rag_context(
        query=f"{country_name} humanitarian outlook {period_label}",
        limit=10,
        filters=rag_filters,
    )
    if rag.is_empty:
        return Scenarios()

    system = build_system_prompt(
        country_name, period_label, format_aggregated_for_prompt(aggregated),
    )
    user = (
        f"Scenarios for {country_name}, {period_label}: "
        "`most_likely`, `best_case`, `worst_case` (2–4 sentences each) and "
        "`description` (key hinge variables). Ground in evidence only.\n"
        "\n"
        "RETRIEVED EVIDENCE:\n"
        f"{rag.formatted_for_prompt}"
    )
    try:
        result = run_structured(
            llm, system_prompt=system, user_prompt=user,
            schema=_ScenariosLLM, cache_key=cache_key, max_tokens=2000,
        )
    except Exception:  # noqa: BLE001 — component-level isolation
        logger.exception("[situation:scenarios] LLM call failed — returning empty component")
        return Scenarios()
    return Scenarios(
        most_likely=result.most_likely,
        best_case=result.best_case,
        worst_case=result.worst_case,
        description=result.description,
        source_report_ids=rag.contributing_report_ids,
    )
