"""LLM-generated narrative components for the situation analysis.

Four components:
  2. AI summary                  (2–4 paragraph prose)
  3. context_risks               (8 sub-domains × bullets)
  4. hazards_and_vulnerabilities (bullets each)
  5. displacement                (push_factors + return_intention)

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

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel, Field

from clear_pipeline.defs.situation.citations import (
    _SENTENCE_RE,
    merge_contributing,
    resolve_bullets,
    resolve_prose,
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
    RiskDomain,
    Scenarios,
    SourcedBullet,
)
from clear_pipeline.providers.llm import LLMProvider

logger = logging.getLogger(__name__)


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


def _risk_domain_field() -> Any:
    """Fresh Field for one context-risk domain — a flat list of bullets."""
    return Field(
        default_factory=list,
        description=(
            "Concise bulleted risks / observations for this domain. "
            "Each bullet a terse fragment (max 15 words), figure or fact first. Prefer 3–6 bullets. Return "
            "an empty list only when no evidence in the retrieved "
            "sources supports any claim in this domain."
        ),
    )


class _ContextRisksLLM(BaseModel):
    """Eight OCHA-style context-risk domains, each a FLAT list of bullet
    strings. Fixed order so the dashboard's tab layout stays stable.

    Flat on purpose: an earlier nested-object shape (one sub-model per
    domain) made the model intermittently return a domain as a
    JSON-encoded string instead of an object, which failed validation and
    blanked the whole component. The narrative components that never
    glitch — hazards, displacement — are all `list[str]`; this matches
    them. `source_report_ids` is attached later from the RAG set, not
    asked of the LLM."""
    demographics: list[str] = _risk_domain_field()
    political: list[str] = _risk_domain_field()
    economy: list[str] = _risk_domain_field()
    socio_culture: list[str] = _risk_domain_field()
    security: list[str] = _risk_domain_field()
    legal_policy: list[str] = _risk_domain_field()
    infrastructure: list[str] = _risk_domain_field()
    environment: list[str] = _risk_domain_field()


class _HazardsVulnerabilitiesLLM(BaseModel):
    hazards: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted list of natural / man-made hazards affecting the "
            "country during the target period. Terse fragment per bullet."
        ),
    )
    vulnerabilities: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted list of pre-crisis structural vulnerabilities "
            "(economic, institutional, environmental, social). Terse "
            "fragment per bullet."
        ),
    )


class _DisplacementLLM(BaseModel):
    push_factors: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted drivers pushing populations to displace — "
            "conflict, drought, insecurity, service collapse, etc. "
            "Terse fragment per bullet, driver first."
        ),
    )
    return_intention: list[str] = Field(
        default_factory=list,
        description=(
            "Bulleted signals about displaced populations' intent to "
            "return - willingness, blockers, conditions. Terse fragment "
            "per bullet."
        ),
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
# Shared system-prompt scaffolding
# ────────────────────────────────────────────────────────────────────


_BASE_INSTRUCTIONS = (
    "You are a humanitarian analyst producing a situation summary for "
    "the Norwegian Refugee Council. Every claim you emit MUST be "
    "grounded in the retrieved evidence — do not invent numbers, "
    "dates, or attributions. When the evidence is thin, prefer "
    "returning fewer bullets over speculating.\n"
    "\n"
    "Rules:\n"
    "- Bullets are terse fragments, NOT full sentences. Aim for 15 words "
    "  or fewer. Lead with the figure or the fact; drop filler openers "
    "  ('there is', 'it is reported that', 'continues to'). One claim per "
    "  bullet.\n"
    "- Report every number the way the source cites it (with unit and "
    "  scale). Do not extrapolate.\n"
    "- Use the country name and admin-region names as they appear in "
    "  the sources. Preserve local naming (e.g. 'Kordofan', 'Darfur').\n"
    "- Prefer verified / reported sources over media when both cover "
    "  the same claim. If they conflict, prefer the more recent report.\n"
    "- Neutral, factual tone. No editorialising. No calls to action.\n"
    "- Cite evidence inline. A bullet's or sentence's marker(s) MUST come at "
    "  its very END — after the full stop — NEVER at the start. Placing a "
    "  marker before the text it cites will mis-attribute it to the previous "
    "  line. Use the bracketed evidence numbers you drew from, e.g. [R2] or "
    "  [R1][R4], matching the [Rn] items in RETRIEVED EVIDENCE. Cite only "
    "  evidence you actually used; a line that uses none gets no marker.\n"
)


def _build_system_prompt(country_name: str, period_label: str, agg_context: str) -> str:
    """One prompt reused across all four component calls in a run.

    Static prefix + country context: this is what Anthropic's prompt
    cache keys on. Keeping it byte-identical across the four calls
    means calls 2–4 read from cache at ~10% of the input cost.
    """
    return (
        f"{_BASE_INSTRUCTIONS}\n"
        f"---\n"
        f"COUNTRY: {country_name}\n"
        f"PERIOD: {period_label}\n"
        f"---\n"
        f"AGGREGATED HEADLINE FIGURES (cached; do not repeat back):\n"
        f"{agg_context}\n"
        f"---"
    )


def _format_aggregated_for_prompt(aggregated: dict[str, Any] | None) -> str:
    """Render the aggregated-datapoints blob in a compact form the
    LLM can reason over. We serialise the whole `data` map because
    the model may want to cross-reference sector PIN figures against
    narrative claims about needs."""
    if not aggregated:
        return "(no aggregated figures available for this period)"
    payload = {
        "reportCount": aggregated.get("reportCount"),
        "dataQualityScore": aggregated.get("dataQualityScore"),
        "newestSourceAt": aggregated.get("newestSourceAt"),
        "oldestSourceAt": aggregated.get("oldestSourceAt"),
        "data": aggregated.get("data") or {},
    }
    return json.dumps(payload, indent=2, ensure_ascii=False, default=str)


def _run_component(
    llm: LLMProvider,
    *,
    system_prompt: str,
    user_prompt: str,
    schema: type[BaseModel],
    cache_key: str,
    max_tokens: int,
) -> BaseModel:
    """Single component call — thin wrapper so all four narrative
    generators go through the same code path (retry policy, cache-key
    convention, max-tokens guard)."""
    return llm.complete_structured(
        system=system_prompt,
        user=user_prompt,
        schema=schema,
        max_tokens=max_tokens,
        cache_key=cache_key,
    )


def _sourced_bullets(
    raw_bullets: list[str], rag: RAGContext,
) -> tuple[list[SourcedBullet], dict[str, list[str]]]:
    """Resolve inline [Rn] markers on a bullet list into SourcedBullets — each
    carrying ONLY its own resolved report ids (empty when that bullet cited
    nothing) — plus the report_id -> [bullets] map for `contributing_sources`.

    Per-bullet ids are the honest signal. A coarse-RAG-union fallback HERE would
    stamp an un-marked terse bullet with every report that fed the search, which
    the dashboard renders literally. The union is still exposed once, at the
    component level (`source_report_ids`), which is where the fallback belongs."""
    clean, per_ids, contributing = resolve_bullets(raw_bullets, rag.hit_report_ids)
    bullets = [
        SourcedBullet(description=c, source_report_ids=ids)
        for c, ids in zip(clean, per_ids)
    ]
    return bullets, contributing


# ────────────────────────────────────────────────────────────────────
# Component 2 — AI Summary
# ────────────────────────────────────────────────────────────────────

# The window the summary leads with: developments this recent come first.
RECENT_DAYS = 30
# Evidence breadth: the recent FRAME band, then one topical search per theme so
# a country summary draws on many reports, not the top hits of one query.
RECENT_LIMIT = 20
THEME_LIMIT = 8
MAX_CHUNKS_PER_REPORT = 2
SUMMARY_THEMES = (
    "armed conflict attacks drone strikes clashes civilian casualties",
    "displacement movement IDPs returns refugees",
    "disease outbreak cholera health facilities",
    "food insecurity famine markets prices livelihoods",
    "humanitarian access aid delivery response funding",
)
# Deterministic guardrails on the output.
SUMMARY_MAX_WORDS = 80
SUMMARY_MAX_SENTENCES = 3
MIN_CITED_FINDINGS = 4
FINDING_MAX_WORDS = 35


def _recent_filters(rag_filters: dict[str, Any] | None, *, days: int) -> dict[str, Any]:
    """The scope's retrieval filters narrowed to the last `days`."""
    now = datetime.now(timezone.utc)
    return {
        **(rag_filters or {}),
        "timeRange": {"from": (now - timedelta(days=days)).isoformat(), "to": now.isoformat()},
    }


def _summary_evidence(scope_label: str, rag_filters: dict[str, Any] | None) -> RAGContext:
    recent = fetch_rag_context(
        query="",
        limit=RECENT_LIMIT,
        filters=_recent_filters(rag_filters, days=RECENT_DAYS),
        mode="FRAME",
    )
    themed = [
        fetch_rag_context(query=f"{scope_label} {terms}", limit=THEME_LIMIT, filters=rag_filters)
        for terms in SUMMARY_THEMES
    ]
    return merge_rag_contexts(recent, *themed, max_per_report=MAX_CHUNKS_PER_REPORT)


def _summary_problems(text: str, findings: list[SourcedBullet]) -> list[str]:
    problems = []
    cited_findings = sum(1 for f in findings if f.source_report_ids)
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
    if rag.is_empty:
        logger.info("[situation:ai_summary] no RAG hits — returning empty summary")
        return AISummary()

    system = _build_system_prompt(
        country_name, period_label, "(not provided for this component: use only RETRIEVED EVIDENCE)",
    )
    user = (
        f"Produce the AI Summary component for {country_name}, {period_label}. "
        "Write an executive summary of 3 short sentences and 4 to 6 key "
        "findings, as in an NRC flash report. A program manager should read "
        "it in under a minute. No filler, no restating the task.\n"
        "Describe patterns at the level of the analysed area and name places "
        "only as evidence: for a country, group incidents across regions "
        "(e.g. 'drone strikes on markets across Darfur and Kordofan'); for a "
        "district or locality, single dated incidents are fine.\n"
        f"Lead with developments from the last {RECENT_DAYS} days and give "
        "dates. Use older evidence only as background. Do not present "
        "cumulative multi-year totals (e.g. counts since 2023) as findings.\n"
        "Every key finding must come from RETRIEVED EVIDENCE and end with its "
        "[Rn] marker(s); a finding you cannot cite will be discarded. Draw on "
        "as many different sources as the evidence supports.\n"
        "\n"
        "RETRIEVED EVIDENCE:\n"
        f"{rag.formatted_for_prompt}"
    )

    def run(prompt: str) -> tuple[str, dict[str, list[str]], list[SourcedBullet], dict[str, list[str]]]:
        result = _run_component(
            llm, system_prompt=system, user_prompt=prompt,
            schema=_AISummaryLLM, cache_key=cache_key, max_tokens=1500,
        )
        text, contributing = resolve_prose(result.text, rag.hit_report_ids)
        findings, findings_contributing = _sourced_bullets(result.key_findings, rag)
        return text, contributing, findings, findings_contributing

    try:
        text, contributing, findings, findings_contributing = run(user)
        problems = _summary_problems(text, findings)
        if problems:
            logger.info("[situation:ai_summary] retrying once: %s", " ".join(problems))
            text, contributing, findings, findings_contributing = run(
                user + "\n\nYOUR PREVIOUS ANSWER HAD THESE PROBLEMS, FIX THEM:\n- " + "\n- ".join(problems)
            )
    except Exception:  # noqa: BLE001 — component-level isolation
        logger.exception("[situation:ai_summary] LLM call failed — returning empty component")
        return AISummary()

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
# Component 3 — Context Risks
# ────────────────────────────────────────────────────────────────────


def generate_context_risks(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
    rag_filters: dict[str, Any] | None = None,
) -> ContextRisks:
    """Eight risk domains in one LLM call. Single broad RAG search
    covers cross-domain context — separate per-domain searches would
    ~8× the cost with marginal quality lift for the POC."""
    rag = fetch_rag_context(
        query=(
            f"{country_name} context risks demographics political economy "
            "society culture security legal policy infrastructure environment"
        ),
        limit=15,
        filters=rag_filters,
    )
    if rag.is_empty:
        return ContextRisks()

    system = _build_system_prompt(country_name, period_label, _format_aggregated_for_prompt(aggregated))
    user = (
        f"Produce the Context Risks component for {country_name}, {period_label}. "
        "For each of the eight domains (demographics, political, economy, "
        "socio_culture, security, legal_policy, infrastructure, "
        "environment), emit 3–6 bulleted risks or observations. Skip a "
        "domain (empty list) only if the evidence doesn't support any "
        "claim in it.\n"
        "\n"
        "RETRIEVED EVIDENCE:\n"
        f"{rag.formatted_for_prompt}"
    )
    try:
        result = _run_component(
            llm, system_prompt=system, user_prompt=user,
            schema=_ContextRisksLLM, cache_key=cache_key, max_tokens=3000,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[situation:context_risks] LLM call failed — returning empty component")
        return ContextRisks()

    # Resolve each domain's inline [Rn] citations independently — the eight
    # domains share one RAG search, but each attributes only the bullets it
    # actually emitted. A domain with no usable markers falls back to the coarse
    # RAG union so its `source_report_ids` never regresses to empty.
    def _domain(raw_bullets: list[str]) -> RiskDomain:
        clean, _ids, contributing = resolve_bullets(raw_bullets, rag.hit_report_ids)
        return RiskDomain(
            bullets=clean,
            source_report_ids=list(contributing) or rag.contributing_report_ids,
            contributing_sources=contributing,
        )

    return ContextRisks(
        demographics=_domain(result.demographics),
        political=_domain(result.political),
        economy=_domain(result.economy),
        socio_culture=_domain(result.socio_culture),
        security=_domain(result.security),
        legal_policy=_domain(result.legal_policy),
        infrastructure=_domain(result.infrastructure),
        environment=_domain(result.environment),
    )


# ────────────────────────────────────────────────────────────────────
# Component 4 — Hazards & Pre-Crisis Vulnerabilities
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
    rag = fetch_rag_context(
        query=(
            f"{country_name} hazards vulnerabilities natural disasters "
            "conflict drought flood economic institutional structural"
        ),
        limit=10,
        filters=rag_filters,
    )
    if rag.is_empty:
        return HazardsAndVulnerabilities()

    system = _build_system_prompt(country_name, period_label, _format_aggregated_for_prompt(aggregated))
    user = (
        f"Produce the Hazards & Pre-Crisis Vulnerabilities component for "
        f"{country_name}, {period_label}. Emit two lists: `hazards` (natural + "
        "man-made shocks) and `vulnerabilities` (structural conditions "
        "that make populations susceptible). One sentence per bullet.\n"
        "\n"
        "RETRIEVED EVIDENCE:\n"
        f"{rag.formatted_for_prompt}"
    )
    try:
        result = _run_component(
            llm, system_prompt=system, user_prompt=user,
            schema=_HazardsVulnerabilitiesLLM, cache_key=cache_key, max_tokens=2000,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[situation:hazards_and_vulnerabilities] LLM call failed — returning empty component")
        return HazardsAndVulnerabilities()

    hazards, haz_contrib = _sourced_bullets(result.hazards, rag)
    vulns, vuln_contrib = _sourced_bullets(result.vulnerabilities, rag)
    return HazardsAndVulnerabilities(
        hazards=hazards,
        vulnerabilities=vulns,
        contributing_sources=merge_contributing(haz_contrib, vuln_contrib),
    )


# ────────────────────────────────────────────────────────────────────
# Component 5 — Displacement Narrative
# ────────────────────────────────────────────────────────────────────


def generate_displacement_narrative(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
    rag_filters: dict[str, Any] | None = None,
) -> DisplacementNarrative:
    rag = fetch_rag_context(
        query=(
            f"{country_name} displacement push factors return intention "
            "IDPs refugees returnees drivers barriers conditions"
        ),
        limit=10,
        filters=rag_filters,
    )
    if rag.is_empty:
        return DisplacementNarrative()

    system = _build_system_prompt(country_name, period_label, _format_aggregated_for_prompt(aggregated))
    user = (
        f"Produce the Displacement Narrative component for {country_name}, "
        f"{period_label}. Emit two lists: `push_factors` (what's driving people to "
        "displace) and `return_intention` (signals about their intent to "
        "return, blockers, or conditions). One sentence per bullet.\n"
        "\n"
        "RETRIEVED EVIDENCE:\n"
        f"{rag.formatted_for_prompt}"
    )
    try:
        result = _run_component(
            llm, system_prompt=system, user_prompt=user,
            schema=_DisplacementLLM, cache_key=cache_key, max_tokens=2000,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[situation:displacement] LLM call failed — returning empty component")
        return DisplacementNarrative()

    push, push_contrib = _sourced_bullets(result.push_factors, rag)
    ret, ret_contrib = _sourced_bullets(result.return_intention, rag)
    return DisplacementNarrative(
        push_factors=push,
        return_intention=ret,
        contributing_sources=merge_contributing(push_contrib, ret_contrib),
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
    not per-line citations. Empty component when there's no evidence."""
    rag = fetch_rag_context(
        query=(
            f"{country_name} outlook trajectory scenario forecast risk escalation "
            f"humanitarian access response capacity {period_label}"
        ),
        limit=10,
        filters=rag_filters,
    )
    if rag.is_empty:
        return Scenarios()

    system = _build_system_prompt(country_name, period_label, _format_aggregated_for_prompt(aggregated))
    user = (
        f"Produce the forward-looking Scenarios component for {country_name}, "
        f"{period_label}. Emit four short prose fields: `most_likely`, `best_case`, "
        "`worst_case` (each a 2–4 sentence trajectory over the coming weeks / "
        "months) and `description` (the key variables the scenarios hinge on). "
        "Ground every projection in the retrieved evidence; do not invent figures.\n"
        "\n"
        "RETRIEVED EVIDENCE:\n"
        f"{rag.formatted_for_prompt}"
    )
    try:
        result = _run_component(
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
