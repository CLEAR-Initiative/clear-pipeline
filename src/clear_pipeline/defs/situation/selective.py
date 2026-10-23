"""Leaf-level selective regeneration for situation analysis (#1067).

A situation analysis is ~24 independently-RAG-grounded LLM leaves: 9 context
pillars + 10 sectors + hazards/displacement/humanitarian_access/scenarios. Today
every regeneration re-runs all of them. This module lets a regeneration redo
ONLY the leaves whose evidence actually changed, reusing the prior analysis's
components for the rest.

The signal is per-leaf and precise: each leaf's RAG retrieval yields a set of
report ids; we record that set at generation time in the payload's
``evidence_index`` and, next time, re-probe each leaf's retrieval (cheap — no
LLM) and regenerate only the leaves whose id-set changed. A leaf absent from the
prior index (first run after this ships, or a schema bump) is treated as stale,
so the first pass is a safe full regen that seeds the index.

The retrieve/generate split in ``generate_component``/``sectors``/``narrative``
is what makes this cheap: the probe's retrieval is reused by the regeneration,
so a regenerated leaf still searches only once.

``ai_summary`` is NOT a leaf here — it is a fan-in over the other leaves (see
``generate_ai_summary_fanin``), regenerated whenever any leaf changed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel

from clear_pipeline.defs.situation.generate_component import (
    BulletComponentSpec,
    bullet_component_rag,
    run_bullet_component_from_rag,
)
from clear_pipeline.defs.situation.narrative import (
    _ACCESS_SPEC,
    _DISPLACEMENT_SPEC,
    _HAZARDS_SPEC,
    context_domain_spec,
)
from clear_pipeline.defs.situation.prompts import format_aggregated_for_prompt
from clear_pipeline.defs.situation.questions import CONTEXT_DOMAIN_KEYS, SECTOR_KEYS
from clear_pipeline.defs.situation.rag_helper import RAGContext
from clear_pipeline.defs.situation.schemas import (
    AISummary,
    ContextRisks,
    DisplacementNarrative,
    HazardsAndVulnerabilities,
    HumanitarianAccess,
    RiskDomain,
    Scenarios,
    SectorAnalysis,
    Sectors,
    SituationAnalysisPayload,
)
from clear_pipeline.defs.situation.sectors import sector_from_rag, sector_rag
from clear_pipeline.providers.llm import LLMProvider

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────
# Leaf registry
# ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Leaf:
    """One independently-regenerable analysis leaf.

    ``key`` is the stable path used in ``evidence_index`` and logs
    ("context_risks.security", "sectors.health", "displacement", …).
    ``container``/``field`` say where the component sits in the payload:
    a nested field of ``context_risks``/``sectors``, or a top-level field.
    """

    key: str
    kind: str  # "bullet" | "sector" | "scenarios"
    container: str | None  # "context_risks" | "sectors" | None (top-level)
    field: str
    spec: BulletComponentSpec | None = None
    sector_key: str | None = None
    sector_display: str | None = None


def build_leaves() -> list[Leaf]:
    """The full leaf set, in generation order (context pillars, singletons,
    sectors, scenarios). Sequential order preserves Anthropic prompt-cache reuse
    across the leaves that do regenerate."""
    leaves: list[Leaf] = []
    for domain_key in CONTEXT_DOMAIN_KEYS:
        leaves.append(Leaf(
            key=f"context_risks.{domain_key}", kind="bullet",
            container="context_risks", field=domain_key,
            spec=context_domain_spec(domain_key),
        ))
    leaves.append(Leaf(
        key="hazards_and_vulnerabilities", kind="bullet", container=None,
        field="hazards_and_vulnerabilities", spec=_HAZARDS_SPEC,
    ))
    leaves.append(Leaf(
        key="displacement", kind="bullet", container=None,
        field="displacement", spec=_DISPLACEMENT_SPEC,
    ))
    leaves.append(Leaf(
        key="humanitarian_access", kind="bullet", container=None,
        field="humanitarian_access", spec=_ACCESS_SPEC,
    ))
    for sector_key, sector_display in SECTOR_KEYS:
        leaves.append(Leaf(
            key=f"sectors.{sector_key}", kind="sector",
            container="sectors", field=sector_key,
            sector_key=sector_key, sector_display=sector_display,
        ))
    leaves.append(Leaf(
        key="scenarios", kind="scenarios", container=None, field="scenarios",
    ))
    return leaves


def _leaf_rag(
    leaf: Leaf, *, country_name: str, period_label: str, rag_filters: dict[str, Any] | None,
) -> tuple[RAGContext, dict[str, Any]]:
    """Retrieval half — the exact search the leaf's generator runs. Returns
    ``(rag, extras)`` where extras carries kind-specific state (sector fallback
    flag) the generation half needs."""
    if leaf.kind == "bullet":
        assert leaf.spec is not None
        return bullet_component_rag(leaf.spec, country_name=country_name, rag_filters=rag_filters), {}
    if leaf.kind == "sector":
        assert leaf.sector_key is not None and leaf.sector_display is not None
        rag, used_fallback = sector_rag(
            sector_key=leaf.sector_key,  # type: ignore[arg-type]
            sector_display_name=leaf.sector_display,
            country_name=country_name, rag_filters=rag_filters,
        )
        return rag, {"used_fallback": used_fallback}
    # scenarios
    from clear_pipeline.defs.situation.narrative import scenarios_rag

    return scenarios_rag(
        country_name=country_name, period_label=period_label, rag_filters=rag_filters,
    ), {}


def _leaf_generate(
    leaf: Leaf, rag: RAGContext, extras: dict[str, Any], *,
    llm: LLMProvider, country_name: str, period_label: str,
    aggregated: dict[str, Any] | None, aggregated_context: str, cache_key: str,
) -> BaseModel:
    """Generation half — LLM + assemble, reusing the probe's ``rag`` so a
    regenerated leaf never searches twice."""
    if leaf.kind == "bullet":
        assert leaf.spec is not None
        return run_bullet_component_from_rag(
            llm, leaf.spec, rag,
            country_name=country_name, period_label=period_label,
            aggregated=None, cache_key=cache_key, aggregated_context=aggregated_context,
        )
    if leaf.kind == "sector":
        assert leaf.sector_key is not None and leaf.sector_display is not None
        return sector_from_rag(
            llm, sector_key=leaf.sector_key,  # type: ignore[arg-type]
            sector_display_name=leaf.sector_display,
            country_name=country_name, period_label=period_label,
            aggregated_context=aggregated_context, cache_key=cache_key,
            rag=rag, used_fallback=extras["used_fallback"],
        )
    from clear_pipeline.defs.situation.narrative import scenarios_from_rag

    return scenarios_from_rag(
        llm, rag, country_name=country_name, period_label=period_label,
        aggregated=aggregated, cache_key=cache_key,
    )


def _rag_fingerprint(rag: RAGContext) -> list[str]:
    """Chunk-level evidence fingerprint: the sorted set of hit (chunk) ids.

    Finer than report ids — a figure re-merge or a re-embed that adds/replaces
    chunks under an EXISTING report id changes the chunk-id set, so a stale
    section is still detected even when its report-id set is unchanged (#1067
    review). Falls back to report ids when hits carry no id (test doubles or
    merged contexts built without raw hits)."""
    ids = [str(h.get("id")) for h in rag.hits if h.get("id")]
    return sorted(ids) if ids else sorted(rag.contributing_report_ids)


def is_stale(leaf_key: str, current_ids: list[str], prior_index: dict[str, list[str]]) -> bool:
    """A leaf is stale when its CURRENT evidence fingerprint differs from the one
    recorded last time — or when it has no recorded baseline (first selective
    pass, or a prior full/legacy generation that didn't seed the index)."""
    if leaf_key not in prior_index:
        return True
    return set(current_ids) != set(prior_index.get(leaf_key) or [])


def _component_has_content(component: BaseModel) -> bool:
    """Whether a generated leaf actually carries content. Used to tell a genuine
    empty (no evidence) from a failed/empty generation that must be retried."""
    if isinstance(component, RiskDomain):
        return bool(component.bullets)
    if isinstance(component, SectorAnalysis):
        return bool(
            component.severity is not None or component.impact
            or component.humanitarian_conditions or component.vulnerable_sections
            or component.top_needs or component.priority_interventions
        )
    if isinstance(component, HazardsAndVulnerabilities):
        return bool(component.hazards or component.vulnerabilities)
    if isinstance(component, DisplacementNarrative):
        return bool(component.push_factors or component.return_intention)
    if isinstance(component, HumanitarianAccess):
        return bool(component.barriers or component.constraints)
    if isinstance(component, Scenarios):
        return bool(
            component.most_likely or component.best_case
            or component.worst_case or component.description
        )
    return True


# ────────────────────────────────────────────────────────────────────
# Leaf build (full or selective)
# ────────────────────────────────────────────────────────────────────


@dataclass
class LeafBuildResult:
    context_risks: ContextRisks
    hazards_and_vulnerabilities: HazardsAndVulnerabilities
    displacement: DisplacementNarrative
    humanitarian_access: HumanitarianAccess
    sectors: Sectors
    scenarios: Scenarios
    evidence_index: dict[str, list[str]]
    regenerated_keys: list[str] = field(default_factory=list)
    reused_keys: list[str] = field(default_factory=list)

    @property
    def any_regenerated(self) -> bool:
        return bool(self.regenerated_keys)


def _prior_component(prior: SituationAnalysisPayload | None, leaf: Leaf) -> BaseModel | None:
    if prior is None:
        return None
    container = getattr(prior, leaf.container) if leaf.container else prior
    return getattr(container, leaf.field, None)


def build_leaf_components(
    llm: LLMProvider,
    *,
    prior_payload: dict[str, Any] | None,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
    rag_filters: dict[str, Any] | None,
    log=None,
) -> LeafBuildResult:
    """Build every leaf, regenerating only the stale ones.

    ``prior_payload`` is the current analysis row's ``data`` blob (or None → full
    regen: every leaf stale). For each leaf we probe its retrieval, diff the
    id-set against ``prior_payload.evidence_index``, and either regenerate (reuse
    the probe's rag) or reuse the prior component. Deterministic leaves
    (datapoints/sources) are built by the caller, not here."""
    log = log or logger
    aggregated_context = format_aggregated_for_prompt(aggregated)

    prior: SituationAnalysisPayload | None = None
    prior_index: dict[str, list[str]] = {}
    if prior_payload:
        try:
            prior = SituationAnalysisPayload.model_validate(prior_payload)
            prior_index = dict(prior.evidence_index or {})
        except Exception as exc:  # noqa: BLE001 — unparseable prior → safe full regen
            log.warning("[analysis:selective] prior payload unparseable (%s) — full regen", exc)
            prior = None
            prior_index = {}

    context_domains: dict[str, BaseModel] = {}
    sectors: dict[str, BaseModel] = {}
    singletons: dict[str, BaseModel] = {}
    evidence_index: dict[str, list[str]] = {}
    regenerated: list[str] = []
    reused: list[str] = []

    for leaf in build_leaves():
        rag, extras = _leaf_rag(
            leaf, country_name=country_name, period_label=period_label, rag_filters=rag_filters,
        )
        current_ids = _rag_fingerprint(rag)
        prior_component = _prior_component(prior, leaf)

        if prior_component is not None and not is_stale(leaf.key, current_ids, prior_index):
            component: BaseModel = prior_component
            # Keep the prior baseline so an unchanged leaf's fingerprint persists.
            evidence_index[leaf.key] = prior_index.get(leaf.key, current_ids)
            reused.append(leaf.key)
        else:
            component = _leaf_generate(
                leaf, rag, extras, llm=llm, country_name=country_name,
                period_label=period_label, aggregated=aggregated,
                aggregated_context=aggregated_context, cache_key=cache_key,
            )
            if not rag.is_empty and not _component_has_content(component):
                # Non-empty evidence but empty output → a failed generation. Don't
                # regress to blank (keep the prior component if any) and DON'T
                # record a fingerprint, so the next run retries this leaf instead
                # of reusing the blank forever (#1067 review).
                log.warning(
                    "[analysis:selective] leaf %s produced no content over %d hit(s) — "
                    "keeping prior, retrying next run", leaf.key, rag.hit_count,
                )
                if prior_component is not None:
                    component = prior_component
                regenerated.append(leaf.key)
            else:
                evidence_index[leaf.key] = current_ids
                regenerated.append(leaf.key)

        if leaf.container == "context_risks":
            context_domains[leaf.field] = component
        elif leaf.container == "sectors":
            sectors[leaf.field] = component
        else:
            singletons[leaf.field] = component

    log.info(
        "[analysis:selective] %d leaves regenerated, %d reused%s",
        len(regenerated), len(reused),
        "" if prior is not None else " (full regen — no usable prior)",
    )

    return LeafBuildResult(
        context_risks=ContextRisks(**context_domains),
        hazards_and_vulnerabilities=singletons["hazards_and_vulnerabilities"],  # type: ignore[arg-type]
        displacement=singletons["displacement"],  # type: ignore[arg-type]
        humanitarian_access=singletons["humanitarian_access"],  # type: ignore[arg-type]
        sectors=Sectors(**sectors),
        scenarios=singletons["scenarios"],  # type: ignore[arg-type]
        evidence_index=evidence_index,
        regenerated_keys=regenerated,
        reused_keys=reused,
    )


# ────────────────────────────────────────────────────────────────────
# AI summary — fan-in over the leaves (not an independent retrieval)
# ────────────────────────────────────────────────────────────────────

_SUMMARY_MAX_WORDS = 80
# Cap the fan-in evidence so the prompt stays bounded on a large analysis.
_SUMMARY_MAX_EVIDENCE = 80


def _summary_evidence(result: LeafBuildResult) -> tuple[str, list[str]]:
    """Build a numbered ``[Rn]`` evidence list from the leaves' ALREADY-RESOLVED
    per-line citations (each leaf's ``contributing_sources`` maps report_id →
    [fact lines]). The fan-in summary cites ``[Rn]`` and we resolve the markers
    back, so ``ai_summary.contributing_sources`` is populated again from the
    reports its sections cite — no second retrieval (#1067 review: the fan-in had
    dropped per-line citations). Returns ``(formatted_block, hit_report_ids)``
    index-aligned to the ``[Rn]`` markers."""
    lines: list[str] = []
    hit_report_ids: list[str] = []
    seen: set[tuple[str, str]] = set()

    def add(cs: dict[str, list[str]] | None) -> None:
        for rid, facts in (cs or {}).items():
            for fact in facts or []:
                if len(hit_report_ids) >= _SUMMARY_MAX_EVIDENCE:
                    return
                key = (rid, fact)
                if key in seen:
                    continue
                seen.add(key)
                hit_report_ids.append(rid)
                lines.append(f"[R{len(hit_report_ids)}] {fact}")

    for dom_key in CONTEXT_DOMAIN_KEYS:
        domain = getattr(result.context_risks, dom_key, None)
        if domain is not None:
            add(getattr(domain, "contributing_sources", None))
    add(result.hazards_and_vulnerabilities.contributing_sources)
    add(result.displacement.contributing_sources)
    add(result.humanitarian_access.contributing_sources)
    for sector_key, _ in SECTOR_KEYS:
        sector = getattr(result.sectors, sector_key, None)
        if sector is not None:
            add(getattr(sector, "contributing_sources", None))

    return "\n".join(lines), hit_report_ids


def generate_ai_summary_fanin(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    result: LeafBuildResult,
    cache_key: str,
) -> AISummary:
    """Executive summary as a FAN-IN over the generated leaves (#1067).

    Does not re-retrieve — it summarises the analysis's own sections. The
    evidence it reads is those sections' already-cited facts presented as a
    numbered ``[Rn]`` list, so the summary carries real per-line citations
    (``contributing_sources`` + per-finding source ids), resolved the same way
    the other components resolve theirs. Returns an empty summary when the
    sections carry no cited content."""
    from clear_pipeline.defs.situation.citations import merge_contributing, resolve_prose
    from clear_pipeline.defs.situation.generate_component import run_structured, sourced_bullets
    from clear_pipeline.defs.situation.narrative import _AISummaryLLM
    from clear_pipeline.defs.situation.prompts import build_system_prompt

    formatted, hit_report_ids = _summary_evidence(result)
    if not formatted.strip():
        logger.info("[analysis:ai_summary] no cited section content — returning empty summary")
        return AISummary()

    # Synthetic RAGContext so the shared citation resolvers ([Rn] → report id)
    # work unchanged over the section-derived evidence.
    rag = RAGContext(
        formatted_for_prompt=formatted,
        contributing_report_ids=list(dict.fromkeys(hit_report_ids)),
        hit_report_ids=hit_report_ids,
        hit_count=len(hit_report_ids),
    )

    system = build_system_prompt(
        country_name, period_label,
        "(summarise ONLY the numbered EVIDENCE below — this analysis's own "
        "sections, already evidence-grounded; cite the [Rn] markers)",
    )
    user = (
        f"Executive summary for {country_name}, {period_label}: 3 short sentences "
        f"(≤{_SUMMARY_MAX_WORDS} words) plus 4–6 key findings ('Subject: finding', "
        "≤30 words each). Base it ONLY on the EVIDENCE below; every key finding "
        "must end with its [Rn] marker(s). Do not introduce new facts and do not "
        "state cumulative multi-year totals.\n"
        "\n"
        "EVIDENCE:\n"
        f"{formatted}"
    )
    try:
        res = run_structured(
            llm, system_prompt=system, user_prompt=user,
            schema=_AISummaryLLM, cache_key=cache_key, max_tokens=1500,
        )
    except Exception:  # noqa: BLE001 — component-level isolation
        logger.exception("[analysis:ai_summary] fan-in LLM call failed — returning empty summary")
        return AISummary()

    text, text_contributing = resolve_prose(getattr(res, "text", "") or "", rag.hit_report_ids)
    findings, findings_contributing = sourced_bullets(
        list(getattr(res, "key_findings", None) or []), rag,
    )
    contributing = merge_contributing(text_contributing, findings_contributing)
    return AISummary(
        text=text.strip(),
        key_findings=findings,
        source_report_ids=list(contributing) or rag.contributing_report_ids,
        contributing_sources=contributing,
    )
