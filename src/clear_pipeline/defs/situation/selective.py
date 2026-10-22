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
    Scenarios,
    Sectors,
    SituationAnalysisPayload,
    SourcedBullet,
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


def is_stale(leaf_key: str, current_ids: list[str], prior_index: dict[str, list[str]]) -> bool:
    """A leaf is stale when its CURRENT retrieval id-set differs from the one
    recorded last time — or when it has no recorded baseline (first selective
    pass, or a prior full/legacy generation that didn't seed the index)."""
    if leaf_key not in prior_index:
        return True
    return set(current_ids) != set(prior_index.get(leaf_key) or [])


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
        current_ids = sorted(rag.contributing_report_ids)
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
_SUMMARY_SECTIONS = (
    ("Hazards & vulnerabilities", "hazards_and_vulnerabilities"),
    ("Displacement", "displacement"),
    ("Humanitarian access", "humanitarian_access"),
)


def _bullets_text(bullets: list[Any]) -> list[str]:
    out: list[str] = []
    for b in bullets or []:
        desc = getattr(b, "description", None)
        if desc:
            out.append(desc)
    return out


def _sections_digest(result: LeafBuildResult) -> tuple[str, list[str]]:
    """Render the generated leaves into a compact digest the summary LLM reads,
    and collect the union of their source report ids (the summary's coarse
    provenance — it summarises already-cited sections rather than re-retrieving)."""
    lines: list[str] = []
    source_ids: list[str] = []

    def add_bullets(label: str, bullets: list[Any]) -> None:
        texts = _bullets_text(bullets)
        if texts:
            lines.append(f"## {label}")
            lines.extend(f"- {t}" for t in texts)

    for field_label, dom_key in (
        ("Demographics", "demographics"), ("Political", "political"),
        ("Economy", "economy"), ("Socio-cultural", "socio_culture"),
        ("Security", "security"), ("Legal & policy", "legal_policy"),
        ("Infrastructure", "infrastructure"), ("Environment", "environment"),
        ("Humanitarian coordination", "humanitarian_coordination"),
    ):
        domain = getattr(result.context_risks, dom_key, None)
        if domain is not None:
            add_bullets(f"Context — {field_label}", getattr(domain, "bullets", []) or [])
            source_ids.extend(getattr(domain, "source_report_ids", None) or [])

    haz = result.hazards_and_vulnerabilities
    add_bullets("Hazards", haz.hazards)
    add_bullets("Vulnerabilities", haz.vulnerabilities)
    dis = result.displacement
    add_bullets("Displacement — push factors", dis.push_factors)
    add_bullets("Displacement — return intention", dis.return_intention)
    acc = result.humanitarian_access
    add_bullets("Access — barriers", acc.barriers)
    add_bullets("Access — constraints", acc.constraints)
    for bset in (haz.hazards, haz.vulnerabilities, dis.push_factors,
                 dis.return_intention, acc.barriers, acc.constraints):
        for b in bset or []:
            source_ids.extend(getattr(b, "source_report_ids", None) or [])

    for sector_key, sector_display in SECTOR_KEYS:
        sector = getattr(result.sectors, sector_key, None)
        if sector is None:
            continue
        sec_lines = (
            (getattr(sector, "impact", None) or [])
            + (getattr(sector, "humanitarian_conditions", None) or [])
            + (getattr(sector, "top_needs", None) or [])
        )
        if sec_lines:
            lines.append(f"## Sector — {sector_display}")
            lines.extend(f"- {t}" for t in sec_lines)
        source_ids.extend(getattr(sector, "source_report_ids", None) or [])

    # De-dupe, preserve order.
    return "\n".join(lines), list(dict.fromkeys(source_ids))


def generate_ai_summary_fanin(
    llm: LLMProvider,
    *,
    country_name: str,
    period_label: str,
    result: LeafBuildResult,
    cache_key: str,
) -> AISummary:
    """Executive summary as a FAN-IN over the generated leaves (#1067).

    Unlike the standalone generator, this does not re-retrieve — it summarises
    the analysis's own already-evidence-grounded sections (the ``result``
    leaves). Provenance is coarse: the union of the summarised sections'
    ``source_report_ids`` (the summary cites sections, not per-line evidence).
    Returns an empty summary when the leaves carry no content."""
    from clear_pipeline.defs.situation.generate_component import run_structured
    from clear_pipeline.defs.situation.narrative import _AISummaryLLM
    from clear_pipeline.defs.situation.prompts import build_system_prompt

    digest, source_ids = _sections_digest(result)
    if not digest.strip():
        logger.info("[analysis:ai_summary] no section content — returning empty summary")
        return AISummary()

    system = build_system_prompt(
        country_name, period_label,
        "(summarise ONLY the SECTION DIGEST below — it is this analysis's other "
        "sections, already evidence-grounded)",
    )
    user = (
        f"Executive summary for {country_name}, {period_label}: 3 short sentences "
        f"(≤{_SUMMARY_MAX_WORDS} words) plus 4–6 key findings ('Subject: finding', "
        "≤30 words each). Base it ONLY on the SECTION DIGEST — do not introduce new "
        "facts and do not state cumulative multi-year totals.\n"
        "\n"
        "SECTION DIGEST:\n"
        f"{digest}"
    )
    try:
        res = run_structured(
            llm, system_prompt=system, user_prompt=user,
            schema=_AISummaryLLM, cache_key=cache_key, max_tokens=1500,
        )
    except Exception:  # noqa: BLE001 — component-level isolation
        logger.exception("[analysis:ai_summary] fan-in LLM call failed — returning empty summary")
        return AISummary()

    text = (getattr(res, "text", "") or "").strip()
    findings = [
        SourcedBullet(description=f.strip(), source_report_ids=[])
        for f in (getattr(res, "key_findings", None) or [])
        if f and f.strip()
    ]
    return AISummary(text=text, key_findings=findings, source_report_ids=source_ids)
