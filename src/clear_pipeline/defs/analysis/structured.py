"""Structured per-node generation (ADR-0015, slice 2).

The archetype-aware sibling of ``descriptive.analyse_node``: instead of a generic
``findings: list[str]``, the analyst fills the node's **archetype** schema (from
the registry) — so Displacement answers the ``displacement`` object, a Sector
answers severity+needs, etc. Returns a :class:`output_schemas.NodeEnvelope`.

Kept separate from ``descriptive.py`` so the existing generic path is untouched;
the assets switch to this behind the ``ANALYSIS_STRUCTURED`` flag.

``render_envelope_text`` renders a structured answer back to text for the stages
that grade/rank on text (the judge, the gaps ranker) so they consume structured
nodes without their own rewrite.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable

from pydantic import BaseModel, Field, create_model

from clear_pipeline.defs.analysis.citations import resolve_marker_tokens, strip_markers
from clear_pipeline.defs.analysis.frame import Frame, build_rag_filters
from clear_pipeline.defs.analysis.output_schemas import NodeEnvelope
from clear_pipeline.defs.analysis.profiles import Caps
from clear_pipeline.defs.analysis.prompts import build_evidence_user_prompt, build_system_prompt
from clear_pipeline.defs.analysis.rag_helper import fetch_rag_context
from clear_pipeline.defs.analysis.registry import provenance_stamp
from clear_pipeline.defs.analysis.runtime import BudgetExhaustedError, run_node
from clear_pipeline.defs.analysis.schema_registry import entry_for
from clear_pipeline.defs.analysis.taxonomy import TaxonomyNode, children_of

# The archetype schemas are nested (RangeFigure values, per-item group/location/
# source_ids), so their JSON is far longer than the generic path's flat
# list[str] — 2000 tokens truncates mid-string on figure-heavy nodes. Give the
# structured path a roomier, env-overridable cap.
_DEFAULT_STRUCTURED_MAX_TOKENS = int(
    os.environ.get("LLM_ANALYSIS_STRUCTURED_MAX_TOKENS", "4000")
)
_DEFAULT_CAPS = Caps(max_turns=1, max_tokens=_DEFAULT_STRUCTURED_MAX_TOKENS)

# Live switch for the structured (archetype) generation path. Default OFF — the
# assets keep their existing generic descriptive behaviour until this is set, so
# the diff below is complete but live behaviour is unchanged until flipped (and
# then needs a dev validation run before prod — ADR-0015, slice 2).
_STRUCTURED_ENV = "ANALYSIS_STRUCTURED"
_TRUE = {"1", "true", "yes", "on"}


def structured_enabled() -> bool:
    """Whether the structured archetype path is live (``ANALYSIS_STRUCTURED``)."""
    return os.environ.get(_STRUCTURED_ENV, "").strip().lower() in _TRUE

# Per-archetype answering hint appended to the evidence prompt.
_ARCHETYPE_HINT: dict[str, str] = {
    "Figures": "Report counts/scale as `figures` — each with a metric, a value "
               "(low/high/point + unit), and group/location where known.",
    "Drivers": "List the drivers in `factors` — each a factor label + description "
               "+ locations + scale_or_severity.",
    "SeverityNeeds": "Set `severity` (none/low/medium/high/critical), list `top_needs`, "
                     "give `affected` numbers if evidenced, and supporting `findings`.",
    "Distribution": "List `options` — each with its share (low/high/point), by_group "
                    "split, and a short description.",
    "StatusGaps": "Set `status`, list `gaps` and `enablers`, and a `comparison` if relevant.",
    "Described": "List `findings` — each a description with its locations and source_ids.",
    "Section": "List `findings` summarising this section.",  # composed from children at assembly
}

_BASE_INSTRUCTION = (
    "Answer the QUESTION by filling the JSON schema from the RETRIEVED EVIDENCE. "
    "Ground every value in the evidence and record the backing report ids in each "
    "item's `source_ids`; omit anything the evidence does not support. Report only "
    "the most significant items (at most 8 per list), each stated concisely — do "
    "not enumerate exhaustively. Rate how completely you answered in `completeness` "
    "(0-1) and list any unanswered parts in `questions_not_answered`. Write a 1-2 "
    "sentence `summary`."
)


def _output_model(archetype_model: type[BaseModel]) -> type[BaseModel]:
    """The LLM output model = the archetype fields + the envelope's self-rating."""
    return create_model(
        f"Node{archetype_model.__name__}Output",
        __base__=archetype_model,
        summary=(str, Field(default="", description="1-2 sentence synthesis.")),
        completeness=(float, Field(default=0.0, ge=0.0, le=1.0)),
        questions_not_answered=(list[str], Field(default_factory=list)),
    )


_ENVELOPE_FIELDS = {"summary", "completeness", "questions_not_answered"}

# Keys whose list values are citation tokens the LLM emitted ("R1"/"[R3]"), to be
# resolved to real report ids against the RAG hit map.
_SOURCE_ID_KEYS = {"source_ids", "source_report_ids"}


def _resolve_structured_citations(obj, hit_report_ids: list[str]):
    """Recursively resolve the LLM's citation markers in a structured archetype
    payload: map every ``source_ids`` list (``R1``/``[R1]``/``1``) to real report
    ids, and strip inline ``[Rn]`` markers out of every free-text string. Returns
    a new structure (the model only saw ``[Rn]`` labels, so it cannot know real
    ids — this is where they get attached, mirroring the generic path's
    ``resolve_bullets``)."""
    if isinstance(obj, dict):
        return {
            k: (
                resolve_marker_tokens(v, hit_report_ids)
                if k in _SOURCE_ID_KEYS and isinstance(v, list)
                else _resolve_structured_citations(v, hit_report_ids)
            )
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_resolve_structured_citations(x, hit_report_ids) for x in obj]
    if isinstance(obj, str):
        return strip_markers(obj)
    return obj


def _report_meta_from_rag(rag) -> dict[str, dict]:
    """Report title/url/date per cited report, lifted from the RAG hits — the
    authoritative title source (covers every cited KB report, unlike a
    datapoint-only lookup). clear-api returns the analysis ``data`` verbatim, so
    these titles must be embedded here or the frontend shows 'Untitled report'."""
    meta: dict[str, dict] = {}
    for hit in getattr(rag, "hits", []) or []:
        rid = hit.get("reportId")
        if not rid or rid in meta:
            continue
        meta[rid] = {
            "report_title": hit.get("reportTitle") or "",
            "source_url": hit.get("sourceUrl") or "",
            "published_at": hit.get("publishedAt") or "",
        }
    return meta


def _empty_envelope(node: TaxonomyNode, archetype: str) -> NodeEnvelope:
    return NodeEnvelope(
        node_key=node.key, name=node.name, task=node.task, level=node.level,
        archetype=archetype, summary="", completeness=0.0,
        questions_not_answered=[node.question], source_report_ids=[], data={},
    )


def analyse_node_structured(
    *,
    node: TaxonomyNode,
    frame: Frame,
    scope_label: str,
    provider,
    aggregated_context: str | None = None,
    budget=None,
    caps: Caps | None = None,
    rag_limit: int = 20,
    include_time_range: bool = False,
    effective_end: str | None = None,
) -> tuple[NodeEnvelope, dict]:
    """Generate one node's STRUCTURED answer (its archetype). Returns the
    :class:`NodeEnvelope` and a run-accounting dict (status/usage/cost/provenance/
    rag_hit_count) — the caller persists both, like ``NodeAnalysis``."""
    caps = caps or _DEFAULT_CAPS
    entry = entry_for(node.key)

    filters = build_rag_filters(
        frame,
        country_scope_id=(frame.location_ids[0] if len(frame.location_ids) == 1 else None),
        include_time_range=include_time_range,
        effective_end=effective_end,
    )
    rag = fetch_rag_context(
        query=f"{scope_label} {node.question}", limit=rag_limit, filters=filters, mode="TOPICAL",
    )

    if rag.is_empty:
        acct = {
            "status": "best_effort",
            "usage": dict(getattr(provider, "total_usage", {}) or {}),
            "cost_usd": 0.0,
            "provenance": provenance_stamp(getattr(provider, "model", "")),
            "rag_hit_count": 0,
            "report_meta": {},
        }
        return _empty_envelope(node, entry.archetype), acct

    system = build_system_prompt(scope_label, _period_label(frame), aggregated_context or "")
    instruction = _BASE_INSTRUCTION + " " + _ARCHETYPE_HINT.get(entry.archetype, _ARCHETYPE_HINT["Described"])
    question_block = f"QUESTION ({node.name}):\n{node.question}"

    def build_user(_evidence):
        return build_evidence_user_prompt(
            question_block, instruction, rag.formatted_for_prompt, include_completeness=True,
        )

    outcome = run_node(
        provider=provider, system=system, build_user=build_user,
        schema=_output_model(entry.model), caps=caps, budget=budget,
    )
    dumped = outcome.output.model_dump()
    raw_data = {k: v for k, v in dumped.items() if k not in _ENVELOPE_FIELDS}
    # Resolve the model's [Rn]/R1 citation markers to real report ids (the model
    # only ever saw [Rn] labels in the evidence — this is where ids get attached).
    data = _resolve_structured_citations(raw_data, rag.hit_report_ids)

    env = NodeEnvelope(
        node_key=node.key, name=node.name, task=node.task, level=node.level,
        archetype=entry.archetype,
        summary=strip_markers(dumped.get("summary", "")),
        completeness=dumped.get("completeness", 0.0),
        questions_not_answered=list(dumped.get("questions_not_answered", [])),
        source_report_ids=list(rag.contributing_report_ids),
        data=data,
    )
    acct = {
        "status": outcome.status, "usage": outcome.usage, "cost_usd": outcome.cost_usd,
        "provenance": outcome.provenance, "rag_hit_count": rag.hit_count,
        "report_meta": _report_meta_from_rag(rag),
    }
    return env, acct


def _period_label(frame: Frame) -> str:
    start = (frame.window_start or "")[:10]
    end = (frame.window_end or "")[:10]
    return f"{start} to {end}" if end else f"since {start}" if start else "the period"


def render_envelope_text(env: NodeEnvelope) -> str:
    """A plain-text rendering of a structured answer, for the stages that grade /
    rank on text (the judge, the gaps ranker). The summary leads; the archetype
    data follows as compact JSON so no signal is lost."""
    parts = [env.summary.strip()] if env.summary.strip() else []
    if env.data:
        parts.append(json.dumps(env.data, ensure_ascii=False, separators=(",", ":")))
    if env.questions_not_answered:
        parts.append("NOT ANSWERED: " + "; ".join(env.questions_not_answered))
    return "\n".join(parts)


# ────────────────────────────────────────────────────────────────────
# L1 composition + backward-compatible artifact (ADR-0015, slice 2 wiring)
# ────────────────────────────────────────────────────────────────────

_SECTION_SUMMARY_CAP = 600


def compose_l1(
    *,
    node: TaxonomyNode,
    frame: Frame,
    scope_label: str,
    make_provider: Callable[[], object],
    aggregated_context: str | None = None,
    budget=None,
    rag_limit: int = 20,
    include_time_range: bool = False,
    effective_end: str | None = None,
    logger=None,
) -> tuple[NodeEnvelope, list[dict]]:
    """Generate ONE L1 node's structured envelope.

    Most L1s are a single archetype call. The two **Section** L1s (Displacement,
    Context) are *composed*: each of their L2 children is generated structurally
    and folded into ``data.subsections`` — this is what lets ``assemble_v5`` fill
    the v5 ``displacement`` / ``context_risks.<domain>`` shapes (non-regression).

    ``make_provider`` returns a FRESH provider each call (one per node — doc §7
    ``total_usage`` isolation). ``logger`` (optional, duck-typed ``.info``) reports
    per-child progress for Section nodes. Returns the envelope and the per-call
    accounting dicts (one per LLM call; a Section returns one per child)."""
    log = logger.info if logger is not None else (lambda *a, **k: None)
    entry = entry_for(node.key)
    if entry.archetype != "Section":
        provider = make_provider()
        env, acct = analyse_node_structured(
            node=node, frame=frame, scope_label=scope_label, provider=provider,
            aggregated_context=aggregated_context, budget=budget, rag_limit=rag_limit,
            include_time_range=include_time_range, effective_end=effective_end,
        )
        return env, [acct]

    children = children_of(node)
    log("[compose] %s [Section]: composing %d L2 subsections", node.key, len(children))
    subsections: dict[str, dict] = {}
    accts: list[dict] = []
    comps: list[float] = []
    qna: list[str] = []
    srcs: list[str] = []
    child_summaries: list[str] = []
    for i, child in enumerate(children, 1):
        provider = make_provider()
        try:
            child_env, acct = analyse_node_structured(
                node=child, frame=frame, scope_label=scope_label, provider=provider,
                aggregated_context=aggregated_context, budget=budget, rag_limit=rag_limit,
                include_time_range=include_time_range, effective_end=effective_end,
            )
        except BudgetExhaustedError:
            # Budget gone mid-Section: keep the subsections already generated and
            # mark this one and the rest unanswered, rather than losing the whole
            # Section (e.g. Context after 8 of 9 domains completed).
            log("[compose] %s: budget exhausted at %s — keeping %d/%d subsections",
                node.key, child.key, len(subsections), len(children))
            for rem in children[i - 1:]:
                subsections[rem.key] = _empty_envelope(
                    rem, entry_for(rem.key).archetype
                ).model_dump()
                qna.append(rem.question)
            break
        except Exception:  # noqa: BLE001 — isolate one child; the Section keeps its others
            log("[compose] %s: subsection %s failed — marking unanswered", node.key, child.key)
            subsections[child.key] = _empty_envelope(
                child, entry_for(child.key).archetype
            ).model_dump()
            qna.append(child.question)
            continue
        log(
            "[compose] %s: subsection (%d/%d) %s [%s] → completeness=%.2f, rag=%d",
            node.key, i, len(children), child.key, entry_for(child.key).archetype,
            child_env.completeness or 0.0, acct.get("rag_hit_count", 0),
        )
        subsections[child.key] = child_env.model_dump()
        accts.append(acct)
        if child_env.completeness is not None:
            comps.append(child_env.completeness)
        qna.extend(child_env.questions_not_answered)
        srcs.extend(child_env.source_report_ids)
        if child_env.summary.strip():
            child_summaries.append(f"{child.name}: {child_env.summary.strip()}")

    overview = " ".join(child_summaries)
    env = NodeEnvelope(
        node_key=node.key, name=node.name, task=node.task, level=node.level,
        archetype="Section",
        summary=overview[:_SECTION_SUMMARY_CAP],
        completeness=(sum(comps) / len(comps) if comps else 0.0),
        questions_not_answered=sorted(set(qna)),
        source_report_ids=sorted(set(srcs)),
        data={"overview": overview, "subsections": subsections, "roll_up": {}},
    )
    return env, accts


# Text-bearing keys across the 7 archetypes — flattened into descriptive findings
# so the judge / gaps / overview read the FULL answer, not just the summary.
_FINDING_TEXT_KEYS = ("description", "factor", "metric", "status", "option")


def _flatten_descriptions(data, *, cap: int = 60) -> list[dict]:
    """Human-readable ``(description, source_report_ids)`` bullets pulled from an
    archetype payload (recursing into a Section's subsections). Bounded to ``cap``
    bullets. The archetype items' ``source_ids`` are already resolved report ids."""
    out: list[dict] = []

    def walk(obj) -> None:
        if len(out) >= cap:
            return
        if isinstance(obj, dict):
            for k in _FINDING_TEXT_KEYS:
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    out.append({
                        "description": v.strip(),
                        "source_report_ids": list(
                            obj.get("source_ids") or obj.get("source_report_ids") or []
                        ),
                    })
                    break
            for v in obj.values():
                if isinstance(v, (list, dict)):
                    walk(v)
        elif isinstance(obj, list):
            for x in obj:
                walk(x)

    walk(data or {})
    return out


def envelope_to_descriptive(env: NodeEnvelope) -> dict:
    """Project a :class:`NodeEnvelope` onto the legacy ``descriptive`` artifact
    shape (a :class:`descriptive.NodeDescriptive` dump) so the gaps / judge /
    revision / overview-synthesis stages read the FULL answer, not just the
    summary. The summary leads; the archetype detail (every domain of a Section
    included) follows as findings. The structured form still rides alongside under
    the artifact's ``envelope`` key (read by ``assemble_v5``)."""
    findings: list[dict] = []
    if env.summary.strip():
        findings.append(
            {"description": env.summary.strip(), "source_report_ids": list(env.source_report_ids)}
        )
    findings.extend(_flatten_descriptions(env.data))
    return {
        "node_key": env.node_key,
        "node_name": env.name,
        "task": env.task,
        "level": env.level,
        "findings": findings,
        "completeness": env.completeness,
        "questions_not_answered": list(env.questions_not_answered),
        "source_report_ids": list(env.source_report_ids),
        "contributing_sources": {},
    }


def _aggregate_accts(accts: list[dict]) -> dict:
    """Fold per-call accounting (a Section has one per child) into one artifact-
    level record: summed cost / rag hits / numeric usage, ok-if-any status."""
    usage: dict[str, float] = {}
    for a in accts:
        for k, v in (a.get("usage") or {}).items():
            if isinstance(v, (int, float)):
                usage[k] = usage.get(k, 0) + v
    status = (
        "ok" if any(a.get("status") == "ok" for a in accts)
        else (accts[0].get("status") if accts else "best_effort")
    )
    return {
        "status": status,
        "usage": usage,
        "cost_usd": sum(float(a.get("cost_usd", 0.0) or 0.0) for a in accts),
        "provenance": accts[0].get("provenance") if accts else {},
        "rag_hit_count": sum(int(a.get("rag_hit_count", 0) or 0) for a in accts),
    }


def build_structured_artifact(
    *, iso3: str, window_kind: str, frame: Frame, node: TaxonomyNode,
    env: NodeEnvelope, accts: list[dict],
) -> dict:
    """The S3 artifact a structured node persists: the legacy ``descriptive``
    projection (for gaps/judge/revision) + the full ``envelope`` (for overview's
    ``assemble_v5``). Same outer ``{iso3, node, descriptive, …}`` shape as the
    generic path, so ``load_descriptive`` / ``load_final_descriptive`` are
    unchanged."""
    agg = _aggregate_accts(accts)
    # Union the per-call report metadata (one call per Section child) so the
    # overview can build sources.reports with real titles.
    report_meta: dict[str, dict] = {}
    for a in accts:
        for rid, m in (a.get("report_meta") or {}).items():
            report_meta.setdefault(rid, m)
    return {
        "iso3": iso3,
        "window_kind": window_kind,
        "frame": frame.upsert_kwargs(),
        "node": {"key": node.key, "name": node.name, "task": node.task, "level": node.level},
        "descriptive": envelope_to_descriptive(env),
        "envelope": env.model_dump(),
        "report_meta": report_meta,
        "status": agg["status"],
        "usage": agg["usage"],
        "cost_usd": agg["cost_usd"],
        "provenance": agg["provenance"],
        "rag_hit_count": agg["rag_hit_count"],
        "structured": True,
    }
