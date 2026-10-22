"""Shared RAG → structured LLM → citation runner for situation components.

Bullet-field components (hazards, displacement, access, context domains)
share one pipeline; sectors use the lower-level helpers with custom
post-processing (scoped-RAG fallback, coverage areas).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field

from clear_pipeline.defs.situation.citations import merge_contributing, resolve_bullets
from clear_pipeline.defs.situation.prompts import (
    build_evidence_user_prompt,
    build_system_prompt,
    format_aggregated_for_prompt,
)
from clear_pipeline.defs.situation.questions import (
    format_question_block,
    question_for_schema_key,
    rag_query_for,
)
from clear_pipeline.defs.situation.rag_helper import RAGContext, fetch_rag_context
from clear_pipeline.defs.situation.schemas import SourcedBullet
from clear_pipeline.providers.llm import LLMProvider

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────
# Shared LLM coverage fields (completeness / unanswered) — mixin for
# guided component schemas so Field descriptions aren't repeated.
# ────────────────────────────────────────────────────────────────────


class GuidedCoverageLLM(BaseModel):
    """Mixin: how completely the analysis question was answered."""

    completeness: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="How completely the analysis question is answered (0–1).",
    )
    questions_not_answered: list[str] = Field(
        default_factory=list,
        description="Parts of the analysis question not supported by the evidence.",
    )


@dataclass(frozen=True)
class BulletComponentSpec:
    """Data-driven spec for a multi-bullet guided component."""

    schema_key: str
    log_label: str
    llm_schema: type[BaseModel]
    result_cls: type[BaseModel]
    bullet_fields: tuple[str, ...]
    map_instruction: str
    rag_limit: int = 20
    max_tokens: int = 2000
    sourced: bool = True
    fallback_topic: str = ""
    # LLM field → result field when names differ (e.g. answer → bullets).
    field_map: dict[str, str] | None = None


def sourced_bullets(
    raw_bullets: list[str],
    rag: RAGContext,
) -> tuple[list[SourcedBullet], dict[str, list[str]]]:
    """Resolve inline [Rn] markers into SourcedBullets + contributing map.

    Per-bullet ids are the honest signal. The RAG-union fallback belongs
    at the component level (``source_report_ids``), not stamped onto
    unmarked bullets.
    """
    clean, per_ids, contributing = resolve_bullets(raw_bullets, rag.hit_report_ids)
    bullets = [
        SourcedBullet(description=c, source_report_ids=ids)
        for c, ids in zip(clean, per_ids)
    ]
    return bullets, contributing


def run_structured(
    llm: LLMProvider,
    *,
    system_prompt: str,
    user_prompt: str,
    schema: type[BaseModel],
    cache_key: str,
    max_tokens: int,
) -> BaseModel:
    """Thin wrapper so every guided call shares retry / cache convention."""
    return llm.complete_structured(
        system=system_prompt,
        user=user_prompt,
        schema=schema,
        max_tokens=max_tokens,
        cache_key=cache_key,
    )


def run_bullet_component(
    llm: LLMProvider,
    spec: BulletComponentSpec,
    *,
    country_name: str,
    period_label: str,
    aggregated: dict[str, Any] | None,
    cache_key: str,
    rag_filters: dict[str, Any] | None = None,
    aggregated_context: str | None = None,
) -> BaseModel:
    """RAG + structured LLM + citation resolve for one bullet component.

    Returns ``spec.result_cls()`` on empty evidence or LLM failure.
    """
    question = question_for_schema_key(spec.schema_key)
    fallback = spec.fallback_topic or (
        question.name if question is not None else spec.schema_key.replace("_", " ")
    )
    rag = fetch_rag_context(
        query=rag_query_for(country_name, question, fallback=fallback),
        limit=spec.rag_limit,
        filters=rag_filters,
        mode="TOPICAL",
    )
    if rag.is_empty:
        logger.info(
            "[situation:%s] no RAG hits — returning empty component",
            spec.log_label,
        )
        return spec.result_cls()

    agg = (
        aggregated_context
        if aggregated_context is not None
        else format_aggregated_for_prompt(aggregated)
    )
    system = build_system_prompt(country_name, period_label, agg)
    user = build_evidence_user_prompt(
        format_question_block(question, fallback_topic=fallback),
        spec.map_instruction,
        rag.formatted_for_prompt,
    )
    try:
        result = run_structured(
            llm,
            system_prompt=system,
            user_prompt=user,
            schema=spec.llm_schema,
            cache_key=cache_key,
            max_tokens=spec.max_tokens,
        )
    except Exception:  # noqa: BLE001 — component-level isolation
        logger.exception(
            "[situation:%s] LLM call failed — returning empty component",
            spec.log_label,
        )
        return spec.result_cls()

    return assemble_bullet_result(spec, result, rag)


def assemble_bullet_result(
    spec: BulletComponentSpec,
    llm_result: BaseModel,
    rag: RAGContext,
) -> BaseModel:
    """Map LLM bullet fields → result schema with citations resolved."""
    field_map = spec.field_map or {f: f for f in spec.bullet_fields}
    field_values: dict[str, Any] = {}
    contribs: list[dict[str, list[str]]] = []

    for llm_field, result_field in field_map.items():
        raw = list(getattr(llm_result, llm_field, None) or [])
        if spec.sourced:
            bullets, contributing = sourced_bullets(raw, rag)
            field_values[result_field] = bullets
        else:
            clean, _ids, contributing = resolve_bullets(raw, rag.hit_report_ids)
            field_values[result_field] = clean
        contribs.append(contributing)

    contributing = merge_contributing(*contribs)
    kwargs: dict[str, Any] = {
        **field_values,
        "completeness": getattr(llm_result, "completeness", 0.0),
        "questions_not_answered": list(
            getattr(llm_result, "questions_not_answered", None) or []
        ),
        "contributing_sources": contributing,
    }
    if "source_report_ids" in spec.result_cls.model_fields:
        kwargs["source_report_ids"] = list(contributing) or rag.contributing_report_ids
    return spec.result_cls(**kwargs)
