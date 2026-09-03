"""Tests for the Phase C narrative generators.

Covers the four LLM-driven components (AI summary, context risks,
hazards & vulnerabilities, displacement narrative). Every test mocks
the LLM provider + the RAG search so no network / no LLM budget.

Focus is on:
  - Output shape — right Pydantic type, empty defaults on empty RAG.
  - `source_report_ids` population — populated from RAG hits, not
    invented by the LLM (coarse-grained citation model).
  - Failure isolation — LLM error returns the empty default rather
    than crashing the whole situation analysis.
"""

from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.defs.situation.narrative import (
    _AISummaryLLM,
    _ContextRisksLLM,
    _DisplacementLLM,
    _HazardsVulnerabilitiesLLM,
    generate_ai_summary,
    generate_context_risks,
    generate_displacement_narrative,
    generate_hazards_and_vulnerabilities,
)
from clear_pipeline.defs.situation.rag_helper import RAGContext


def _fake_rag_context(
    *,
    hits: int,
    report_ids: list[str] | None = None,
    hit_report_ids: list[str] | None = None,
) -> RAGContext:
    ids = report_ids if report_ids is not None else [f"r-{i}" for i in range(hits)]
    # Per-hit ids ([Rn]-aligned). Default: one hit per unique id, cycling if the
    # deduped `ids` is shorter than `hits`, so [Rn] markers resolve in tests.
    if hit_report_ids is None:
        hit_report_ids = [ids[i % len(ids)] if ids else "" for i in range(hits)]
    return RAGContext(
        formatted_for_prompt="[R1] ...\n" * hits,
        contributing_report_ids=ids,
        hit_report_ids=hit_report_ids,
        hit_count=hits,
    )


# ────────────────────────────────────────────────────────────────────
# AI Summary
# ────────────────────────────────────────────────────────────────────


class TestGenerateAISummary:
    def test_empty_rag_returns_empty_default(self):
        # If the search returns nothing, we DO NOT call the LLM —
        # zero-evidence prose invites hallucination. Return the
        # empty default and let the dashboard render an empty state.
        with patch(
            "clear_pipeline.defs.situation.narrative.fetch_rag_context",
            return_value=_fake_rag_context(hits=0),
        ):
            llm = MagicMock()
            result = generate_ai_summary(
                llm, country_name="Sudan", period_label="2026",
                aggregated=None, cache_key="k",
            )
        assert result.text == ""
        assert result.source_report_ids == []
        llm.complete_structured.assert_not_called()

    def test_happy_path_populates_text_and_source_ids(self):
        with patch(
            "clear_pipeline.defs.situation.narrative.fetch_rag_context",
            return_value=_fake_rag_context(hits=3, report_ids=["r-a", "r-b", "r-c"]),
        ):
            llm = MagicMock()
            llm.complete_structured.return_value = _AISummaryLLM(
                text="A concise briefing paragraph.",
            )
            result = generate_ai_summary(
                llm, country_name="Sudan", period_label="2026",
                aggregated={"reportCount": 5}, cache_key="k",
            )
        assert result.text == "A concise briefing paragraph."
        # source_report_ids come from RAG dedup, NOT from the LLM —
        # this is the load-bearing invariant of coarse-grained citation.
        assert result.source_report_ids == ["r-a", "r-b", "r-c"]
        llm.complete_structured.assert_called_once()

    def test_llm_error_returns_empty_component(self):
        # A failed narrative call shouldn't drop the whole analysis —
        # the caller (`generate_and_upsert_for_country_year`) still
        # ships datapoints + sources + the other narrative sections.
        with patch(
            "clear_pipeline.defs.situation.narrative.fetch_rag_context",
            return_value=_fake_rag_context(hits=2),
        ):
            llm = MagicMock()
            llm.complete_structured.side_effect = RuntimeError("provider down")
            result = generate_ai_summary(
                llm, country_name="Sudan", period_label="2026",
                aggregated=None, cache_key="k",
            )
        assert result.text == ""
        assert result.source_report_ids == []


# ────────────────────────────────────────────────────────────────────
# Context Risks — 8 domains from one LLM call
# ────────────────────────────────────────────────────────────────────


class TestGenerateContextRisks:
    def test_empty_rag_returns_empty_default(self):
        with patch(
            "clear_pipeline.defs.situation.narrative.fetch_rag_context",
            return_value=_fake_rag_context(hits=0),
        ):
            llm = MagicMock()
            result = generate_context_risks(
                llm, country_name="Sudan", period_label="2026",
                aggregated=None, cache_key="k",
            )
        # Every domain sub-object exists but has empty bullets.
        assert result.demographics.bullets == []
        assert result.political.bullets == []
        assert result.security.bullets == []
        llm.complete_structured.assert_not_called()

    def test_all_eight_domains_populated_with_shared_source_ids(self):
        # One RAG search feeds all 8 domains. Each domain's
        # `source_report_ids` equals the shared RAG contribution list.
        with patch(
            "clear_pipeline.defs.situation.narrative.fetch_rag_context",
            return_value=_fake_rag_context(hits=4, report_ids=["r-1", "r-2"]),
        ):
            llm = MagicMock()
            llm.complete_structured.return_value = _ContextRisksLLM(
                demographics=["Population aged pyramid inverted"],
                political=["Ongoing power struggle"],
                economy=["Inflation at 200%"],
                socio_culture=["Ethnic tensions in the east"],
                security=["Armed clashes weekly"],
                legal_policy=["Border controls tightened"],
                infrastructure=["Power grid at 30%"],
                environment=["Drought since 2024"],
            )
            result = generate_context_risks(
                llm, country_name="Sudan", period_label="2026",
                aggregated={}, cache_key="k",
            )
        # Every domain carries the same shared source list.
        for domain_name in (
            "demographics", "political", "economy", "socio_culture",
            "security", "legal_policy", "infrastructure", "environment",
        ):
            domain = getattr(result, domain_name)
            assert len(domain.bullets) == 1
            assert domain.source_report_ids == ["r-1", "r-2"]

    def test_llm_error_returns_empty(self):
        with patch(
            "clear_pipeline.defs.situation.narrative.fetch_rag_context",
            return_value=_fake_rag_context(hits=2),
        ):
            llm = MagicMock()
            llm.complete_structured.side_effect = RuntimeError("provider down")
            result = generate_context_risks(
                llm, country_name="Sudan", period_label="2026",
                aggregated=None, cache_key="k",
            )
        assert result.demographics.bullets == []

    def test_flat_list_shape_validates(self):
        # Domains are flat list[str] (not nested objects) precisely so the
        # model can't intermittently stringify a nested domain. A plain
        # dict of lists validates; absent domains default to empty.
        model = _ContextRisksLLM.model_validate({
            "demographics": ["Inverted age pyramid"],
            "security": ["Weekly clashes", "New front"],
        })
        assert model.demographics == ["Inverted age pyramid"]
        assert model.security == ["Weekly clashes", "New front"]
        assert model.economy == []  # absent domain -> empty default


# ────────────────────────────────────────────────────────────────────
# Hazards & Vulnerabilities
# ────────────────────────────────────────────────────────────────────


class TestGenerateHazardsAndVulnerabilities:
    def test_empty_rag_returns_empty_default(self):
        with patch(
            "clear_pipeline.defs.situation.narrative.fetch_rag_context",
            return_value=_fake_rag_context(hits=0),
        ):
            llm = MagicMock()
            result = generate_hazards_and_vulnerabilities(
                llm, country_name="Sudan", period_label="2026",
                aggregated=None, cache_key="k",
            )
        assert result.hazards == []
        assert result.vulnerabilities == []
        llm.complete_structured.assert_not_called()

    def test_resolves_per_bullet_from_inline_markers(self):
        # LLM emits bullets with inline [Rn] markers; the generator strips them
        # and attributes each bullet to ONLY the reports it cited (not the
        # coarse union), and builds the container's report_id -> [lines] map.
        with patch(
            "clear_pipeline.defs.situation.narrative.fetch_rag_context",
            return_value=_fake_rag_context(hits=2, report_ids=["r-x", "r-y"]),
        ):
            llm = MagicMock()
            llm.complete_structured.return_value = _HazardsVulnerabilitiesLLM(
                hazards=["Drought [R1]", "Armed clashes [R2]"],
                vulnerabilities=["Weak health system [R1]", "High poverty rate [R2]"],
            )
            result = generate_hazards_and_vulnerabilities(
                llm, country_name="Sudan", period_label="2026",
                aggregated={}, cache_key="k",
            )
        assert [h.description for h in result.hazards] == ["Drought", "Armed clashes"]
        # Per-bullet: resolved-only, NOT the coarse union.
        assert result.hazards[0].source_report_ids == ["r-x"]
        assert result.hazards[1].source_report_ids == ["r-y"]
        assert result.vulnerabilities[0].source_report_ids == ["r-x"]
        assert result.vulnerabilities[1].source_report_ids == ["r-y"]
        # Container map inverts across both lists.
        assert result.contributing_sources == {
            "r-x": ["Drought", "Weak health system"],
            "r-y": ["Armed clashes", "High poverty rate"],
        }


# ────────────────────────────────────────────────────────────────────
# Displacement Narrative
# ────────────────────────────────────────────────────────────────────


class TestGenerateDisplacementNarrative:
    def test_empty_rag_returns_empty_default(self):
        with patch(
            "clear_pipeline.defs.situation.narrative.fetch_rag_context",
            return_value=_fake_rag_context(hits=0),
        ):
            llm = MagicMock()
            result = generate_displacement_narrative(
                llm, country_name="Sudan", period_label="2026",
                aggregated=None, cache_key="k",
            )
        assert result.push_factors == []
        assert result.return_intention == []
        llm.complete_structured.assert_not_called()

    def test_populates_push_factors_and_return_intention(self):
        # Regression guard for the `return_intention]` typo we hit in
        # Phase C — the list comprehension must close cleanly with `]`.
        # Bullets carry inline [R1] markers → resolved per-bullet.
        with patch(
            "clear_pipeline.defs.situation.narrative.fetch_rag_context",
            return_value=_fake_rag_context(hits=3, report_ids=["r-1"]),
        ):
            llm = MagicMock()
            llm.complete_structured.return_value = _DisplacementLLM(
                push_factors=["Active conflict in Darfur [R1]"],
                return_intention=["Wait until security stabilises [R1]"],
            )
            result = generate_displacement_narrative(
                llm, country_name="Sudan", period_label="2026",
                aggregated={}, cache_key="k",
            )
        assert len(result.push_factors) == 1
        assert result.push_factors[0].description == "Active conflict in Darfur"
        assert result.push_factors[0].source_report_ids == ["r-1"]
        assert len(result.return_intention) == 1
        assert result.return_intention[0].description == "Wait until security stabilises"
        assert result.return_intention[0].source_report_ids == ["r-1"]

    def test_llm_error_returns_empty(self):
        with patch(
            "clear_pipeline.defs.situation.narrative.fetch_rag_context",
            return_value=_fake_rag_context(hits=2),
        ):
            llm = MagicMock()
            llm.complete_structured.side_effect = RuntimeError("provider down")
            result = generate_displacement_narrative(
                llm, country_name="Sudan", period_label="2026",
                aggregated=None, cache_key="k",
            )
        assert result.push_factors == []
        assert result.return_intention == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
