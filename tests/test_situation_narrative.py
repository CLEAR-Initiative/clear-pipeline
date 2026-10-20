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
    _CONTEXT_DOMAIN_KEYS,
    _DisplacementLLM,
    _GuidedAnswerLLM,
    _HazardsVulnerabilitiesLLM,
    _HumanitarianAccessLLM,
    generate_ai_summary,
    generate_context_risks,
    generate_displacement_narrative,
    generate_hazards_and_vulnerabilities,
    generate_humanitarian_access,
)
from clear_pipeline.defs.situation.rag_helper import RAGContext


def _guided(answer: list[str], *, completeness: float = 1.0, unanswered: list[str] | None = None) -> _GuidedAnswerLLM:
    return _GuidedAnswerLLM(
        answer=answer,
        completeness=completeness,
        questions_not_answered=unanswered or [],
    )


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


FOUR_CITED = [
    "Drone strikes on civilian sites: markets hit in Darfur and Kordofan in August. [R2]",
    "Cholera: new hotspot in West Kordofan. [R3][R1]",
    "Displacement: 3,650 fled Umm Badr on 23 Sep. [R1]",
    "Access: rains cut the Habila road. [R2]",
]


def _summary(llm_results, *, hits=3, report_ids=("r-a", "r-b", "r-c"), rag_filters=None, aggregated=None):
    """Run generate_ai_summary with every search returning the same fake context."""
    with patch(
        "clear_pipeline.defs.situation.narrative.fetch_rag_context",
        return_value=_fake_rag_context(hits=hits, report_ids=list(report_ids)),
    ) as fetch:
        llm = MagicMock()
        llm.complete_structured.side_effect = llm_results
        result = generate_ai_summary(
            llm, country_name="Sudan", period_label="2026",
            aggregated=aggregated, cache_key="k", rag_filters=rag_filters,
        )
    return result, llm, fetch


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

    def test_happy_path_populates_text_findings_and_sources(self):
        result, llm, _ = _summary([_AISummaryLLM(text="A concise briefing. [R1]", key_findings=FOUR_CITED)])
        assert result.text == "A concise briefing."
        assert len(result.key_findings) == 4
        assert set(result.source_report_ids) == {"r-a", "r-b", "r-c"}
        llm.complete_structured.assert_called_once()

    def test_key_findings_carry_their_own_sources(self):
        result, _, _ = _summary([_AISummaryLLM(text="Conflict is spreading east. [R1]", key_findings=FOUR_CITED)])
        assert result.key_findings[0].description == (
            "Drone strikes on civilian sites: markets hit in Darfur and Kordofan in August."
        )
        assert [f.source_report_ids for f in result.key_findings][:2] == [["r-b"], ["r-c", "r-a"]]
        # The summary's sentence citations and the findings' share one map.
        assert "Conflict is spreading east." in result.contributing_sources["r-a"]
        assert "Cholera: new hotspot in West Kordofan." in result.contributing_sources["r-a"]

    def test_uncited_findings_are_dropped(self):
        result, _, _ = _summary([_AISummaryLLM(
            text="x. [R1]",
            key_findings=[*FOUR_CITED, "Health: 224 facilities damaged since 2023."],
        )])
        assert "Health: 224 facilities damaged since 2023." not in [f.description for f in result.key_findings]
        assert all(f.source_report_ids for f in result.key_findings)
        assert all(
            "Health: 224 facilities damaged since 2023." not in lines
            for lines in result.contributing_sources.values()
        )

    def test_retries_once_when_too_long_or_undercited_then_trims(self):
        long_text = " ".join(f"Sentence number {i} has quite a few words in it for length. [R1]" for i in range(8))
        result, llm, _ = _summary([
            _AISummaryLLM(text=long_text, key_findings=FOUR_CITED[:1]),
            _AISummaryLLM(text=long_text, key_findings=FOUR_CITED),
        ])
        assert llm.complete_structured.call_count == 2
        retry_prompt = llm.complete_structured.call_args_list[1].kwargs["user"]
        assert "PREVIOUS ANSWER HAD THESE PROBLEMS" in retry_prompt
        assert "words" in retry_prompt and "citation" in retry_prompt
        # Still too long after the retry: cut to the first three sentences.
        assert result.text.count("Sentence number") == 3
        assert len(result.key_findings) == 4

    def test_retries_when_findings_are_too_long(self):
        wordy = "Displacement: " + " ".join(["word"] * 40) + ". [R1]"
        _, llm, _ = _summary([
            _AISummaryLLM(text="x. [R1]", key_findings=[*FOUR_CITED, wordy]),
            _AISummaryLLM(text="x. [R1]", key_findings=FOUR_CITED),
        ])
        assert llm.complete_structured.call_count == 2
        assert "exceed 35 words" in llm.complete_structured.call_args_list[1].kwargs["user"]

    def test_evidence_is_recent_frame_band_then_situation_question_searches(self):
        from clear_pipeline.defs.situation.questions import situation_questions

        _, llm, fetch = _summary(
            [_AISummaryLLM(text="x. [R1]", key_findings=FOUR_CITED)],
            rag_filters={"countryLocationId": "sdn"},
            aggregated={"data": {"health_facilities_damaged": {"value": 224}}},
        )
        recent, *themed = fetch.call_args_list
        assert recent.kwargs["mode"] == "FRAME"
        assert recent.kwargs["query"] == ""
        assert recent.kwargs["filters"]["countryLocationId"] == "sdn"
        assert set(recent.kwargs["filters"]["timeRange"]) == {"from", "to"}
        sit_qs = situation_questions()
        assert len(themed) == len(sit_qs)
        assert all(c.kwargs["filters"] == {"countryLocationId": "sdn"} for c in themed)
        assert all("mode" not in c.kwargs for c in themed)
        # Each themed search uses the CSV analysis question as the query.
        for call, q in zip(themed, sit_qs):
            assert call.kwargs["query"].startswith("Sudan ")
            assert q.question in call.kwargs["query"]
        call = llm.complete_structured.call_args.kwargs
        assert "30 days up to" in call["user"]
        assert "cumulative multi-year totals" in call["user"]
        # The aggregated figures never reach the summary prompt.
        assert "health_facilities_damaged" not in call["system"]

    def test_failed_retry_keeps_the_first_answer(self):
        result, llm, _ = _summary([
            _AISummaryLLM(text="First answer. [R1]", key_findings=FOUR_CITED[:3]),
            RuntimeError("429 from provider"),
        ])
        assert llm.complete_structured.call_count == 2
        assert result.text == "First answer."
        assert len(result.key_findings) == 3

    def test_worse_retry_is_discarded(self):
        result, llm, _ = _summary([
            _AISummaryLLM(text="First answer. [R1]", key_findings=FOUR_CITED[:3]),
            _AISummaryLLM(text="Second answer. [R1]", key_findings=FOUR_CITED[:1]),
        ])
        assert llm.complete_structured.call_count == 2
        assert result.text == "First answer."
        assert len(result.key_findings) == 3

    def test_better_retry_is_kept(self):
        result, _, _ = _summary([
            _AISummaryLLM(text="First answer. [R1]", key_findings=FOUR_CITED[:3]),
            _AISummaryLLM(text="Second answer. [R1]", key_findings=FOUR_CITED),
        ])
        assert result.text == "Second answer."
        assert len(result.key_findings) == 4

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
# Context Risks — one LLM call per context pillar
# ────────────────────────────────────────────────────────────────────


_DOMAIN_ANSWERS = {
    "demographics": _guided(["Population aged pyramid inverted"]),
    "political": _guided(["Ongoing power struggle"], completeness=0.5, unanswered=["gatekeepers"]),
    "economy": _guided(["Inflation at 200%"]),
    "socio_culture": _guided(["Ethnic tensions in the east"]),
    "security": _guided(["Armed clashes weekly"]),
    "legal_policy": _guided(["Border controls tightened"]),
    "infrastructure": _guided(["Power grid at 30%"]),
    "environment": _guided(["Drought since 2024"]),
    "humanitarian_coordination": _guided(["Cluster lead active"]),
}


class TestGenerateContextRisks:
    def test_empty_rag_returns_empty_default(self):
        with patch(
            "clear_pipeline.defs.situation.generate_component.fetch_rag_context",
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
        assert result.humanitarian_coordination.bullets == []
        llm.complete_structured.assert_not_called()

    def test_one_llm_call_and_rag_per_domain(self):
        # One RAG + one LLM call per context name; each prompt carries
        # only that domain's CSV analysis question.
        from clear_pipeline.defs.situation.questions import question_for_schema_key

        answers = [_DOMAIN_ANSWERS[k] for k in _CONTEXT_DOMAIN_KEYS]
        with patch(
            "clear_pipeline.defs.situation.generate_component.fetch_rag_context",
            return_value=_fake_rag_context(hits=4, report_ids=["r-1", "r-2"]),
        ) as fetch:
            llm = MagicMock()
            llm.complete_structured.side_effect = answers
            result = generate_context_risks(
                llm, country_name="Sudan", period_label="2026",
                aggregated={}, cache_key="k",
            )
        assert fetch.call_count == len(_CONTEXT_DOMAIN_KEYS)
        assert llm.complete_structured.call_count == len(_CONTEXT_DOMAIN_KEYS)
        demo_q = question_for_schema_key("demographics")
        assert demo_q is not None
        assert fetch.call_args_list[0].kwargs["query"] == f"Sudan {demo_q.question}"
        first_user = llm.complete_structured.call_args_list[0].kwargs["user"]
        assert "QUESTION (Demographics)" in first_user
        assert demo_q.question in first_user
        assert "QUESTIONS:" not in first_user
        for domain_name in _CONTEXT_DOMAIN_KEYS:
            domain = getattr(result, domain_name)
            assert len(domain.bullets) == 1
            assert domain.source_report_ids == ["r-1", "r-2"]
            assert domain.completeness is not None
        assert result.political.completeness == 0.5
        assert result.political.questions_not_answered == ["gatekeepers"]

    def test_one_bad_domain_isolates_from_the_others(self):
        def llm_side(**_kwargs):
            attempt = llm_call_counter[0]
            llm_call_counter[0] += 1
            if attempt == 1:  # political (index 1)
                raise RuntimeError("simulated political LLM failure")
            return _DOMAIN_ANSWERS[_CONTEXT_DOMAIN_KEYS[attempt]]

        llm_call_counter = [0]
        with patch(
            "clear_pipeline.defs.situation.generate_component.fetch_rag_context",
            return_value=_fake_rag_context(hits=2, report_ids=["r-1"]),
        ):
            llm = MagicMock()
            llm.complete_structured.side_effect = llm_side
            result = generate_context_risks(
                llm, country_name="Sudan", period_label="2026",
                aggregated=None, cache_key="k",
            )
        assert result.political.bullets == []
        assert result.demographics.bullets == ["Population aged pyramid inverted"]
        assert result.economy.bullets == ["Inflation at 200%"]

    def test_all_llm_errors_return_empty_domains(self):
        with patch(
            "clear_pipeline.defs.situation.generate_component.fetch_rag_context",
            return_value=_fake_rag_context(hits=2),
        ):
            llm = MagicMock()
            llm.complete_structured.side_effect = RuntimeError("provider down")
            result = generate_context_risks(
                llm, country_name="Sudan", period_label="2026",
                aggregated=None, cache_key="k",
            )
        assert result.demographics.bullets == []
        assert result.security.bullets == []

    def test_guided_answer_shape_validates(self):
        model = _GuidedAnswerLLM.model_validate({
            "answer": ["Inverted age pyramid"],
            "completeness": 0.8,
        })
        assert model.answer == ["Inverted age pyramid"]
        assert model.completeness == 0.8
        assert model.questions_not_answered == []


# ────────────────────────────────────────────────────────────────────
# Hazards & Vulnerabilities
# ────────────────────────────────────────────────────────────────────


class TestGenerateHazardsAndVulnerabilities:
    def test_empty_rag_returns_empty_default(self):
        with patch(
            "clear_pipeline.defs.situation.generate_component.fetch_rag_context",
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
        from clear_pipeline.defs.situation.questions import question_for_schema_key

        with patch(
            "clear_pipeline.defs.situation.generate_component.fetch_rag_context",
            return_value=_fake_rag_context(hits=2, report_ids=["r-x", "r-y"]),
        ) as fetch:
            llm = MagicMock()
            llm.complete_structured.return_value = _HazardsVulnerabilitiesLLM(
                hazards=["Drought [R1]", "Armed clashes [R2]"],
                vulnerabilities=["Weak health system [R1]", "High poverty rate [R2]"],
                completeness=0.6,
                questions_not_answered=["preparedness measures"],
            )
            result = generate_hazards_and_vulnerabilities(
                llm, country_name="Sudan", period_label="2026",
                aggregated={}, cache_key="k",
            )
        shock_q = question_for_schema_key("hazards_and_vulnerabilities")
        assert shock_q is not None
        assert fetch.call_args.kwargs["query"] == f"Sudan {shock_q.question}"
        user = llm.complete_structured.call_args.kwargs["user"]
        assert "QUESTION (Shock-Event)" in user
        assert shock_q.question in user
        assert "Produce the Hazards" not in user
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
        assert result.completeness == 0.6
        assert result.questions_not_answered == ["preparedness measures"]


# ────────────────────────────────────────────────────────────────────
# Displacement Narrative
# ────────────────────────────────────────────────────────────────────


class TestGenerateDisplacementNarrative:
    def test_empty_rag_returns_empty_default(self):
        with patch(
            "clear_pipeline.defs.situation.generate_component.fetch_rag_context",
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
            "clear_pipeline.defs.situation.generate_component.fetch_rag_context",
            return_value=_fake_rag_context(hits=3, report_ids=["r-1"]),
        ):
            llm = MagicMock()
            llm.complete_structured.return_value = _DisplacementLLM(
                push_factors=["Active conflict in Darfur [R1]"],
                return_intention=["Wait until security stabilises [R1]"],
                completeness=0.4,
                questions_not_answered=["local integration support requirements"],
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
        assert result.completeness == 0.4
        assert result.questions_not_answered == ["local integration support requirements"]

    def test_llm_error_returns_empty(self):
        with patch(
            "clear_pipeline.defs.situation.generate_component.fetch_rag_context",
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


# ────────────────────────────────────────────────────────────────────
# Humanitarian Access
# ────────────────────────────────────────────────────────────────────


class TestGenerateHumanitarianAccess:
    def test_empty_rag_returns_empty_default(self):
        with patch(
            "clear_pipeline.defs.situation.generate_component.fetch_rag_context",
            return_value=_fake_rag_context(hits=0),
        ):
            llm = MagicMock()
            result = generate_humanitarian_access(
                llm, country_name="Sudan", period_label="2026",
                aggregated=None, cache_key="k",
            )
        assert result.barriers == []
        assert result.constraints == []
        llm.complete_structured.assert_not_called()

    def test_populates_barriers_and_constraints(self):
        with patch(
            "clear_pipeline.defs.situation.generate_component.fetch_rag_context",
            return_value=_fake_rag_context(hits=2, report_ids=["r-1"]),
        ):
            llm = MagicMock()
            llm.complete_structured.return_value = _HumanitarianAccessLLM(
                barriers=["Checkpoint delays for convoys [R1]"],
                constraints=["Seasonal flooding cuts corridor [R1]"],
                completeness=0.7,
                questions_not_answered=["prospects"],
            )
            result = generate_humanitarian_access(
                llm, country_name="Sudan", period_label="2026",
                aggregated={}, cache_key="k",
            )
        assert len(result.barriers) == 1
        assert result.barriers[0].description == "Checkpoint delays for convoys"
        assert result.barriers[0].source_report_ids == ["r-1"]
        assert len(result.constraints) == 1
        assert result.constraints[0].description == "Seasonal flooding cuts corridor"
        assert result.completeness == 0.7
        assert result.questions_not_answered == ["prospects"]

    def test_llm_error_returns_empty(self):
        with patch(
            "clear_pipeline.defs.situation.generate_component.fetch_rag_context",
            return_value=_fake_rag_context(hits=2),
        ):
            llm = MagicMock()
            llm.complete_structured.side_effect = RuntimeError("provider down")
            result = generate_humanitarian_access(
                llm, country_name="Sudan", period_label="2026",
                aggregated=None, cache_key="k",
            )
        assert result.barriers == []
        assert result.constraints == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


class TestRecentWindow:
    """The recent band is the last 30 days OF THE ANALYSIS WINDOW (review #75)."""

    def test_country_path_without_time_range_ends_now(self):
        from datetime import datetime, timezone

        from clear_pipeline.defs.situation.narrative import _recent_filters

        f = _recent_filters({"countryLocationId": "sdn"}, days=30)
        start = datetime.fromisoformat(f["timeRange"]["from"])
        end = datetime.fromisoformat(f["timeRange"]["to"])
        assert f["countryLocationId"] == "sdn"
        assert (end - start).days == 30
        assert abs((datetime.now(timezone.utc) - end).total_seconds()) < 60

    def test_fixed_past_window_stays_inside_the_window(self):
        from clear_pipeline.defs.situation.narrative import _recent_filters

        f = _recent_filters(
            {"locationIds": ["shk"], "timeRange": {"from": "2026-01-01T00:00:00Z", "to": "2026-03-31T23:59:59Z"}},
            days=30,
        )
        assert f["timeRange"] == {"from": "2026-03-01T23:59:59+00:00", "to": "2026-03-31T23:59:59+00:00"}

    def test_window_ending_in_the_future_ends_now(self):
        from datetime import datetime, timezone

        from clear_pipeline.defs.situation.narrative import _recent_filters

        year = datetime.now(timezone.utc).year
        f = _recent_filters(
            {"timeRange": {"from": f"{year}-01-01T00:00:00Z", "to": f"{year + 1}-12-31T23:59:59Z"}}, days=30,
        )
        end = datetime.fromisoformat(f["timeRange"]["to"])
        assert abs((datetime.now(timezone.utc) - end).total_seconds()) < 60

    def test_short_window_clamps_to_its_start(self):
        from clear_pipeline.defs.situation.narrative import _recent_filters

        f = _recent_filters({"timeRange": {"from": "2026-03-20T00:00:00Z", "to": "2026-03-31T00:00:00Z"}}, days=30)
        assert f["timeRange"] == {"from": "2026-03-20T00:00:00+00:00", "to": "2026-03-31T00:00:00+00:00"}

    def test_prompt_names_the_window_end(self):
        _, llm, _ = _summary(
            [_AISummaryLLM(text="x. [R1]", key_findings=FOUR_CITED)],
            rag_filters={"timeRange": {"from": "2026-01-01T00:00:00Z", "to": "2026-03-31T23:59:59Z"}},
        )
        user = llm.complete_structured.call_args.kwargs["user"]
        assert "30 days up to 31 March 2026" in user
