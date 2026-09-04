"""Tests for the Phase D sector generator.

The sector code has two behaviours worth covering that don't appear
elsewhere:

  1. Sector-scoped RAG search + fallback to unfiltered search when
     the scoped one returns nothing (bad sector coverage in the
     corpus shouldn't drop a section that broader evidence supports).
  2. Per-sector failure isolation — one bad sector must not affect
     the other five.
  3. `information_coverage.report_count` is deterministic (set from
     the RAG hit count, not emitted by the LLM).
"""

from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.defs.situation.rag_helper import RAGContext
from clear_pipeline.defs.situation.schemas import Sectors
from clear_pipeline.defs.situation.sectors import (
    _SECTOR_KEYS,
    _InformationCoverageAreaLLM,
    _SectorLLM,
    _generate_one_sector,
    generate_all_sectors,
)


def _fake_rag_context(*, hits: int, report_ids: list[str] | None = None) -> RAGContext:
    ids = report_ids if report_ids is not None else [f"r-{i}" for i in range(hits)]
    return RAGContext(
        formatted_for_prompt=f"[R1] chunk\n" * hits,
        contributing_report_ids=ids,
        hit_count=hits,
    )


def _fake_sector_llm_output(severity: str = "high") -> _SectorLLM:
    """Consistent per-sector LLM stub."""
    return _SectorLLM(
        severity=severity,
        impact=["Impact bullet 1", "Impact bullet 2"],
        humanitarian_conditions=["Condition bullet"],
        vulnerable_sections=["Children under 5"],
        top_needs=["Need 1", "Need 2"],
        priority_interventions=["Intervention 1"],
        information_coverage=[
            _InformationCoverageAreaLLM(area="Coverage area A", rating_out_of_10=7),
            _InformationCoverageAreaLLM(area="Coverage area B", rating_out_of_10=4),
        ],
    )


# ────────────────────────────────────────────────────────────────────
# _generate_one_sector — single-sector generation
# ────────────────────────────────────────────────────────────────────


class TestGenerateOneSector:
    def test_populates_all_fields_on_happy_path(self):
        with patch(
            "clear_pipeline.defs.situation.sectors.fetch_rag_context",
            return_value=_fake_rag_context(hits=3, report_ids=["r-a", "r-b"]),
        ):
            llm = MagicMock()
            llm.complete_structured.return_value = _fake_sector_llm_output()
            result = _generate_one_sector(
                llm,
                sector_key="health",
                sector_display_name="Health",
                country_name="Sudan",
                period_label="2026",
                aggregated_context="{}",
                cache_key="k",
            )
        assert result.severity == "high"
        assert result.top_needs == ["Need 1", "Need 2"]
        assert result.source_report_ids == ["r-a", "r-b"]
        # `report_count` on every coverage area = length of the RAG
        # contribution list (2 in this test) — deterministic, not
        # emitted by the LLM.
        assert all(area.report_count == 2 for area in result.information_coverage)

    def test_scoped_rag_uses_need_sectors_filter(self):
        # First arg is the sector-scoped search: must pass the
        # `needSectors` filter with the DISPLAY name (matches the
        # extractor's taxonomy in knowledgebase.needSectors).
        rag_call_kwargs = []

        def capture(**kwargs):
            rag_call_kwargs.append(kwargs)
            return _fake_rag_context(hits=2)

        with patch(
            "clear_pipeline.defs.situation.sectors.fetch_rag_context",
            side_effect=capture,
        ):
            llm = MagicMock()
            llm.complete_structured.return_value = _fake_sector_llm_output()
            _generate_one_sector(
                llm,
                sector_key="food_security",
                sector_display_name="Food Security",
                country_name="Sudan",
                period_label="2026",
                aggregated_context="{}",
                cache_key="k",
            )
        # Only one RAG call needed on the happy path (sector-scoped
        # hits > 0).
        assert len(rag_call_kwargs) == 1
        assert rag_call_kwargs[0]["filters"] == {"needSectors": ["Food Security"]}

    def test_falls_back_to_unfiltered_when_sector_scoped_returns_nothing(self):
        # Sector-scoped search comes back empty → we retry with no
        # filter. Better a broad set of hits than an empty section.
        call_sequence = [
            _fake_rag_context(hits=0),           # first call: filtered → empty
            _fake_rag_context(hits=3, report_ids=["r-broad-1", "r-broad-2"]),
        ]

        def side_effect(**kwargs):
            return call_sequence.pop(0)

        with patch(
            "clear_pipeline.defs.situation.sectors.fetch_rag_context",
            side_effect=side_effect,
        ):
            llm = MagicMock()
            llm.complete_structured.return_value = _fake_sector_llm_output()
            result = _generate_one_sector(
                llm,
                sector_key="wash",
                sector_display_name="WASH",
                country_name="Sudan",
                period_label="2026",
                aggregated_context="{}",
                cache_key="k",
            )
        # Fell back to broad search → still populated.
        assert result.severity == "high"
        assert result.source_report_ids == ["r-broad-1", "r-broad-2"]

    def test_returns_empty_when_both_searches_yield_nothing(self):
        # No sector-scoped hits AND no broad hits — return empty
        # default. Don't call the LLM (invites hallucination without
        # evidence).
        with patch(
            "clear_pipeline.defs.situation.sectors.fetch_rag_context",
            side_effect=[_fake_rag_context(hits=0), _fake_rag_context(hits=0)],
        ):
            llm = MagicMock()
            result = _generate_one_sector(
                llm,
                sector_key="education",
                sector_display_name="Education",
                country_name="Sudan",
                period_label="2026",
                aggregated_context="{}",
                cache_key="k",
            )
        assert result.severity is None
        assert result.top_needs == []
        assert result.source_report_ids == []
        llm.complete_structured.assert_not_called()

    def test_llm_error_returns_empty_default(self):
        with patch(
            "clear_pipeline.defs.situation.sectors.fetch_rag_context",
            return_value=_fake_rag_context(hits=2),
        ):
            llm = MagicMock()
            llm.complete_structured.side_effect = RuntimeError("provider down")
            result = _generate_one_sector(
                llm,
                sector_key="protection",
                sector_display_name="Protection",
                country_name="Sudan",
                period_label="2026",
                aggregated_context="{}",
                cache_key="k",
            )
        assert result.severity is None
        assert result.top_needs == []


# ────────────────────────────────────────────────────────────────────
# generate_all_sectors — 6-sector orchestrator
# ────────────────────────────────────────────────────────────────────


class TestGenerateAllSectors:
    def test_ships_all_six_sectors_in_stable_order(self):
        # Every sector present in the output, keys match the fixed set.
        with patch(
            "clear_pipeline.defs.situation.sectors.fetch_rag_context",
            return_value=_fake_rag_context(hits=2),
        ):
            llm = MagicMock()
            llm.complete_structured.return_value = _fake_sector_llm_output()
            result = generate_all_sectors(
                llm, country_name="Sudan", period_label="2026",
                aggregated={}, cache_key="k",
            )
        assert isinstance(result, Sectors)
        # All six sectors accessible; dashboard tab layout stability.
        for sector_key, _ in _SECTOR_KEYS:
            sector = getattr(result, sector_key)
            assert sector.severity == "high"
            assert sector.top_needs == ["Need 1", "Need 2"]

    def test_one_bad_sector_isolates_from_the_other_five(self):
        # Simulate the health sector failing. The other five should
        # still ship populated. This is the per-sector isolation
        # invariant the doc §5.4 promises for Phase D.
        def rag_side(**kwargs):
            filters = kwargs.get("filters") or {}
            # Health search returns hits (so we exercise the LLM
            # path); other sectors also return hits.
            return _fake_rag_context(hits=2)

        def llm_side(**kwargs):
            # Route by the schema class + the country prompt content
            # can't easily inspect the sector name, so we key off
            # attempts and fail the 3rd (Health, per _SECTOR_KEYS order).
            attempt = llm_call_counter[0]
            llm_call_counter[0] += 1
            if attempt == 2:  # 3rd sector (0-indexed) is Health
                raise RuntimeError("simulated Health LLM failure")
            return _fake_sector_llm_output()

        llm_call_counter = [0]
        with patch(
            "clear_pipeline.defs.situation.sectors.fetch_rag_context",
            side_effect=rag_side,
        ):
            llm = MagicMock()
            llm.complete_structured.side_effect = llm_side
            result = generate_all_sectors(
                llm, country_name="Sudan", period_label="2026",
                aggregated={}, cache_key="k",
            )
        # Health is the empty default; the other five have the fake output.
        assert result.health.severity is None
        assert result.education.severity == "high"
        assert result.food_security.severity == "high"
        assert result.shelter.severity == "high"
        assert result.wash.severity == "high"
        assert result.protection.severity == "high"

    def test_sector_key_taxonomy_covers_the_six_saf_sectors(self):
        # Regression guard: adding a sector requires updating
        # _SECTOR_KEYS AND the Sectors pydantic model — this test
        # catches a mismatch between the two.
        keys = {k for k, _ in _SECTOR_KEYS}
        assert keys == {
            "education", "food_security", "health",
            "shelter", "wash", "protection",
        }
        # Every key must be a valid Sectors field.
        for key in keys:
            assert hasattr(Sectors(), key)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
