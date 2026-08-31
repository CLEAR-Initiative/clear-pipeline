"""Tests for the crisis-enrichment drain (ported from clear-pipeline's Celery
`enrich_crisis`, plus knowledgebase RAG grounding).

Every test mocks clear-api, the LLM provider, and the RAG search — no network,
no LLM budget. Focus:
  - Orchestration: enrich_one_crisis gathers events, writes narrative +
    scenarios + needs + population, and flips the crisis to ENRICHED.
  - #2 grounding: the RAG search is scoped to the crisis's country (A0) and its
    event types.
  - Failure isolation: one generator failing still marks ENRICHED (leaves the
    queue); a crisis with no resolvable events is marked ENRICHED without any
    LLM call.
  - Schema: needs-analysis rejects hallucinated sector keys.
"""

from unittest.mock import MagicMock, patch

import pytest

from clear_context_pipeline.defs.crisis import enrich
from clear_context_pipeline.defs.crisis.enrich import (
    EMPTY,
    ENRICHED,
    collect_district_ids,
    collect_event_types,
    collect_location_names,
    compute_time_range,
    enrich_one_crisis,
    resolve_country_id,
)
from clear_context_pipeline.defs.crisis.schemas import (
    NEEDS_SECTORS,
    CrisisNarrative,
    CrisisNeedsAnalysis,
    CrisisScenarios,
    SectorAnalysis,
)

# ── fixtures ────────────────────────────────────────────────────────────────

def _event(eid, *, types, gen_loc_id=None, gen_loc_name=None, ancestors=None,
           valid_from="2026-01-01T00:00:00Z", valid_to="2026-01-31T00:00:00Z"):
    return {
        "id": eid,
        "title": f"Event {eid}",
        "description": "desc",
        "types": types,
        "severity": 4,
        "populationAffected": None,
        "validFrom": valid_from,
        "validTo": valid_to,
        "originLocation": None,
        "destinationLocation": None,
        "generalLocation": (
            {"id": gen_loc_id, "name": gen_loc_name, "ancestorIds": ancestors or [], "metadata": []}
            if gen_loc_id or gen_loc_name
            else None
        ),
    }


def _narrative():
    return CrisisNarrative(title="Floods in Kassala", description="A flood.", tldr=["a", "b", "c"])


def _scenarios():
    return CrisisScenarios(most_likely="ml", best_case="bc", worst_case="wc", description="d")


def _needs():
    sector = {
        s: SectorAnalysis(description="d", severity="Severe", responseGap=True, nrcRelevant=True)
        for s in ("Shelter", "WASH", "Protection", "Health", "Food Security", "Education")
    }
    return CrisisNeedsAnalysis(generalSummary=["1", "2", "3", "4"], sector=sector)


# ── collect helpers + country resolution ────────────────────────────────────

class TestCollectors:
    def test_collect_dedups_names_types_ids(self):
        events = [
            _event("e1", types=["FL", "TC"], gen_loc_id="kassala", gen_loc_name="Kassala"),
            _event("e2", types=["FL"], gen_loc_id="kassala", gen_loc_name="Kassala"),
        ]
        assert collect_location_names(events) == ["Kassala"]
        assert collect_event_types(events) == ["FL", "TC"]
        assert collect_district_ids(events) == ["kassala"]

    def test_resolve_country_id_finds_a0_in_ancestors(self):
        crisis = {"id": "c1", "generalLocation": {"id": "kassala", "ancestorIds": ["sudan-a0", "east"]}}
        a0_ids = {"sudan-a0", "chad-a0"}
        assert resolve_country_id(crisis, [], a0_ids) == "sudan-a0"

    def test_resolve_country_id_falls_back_to_event_locations(self):
        crisis = {"id": "c1", "generalLocation": None}
        events = [_event("e1", types=["FL"], gen_loc_id="kassala", ancestors=["sudan-a0"])]
        assert resolve_country_id(crisis, events, {"sudan-a0"}) == "sudan-a0"

    def test_resolve_country_id_none_when_no_a0_in_reach(self):
        crisis = {"id": "c1", "generalLocation": {"id": "x", "ancestorIds": ["y"]}}
        assert resolve_country_id(crisis, [], {"sudan-a0"}) is None


# ── orchestration ───────────────────────────────────────────────────────────

class TestEnrichOneCrisis:
    def _patch_all(self, *, capture_rag=None):
        """Patch clear_api + LLM + RAG + population inside enrich.py. Returns the
        clear_api mock so tests can assert the write-backs."""
        clear_api = MagicMock()
        clear_api.get_event_for_crisis.side_effect = lambda eid: _event(
            eid, types=["FL"], gen_loc_id="kassala", gen_loc_name="Kassala", ancestors=["sudan-a0"],
        )
        llm = MagicMock()
        llm.complete_structured.side_effect = [_narrative(), _scenarios(), _needs()]
        rag = MagicMock()
        rag.is_empty = False
        rag.formatted_for_prompt = "[R1] evidence"
        fetch = capture_rag or MagicMock(return_value=rag)

        return clear_api, llm, fetch, patch.multiple(
            enrich,
            clear_api=clear_api,
            make_llm_provider=MagicMock(return_value=llm),
            fetch_rag_context=fetch,
            compute_population_in_area=MagicMock(return_value=123456),
        )

    def test_full_enrichment_writes_all_and_marks_enriched(self):
        clear_api, _llm, _fetch, ctx = self._patch_all()
        crisis = {"id": "c1", "events": [{"id": "e1"}], "generalLocation": {"id": "kassala", "ancestorIds": ["sudan-a0"]}}
        with ctx:
            outcome = enrich_one_crisis(crisis, a0_ids={"sudan-a0"})

        assert outcome == ENRICHED
        # narrative + scenarios + population written in one mutation
        _, kwargs = clear_api.update_crisis_population.call_args
        assert kwargs["title"] == "Floods in Kassala"
        assert '"description": "A flood."' in kwargs["summary"]
        assert kwargs["scenarios"]["most_likely"] == "ml"
        assert kwargs["population_in_area"] == 123456
        # needs written separately + crisis flipped ENRICHED
        clear_api.set_crisis_needs_analysis.assert_called_once()
        clear_api.mark_crisis_enriched.assert_called_once_with("c1")

    def test_rag_is_scoped_to_country_type_time_and_sectors(self):
        # #2: the knowledgebase search must carry the crisis's country A0, event
        # types, and temporal window on every generator; the needs search adds
        # the NRC sectors on top.
        capture = MagicMock()
        rag = MagicMock(is_empty=False, formatted_for_prompt="[R1] e")
        capture.return_value = rag
        _clear_api, _llm, _fetch, ctx = self._patch_all(capture_rag=capture)
        crisis = {"id": "c1", "events": [{"id": "e1"}], "generalLocation": {"id": "kassala", "ancestorIds": ["sudan-a0"]}}
        with ctx:
            enrich_one_crisis(crisis, a0_ids={"sudan-a0"})

        assert capture.call_count == 3  # narrative + scenarios + needs
        expected_time = {"from": "2026-01-01T00:00:00Z", "to": "2026-01-31T00:00:00Z"}
        needs_seen = 0
        for _, kwargs in capture.call_args_list:
            assert kwargs["country_id"] == "sudan-a0"
            assert kwargs["filters"]["eventTypes"] == ["FL"]
            assert kwargs["filters"]["timeRange"] == expected_time
            if "needSectors" in kwargs["filters"]:
                assert kwargs["filters"]["needSectors"] == list(NEEDS_SECTORS)
                needs_seen += 1
        assert needs_seen == 1  # only the needs search is sector-scoped

    def test_time_range_spans_all_events(self):
        events = [
            _event("e1", types=["FL"], valid_from="2026-03-05T00:00:00Z", valid_to="2026-03-10T00:00:00Z"),
            _event("e2", types=["FL"], valid_from="2026-02-01T00:00:00Z", valid_to="2026-03-20T00:00:00Z"),
        ]
        assert compute_time_range(events) == {
            "from": "2026-02-01T00:00:00Z", "to": "2026-03-20T00:00:00Z",
        }

    def test_time_range_none_when_no_bounds(self):
        events = [_event("e1", types=["FL"], valid_from=None, valid_to=None)]
        assert compute_time_range(events) is None

    def test_generator_failure_still_marks_enriched(self):
        clear_api = MagicMock()
        clear_api.get_event_for_crisis.side_effect = lambda eid: _event(eid, types=["FL"])
        llm = MagicMock()
        llm.complete_structured.side_effect = RuntimeError("LLM down")
        with patch.multiple(
            enrich,
            clear_api=clear_api,
            make_llm_provider=MagicMock(return_value=llm),
            fetch_rag_context=MagicMock(return_value=MagicMock(is_empty=True, formatted_for_prompt="")),
            compute_population_in_area=MagicMock(return_value=None),
        ):
            outcome = enrich_one_crisis(
                {"id": "c1", "events": [{"id": "e1"}], "generalLocation": None}, a0_ids=set(),
            )
        # all three generators failed → no field write, but the crisis still
        # leaves the queue (ENRICHED) so it can't poison the head.
        assert outcome == ENRICHED
        clear_api.update_crisis_population.assert_not_called()
        clear_api.set_crisis_needs_analysis.assert_not_called()
        clear_api.mark_crisis_enriched.assert_called_once_with("c1")

    def test_no_resolvable_events_marks_enriched_without_llm(self):
        clear_api = MagicMock()
        clear_api.get_event_for_crisis.return_value = None
        make_llm = MagicMock()
        with patch.multiple(enrich, clear_api=clear_api, make_llm_provider=make_llm):
            outcome = enrich_one_crisis(
                {"id": "c1", "events": [{"id": "e1"}], "generalLocation": None}, a0_ids=set(),
            )
        assert outcome == EMPTY
        make_llm.assert_not_called()
        clear_api.mark_crisis_enriched.assert_called_once_with("c1")


# ── schema ──────────────────────────────────────────────────────────────────

class TestNeedsSchema:
    def test_rejects_hallucinated_sector_key(self):
        with pytest.raises(ValueError, match="Unknown sector keys"):
            CrisisNeedsAnalysis(
                generalSummary=["a"],
                sector={
                    "Nutrition": SectorAnalysis(
                        description="d", severity="Severe", responseGap=True, nrcRelevant=False,
                    )
                },
            )

    def test_rejects_empty_general_summary(self):
        with pytest.raises(ValueError, match="at least one bullet"):
            CrisisNeedsAnalysis(generalSummary=[], sector={})
