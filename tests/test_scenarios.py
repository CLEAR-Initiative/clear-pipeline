"""Native forward-looking scenarios generation (ADR-0015). Mocked RAG + LLM."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import clear_pipeline.defs.analysis.scenarios as sc
from clear_pipeline.defs.analysis.frame import Frame


def _rag(empty=False):
    return SimpleNamespace(
        is_empty=empty, formatted_for_prompt="EVIDENCE",
        contributing_report_ids=["r1", "r2"], hit_report_ids=["r1", "r2"], hits=[], hit_count=2,
    )


def _outcome():
    out = MagicMock()
    out.output = SimpleNamespace(
        most_likely="Fighting continues.", best_case="Access opens.",
        worst_case="Famine spreads.", description="Funding and access are the hinges.",
    )
    out.status = "ok"
    out.usage = {"input_tokens": 5}
    out.cost_usd = 0.02
    out.provenance = {"model": "deepseek/deepseek-v3.2"}
    return out


_FRAME = Frame.build(window_start="2026-01-01", window_end="2026-12-31", location_ids=["loc-a0"])


def test_generate_scenarios_emits_three_trajectories():
    with patch.object(sc, "fetch_rag_context", return_value=_rag()), \
         patch.object(sc, "run_node", return_value=_outcome()):
        res = sc.generate_scenarios(
            provider=MagicMock(total_usage={}, model="m"), frame=_FRAME, scope_label="Sudan",
        )
    assert res.most_likely == "Fighting continues."
    assert res.worst_case == "Famine spreads."
    assert res.source_report_ids == ["r1", "r2"]
    assert res.status == "ok"
    payload = res.to_payload()
    assert set(payload) == {
        "most_likely", "best_case", "worst_case", "description", "source_report_ids"
    }


def test_empty_rag_skips_llm_and_returns_empty_scenarios():
    with patch.object(sc, "fetch_rag_context", return_value=_rag(empty=True)), \
         patch.object(sc, "run_node") as rn:
        res = sc.generate_scenarios(
            provider=MagicMock(total_usage={}, model="m"), frame=_FRAME, scope_label="Sudan",
        )
    rn.assert_not_called()
    assert res.most_likely == "" and res.best_case == "" and res.worst_case == ""
    assert res.cost_usd == 0.0
    assert res.status == "best_effort"
