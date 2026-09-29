"""The on-demand drain's window end (ADR-0007 §4): a rolling request (no
window_end) aggregates datapoints up to now, like the automation drain; a
fixed request keeps its own window end."""

from unittest.mock import MagicMock, patch

from clear_pipeline.defs.analysis.stages import _GENERATED, _process_one_request


def _run(req: dict):
    with patch(
        "clear_pipeline.defs.analysis.stages._run_frame_generation", return_value=object(),
    ) as mock_gen, patch(
        "clear_pipeline.defs.analysis.stages.clear_api.mark_analysis_request_generated",
    ):
        outcome = _process_one_request(MagicMock(), req)
    return outcome, mock_gen.call_args.kwargs["effective_end"]


def test_rolling_request_materialises_now():
    outcome, effective_end = _run(
        {"id": "req-1", "windowStart": "2026-07-01T00:00:00Z", "windowEnd": None, "locationIds": ["sheikan"]},
    )
    assert outcome == _GENERATED
    assert effective_end is not None
    assert effective_end >= "2026-07-01"


def test_fixed_request_keeps_its_window_end():
    _, effective_end = _run(
        {"id": "req-2", "windowStart": "2026-01-01", "windowEnd": "2026-03-31", "locationIds": ["sheikan"]},
    )
    assert effective_end is None


def test_rolling_request_now_reaches_the_aggregated_fetch():
    with patch(
        "clear_pipeline.defs.analysis.stages.clear_api.get_aggregated_datapoint",
        return_value={"data": {}, "reportCount": 1},
    ) as mock_agg, patch(
        "clear_pipeline.defs.analysis.stages.generate_and_upsert_for_frame", return_value=object(),
    ), patch(
        "clear_pipeline.defs.analysis.stages.build_rag_filters", return_value={},
    ), patch(
        "clear_pipeline.defs.analysis.stages.clear_api.mark_analysis_request_generated",
    ):
        _process_one_request(
            MagicMock(),
            {"id": "req-3", "windowStart": "2026-07-01T00:00:00Z", "windowEnd": None, "locationIds": ["sheikan"]},
        )
    mock_agg.assert_called_once()
    assert mock_agg.call_args.kwargs["window_end"] > "2026-07-01"
