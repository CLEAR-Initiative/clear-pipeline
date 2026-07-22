"""Unit test for the report_datapoints_exist clear-api client helper.

It backs the datapoint asset's skip-if-exists: the DB is the source of truth
(the S3 debug snapshot is written before the upsert, so it can't confirm the
write landed). Existence = the `reportDatapoint(reportId)` query returns a row.
"""

from unittest.mock import patch

from clear_context_pipeline.providers import clear_api


def test_true_when_row_present():
    with patch(
        "clear_context_pipeline.providers.clear_api._execute",
        return_value={"reportDatapoint": {"id": "abc"}},
    ):
        assert clear_api.report_datapoints_exist("report-1") is True


def test_false_when_null():
    with patch(
        "clear_context_pipeline.providers.clear_api._execute",
        return_value={"reportDatapoint": None},
    ):
        assert clear_api.report_datapoints_exist("report-1") is False
