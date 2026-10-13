"""Unit test for the report_datapoints_exist clear-api client helper.

It backs the datapoint asset's skip-if-exists: the DB is the source of truth
(the S3 debug snapshot is written before the upsert, so it can't confirm the
write landed). Existence = the `reportDatapoint(reportId)` query returns a row
AT THE CURRENT schema version — a version bump must re-extract, not skip.
"""

from unittest.mock import patch

from clear_pipeline.providers import clear_api


def test_true_when_row_present_at_matching_version():
    with patch(
        "clear_pipeline.providers.clear_api._execute",
        return_value={"reportDatapoint": {"id": "abc", "schemaVersion": "v2"}},
    ):
        assert clear_api.report_datapoints_exist("report-1", schema_version="v2") is True


def test_false_when_null():
    with patch(
        "clear_pipeline.providers.clear_api._execute",
        return_value={"reportDatapoint": None},
    ):
        assert clear_api.report_datapoints_exist("report-1", schema_version="v2") is False


def test_false_when_row_is_a_prior_schema_version():
    # #27-H1: a v1 row must NOT count as already-extracted under v2 — otherwise
    # a schema bump can never re-extract and the v2 buckets stay empty.
    with patch(
        "clear_pipeline.providers.clear_api._execute",
        return_value={"reportDatapoint": {"id": "abc", "schemaVersion": "v1"}},
    ):
        assert clear_api.report_datapoints_exist("report-1", schema_version="v2") is False


def test_supports_source_attribution_probe():
    # #27-Critical: read-only introspection probe → True only when the deployed
    # UpsertReportDatapointsInput exposes `sourceId`.
    with patch(
        "clear_pipeline.providers.clear_api._execute",
        return_value={"__type": {"inputFields": [{"name": "reportId"}, {"name": "sourceId"}]}},
    ):
        assert clear_api.supports_source_attribution() is True
    with patch(
        "clear_pipeline.providers.clear_api._execute",
        return_value={"__type": {"inputFields": [{"name": "reportId"}]}},
    ):
        assert clear_api.supports_source_attribution() is False
    # An undeployed schema returns no such type at all.
    with patch(
        "clear_pipeline.providers.clear_api._execute",
        return_value={"__type": None},
    ):
        assert clear_api.supports_source_attribution() is False


def test_supports_source_attribution_assumes_deployed_when_introspection_disabled():
    # Production runs Apollo with introspection OFF, so the __type probe 400s with
    # INTROSPECTION_DISABLED. We can't verify that way in prod, so assume deployed
    # (source attribution has been live since PR #110) rather than hard-failing.
    body = (
        "clear-api 400 (non-retryable) for https://api.clearinitiative.io/graphql: "
        '{"errors":[{"message":"GraphQL introspection is not allowed by Apollo '
        'Server","extensions":{"validationErrorCode":"INTROSPECTION_DISABLED",'
        '"code":"GRAPHQL_VALIDATION_FAILED"}}]}'
    )
    with patch(
        "clear_pipeline.providers.clear_api._execute",
        side_effect=clear_api.ClearApiError(body),
    ):
        assert clear_api.supports_source_attribution() is True


def test_supports_source_attribution_reraises_other_client_errors():
    # A non-introspection 400 (genuine broken request) must still propagate.
    with patch(
        "clear_pipeline.providers.clear_api._execute",
        side_effect=clear_api.ClearApiError("clear-api 400 (non-retryable): boom"),
    ):
        try:
            clear_api.supports_source_attribution()
        except clear_api.ClearApiError:
            pass
        else:  # pragma: no cover
            raise AssertionError("expected ClearApiError to propagate")
