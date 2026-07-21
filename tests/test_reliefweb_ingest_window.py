"""The ReliefWeb fetch reaches back the wider initial lookback on its first
run (empty S3), and the 7-day delta thereafter — so the datapoint
aggregation's 90-day first-run backfill has a matching window to work over.
"""

from unittest.mock import MagicMock

from clear_context_pipeline.defs import reliefweb_to_s3 as rw


def test_is_first_ingest_true_when_no_reports():
    s3 = MagicMock()
    s3.list_objects_v2.return_value = {"KeyCount": 0}
    assert rw._is_first_ingest(s3, "bucket") is True
    # It scopes the existence check to this country/format's report prefix.
    _, kwargs = s3.list_objects_v2.call_args
    assert kwargs["Prefix"] == rw._reports_prefix()
    assert kwargs["Bucket"] == "bucket"


def test_is_first_ingest_false_when_a_report_exists():
    s3 = MagicMock()
    s3.list_objects_v2.return_value = {"KeyCount": 1}
    assert rw._is_first_ingest(s3, "bucket") is False


def test_is_first_ingest_missing_keycount_treated_as_empty():
    # Some S3-compatible backends omit KeyCount; absent -> first run.
    s3 = MagicMock()
    s3.list_objects_v2.return_value = {}
    assert rw._is_first_ingest(s3, "bucket") is True


def test_lookback_defaults():
    assert rw._DEFAULT_LOOKBACK_DAYS == 7
    assert rw._DEFAULT_INITIAL_LOOKBACK_DAYS == 90
