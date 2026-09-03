"""Tests for `_existing_enriched_count` — the enrich asset's skip-if-exists
check that lets a resume reuse a prior run's (expensive) LLM enrichment from
S3 instead of re-paying for it.
"""

from unittest.mock import MagicMock

from clear_pipeline.defs.knowledgebase import enrich as en


def _s3_returning(body: bytes) -> MagicMock:
    s3 = MagicMock()
    obj_body = MagicMock()
    obj_body.read.return_value = body
    s3.get_object.return_value = {"Body": obj_body}
    return s3


def test_counts_enriched_lines():
    s3 = _s3_returning(b'{"chunk_index":0}\n{"chunk_index":1}\n')
    assert en._existing_enriched_count(s3, "bucket", "key") == 2


def test_none_when_not_yet_enriched():
    s3 = MagicMock()
    s3.get_object.side_effect = Exception("NoSuchKey")
    assert en._existing_enriched_count(s3, "bucket", "key") is None


def test_none_when_empty():
    s3 = _s3_returning(b"")
    assert en._existing_enriched_count(s3, "bucket", "key") is None
