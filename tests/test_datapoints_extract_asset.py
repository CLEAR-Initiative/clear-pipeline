"""End-to-end tests for the reliefweb_weekly_datapoints extraction asset.

All external I/O is mocked:
  - S3: returns a canned doc-text JSONL body
  - LLM provider: emits a pre-built Pydantic instance per domain call
  - clear-api provider: resolve_location + upsert_report_datapoints

Coverage:
  - Happy path: 6 domains extract cleanly → single upsert with merged blob
  - Partial failure: 1 domain raises → merged.data[domain] = null,
    rest still upsert
  - Total failure: every domain raises → skip upsert entirely
  - Kill-switch: KB_SKIP_CONTEXTUALIZATION set → no LLM calls, no upsert
  - Empty upstream: no reports → asset returns empty summary
"""

import json
import os
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from clear_context_pipeline.defs.knowledgebase.datapoints_extract import (
    reliefweb_weekly_datapoints,
)
from clear_context_pipeline.defs.knowledgebase.datapoints_schemas import (
    Casualties,
    CasualtyDisaggregation,
    Displacement,
    NumericField,
    TextField,
    TimingAndScope,
    NeedsAndFunding,
    AccessAndIncidents,
    NarrativeAndConfidence,
    LocationRef,
)


PDF_TEXT_SUMMARY = {
    "report_id": "test:sudan-2026-w27",
    "report_title": "Sudan sitrep — week 27",
    "source_url": "https://reliefweb.int/report/sudan/…",
    "s3_key": "reliefweb/pdfs/sdn/situation-report/sudan-w27.pdf",
    "published_at": "2026-07-10T00:00:00+00:00",
    "s3_text_key": "reliefweb/kb/text/sdn/situation-report/test:sudan-2026-w27.jsonl",
    "num_pages": 3,
}


def _canned_domain_output(domain_name: str):
    """Build a realistic per-domain Pydantic instance so `model_dump`
    produces well-formed JSON downstream."""
    nf = NumericField(
        value=42000, unit="people", confidence="reported",
        source_quote="42,000 IDPs in Kordofan.", chunk_index=0, page_number=2,
    )
    if domain_name == "timing_and_scope":
        return TimingAndScope(
            reporting_period_start="2026-06-30",
            reporting_period_end="2026-07-06",
            reporting_period_confidence="reported",
            locations=[LocationRef(pcode="SD0701", name="Kordofan", admin_level=1)],
            event_types=["conflict", "displacement"],
            active_clusters=["Protection", "WASH"],
        )
    if domain_name == "casualties":
        return Casualties(
            killed=CasualtyDisaggregation(total=NumericField(
                value=15, unit="people", confidence="verified",
                source_quote="15 killed", chunk_index=1, page_number=2,
            )),
        )
    if domain_name == "displacement":
        return Displacement(idp_stock=nf, new_displacements=nf)
    if domain_name == "needs_and_funding":
        # Affected (widest circle) is a distinct, wider figure than PIN.
        affected = NumericField(
            value=100000, unit="people", confidence="reported",
            source_quote="100,000 people affected in Kordofan.",
            chunk_index=0, page_number=2,
        )
        return NeedsAndFunding(overall_pin=nf, overall_affected=affected)
    if domain_name == "access_and_incidents":
        return AccessAndIncidents(security_incidents_count=nf)
    if domain_name == "narrative_and_confidence":
        return NarrativeAndConfidence(
            brief_summary=TextField(
                value="Sudan conflict intensifies in Kordofan; displacement up.",
                confidence="reported",
                source_quote="Sudan conflict intensifies in Kordofan.",
                chunk_index=2, page_number=3,
            ),
            overall_confidence="reported",
        )
    raise ValueError(f"unexpected domain {domain_name}")


def _mock_s3_client(text_body: str) -> MagicMock:
    """S3 stub — get_object returns the doc text as pipeline expects."""
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: text_body.encode("utf-8")),
    }
    return s3


def _canned_doc_text_body() -> str:
    return "\n".join(
        json.dumps({"report_id": "test:sudan-2026-w27", "page_num": i, "text": f"Sample text for page {i}"})
        for i in (1, 2, 3)
    )


def _build_asset_context():
    return dg.build_asset_context()


@pytest.fixture(autouse=True)
def _clean_env():
    # Guardrail env vars leak between tests otherwise — the skip flag
    # in particular must default off.
    for var in ("KB_SKIP_CONTEXTUALIZATION", "KB_MAX_CHUNKS_PER_REPORT"):
        os.environ.pop(var, None)
    yield


def _fake_complete_structured(**kwargs):
    """Router used across all extraction-asset tests — routes each
    domain call to its canned Pydantic output by schema class name."""
    schema = kwargs["schema"]
    mapping = {
        "TimingAndScope": "timing_and_scope",
        "Casualties": "casualties",
        "Displacement": "displacement",
        "NeedsAndFunding": "needs_and_funding",
        "AccessAndIncidents": "access_and_incidents",
        "NarrativeAndConfidence": "narrative_and_confidence",
    }
    return _canned_domain_output(mapping[schema.__name__])


def _configure_llm_mock(mock_make_llm):
    """Configure the auto-generated MagicMock chain so `.model`
    returns a real string (Dagster metadata rejects non-strings) and
    `.complete_structured` routes to `_fake_complete_structured`.

    Setting attributes directly on `.return_value` rather than
    replacing it with a fresh MagicMock avoids a subtle interaction
    where the auto-generated chain gets shadowed but not fully."""
    mock_make_llm.return_value.model = "claude-sonnet-4-6"
    mock_make_llm.return_value.complete_structured.side_effect = _fake_complete_structured


@pytest.fixture(autouse=True)
def _datapoints_not_already_extracted(monkeypatch):
    """Default the idempotency check to 'not extracted' and the clear-api
    capability preflight to 'deployed', so the extraction tests exercise the
    full path. Specific tests override these."""
    monkeypatch.setattr(
        "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.report_datapoints_exist",
        lambda report_id, *, schema_version: False,
    )
    monkeypatch.setattr(
        "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.supports_source_attribution",
        lambda: True,
    )


class TestExtractionAsset:
    def test_happy_path_all_six_domains_upserted(self):
        # 6 domains each emit a canned Pydantic model → asset merges
        # into a single data blob and upserts once.
        with (
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract._s3_client",
                return_value=_mock_s3_client(_canned_doc_text_body()),
            ),
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.make_llm_provider",
            ) as mock_make_llm,
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.resolve_location",
                return_value="loc-kordofan",
            ),
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.upsert_report_datapoints",
            ) as mock_upsert,
        ):
            _configure_llm_mock(mock_make_llm)
            mock_upsert.return_value = {
                "reportId": PDF_TEXT_SUMMARY["report_id"],
                "schemaVersion": "v1",
                "createdOrReplaced": False,
            }

            result = reliefweb_weekly_datapoints(
                _build_asset_context(), reliefweb_weekly_pdf_text=[PDF_TEXT_SUMMARY],
            )

        # All 6 domains attempted.
        assert mock_make_llm.return_value.complete_structured.call_count == 6
        # One upsert.
        assert mock_upsert.call_count == 1
        upsert_kwargs = mock_upsert.call_args.kwargs
        # Merged data blob has ALL six domain keys populated.
        merged_data = upsert_kwargs["data"]
        assert set(merged_data.keys()) == {
            "timing_and_scope", "casualties", "displacement",
            "needs_and_funding", "access_and_incidents", "narrative_and_confidence",
        }
        for domain in merged_data.values():
            assert domain is not None
        # Hot totals hoisted from the merged blob.
        assert upsert_kwargs["total_killed"] == 15
        assert upsert_kwargs["total_displaced"] == 42000
        # total_affected now hoists overall_affected (Population Affected),
        # NOT overall_pin — they are different populations (ADR-0001).
        assert upsert_kwargs["total_affected"] == 100000
        # Locations resolved (dedup'd across the many refs).
        assert upsert_kwargs["location_ids"] == ["loc-kordofan"]
        # Event types + reporting period passed through from timing.
        assert upsert_kwargs["event_types"] == ["conflict", "displacement"]
        assert upsert_kwargs["reporting_period_end"] == "2026-07-06"
        # Extraction summary returned.
        assert len(result) == 1
        assert result[0]["domains_ok"] == list(merged_data.keys())
        assert result[0]["domains_failed"] == []

    def test_skips_report_with_existing_datapoints(self):
        # A report whose datapoints already exist in clear-api is skipped:
        # no LLM extraction, no upsert, and the summary marks it reused.
        with (
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract._s3_client",
                return_value=_mock_s3_client(_canned_doc_text_body()),
            ),
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.make_llm_provider",
            ) as mock_make_llm,
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.report_datapoints_exist",
                return_value=True,
            ),
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.upsert_report_datapoints",
            ) as mock_upsert,
        ):
            _configure_llm_mock(mock_make_llm)
            result = reliefweb_weekly_datapoints(
                _build_asset_context(), reliefweb_weekly_pdf_text=[PDF_TEXT_SUMMARY],
            )

        # No domain extraction, no upsert — the report was reused.
        assert mock_make_llm.return_value.complete_structured.call_count == 0
        assert mock_upsert.call_count == 0
        assert result == [{"report_id": PDF_TEXT_SUMMARY["report_id"], "reused": True}]

    def test_undeployed_clear_api_fails_loud_before_any_llm_spend(self):
        # #27-Critical: if clear-api lacks source attribution (PR #110 not
        # deployed), the asset must fail LOUD up front — not silently 400 every
        # report after paying for its 6 LLM calls and finish green with 0 data.
        with (
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract._s3_client",
                return_value=_mock_s3_client(_canned_doc_text_body()),
            ),
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.make_llm_provider",
            ) as mock_make_llm,
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.supports_source_attribution",
                return_value=False,
            ),
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.upsert_report_datapoints",
            ) as mock_upsert,
        ):
            _configure_llm_mock(mock_make_llm)
            with pytest.raises(dg.Failure, match="sourceId"):
                reliefweb_weekly_datapoints(
                    _build_asset_context(), reliefweb_weekly_pdf_text=[PDF_TEXT_SUMMARY],
                )
        # Failed before spending anything.
        assert mock_make_llm.return_value.complete_structured.call_count == 0
        assert mock_upsert.call_count == 0

    def test_empty_batch_skips_the_preflight(self):
        # No reports → no clear-api dependency; the preflight probe must not run
        # (and an empty week must not fail even if clear-api is down).
        with (
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.make_llm_provider",
            ) as mock_make_llm,
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.supports_source_attribution",
                side_effect=AssertionError("preflight ran on an empty batch"),
            ),
        ):
            mock_make_llm.return_value.model = "claude-sonnet-4-6"
            result = reliefweb_weekly_datapoints(
                _build_asset_context(), reliefweb_weekly_pdf_text=[],
            )
        assert result == []

    def test_partial_domain_failure_writes_null_and_continues(self):
        # `Casualties` raises → merged.data.casualties = None,
        # remaining 5 domains still upsert. This is the failure
        # isolation the doc §5.4 promises.
        def raising(**kwargs):
            if kwargs["schema"].__name__ == "Casualties":
                raise RuntimeError("simulated LLM parse failure")
            return _fake_complete_structured(**kwargs)

        with (
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract._s3_client",
                return_value=_mock_s3_client(_canned_doc_text_body()),
            ),
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.make_llm_provider",
            ) as mock_make_llm,
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.resolve_location",
                return_value=None,
            ),
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.upsert_report_datapoints",
                return_value={
                    "reportId": PDF_TEXT_SUMMARY["report_id"],
                    "schemaVersion": "v1",
                    "createdOrReplaced": False,
                },
            ) as mock_upsert,
        ):
            mock_make_llm.return_value.model = "claude-sonnet-4-6"
            mock_make_llm.return_value.complete_structured.side_effect = raising

            result = reliefweb_weekly_datapoints(
                _build_asset_context(), reliefweb_weekly_pdf_text=[PDF_TEXT_SUMMARY],
            )

        assert mock_upsert.call_count == 1
        upsert_kwargs = mock_upsert.call_args.kwargs
        # Failed domain shows up as null — not missing.
        assert upsert_kwargs["data"]["casualties"] is None
        assert upsert_kwargs["data"]["displacement"] is not None
        # total_killed hot total should be None when the source was null.
        assert upsert_kwargs["total_killed"] is None
        # Summary reports the failed domain.
        assert result[0]["domains_failed"] == ["casualties"]
        assert "casualties" not in result[0]["domains_ok"]

    def test_all_domains_fail_skips_upsert(self):
        # Every domain raises → nothing worth writing. Asset must NOT
        # upsert a row full of nulls (that would poison the schema
        # version's aggregation).
        with (
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract._s3_client",
                return_value=_mock_s3_client(_canned_doc_text_body()),
            ),
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.make_llm_provider",
            ) as mock_make_llm,
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.upsert_report_datapoints",
            ) as mock_upsert,
        ):
            mock_make_llm.return_value.model = "claude-sonnet-4-6"
            mock_make_llm.return_value.complete_structured.side_effect = RuntimeError(
                "provider down",
            )

            result = reliefweb_weekly_datapoints(
                _build_asset_context(), reliefweb_weekly_pdf_text=[PDF_TEXT_SUMMARY],
            )

        mock_upsert.assert_not_called()
        assert result == []

    def test_kill_switch_skips_entire_extraction(self):
        # KB_SKIP_CONTEXTUALIZATION set → no LLM calls, no upserts.
        # Doubles as the KB emergency lever for cost containment.
        os.environ["KB_SKIP_CONTEXTUALIZATION"] = "1"
        with (
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.make_llm_provider",
            ) as mock_make_llm,
            patch(
                "clear_context_pipeline.defs.knowledgebase.datapoints_extract.clear_api.upsert_report_datapoints",
            ) as mock_upsert,
        ):
            result = reliefweb_weekly_datapoints(
                _build_asset_context(), reliefweb_weekly_pdf_text=[PDF_TEXT_SUMMARY],
            )

        mock_make_llm.assert_not_called()
        mock_upsert.assert_not_called()
        assert result == []

    def test_empty_upstream_produces_no_work(self):
        with patch(
            "clear_context_pipeline.defs.knowledgebase.datapoints_extract.make_llm_provider",
        ) as mock_make_llm:
            # `.model` must be a string for the metadata write at the
            # end of the asset — it's still emitted even when no
            # reports process.
            mock_make_llm.return_value.model = "claude-sonnet-4-6"
            result = reliefweb_weekly_datapoints(
                _build_asset_context(), reliefweb_weekly_pdf_text=[],
            )
        # LLM provider is instantiated once at asset entry (needed to
        # pick the right model), but no complete_structured calls fire.
        assert mock_make_llm.return_value.complete_structured.call_count == 0
        assert result == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
