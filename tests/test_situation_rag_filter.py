"""Regression: situation-analysis RAG must scope to the country.

The bug: `fetch_rag_context` ran `searchKnowledgebase` with no location filter,
so a country's situation analysis cited knowledge-base chunks from reports about
OTHER countries in the shared KB. Fix: thread `country_id` → the
`countryLocationId` filter. These tests lock the threading in place.
"""

from unittest.mock import patch

from clear_pipeline.defs.situation import rag_helper


def _capture(monkey_target="clear_pipeline.defs.situation.rag_helper.clear_api.search_knowledgebase"):
    return patch(monkey_target, return_value=[])


def test_country_id_becomes_countryLocationId_filter():
    with _capture() as mock_search:
        rag_helper.fetch_rag_context(query="q", country_id="sudan-a0")
    _, kwargs = mock_search.call_args
    assert kwargs["filters"] == {"countryLocationId": "sudan-a0"}


def test_country_id_merges_with_existing_filters():
    # The sector path passes needSectors; the country scope must be ADDED, not
    # replace it.
    with _capture() as mock_search:
        rag_helper.fetch_rag_context(
            query="q", filters={"needSectors": ["Health"]}, country_id="afg-a0",
        )
    filters = mock_search.call_args.kwargs["filters"]
    assert filters == {"needSectors": ["Health"], "countryLocationId": "afg-a0"}


def test_no_country_id_leaves_search_unscoped():
    # Off the situation path (country_id=None) the search stays unfiltered.
    with _capture() as mock_search:
        rag_helper.fetch_rag_context(query="q")
    assert mock_search.call_args.kwargs["filters"] is None
