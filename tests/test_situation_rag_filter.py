"""Regression: situation-analysis RAG must stay scoped to its frame.

The bug this guards against: `fetch_rag_context` running `searchKnowledgebase`
with no location filter, so a country's situation analysis cited knowledge-base
chunks from reports about OTHER countries in the shared KB.

Under ADR-0007 the frame's scope (including the `countryLocationId` subtree
filter) is built by `situation/frame.build_rag_filters` and threaded through as
`filters` — those cases are asserted in `test_situation_frame.py`. Here we lock
in that `fetch_rag_context` forwards whatever `filters` it is given to the search
VERBATIM, so the frame scope actually reaches `searchKnowledgebase`.
"""

from unittest.mock import patch

from clear_pipeline.defs.situation import rag_helper


def _capture(monkey_target="clear_pipeline.defs.situation.rag_helper.clear_api.search_knowledgebase"):
    return patch(monkey_target, return_value=[])


def test_filters_are_forwarded_verbatim():
    # The frame scope (here a countryLocationId subtree filter) must reach the
    # search unchanged — this is what keeps a country's analysis citing only
    # that country's reports.
    scope = {"countryLocationId": "sudan-a0", "needSectors": ["Health"]}
    with _capture() as mock_search:
        rag_helper.fetch_rag_context(query="q", filters=scope)
    assert mock_search.call_args.kwargs["filters"] == scope


def test_no_filters_leaves_search_unscoped():
    # Off the frame path (filters=None) the search stays unfiltered.
    with _capture() as mock_search:
        rag_helper.fetch_rag_context(query="q")
    assert mock_search.call_args.kwargs["filters"] is None
