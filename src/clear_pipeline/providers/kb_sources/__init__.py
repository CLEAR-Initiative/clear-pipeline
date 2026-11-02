"""Knowledge-base source connectors.

Multiple sources (ReliefWeb today; others + manual upload next) fetch PDF
reports and stage them into ONE shared raw-document contract, so the
source-agnostic downstream (pdf_text → chunks → enrich → upsert → datapoints →
figures) processes any document regardless of where it came from — mirroring how
the signals pipeline fans many providers (acled/gdacs/…) into one drain.

The contract is `KbRawDocument`; a source implements `KbConnector`.
"""

from clear_pipeline.providers.kb_sources.base import (
    KbConnector,
    KbRawDocument,
    all_connectors,
    get_connector,
    register_connector,
)

__all__ = [
    "KbConnector",
    "KbRawDocument",
    "all_connectors",
    "get_connector",
    "register_connector",
]
