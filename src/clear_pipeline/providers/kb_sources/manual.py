"""Manual-upload knowledge-base connector (ADR-0013 phase 3).

A **push** source: there is no API to poll, so it has no ``fetch``. A user
uploads a PDF through clear-api's ``uploadKnowledgebaseDocument``; clear-api
stages the PDF + writes one `KbRawDocument` as a ``<ts>.staged.jsonl`` marker
under this connector's ``manual/reports/<iso3>/<slug>/`` prefix, then launches
``kb_downstream_job`` for the document's country partition. The source-neutral
downstream picks the staged doc up via `read_latest_staged_documents`, exactly
as it does ReliefWeb's — so a manual upload gets the identical treatment (text →
chunks → enrich → upsert, figures, datapoints, aggregations, situation refresh),
retiring the bespoke ``manual_ingest`` op job.

``has_fetch()`` returns False for this connector, so schedulers skip it — it is
driven entirely by the clear-api push.

The S3 key scheme here MUST match what clear-api writes (clear-api can't import
this module — it's TypeScript), so the layout is pinned by
``test_kb_source_paths.py``. `report_id` is ``manual:<sha>`` so dedup stays
source-namespaced.
"""

from __future__ import annotations

from clear_pipeline.providers.kb_sources import paths
from clear_pipeline.providers.kb_sources.base import register_connector

SOURCE = "manual"
# Manual uploads are arbitrary documents (no ReliefWeb-style format taxonomy);
# one neutral slug keeps the per-source S3 layout uniform with the pull sources.
FORMAT_SLUG = "upload"


class ManualConnector:
    """Manual upload as a KB source. Push-only — no ``fetch``; clear-api stages
    documents into this connector's namespace and the downstream drains them."""

    key = SOURCE
    format_slug = FORMAT_SLUG

    def reports_prefix(self, iso3: str) -> str:
        return paths.reports_prefix(self.key, iso3, self.format_slug)

    def pdf_prefix(self, iso3: str) -> str:
        return paths.pdf_prefix(self.key, iso3, self.format_slug)

    def pdf_key(self, iso3: str, report_id: str, filename: str) -> str:
        return paths.pdf_key(self.key, iso3, self.format_slug, report_id, filename)

    def text_prefix(self, iso3: str) -> str:
        return paths.text_prefix(self.key, iso3, self.format_slug)

    def datapoints_prefix(self, iso3: str) -> str:
        return paths.datapoints_prefix(self.key, iso3, self.format_slug)


register_connector(ManualConnector())
