"""ReliefWeb knowledge-base connector.

The first `KbConnector`. It binds `source="reliefweb"` and the ReliefWeb
`FORMAT_SLUG` to the source-parameterized S3 layout so its keys are identical to
today's (see `test_kb_source_paths.py`). The `fetch` body — querying the
ReliefWeb API, building the PDF manifest, and downloading PDFs — is extracted
from `defs/reliefweb_to_s3.py` in phase 2b; this module establishes the connector
identity + path binding that phase 2b and the fan-in build on.
"""

from __future__ import annotations

from clear_pipeline.defs.reliefweb_partitions import FORMAT_SLUG
from clear_pipeline.providers.kb_sources import paths
from clear_pipeline.providers.kb_sources.base import KbRawDocument, register_connector

SOURCE = "reliefweb"


class ReliefWebConnector:
    """ReliefWeb as a KB source. `key` + `format_slug` drive the S3 namespace;
    the path helpers delegate to the source-parameterized builders so ReliefWeb's
    keys stay byte-identical to the legacy `reliefweb_partitions` layout."""

    key = SOURCE
    format_slug = FORMAT_SLUG

    def text_prefix(self, iso3: str) -> str:
        return paths.text_prefix(self.key, iso3, self.format_slug)

    def pdf_key(self, iso3: str, report_id: str, filename: str) -> str:
        return paths.pdf_key(self.key, iso3, self.format_slug, report_id, filename)

    def datapoints_prefix(self, iso3: str) -> str:
        return paths.datapoints_prefix(self.key, iso3, self.format_slug)

    def fetch(
        self, *, country_iso3: str, window_start: str, window_end: str
    ) -> list[KbRawDocument]:
        # Phase 2b: lift the ReliefWeb API query + PDF manifest + download out of
        # the reliefweb_weekly_* assets into here, returning one KbRawDocument per
        # staged PDF (source=SOURCE, pdf_s3_key via self.pdf_key(...), publisher
        # from fields.source[0], etc.). Kept explicit so the asset-graph wiring is
        # reviewed on its own rather than smuggled into this scaffold.
        raise NotImplementedError("ReliefWebConnector.fetch is wired in phase 2b")


register_connector(ReliefWebConnector())
