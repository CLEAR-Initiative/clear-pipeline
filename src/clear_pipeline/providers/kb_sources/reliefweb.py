"""ReliefWeb knowledge-base connector.

The first `KbConnector`. `fetch` reuses the *existing, proven* ReliefWeb client
+ download helpers from `defs/reliefweb_to_s3.py` (`_fetch_window`, `_upload_pdf`,
`_s3_object_exists`) and stages PDFs to the **same** S3 keys the live assets use
(via the source-parameterized builders, which reproduce ReliefWeb's keys
byte-for-byte). So it does not change behavior or paths — it just packages the
same fetch as a connector returning `KbRawDocument`s.

It does NOT touch the live `reliefweb_weekly_*` asset graph. Phase 2b's fan-in
asset will call `fetch` (computing the window the way the current ingest asset
does) and feed the union of all connectors into the unchanged downstream.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from types import SimpleNamespace

from clear_pipeline.defs.reliefweb_partitions import FORMAT_SLUG
from clear_pipeline.providers.kb_sources import paths
from clear_pipeline.providers.kb_sources.base import KbRawDocument, register_connector

logger = logging.getLogger(__name__)

SOURCE = "reliefweb"


def _parse_dt(value: str) -> datetime:
    """ISO-8601 → aware UTC datetime (accepts a trailing 'Z')."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


class ReliefWebConnector:
    """ReliefWeb as a KB source. `key` + `format_slug` drive the S3 namespace;
    the path helpers delegate to the source-parameterized builders so ReliefWeb's
    keys stay byte-identical to the legacy `reliefweb_partitions` layout."""

    key = SOURCE
    format_slug = FORMAT_SLUG

    def reports_prefix(self, iso3: str) -> str:
        return paths.reports_prefix(self.key, iso3, self.format_slug)

    def text_prefix(self, iso3: str) -> str:
        return paths.text_prefix(self.key, iso3, self.format_slug)

    def pdf_key(self, iso3: str, report_id: str, filename: str) -> str:
        return paths.pdf_key(self.key, iso3, self.format_slug, report_id, filename)

    def datapoints_prefix(self, iso3: str) -> str:
        return paths.datapoints_prefix(self.key, iso3, self.format_slug)

    def fetch(
        self, *, country_iso3: str, window_start: str, window_end: str
    ) -> list[KbRawDocument]:
        """Fetch ReliefWeb reports for the window, stage each PDF to S3 (same keys
        the live assets use, HEAD-skipping ones already there), and return one
        `KbRawDocument` per staged PDF. Reuses the proven client/download helpers;
        the metadata mapping mirrors `kb_pdf_text._report_summary`."""
        # Imported lazily to avoid importing the Dagster defs module at connector
        # import time (registration below must stay import-light).
        from clear_pipeline.defs.reliefweb_to_s3 import (
            _fetch_window,
            _require_env,
            _s3_client,
            _s3_object_exists,
            _upload_pdf,
        )

        appname = _require_env("RELIEFWEB_APPNAME")
        bucket = _require_env("S3_BUCKET")
        s3 = _s3_client()
        # `_fetch_window` logs via `context.log`; give it a context-shaped shim so
        # we can reuse it verbatim outside a Dagster asset.
        ctx = SimpleNamespace(log=logger)
        reports = _fetch_window(
            ctx, appname, _parse_dt(window_start), _parse_dt(window_end), country_iso3,
        )

        docs: list[KbRawDocument] = []
        for report in reports:
            fields = report.get("fields") or {}
            files = fields.get("file") or []
            if not files:
                continue
            report_id = str(report.get("id") or "").strip()
            if not report_id:
                continue
            dates = fields.get("date") or {}
            published_at = (
                dates.get("original")
                or dates.get("created")
                or datetime.now(timezone.utc).isoformat()
            )
            sources = fields.get("source") or []
            publisher = sources[0] if sources else {}
            for f in files:
                url = f.get("url")
                filename = f.get("filename")
                if not url or not filename:
                    logger.warning("[reliefweb] report %s: attachment missing url/filename", report_id)
                    continue
                key = self.pdf_key(country_iso3, report_id, filename)
                if not _s3_object_exists(s3, bucket, key):
                    _upload_pdf(s3, bucket, url, key)
                docs.append(
                    KbRawDocument(
                        source=self.key,
                        report_id=report_id,
                        pdf_s3_key=key,
                        title=fields.get("title") or filename,
                        published_at=published_at,
                        source_url=fields.get("url") or url,
                        publisher_name=publisher.get("name") or publisher.get("shortname"),
                        publisher_homepage=publisher.get("homepage"),
                        country_iso3=country_iso3,
                        filename=filename,
                    ),
                )
        logger.info("[reliefweb] fetched %d PDF document(s) for %s", len(docs), country_iso3)
        return docs


register_connector(ReliefWebConnector())
