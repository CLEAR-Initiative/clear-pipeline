"""Reading staged KB documents back from S3, source-neutrally.

Every KB source — pull (``reliefweb_fetch`` on its schedule) or push (the manual
upload path, on demand) — writes a ``<tag>.staged.jsonl`` marker under its own
``<source>/reports/<iso3>/<slug>/`` prefix when it stages new PDFs. Each line is
one `KbRawDocument` (as a dict).

The source-neutral downstream (``kb_pdf_text`` / ``kb_figures``) uses
`read_latest_staged_documents` to union the latest batch from EVERY registered
source for a country, so it processes whatever was just staged regardless of
where it came from — and it needs no typed dependency on any particular fetch
asset (which also means a manual upload to a country with no prior ReliefWeb run
works, where a typed ``reliefweb_fetch`` input would have had nothing to load).
"""

from __future__ import annotations

import json
import logging

from clear_pipeline.providers.kb_sources.base import all_connectors

logger = logging.getLogger(__name__)

_STAGED_SUFFIX = ".staged.jsonl"


def _ensure_connectors_registered() -> None:
    """Import the connector modules for their registration side effect, so
    `all_connectors()` enumerates every source. A new source adds its import
    here (and the registry does the rest)."""
    from clear_pipeline.providers.kb_sources import (  # noqa: F401
        manual,
        reliefweb,
    )


def _reports_prefix(connector, iso3: str) -> str | None:
    """A connector's reports prefix for this country, or None if it doesn't
    expose one (a malformed/partial connector — skipped rather than fatal)."""
    fn = getattr(connector, "reports_prefix", None)
    if not callable(fn):
        return None
    return fn(iso3)


def _latest_staged_key(s3, bucket: str, prefix: str) -> str | None:
    """The most recent ``*.staged.jsonl`` object under ``prefix``.

    Marker keys embed the ISO week tag (``YYYY-Www``) or an upload timestamp,
    both of which sort lexicographically in chronological order, so ``max`` is
    the newest batch — the one that just triggered this run. (Legacy ReliefWeb
    ``<week>.jsonl`` report dumps, which are NOT ``.staged.jsonl``, are ignored.)
    """
    keys: list[str] = []
    token: str | None = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []) or []:
            key = obj.get("Key", "")
            if key.endswith(_STAGED_SUFFIX):
                keys.append(key)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
        if not token:
            break
    return max(keys) if keys else None


def _read_jsonl(s3, bucket: str, key: str) -> list[dict]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    docs: list[dict] = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            docs.append(json.loads(line))
        except json.JSONDecodeError:
            logger.warning("[kb] skipping malformed staged-doc line in s3://%s/%s", bucket, key)
    return docs


def read_latest_staged_documents(s3, bucket: str, iso3: str) -> list[dict]:
    """Union the latest staged `KbRawDocument` batch (as dicts) across every
    registered KB source for this country partition. Deduped by
    ``(source, report_id, pdf_s3_key)`` so a source that re-emits a doc can't
    double it within one run."""
    _ensure_connectors_registered()
    seen: set[tuple] = set()
    docs: list[dict] = []
    for connector in all_connectors():
        prefix = _reports_prefix(connector, iso3)
        if not prefix:
            continue
        key = _latest_staged_key(s3, bucket, prefix)
        if not key:
            continue
        for doc in _read_jsonl(s3, bucket, key):
            ident = (doc.get("source"), doc.get("report_id"), doc.get("pdf_s3_key"))
            if ident in seen:
                continue
            seen.add(ident)
            docs.append(doc)
    return docs
