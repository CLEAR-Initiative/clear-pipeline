"""Source-parameterized S3 layout for the knowledge-base pipeline.

The leading path segment is the SOURCE; everything after it — `reports/`,
`pdfs/`, `kb/{text,chunks,enriched,datapoints,figures}/`, then
`<iso3>/<format_slug>/…` — is identical across sources. The `<iso3>` country
segment is retained for every source (the downstream + situation analysis are
country-scoped).

With ``source="reliefweb"`` and ``format_slug="situation-report"`` these
reproduce `defs/reliefweb_partitions.py`'s existing keys **byte-for-byte**, so
ReliefWeb's artifacts — and the skip-if-exists idempotency keyed off them — are
untouched when the downstream is generalized. A new source lands under its own
``<source>/`` namespace, so sources never collide. `test_kb_source_paths.py`
pins the ReliefWeb equivalence.

Trailing-slash convention matches the originals: `reports_prefix` / `pdf_prefix`
end with `/` (callers append directly); the `kb/*` prefixes do not (callers
append `/<report_id>…`).
"""

from __future__ import annotations


def reports_prefix(source: str, iso3: str, format_slug: str) -> str:
    return f"{source}/reports/{iso3}/{format_slug}/"


def pdf_prefix(source: str, iso3: str, format_slug: str) -> str:
    return f"{source}/pdfs/{iso3}/{format_slug}/"


def pdf_key(source: str, iso3: str, format_slug: str, report_id: str, filename: str) -> str:
    """Stable S3 key for one report's PDF attachment."""
    return f"{pdf_prefix(source, iso3, format_slug)}{report_id}/{filename}"


def text_prefix(source: str, iso3: str, format_slug: str) -> str:
    return f"{source}/kb/text/{iso3}/{format_slug}"


def chunks_prefix(source: str, iso3: str, format_slug: str) -> str:
    return f"{source}/kb/chunks/{iso3}/{format_slug}"


def enriched_prefix(source: str, iso3: str, format_slug: str) -> str:
    return f"{source}/kb/enriched/{iso3}/{format_slug}"


def datapoints_prefix(source: str, iso3: str, format_slug: str) -> str:
    return f"{source}/kb/datapoints/{iso3}/{format_slug}"


def figures_prefix(source: str, iso3: str, format_slug: str) -> str:
    return f"{source}/kb/figures/{iso3}/{format_slug}"
