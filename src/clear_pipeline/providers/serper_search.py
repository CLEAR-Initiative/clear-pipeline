"""Serper — Google Search API (search + optional scrape).

Serper relays **Google** results in ~1-2s. It's an alternative web-search
provider for the analysis gap-fill (``WEB_SEARCH_PROVIDER=serper``), chosen
chiefly for Google-grade recall on non-English / regional queries (e.g. Arabic
via ``SERPER_GL=sa`` + ``SERPER_HL=ar``) that an independent index underserves.

Returns the SAME result shape as the Brave provider (``url`` / ``title`` /
``snippets`` / ``site_name``) so :func:`web_gapfill.fill_gap` consumes either.
Each organic result's snippet is the evidence text by default; with
``SERPER_SCRAPE=true`` the snippet is replaced by the full page content from the
separate ``scrape.serper.dev`` endpoint (best-effort — a scrape failure keeps the
SERP snippet). The scrape response shape is unverified against Serper's own docs
(their docs host was unreachable), so it is read defensively and defaults OFF.

Endpoint: ``POST https://google.serper.dev/search`` (JSON body)
Auth:     ``X-API-KEY`` header (env ``SERPER_API_KEY``)
Docs:     https://serper.dev
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from urllib.parse import urlsplit

import httpx

from clear_pipeline.providers.web_search import WebSearchError

logger = logging.getLogger(__name__)

_URL = "https://google.serper.dev/search"
_SCRAPE_URL = "https://scrape.serper.dev"
_QUERY_MAX_CHARS = 2048  # generous Google cap; keep a sane bound
_DEFAULT_TIMEOUT = float(os.environ.get("SERPER_TIMEOUT_SECONDS", "15"))
_DEFAULT_SCRAPE_MAX_CHARS = int(os.environ.get("SERPER_SCRAPE_MAX_CHARS", "8000"))

# Brave freshness codes (pd/pw/pm/py) → Google `tbs` recency ranges, so a caller
# written against the Brave interface gets the same freshness intent on Serper.
_FRESHNESS_TO_TBS = {"pd": "qdr:d", "pw": "qdr:w", "pm": "qdr:m", "py": "qdr:y"}

# Response keys scrape.serper.dev may put the page body under. The scrape endpoint
# isn't in Serper's (currently unreachable) published docs — this shape is taken
# from downstream tools, so we read defensively and fall back to the SERP snippet.
_SCRAPE_BODY_KEYS = ("markdown", "text", "body", "content")


def _env_bool(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes")


class SerperSearchError(WebSearchError):
    """Serper request failed (missing key, HTTP error, rate limit)."""


@dataclass(frozen=True)
class SerperResult:
    """One Google organic result — same shape as ``BraveResult`` so fill_gap
    treats Serper and Brave identically."""

    url: str
    title: str
    snippets: list[str] = field(default_factory=list)
    site_name: str | None = None


def _host(url: str) -> str | None:
    """Bare host for source-tier classification (``www.`` stripped)."""
    try:
        host = urlsplit(url).hostname
    except ValueError:
        return None
    if host and host.startswith("www."):
        host = host[4:]
    return host or None


def serper_search(
    query: str,
    *,
    count: int = 20,
    freshness: str | None = None,
    max_urls: int | None = None,
    threshold_mode: str | None = None,
    gl: str | None = None,
    hl: str | None = None,
    scrape: bool | None = None,
    timeout: float = _DEFAULT_TIMEOUT,
    api_key: str | None = None,
) -> list[SerperResult]:
    """Query Serper's Google Search endpoint → organic results.

    ``gl``/``hl`` (Google country / UI language) default to ``SERPER_GL`` /
    ``SERPER_HL`` env (then ``us``/``en``) — set them per target locale for
    regional recall. ``freshness`` maps Brave's pd/pw/pm/py to Google ``tbs``.
    ``threshold_mode`` has no Serper analogue (Google's ranking is the filter) and
    is accepted for interface parity.

    When ``scrape`` is true (default: the ``SERPER_SCRAPE`` env flag), each
    result's short SERP snippet is replaced by the full page content from
    ``scrape.serper.dev`` (best-effort — a scrape failure keeps the snippet), so
    the evidence the LLM sees is the page, not a one-line summary. Raises
    :class:`SerperSearchError` on a missing key or a failed SEARCH; returns ``[]``
    on no results.
    """
    key = api_key or os.environ.get("SERPER_API_KEY")
    if not key:
        raise SerperSearchError("SERPER_API_KEY is not set")

    num = max(1, min(max_urls or count, 100))
    body: dict[str, object] = {
        "q": (query or "").strip()[:_QUERY_MAX_CHARS],
        "num": num,
        "gl": (gl or os.environ.get("SERPER_GL") or "us").lower(),
        "hl": (hl or os.environ.get("SERPER_HL") or "en").lower(),
    }
    tbs = _FRESHNESS_TO_TBS.get(freshness) if freshness else None
    if tbs:
        body["tbs"] = tbs

    headers = {"X-API-KEY": key, "Content-Type": "application/json"}
    try:
        resp = httpx.post(_URL, json=body, headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as exc:
        raise SerperSearchError(f"Serper search request failed: {exc}") from exc

    results = _parse(data)
    do_scrape = scrape if scrape is not None else _env_bool("SERPER_SCRAPE")
    if do_scrape and results:
        results = [_enrich_with_scrape(r, api_key=key, timeout=timeout) for r in results]
    return results


def serper_scrape(
    url: str,
    *,
    include_markdown: bool = True,
    max_chars: int = _DEFAULT_SCRAPE_MAX_CHARS,
    timeout: float = _DEFAULT_TIMEOUT,
    api_key: str | None = None,
) -> str | None:
    """Full page content for one URL via ``scrape.serper.dev`` — markdown when
    available. **Best-effort**: returns the (length-capped) body, or ``None`` on
    any failure (missing key, HTTP error, or an unrecognised response shape) so an
    individual scrape never breaks the search. See ``_SCRAPE_BODY_KEYS`` — the
    response shape is unverified against Serper's own docs.
    """
    key = api_key or os.environ.get("SERPER_API_KEY")
    if not key:
        return None
    headers = {"X-API-KEY": key, "Content-Type": "application/json"}
    payload = {"url": url, "includeMarkdown": include_markdown}
    try:
        resp = httpx.post(_SCRAPE_URL, json=payload, headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except (httpx.HTTPError, ValueError) as exc:  # ValueError = bad JSON
        logger.warning("[serper] scrape failed for %s: %s", url, exc)
        return None
    for k in _SCRAPE_BODY_KEYS:
        val = data.get(k)
        if isinstance(val, str) and val.strip():
            return val.strip()[:max_chars]
    return None


def _enrich_with_scrape(r: SerperResult, *, api_key: str, timeout: float) -> SerperResult:
    """Replace a result's SERP snippet with scraped full content when available."""
    content = serper_scrape(r.url, api_key=api_key, timeout=timeout)
    return replace(r, snippets=[content]) if content else r


def _parse(data: dict) -> list[SerperResult]:
    out: list[SerperResult] = []
    for item in data.get("organic") or []:
        url = item.get("link") or ""
        if not url:
            continue
        snippet = (item.get("snippet") or "").strip()
        out.append(
            SerperResult(
                url=url,
                title=item.get("title") or "",
                snippets=[snippet] if snippet else [],
                site_name=_host(url),
            )
        )
    return out


# Type alias for injecting a fake in tests / the asset.
SerperClient = Callable[..., list[SerperResult]]
