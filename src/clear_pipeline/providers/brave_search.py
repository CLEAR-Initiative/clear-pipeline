"""Brave Search API — LLM Context endpoint (agentic search + extraction).

One call searches Brave's index AND returns pre-extracted, relevance-ranked
text chunks per source (no separate scraping), which is exactly what the
analysis web gap-fill needs.

Endpoint: ``GET https://api.search.brave.com/res/v1/llm/context``
Auth:     ``x-subscription-token`` header (env ``BRAVE_SEARCH_API_KEY``)
Docs:     https://api-dashboard.search.brave.com/api-reference/ai/llm_context/get
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)

_URL = "https://api.search.brave.com/res/v1/llm/context"
_QUERY_MAX_CHARS = 600  # endpoint hard cap
_DEFAULT_TIMEOUT = float(os.environ.get("BRAVE_TIMEOUT_SECONDS", "30"))


class BraveSearchError(RuntimeError):
    """Brave request failed (missing key, HTTP error, rate limit)."""


@dataclass(frozen=True)
class BraveResult:
    """One grounding source with its extracted, relevance-ranked chunks."""

    url: str
    title: str
    snippets: list[str] = field(default_factory=list)
    site_name: str | None = None


def llm_context(
    query: str,
    *,
    count: int = 20,
    freshness: str | None = None,
    max_urls: int | None = None,
    max_tokens: int = 8192,
    threshold_mode: str | None = "balanced",
    country: str = "ALL",
    search_lang: str = "en",
    timeout: float = _DEFAULT_TIMEOUT,
    api_key: str | None = None,
) -> list[BraveResult]:
    """Query the Brave LLM Context endpoint → extracted sources.

    ``freshness`` is ``pd``/``pw``/``pm``/``py`` or ``YYYY-MM-DDtoYYYY-MM-DD``;
    ``threshold_mode`` (``strict``/``balanced``/``lenient``/``disabled``) filters
    low-relevance noise. Returns ``[]`` on no results. Raises
    :class:`BraveSearchError` on a missing key or HTTP failure.
    """
    key = api_key or os.environ.get("BRAVE_SEARCH_API_KEY")
    if not key:
        raise BraveSearchError("BRAVE_SEARCH_API_KEY is not set")

    params: dict[str, object] = {
        "q": (query or "").strip()[:_QUERY_MAX_CHARS],
        "count": max(1, min(count, 50)),
        "country": country,
        "search_lang": search_lang,
        "maximum_number_of_tokens": max_tokens,
        "enable_source_metadata": "true",  # gives site_name for tier classification
    }
    if freshness:
        params["freshness"] = freshness
    if max_urls:
        params["maximum_number_of_urls"] = max(1, min(max_urls, 50))
    if threshold_mode:
        params["context_threshold_mode"] = threshold_mode

    headers = {"x-subscription-token": key, "accept": "application/json"}
    try:
        resp = httpx.get(_URL, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as exc:
        raise BraveSearchError(f"Brave LLM Context request failed: {exc}") from exc

    return _parse(data)


def _parse(data: dict) -> list[BraveResult]:
    grounding = data.get("grounding") or {}
    generic = grounding.get("generic") or []
    sources = data.get("sources") or {}
    out: list[BraveResult] = []
    for item in generic:
        url = item.get("url") or ""
        if not url:
            continue
        meta = sources.get(url) or {}
        out.append(
            BraveResult(
                url=url,
                title=item.get("title") or "",
                snippets=[s for s in (item.get("snippets") or []) if s and s.strip()],
                site_name=meta.get("site_name"),
            )
        )
    return out


# Type alias for injecting a fake in tests / the asset.
BraveClient = Callable[..., list[BraveResult]]
