"""Web-search provider selection for the analysis gap-fill (doc §9).

Two interchangeable providers expose the SAME callable interface — a search
``client(query, *, count, max_urls, freshness, threshold_mode, ...)`` returning a
list of results that each carry ``url`` / ``title`` / ``snippets`` / ``site_name``
— so :func:`web_gapfill.fill_gap` consumes either without caring which answered:

- ``serper`` (default) — Serper (Google SERP): real Google recall, e.g. Arabic
  via ``SERPER_GL``/``SERPER_HL`` (``providers/serper_search.py``).
- ``brave``            — Brave LLM Context: search + extracted chunks in one call,
  independent index (``providers/brave_search.py``).

Selected by ``WEB_SEARCH_PROVIDER`` (``serper`` | ``brave``); unknown values fall
back to ``serper``. Both providers raise :class:`WebSearchError` on failure, so the
asset catches one error type regardless of the configured provider.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable

logger = logging.getLogger(__name__)


class WebSearchError(RuntimeError):
    """A web-search provider request failed (missing key, HTTP error, rate limit).

    Base class for the per-provider errors (``BraveSearchError``,
    ``SerperSearchError``) so callers can catch one type.
    """


# A provider is any callable returning objects with url/title/snippets/site_name.
WebSearchClient = Callable[..., list]


def resolve_web_search() -> tuple[WebSearchClient, str, str]:
    """Resolve ``WEB_SEARCH_PROVIDER`` → ``(client, api_key_env, provider_name)``.

    ``api_key_env`` is the env var the asset checks to decide whether the provider
    is configured (an empty key disables web gap-fill). Imports are lazy so a
    deploy without one provider's deps still loads.
    """
    provider = (os.environ.get("WEB_SEARCH_PROVIDER") or "serper").strip().lower()
    if provider == "brave":
        from clear_pipeline.providers.brave_search import llm_context

        return llm_context, "BRAVE_SEARCH_API_KEY", "brave"
    if provider not in ("serper", ""):
        logger.warning(
            "[web_search] unknown WEB_SEARCH_PROVIDER=%r — falling back to serper", provider
        )
    from clear_pipeline.providers.serper_search import serper_search

    return serper_search, "SERPER_API_KEY", "serper"
