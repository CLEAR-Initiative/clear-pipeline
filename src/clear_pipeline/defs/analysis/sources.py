"""Source-category classification for web gap-fill (doc §9.1).

Rank a small fixed set of **categories**, classify each encountered source into
one at runtime — never maintain an exhaustive source allowlist (it would rot and
never achieve coverage). Cheap signals first (TLD / known high-tier anchors),
then an optional LLM call for the long tail, with an observed ``domain→category``
cache that grows by observation (not a curated registry). A per-frame
operator **pinned-sites** map can pin a domain to a category (answer #5).

The category maps onto ``data_sources.reliability`` so web, KB and manual
evidence share one comparable reliability number, and so evaluation / numbers
reconciliation can discount low-tier web data against official sources.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Category → reliability (0–10 scale, matching data_sources.reliability / ADR-0005).
CATEGORY_GOVERNMENT = 1  # national stats offices, ministries; UN (OCHA/UNHCR/IOM/WHO)
CATEGORY_ORGS = 2        # established NGOs, research institutes, ReliefWeb-class
CATEGORY_NEWS = 3        # established news outlets
CATEGORY_SOCIAL = 4      # verified official orgs/officials on social platforms
CATEGORY_UNKNOWN = 5     # everything unclassified

_CATEGORY_RELIABILITY: dict[int, int] = {
    CATEGORY_GOVERNMENT: 4,
    CATEGORY_ORGS: 3,
    CATEGORY_NEWS: 2,
    CATEGORY_SOCIAL: 2,
    CATEGORY_UNKNOWN: 1,
}

# A tiny seed of high-tier anchors — NOT a master list; the classifier resolves
# everything else by heuristics / LLM / cache.
_UN_ANCHORS = (
    "un.org", "unhcr.org", "iom.int", "who.int", "wfp.org", "unicef.org",
    "ochaopt.org", "unocha.org", "fao.org", "undp.org", "reliefweb.int",
    "ipcinfo.org", "acleddata.com",
)
_ORG_ANCHORS = (
    "reliefweb.int", "icrc.org", "msf.org", "nrc.no", "savethechildren.net",
    "rescue.org", "care.org", "oxfam.org", "acaps.org",
)
_SOCIAL_HOSTS = ("twitter.com", "x.com", "facebook.com", "t.me", "telegram.me", "youtube.com")


@dataclass(frozen=True)
class ClassifiedSource:
    domain: str
    category: int
    reliability: int
    how: str  # pinned | heuristic | llm | cache | default


def reliability_for(category: int) -> int:
    return _CATEGORY_RELIABILITY.get(category, 1)


def domain_of(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def _heuristic(domain: str) -> int | None:
    """Cheap TLD / anchor signals — returns a category or None (undecided)."""
    if not domain:
        return None
    if domain.endswith(".gov") or ".gov." in domain or domain.endswith(".mil"):
        return CATEGORY_GOVERNMENT
    if domain.endswith(".int"):
        return CATEGORY_GOVERNMENT
    if any(domain == a or domain.endswith("." + a) for a in _UN_ANCHORS):
        return CATEGORY_GOVERNMENT
    if any(domain == a or domain.endswith("." + a) for a in _ORG_ANCHORS):
        return CATEGORY_ORGS
    if domain.endswith(".edu") or ".edu." in domain or domain.endswith(".ac.uk"):
        return CATEGORY_ORGS
    if any(domain == h or domain.endswith("." + h) for h in _SOCIAL_HOSTS):
        return CATEGORY_SOCIAL
    return None


def classify(
    url: str,
    *,
    pinned_sites: dict[str, int] | None = None,
    cache: dict[str, int] | None = None,
    llm_classify: Callable[[str], int] | None = None,
) -> ClassifiedSource:
    """Classify a source URL into a category.

    Order: per-frame ``pinned_sites`` → cheap heuristics → observed ``cache`` →
    optional ``llm_classify`` (domain → category) for the long tail → default
    unknown. A resolved ``(domain → category)`` is written back to ``cache``.
    """
    domain = domain_of(url)

    if pinned_sites and domain in pinned_sites:
        cat = pinned_sites[domain]
        return ClassifiedSource(domain, cat, reliability_for(cat), "pinned")

    cat = _heuristic(domain)
    if cat is not None:
        return ClassifiedSource(domain, cat, reliability_for(cat), "heuristic")

    if cache is not None and domain in cache:
        cat = cache[domain]
        return ClassifiedSource(domain, cat, reliability_for(cat), "cache")

    if llm_classify is not None and domain:
        try:
            cat = int(llm_classify(domain))
        except Exception:  # noqa: BLE001 — a bad classification must not fail gap-fill
            cat = CATEGORY_UNKNOWN
        if cat not in _CATEGORY_RELIABILITY:
            cat = CATEGORY_UNKNOWN
        if cache is not None and domain:
            cache[domain] = cat  # grows by observation
        return ClassifiedSource(domain, cat, reliability_for(cat), "llm")

    return ClassifiedSource(domain, CATEGORY_UNKNOWN, reliability_for(CATEGORY_UNKNOWN), "default")
