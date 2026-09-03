"""darfur24.com news client — polls the site's WordPress RSS feed for articles.

darfur24.com is a Sudanese news outlet (the most-cited source in the HSS DAO
group) running WordPress, which exposes standard RSS 2.0 feeds — no
authentication and no scraping needed:

  - https://darfur24.com/en/feed/  (English edition — our default)
  - https://darfur24.com/feed/     (Arabic — primary edition)

We poll the English edition by default because the pipeline's canonical
content language is English (see `target_locales` in src/config.py). The
Arabic feed can be added via DARFUR24_FEED_URLS if coverage gaps show up —
but note both editions carry the same stories, so polling both creates
near-duplicate signals with different slugs.

Each feed serves the ~10 most recent articles. Dedup is two-layered:
  1. Redis seen-set (`darfur24:seen:{slug}`, `dedup_ttl_hours` TTL) stops us
     re-emitting an article across poll rounds. Fetching only *reads* the
     seen-set; articles are marked via `mark_seen()` — called by the poll
     task once signal creation succeeded — so a failed or skipped creation
     leaves the article eligible for the next poll (expo-383).
  2. The CLEAR API's (sourceId, externalId) uniqueness — externalId is
     `darfur24:{slug}` — keeps createSignal idempotent even after the Redis
     TTL expires (the API returns the existing row instead of inserting).

Ported verbatim from clear-pipeline for the Dagster consolidation.
"""

import html
import logging
import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import unquote, urlparse

import httpx
import redis

from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

# RSS 2.0 module namespaces used by WordPress feeds.
_NS = {
    "content": "http://purl.org/rss/1.0/modules/content/",
    "dc": "http://purl.org/dc/elements/1.1/",
}

# WordPress appends a "The post <a>…</a> appeared first on <a>…</a>." footer
# paragraph to every feed description — pure boilerplate, strip it.
_FOOTER_RE = re.compile(r"<p>\s*The post .*? appeared first on .*?</p>", re.DOTALL)

_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    """Reduce a WordPress HTML fragment to plain text."""
    text = _FOOTER_RE.sub(" ", text)
    text = _TAG_RE.sub(" ", text)
    text = html.unescape(text)
    return " ".join(text.split())


def _slug_from_link(link: str) -> str | None:
    """Extract the WordPress post slug from a permalink.

    Permalinks look like https://darfur24.com/en/2026/08/04/{slug}/ — the
    slug is the last non-empty path segment. Arabic-edition slugs arrive
    percent-encoded; unquote them so the externalId is stable and readable.
    """
    path = urlparse(link).path
    segments = [s for s in path.split("/") if s]
    if not segments:
        return None
    slug = unquote(segments[-1]).strip()
    return slug or None


def _parse_item(item: ET.Element) -> dict | None:
    """Normalize one RSS <item> into our article dict.

    Returns None when the item lacks the fields we key on (link → slug,
    title).
    """
    link = (item.findtext("link") or "").strip()
    title = (item.findtext("title") or "").strip()
    if not link or not title:
        return None

    slug = _slug_from_link(link)
    if not slug:
        return None

    pub_date_raw = (item.findtext("pubDate") or "").strip()
    published_at = None
    if pub_date_raw:
        try:
            published_at = parsedate_to_datetime(pub_date_raw).astimezone(UTC).isoformat()
        except (ValueError, TypeError):
            logger.warning("[DARFUR24] unparseable pubDate %r for %s", pub_date_raw, slug)

    description_raw = (
        item.findtext("description") or item.findtext("content:encoded", namespaces=_NS) or ""
    ).strip()
    description = _strip_html(description_raw)[:500] if description_raw else None

    categories = [c.text.strip() for c in item.findall("category") if c.text and c.text.strip()]
    creator = (item.findtext("dc:creator", namespaces=_NS) or "").strip()
    guid = (item.findtext("guid") or "").strip()

    return {
        "darfur24_id": slug,
        "title": title,
        "description": description,
        "url": link,
        "published_at": published_at,
        # JSON-serializable snapshot of the raw item for the signal's rawData.
        "raw": {
            "title": title,
            "link": link,
            "guid": guid,
            "pub_date": pub_date_raw,
            "description": description_raw,
            "categories": categories,
            "creator": creator,
        },
    }


def _fetch_feed(url: str) -> str | None:
    """Fetch one RSS feed; returns the response body or None on failure."""
    logger.info("[DARFUR24] Fetching feed url=%s", url)
    try:
        resp = httpx.get(url, timeout=30, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.error("[DARFUR24] feed request failed for %s: %s", url, e)
        return None

    if not resp.text.strip():
        logger.warning("[DARFUR24] empty response for %s (status=%d)", url, resp.status_code)
        return None
    return resp.text


def fetch_darfur24_articles() -> list[dict]:
    """
    Fetch new articles from all configured darfur24 feeds.

    RSS has no server-side time window (each feed just serves the latest ~10
    items), so unlike ACLED/GDACS there is no `since` parameter — every poll
    parses the whole feed and relies on the Redis seen-set plus the API's
    (sourceId, externalId) uniqueness to stay idempotent.

    This function only *reads* the seen-set. It never marks articles seen —
    that is the caller's job via `mark_seen()`, after the article's signal
    has actually been created (or confirmed as an existing row) in the CLEAR
    API. Marking during fetch would permanently skip articles whose creation
    failed (expo-383).

    Returns a list of normalized article dicts.
    """
    feed_urls = [u.strip() for u in settings.darfur24_feed_urls.split(",") if u.strip()]
    logger.info("[DARFUR24] Configured feeds: %s", feed_urls)

    all_items: list[ET.Element] = []
    for url in feed_urls:
        body = _fetch_feed(url)
        if not body:
            continue
        try:
            root = ET.fromstring(body)
        except ET.ParseError as e:
            logger.error("[DARFUR24] XML parse failed for %s: %s", url, e)
            continue
        items = root.findall("./channel/item")
        logger.info("[DARFUR24] %s: %d raw items", url, len(items))
        all_items.extend(items)

    # Parse and deduplicate
    articles: list[dict] = []
    batch_slugs: set[str] = set()
    parse_failed = 0
    deduped = 0
    for item in all_items:
        parsed = _parse_item(item)
        if not parsed:
            parse_failed += 1
            continue

        slug = parsed["darfur24_id"]
        if slug in batch_slugs or _redis.exists(f"darfur24:seen:{slug}"):
            deduped += 1
            continue

        batch_slugs.add(slug)
        articles.append(parsed)

    if articles:
        set_last_synced(datetime.now(UTC))

    logger.info(
        "[DARFUR24] Result: %d new articles (parse_failed=%d, already_seen=%d) out of %d raw",
        len(articles), parse_failed, deduped, len(all_items),
    )
    return articles


def mark_seen(slug: str) -> None:
    """Mark one article as ingested in the Redis seen-set.

    Called by the poll task only after the CLEAR API confirmed the signal
    (created, or returned the existing row for a duplicate externalId).
    Never called for articles whose creation failed — those must stay
    eligible for the next poll round.
    """
    _redis.setex(f"darfur24:seen:{slug}", settings.dedup_ttl_hours * 3600, "1")


def get_last_synced() -> datetime | None:
    val = _redis.get("darfur24:last_synced")
    if val:
        return datetime.fromisoformat(val)
    return None


def set_last_synced(ts: datetime) -> None:
    _redis.set("darfur24:last_synced", ts.isoformat())


def build_darfur24_signal_input(article: dict, source_id: str, location_id: str | None) -> dict:
    """Convert a parsed darfur24 article into a CLEAR CreateSignalInput dict.

    News articles carry no structured casualty or coordinate data — unlike
    ACLED/GDACS we deliberately set none of those. Location is the country
    L0 (see `darfur24_default_country`); finer-grained resolution is the
    classification follow-up, not this tracer.
    """
    input_data = {
        "sourceId": source_id,
        # Dedup key — (sourceId, externalId) is unique in the CLEAR API, so
        # re-ingesting the same article (Redis seen-set expired, feed replay)
        # returns the existing row instead of creating a duplicate.
        "externalId": f"darfur24:{article['darfur24_id']}",
        "rawData": article["raw"],
        "publishedAt": article.get("published_at") or datetime.now(UTC).isoformat(),
        "url": article["url"],
        "title": article["title"],
        "description": article.get("description"),
        # Documented informational default for news-source signals: 1 is the
        # scale floor ("informational"), not an estimated threat level. A
        # null severity makes the signal vanish from any severity-filtered
        # view (the API's gte/lte range filter drops null rows) — expo-385.
        "severity": 1,
    }
    if location_id is not None:
        input_data["locationId"] = location_id
    return input_data
