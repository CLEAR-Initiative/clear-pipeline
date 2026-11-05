"""S3 key layout for analysis-module artifacts (doc §13).

Per-node descriptive artifacts are content-addressed by
``(iso3, window_kind, prompt_version, schema_version, node_key)`` so a re-run
reuses them (skip-if-exists) and a prompt/schema bump re-keys cleanly (old
versions coexist). The versions sit in a path segment so a bump lands every
node's artifact under a fresh prefix.

``node_key`` is the taxonomy slug (e.g. ``pillars_1d/casualties``) — it already
contains ``/``, so artifacts nest by task/pillar naturally. Keys are pure +
byte-stable, like the KB layout in ``providers/kb_sources/paths.py``.
"""

from __future__ import annotations

_ROOT = "analysis"


def descriptive_prefix(
    iso3: str, window_kind: str, prompt_version: str, schema_version: str
) -> str:
    """Prefix for all per-node descriptive artifacts of one (country, window,
    versions). No trailing slash — callers append ``/<node_key>.json``."""
    return f"{_ROOT}/descriptive/{iso3}/{window_kind}/{prompt_version}_{schema_version}"


def descriptive_key(
    iso3: str, window_kind: str, prompt_version: str, schema_version: str, node_key: str
) -> str:
    """S3 key for one taxonomy node's descriptive artifact."""
    return f"{descriptive_prefix(iso3, window_kind, prompt_version, schema_version)}/{node_key}.json"


def gaps_key(iso3: str, window_kind: str, prompt_version: str, schema_version: str) -> str:
    """S3 key for the per-country ranked information-gaps artifact."""
    return f"{_ROOT}/gaps/{iso3}/{window_kind}/{prompt_version}_{schema_version}.json"


def evaluation_key(iso3: str, window_kind: str, prompt_version: str, schema_version: str) -> str:
    """S3 key for the per-country evaluated-package (programmatic filter) artifact."""
    return f"{_ROOT}/evaluation/{iso3}/{window_kind}/{prompt_version}_{schema_version}.json"


def numbers_key(iso3: str, window_kind: str, prompt_version: str, schema_version: str) -> str:
    """S3 key for the per-country extracted-numbers artifact (pre-gap, §10)."""
    return f"{_ROOT}/numbers/{iso3}/{window_kind}/{prompt_version}_{schema_version}.json"


def gapfill_key(
    iso3: str, window_kind: str, prompt_version: str, schema_version: str, node_key: str
) -> str:
    """S3 key for one gap's web gap-fill result (keyed by the gap's node)."""
    return f"{_ROOT}/gapfill/{iso3}/{window_kind}/{prompt_version}_{schema_version}/{node_key}.json"


def pinned_sites_key(iso3: str) -> str:
    """S3 key for the operator-maintained per-frame pinned-sites map
    (``{domain: category}``) consumed by the source classifier (§9.1)."""
    return f"{_ROOT}/config/pinned_sites/{iso3}.json"
