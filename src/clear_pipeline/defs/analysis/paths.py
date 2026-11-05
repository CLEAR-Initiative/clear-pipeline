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
