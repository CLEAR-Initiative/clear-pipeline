"""S3 read/write helpers for analysis-module artifacts (doc §13).

Thin wrappers over the S3 client so the assets stay readable and the gaps /
evaluation stages can load the descriptive artifacts ``analysis_sub`` wrote.
Reads tolerate a missing node (skipped/failed upstream) — the gaps/eval logic
treats an absent node as a gap rather than failing the whole country.
"""

from __future__ import annotations

import json

from . import paths
from .registry import PROMPT_VERSION, SCHEMA_VERSION
from .taxonomy import TaxonomyNode


def load_descriptive(
    s3, bucket: str, iso3: str, window_kind: str, nodes: list[TaxonomyNode]
) -> dict[str, dict]:
    """Load the per-node descriptive artifacts for ``nodes`` → ``{node_key: artifact}``.

    A node whose artifact is absent/unreadable is simply omitted (the caller
    treats it as not-generated)."""
    out: dict[str, dict] = {}
    for node in nodes:
        key = paths.descriptive_key(iso3, window_kind, PROMPT_VERSION, SCHEMA_VERSION, node.key)
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            out[node.key] = json.loads(obj["Body"].read())
        except Exception:  # noqa: BLE001 — missing/unreadable → omit (= gap)
            continue
    return out


def save_json(s3, bucket: str, key: str, payload: dict) -> None:
    """Persist a JSON artifact (compact, UTF-8)."""
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
