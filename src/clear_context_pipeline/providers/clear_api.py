"""clear-api GraphQL client for the knowledge-base pipeline.

Mirrors clear-pipeline's client pattern so future readers can move
between the two codebases without re-learning:
  - httpx POST with ``Authorization: Bearer <api_key>``
  - 4xx are non-retryable (bug in caller); 5xx / connection errors
    retry with exponential backoff
  - a small, typed public API — one function per operation

No direct DB access. All knowledgebase writes and location lookups
route through clear-api so authz, schema validation, and pgvector
casts live in exactly one place.

Env vars:
    CLEAR_API_URL   — full GraphQL endpoint, e.g.
                      "http://localhost:4000/graphql"
    CLEAR_API_KEY   — pipeline-scoped API key (`sk_live_…`)
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class ClearApiError(RuntimeError):
    """Non-retryable clear-api failure — schema mismatch, auth error,
    validation reject, etc. Callers should surface + skip the batch
    rather than retrying and amplifying the bad request."""


_RESOLVE_LOCATION = """
query ResolveKnowledgebaseLocation($pcode: String, $name: String, $adminLevel: Int) {
  resolveKnowledgebaseLocation(pcode: $pcode, name: $name, adminLevel: $adminLevel)
}
"""

_UPSERT_KNOWLEDGEBASE = """
mutation UpsertKnowledgebaseChunks(
  $reportId: String!,
  $reportTitle: String!,
  $sourceUrl: String!,
  $s3Key: String!,
  $publishedAt: DateTime!,
  $chunks: [KnowledgebaseChunkInput!]!,
) {
  upsertKnowledgebaseChunks(
    reportId: $reportId,
    reportTitle: $reportTitle,
    sourceUrl: $sourceUrl,
    s3Key: $s3Key,
    publishedAt: $publishedAt,
    chunks: $chunks,
  ) {
    reportId
    chunksDeleted
    chunksInserted
  }
}
"""


def _execute(
    query: str,
    variables: dict[str, Any] | None = None,
    *,
    retries: int = 3,
) -> dict[str, Any]:
    """POST a GraphQL operation with the same retry semantics
    clear-pipeline uses.

    - 4xx → raise ``ClearApiError`` immediately, no retry (broken
      request; retrying just amplifies the damage — see
      clear-pipeline's populationDisplaced incident).
    - 5xx / connection errors → exponential backoff, up to
      ``retries`` attempts, then re-raise the last error.
    - ``errors`` in the JSON body → treated as a hard failure via
      ``RuntimeError`` (retryable — sometimes reflects transient
      server-side state like a lock conflict).
    """
    url = _require_env("CLEAR_API_URL")
    api_key = _require_env("CLEAR_API_KEY")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload: dict[str, Any] = {"query": query}
    if variables:
        payload["variables"] = variables

    for attempt in range(1, retries + 1):
        try:
            resp = httpx.post(url, json=payload, headers=headers, timeout=60)

            if 400 <= resp.status_code < 500:
                snippet = resp.text[:500] if resp.text else "(empty)"
                msg = (
                    f"clear-api {resp.status_code} (non-retryable) for {url}: "
                    f"{snippet}"
                )
                logger.error(msg)
                raise ClearApiError(msg)

            resp.raise_for_status()
            result = resp.json()

            if "errors" in result:
                logger.error("clear-api GraphQL errors: %s", result["errors"])
                raise RuntimeError(f"clear-api GraphQL errors: {result['errors']}")

            return result["data"]

        except ClearApiError:
            raise
        except (httpx.HTTPError, RuntimeError) as exc:
            if attempt < retries:
                wait = 2 ** attempt
                logger.warning(
                    "clear-api request failed (attempt %d/%d), retrying in %ds: %s",
                    attempt, retries, wait, exc,
                )
                time.sleep(wait)
            else:
                logger.error("clear-api request failed after %d attempts: %s", retries, exc)
                raise


# ────────────────────────────────────────────────────────────────────
# Public API — one function per operation
# ────────────────────────────────────────────────────────────────────


def resolve_location(
    *,
    pcode: str | None = None,
    name: str | None = None,
    admin_level: int | None = None,
) -> str | None:
    """Return the ``locations.id`` matching this LLM-emitted ref, or
    ``None`` when no match.

    Pcode wins over name; ``admin_level`` narrows the name match. The
    normalisation (case-insensitive, L4 exclusion) lives on the
    clear-api side so downstream callers don't reimplement it.
    """
    if not pcode and not name:
        return None
    data = _execute(
        _RESOLVE_LOCATION,
        {"pcode": pcode, "name": name, "adminLevel": admin_level},
    )
    return data.get("resolveKnowledgebaseLocation")


def upsert_knowledgebase_chunks(
    *,
    report_id: str,
    report_title: str,
    source_url: str,
    s3_key: str,
    published_at: str,
    chunks: list[dict[str, Any]],
) -> dict[str, int]:
    """Replace all knowledgebase rows for ``report_id`` with ``chunks``.

    ``published_at`` and per-chunk ``time_range_start`` / ``time_range_end``
    are passed as ISO-8601 strings so the caller doesn't have to
    remember Python-side datetime serialisation quirks. clear-api's
    DateTime scalar accepts either.

    Returns the summary counts the server logs.
    """
    data = _execute(
        _UPSERT_KNOWLEDGEBASE,
        {
            "reportId": report_id,
            "reportTitle": report_title,
            "sourceUrl": source_url,
            "s3Key": s3_key,
            "publishedAt": published_at,
            "chunks": chunks,
        },
    )
    result = data["upsertKnowledgebaseChunks"]
    return {
        "reportId": result["reportId"],
        "chunksDeleted": int(result["chunksDeleted"]),
        "chunksInserted": int(result["chunksInserted"]),
    }


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing env var {name}. Set it in .env or export it.")
    return value
