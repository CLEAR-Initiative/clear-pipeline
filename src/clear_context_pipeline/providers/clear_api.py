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

_UPSERT_REPORT_DATAPOINTS = """
mutation UpsertReportDatapoints($input: UpsertReportDatapointsInput!) {
  upsertReportDatapoints(input: $input) {
    reportId
    schemaVersion
    createdOrReplaced
  }
}
"""

_REFRESH_AGGREGATED_DATAPOINTS = """
mutation RefreshAggregatedDatapoints($from: DateTime!, $to: DateTime!, $schemaVersion: String!) {
  refreshAggregatedDatapoints(from: $from, to: $to, schemaVersion: $schemaVersion) {
    computedBuckets
    supersededBuckets
    situationAnalysesInvalidated
    schemaVersion
  }
}
"""

_HAS_AGGREGATED_DATAPOINTS = """
query HasAggregatedDatapoints($schemaVersion: String!) {
  hasAggregatedDatapoints(schemaVersion: $schemaVersion)
}
"""

# ── Situation analysis ─────────────────────────────────────────────

_GET_AGGREGATED_DATAPOINT = """
query AggregatedDatapoint(
  $locationId: String,
  $windowStart: DateTime!,
  $windowEnd: DateTime!,
  $windowKind: String!,
  $schemaVersion: String,
) {
  aggregatedDatapoint(
    locationId: $locationId,
    windowStart: $windowStart,
    windowEnd: $windowEnd,
    windowKind: $windowKind,
    schemaVersion: $schemaVersion,
  ) {
    id
    windowStart
    windowEnd
    windowKind
    locationId
    data
    contributingReportIds
    newestSourceAt
    oldestSourceAt
    dataQualityScore
    reportCount
    validFrom
    validTo
    schemaVersion
    onDemand
  }
}
"""

_GET_PIPELINE_COUNTRIES = """
query PipelineCountriesForSituation {
  pipelineCountries { name bbox }
}
"""

# We already have `resolveKnowledgebaseLocation` for pcode/name → id
# lookups. Situation-analysis needs the reverse — a specific country
# location by name — so we reuse that resolver by passing name only.

_UPSERT_SITUATION_ANALYSIS = """
mutation UpsertSituationAnalysis($input: UpsertSituationAnalysisInput!) {
  upsertSituationAnalysis(input: $input) {
    situationAnalysisId
    countryLocationId
    supersededPrevious
  }
}
"""

_SEARCH_KNOWLEDGEBASE = """
query SearchKnowledgebaseForSituation(
  $query: String!,
  $filters: KnowledgebaseFilters,
  $limit: Int,
) {
  searchKnowledgebase(query: $query, filters: $filters, limit: $limit) {
    id
    reportId
    reportTitle
    sourceUrl
    publishedAt
    pageStart
    pageEnd
    chunkText
    score
    locationIds
    eventTypes
    needSectors
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


def upsert_report_datapoints(
    *,
    report_id: str,
    report_title: str,
    source_url: str,
    published_at: str,
    reporting_period_start: str | None,
    reporting_period_end: str | None,
    location_ids: list[str],
    location_pcodes: list[str],
    event_types: list[str],
    total_affected: int | None,
    total_displaced: int | None,
    total_killed: int | None,
    data: dict[str, Any],
    schema_version: str,
    extracted_by_model: str,
) -> dict[str, Any]:
    """Replace the ``report_datapoints`` row for ``report_id``.

    The `data` blob follows the Pydantic sub-schema layout defined in
    ``datapoints_schemas.py`` — one top-level key per domain, each
    holding the domain's dumped model (or None if that domain's
    extraction failed and the operator wants to re-run it later).

    Timestamps are ISO-8601 strings; clear-api's DateTime scalar
    parses either date or datetime forms.
    """
    payload = {
        "reportId": report_id,
        "reportTitle": report_title,
        "sourceUrl": source_url,
        "publishedAt": published_at,
        "reportingPeriodStart": reporting_period_start,
        "reportingPeriodEnd": reporting_period_end,
        "locationIds": location_ids,
        "locationPcodes": location_pcodes,
        "eventTypes": event_types,
        "totalAffected": total_affected,
        "totalDisplaced": total_displaced,
        "totalKilled": total_killed,
        "data": data,
        "schemaVersion": schema_version,
        "extractedByModel": extracted_by_model,
    }
    result = _execute(_UPSERT_REPORT_DATAPOINTS, {"input": payload})
    return result["upsertReportDatapoints"]


def has_aggregated_datapoints(schema_version: str) -> bool:
    """Cheap existence check — is there at least one current
    aggregated_datapoints row for this schema version?

    Used by the aggregation asset to distinguish first-run backfill
    (needs a wide lookback window to catch existing history) from
    routine weekly refreshes (narrow window is enough).
    """
    data = _execute(
        _HAS_AGGREGATED_DATAPOINTS, {"schemaVersion": schema_version},
    )
    return bool(data.get("hasAggregatedDatapoints"))


def refresh_aggregated_datapoints(
    *,
    from_iso: str,
    to_iso: str,
    schema_version: str,
) -> dict[str, Any]:
    """Trigger clear-api's four-tier aggregation refresh for every
    report whose ``reportingPeriodEnd`` falls in ``[from_iso, to_iso]``.

    Returns the server-side summary: ``{ computedBuckets,
    supersededBuckets, schemaVersion }``. clear-api walks the
    hierarchy (A2 → A1 → A0) internally so a single call refreshes
    weekly-A2, monthly-A1, yearly-country, and all-time-country
    tiers atomically per bucket.
    """
    data = _execute(
        _REFRESH_AGGREGATED_DATAPOINTS,
        {"from": from_iso, "to": to_iso, "schemaVersion": schema_version},
    )
    return data["refreshAggregatedDatapoints"]


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


def get_aggregated_datapoint(
    *,
    location_id: str | None,
    window_start: str,
    window_end: str,
    window_kind: str,
    schema_version: str | None = None,
) -> dict[str, Any] | None:
    """Fetch a single aggregated-datapoints bucket. Returns None when
    no snapshot exists (cache miss AND no contributing reports).

    Used by the situation-analysis generator to hoist the deterministic
    Datapoints component out of the pre-computed cache."""
    data = _execute(
        _GET_AGGREGATED_DATAPOINT,
        {
            "locationId": location_id,
            "windowStart": window_start,
            "windowEnd": window_end,
            "windowKind": window_kind,
            "schemaVersion": schema_version,
        },
    )
    return data.get("aggregatedDatapoint")


def get_pipeline_countries() -> list[dict[str, Any]]:
    """List of countries the pipeline currently publishes analysis
    for. Currently: Sudan (POC scope)."""
    data = _execute(_GET_PIPELINE_COUNTRIES)
    return data.get("pipelineCountries") or []


def resolve_country_location_id(country_name: str) -> str | None:
    """Reverse-lookup a country's `locations.id` from its name.
    Reuses the knowledgebase location resolver with admin_level=0.

    Kept as a wrapper so future callers (dashboards, exports) can
    hit one function even if the underlying resolver evolves."""
    return resolve_location(name=country_name, admin_level=0)


def search_knowledgebase(
    *,
    query: str,
    filters: dict[str, Any] | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Hybrid dense + BM25 retrieval over the knowledgebase.

    Returns a list of hits ordered by RRF score, each carrying its
    source report metadata + page range so the narrative generator
    can attribute bullets back to reports without a second lookup.

    Filters mirror `KnowledgebaseFilters` on the GraphQL side —
    passing None applies no filter (semantic ranking only). For the
    situation-analysis path we skip location filtering because
    knowledgebase rows are tagged at admin-2 level but our scope is
    the country (A0); semantic relevance handles the geo scoping.
    """
    data = _execute(
        _SEARCH_KNOWLEDGEBASE,
        {"query": query, "filters": filters, "limit": limit},
    )
    return data.get("searchKnowledgebase") or []


def upsert_situation_analysis(
    *,
    country_location_id: str,
    window_start: str,
    window_end: str,
    window_kind: str,
    data: dict[str, Any],
    source_report_ids: list[str],
    aggregated_datapoint_id: str | None,
    generated_by_model: str,
    generation_cost_usd: float | None,
    schema_version: str,
) -> dict[str, Any]:
    """Insert a new situation-analysis snapshot and supersede the
    previous "current" row for the same
    (country, window_kind, window_start, schema_version). One
    transaction on the clear-api side — no half-written state on
    partial failure.

    `window_kind` ("yearly") is part of the bucket key; `window_end` is
    stored but never matched on. The two sides build the calendar window
    independently, in different languages, so an end-of-day that differs
    by a millisecond would otherwise write rows no reader could find.
    """
    payload = {
        "countryLocationId": country_location_id,
        "windowStart": window_start,
        "windowEnd": window_end,
        "windowKind": window_kind,
        "data": data,
        "sourceReportIds": source_report_ids,
        "aggregatedDatapointId": aggregated_datapoint_id,
        "generatedByModel": generated_by_model,
        "generationCostUsd": generation_cost_usd,
        "schemaVersion": schema_version,
    }
    result = _execute(_UPSERT_SITUATION_ANALYSIS, {"input": payload})
    return result["upsertSituationAnalysis"]


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing env var {name}. Set it in .env or export it.")
    return value
