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

import json
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

_REPORT_DATAPOINT_EXISTS = """
query ReportDatapointExists($reportId: String!) {
  reportDatapoint(reportId: $reportId) {
    id
  }
}
"""

# ── Situation analysis ─────────────────────────────────────────────

_GET_SITUATION_ANALYSIS = """
query SituationAnalysis($countryLocationId: String!, $year: Int, $schemaVersion: String) {
  situationAnalysis(countryLocationId: $countryLocationId, year: $year, schemaVersion: $schemaVersion) {
    data
    generatedAt
  }
}
"""


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
  pipelineCountries { name iso3 bbox }
}
"""

# Location-metadata ingests (HAPI, IOM DTM) fetch external rows keyed by admin
# pcode and need to map them onto clear-api location ids. `locations(level:)`
# returns the whole admin layer at once so a caller can build a pcode→id map in
# one round-trip instead of N single resolves.
_GET_LOCATIONS_BY_LEVEL = """
query LocationsByLevel($level: Int!) {
  locations(level: $level) {
    id
    name
    level
    pCode
    population
  }
}
"""

_UPSERT_LOCATION_METADATA_BATCH = """
mutation UpsertLocationMetadataBatch($inputs: [UpsertLocationMetadataInput!]!) {
  upsertLocationMetadataBatch(inputs: $inputs) { id type }
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


def report_datapoints_exist(report_id: str) -> bool:
    """True when this report's datapoints have already been extracted and
    upserted (a ``report_datapoints`` row exists for it).

    Lets the datapoint asset skip the 6 LLM extraction calls for a report a
    prior run already finished. The DB is the source of truth on purpose — the
    S3 debug snapshot is written BEFORE the upsert, so it can't confirm the
    write actually landed.
    """
    data = _execute(_REPORT_DATAPOINT_EXISTS, {"reportId": report_id})
    return data.get("reportDatapoint") is not None


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


def get_situation_analysis(
    *,
    country_location_id: str,
    year: int,
    schema_version: str | None = None,
) -> dict[str, Any] | None:
    """Fetch the current (yearly) situation-analysis snapshot for a country.
    Used by the generator to read the PRIOR snapshot before it upserts the
    new one, so it can diff them for the "what changed" notes. Returns None
    when no snapshot exists yet."""
    data = _execute(
        _GET_SITUATION_ANALYSIS,
        {
            "countryLocationId": country_location_id,
            "year": year,
            "schemaVersion": schema_version,
        },
    )
    return data.get("situationAnalysis")


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
    """Countries the pipeline currently publishes analysis / ingests context
    for. Each row is ``{name, iso3, bbox}`` — ``iso3`` scopes external-API
    ingests (HAPI ``location_code``, IOM DTM ``Admin0Pcode``)."""
    data = _execute(_GET_PIPELINE_COUNTRIES)
    return data.get("pipelineCountries") or []


def get_locations_by_level(level: int) -> list[dict[str, Any]]:
    """Every clear-api location at one admin level (``{id, name, level, pCode,
    population}``). Location-metadata ingests use this to build a pcode→id map
    for the level they're writing, rather than resolving pcodes one at a time."""
    data = _execute(_GET_LOCATIONS_BY_LEVEL, {"level": level})
    return data.get("locations") or []


# Upsert chunking. Each chunk is one clear-api DB transaction, and clear-api's
# Postgres is reached over an SSH tunnel — so the cost is dominated by BYTES
# moved (payload in + the unchanged-guard reading open rows + reading the result
# back), not row count. A fixed row count is wrong: 50 humanitarian-needs blobs
# (~100 KB each, finely disaggregated PIN) is ~5 MB/chunk and times out, while 50
# tiny funding blobs is nothing. So chunk by cumulative payload BYTES, with a row
# cap as a backstop. Tune with LOCATION_METADATA_UPSERT_MAX_BYTES /
# LOCATION_METADATA_UPSERT_CHUNK.
#
# Read lazily (not at import): every defs/ module calls load_dotenv AFTER
# importing this provider, so import-time os.environ.get would miss .env — the
# documented knobs would silently never apply. Parsing is defensive: a bad value
# logs and falls back rather than raising at import and taking down the whole
# Dagster code location.
_DEFAULT_UPSERT_MAX_BYTES = 400_000
_DEFAULT_UPSERT_MAX_ROWS = 50


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
        if value <= 0:
            raise ValueError("must be positive")
        return value
    except ValueError:
        logger.warning("%s=%r is not a positive integer — using default %d", name, raw, default)
        return default


def _upsert_max_bytes() -> int:
    return _int_env("LOCATION_METADATA_UPSERT_MAX_BYTES", _DEFAULT_UPSERT_MAX_BYTES)


def _upsert_max_rows() -> int:
    return _int_env("LOCATION_METADATA_UPSERT_CHUNK", _DEFAULT_UPSERT_MAX_ROWS)


def _size_chunks(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Split rows into chunks bounded by cumulative byte size (and a row cap). A
    single row larger than the byte cap still goes in its own chunk — we never
    split one blob. The whole row is measured (locationId + type + the JSON
    envelope, not just ``data``), since bytes over the DB tunnel are the cost."""
    max_bytes = _upsert_max_bytes()
    max_rows = _upsert_max_rows()
    chunks: list[list[dict[str, Any]]] = []
    cur: list[dict[str, Any]] = []
    cur_bytes = 0
    for row in rows:
        row_bytes = len(json.dumps(row, default=str))
        if cur and (cur_bytes + row_bytes > max_bytes or len(cur) >= max_rows):
            chunks.append(cur)
            cur, cur_bytes = [], 0
        cur.append(row)
        cur_bytes += row_bytes
    if cur:
        chunks.append(cur)
    return chunks


def upsert_location_metadata_batch(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Bulk-upsert location-metadata rows. Each row is ``{locationId, type,
    data}``; clear-api closes the currently-open row for each (locationId, type)
    and inserts the new one in a single transaction (bitemporal), skipping rows
    whose blob is unchanged. Rows whose ``locationId`` doesn't exist are skipped
    server-side. Requires the pipeline API key to carry the admin/pipeline role.

    Sent in payload-size-bounded chunks so a large batch (many locations, or big
    blobs) can't blow the request/transaction timeout over the DB tunnel. Returns
    every current row clear-api reported across the chunks."""
    if not rows:
        return []
    out: list[dict[str, Any]] = []
    chunks = _size_chunks(rows)
    logger.info(
        "[location_metadata] upserting %d rows in %d size-bounded chunk(s)",
        len(rows), len(chunks),
    )
    for chunk in chunks:
        data = _execute(_UPSERT_LOCATION_METADATA_BATCH, {"inputs": chunk})
        out.extend(data.get("upsertLocationMetadataBatch") or [])
    return out


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
