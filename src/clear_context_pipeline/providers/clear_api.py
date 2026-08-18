"""clear-api GraphQL client for the knowledge-base pipeline.

Mirrors clear-pipeline's client pattern so future readers can move
between the two codebases without re-learning:
  - httpx POST with ``Authorization: Bearer <api_key>``
  - 4xx are non-retryable (bug in caller); 5xx / connection errors
    retry with exponential backoff
  - a small, typed public API - one function per operation

No direct DB access. All knowledgebase writes and location lookups
route through clear-api so authz, schema validation, and pgvector
casts live in exactly one place.

Env vars:
    CLEAR_API_URL   - full GraphQL endpoint, e.g.
                      "http://localhost:4000/graphql"
    CLEAR_API_KEY   - pipeline-scoped API key (`sk_live_…`)
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
    """Non-retryable clear-api failure - schema mismatch, auth error,
    validation reject, etc. Callers should surface + skip the batch
    rather than retrying and amplifying the bad request."""


_RESOLVE_LOCATION = """
query ResolveKnowledgebaseLocation($pcode: String, $name: String, $adminLevel: Int) {
  resolveKnowledgebaseLocation(pcode: $pcode, name: $name, adminLevel: $adminLevel)
}
"""

_RESOLVE_DATA_SOURCE = """
mutation ResolveDataSource($name: String!, $homepage: String) {
  resolveDataSource(name: $name, homepage: $homepage)
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
mutation RefreshAggregatedDatapoints($from: DateTime!, $to: DateTime!, $schemaVersion: String!, $countryLocationId: String) {
  refreshAggregatedDatapoints(from: $from, to: $to, schemaVersion: $schemaVersion, countryLocationId: $countryLocationId) {
    computedBuckets
    supersededBuckets
    situationAnalysesInvalidated
    schemaVersion
  }
}
"""

_HAS_AGGREGATED_DATAPOINTS = """
query HasAggregatedDatapoints($schemaVersion: String!, $countryLocationId: String) {
  hasAggregatedDatapoints(schemaVersion: $schemaVersion, countryLocationId: $countryLocationId)
}
"""

_REPORT_DATAPOINT_EXISTS = """
query ReportDatapointExists($reportId: String!) {
  reportDatapoint(reportId: $reportId) {
    id
    schemaVersion
  }
}
"""

# Read-only introspection probe for the deploy-order preflight. Deliberately NOT
# `resolveDataSource` (a mutation that seeds a junk data_sources row on miss).
_PREFLIGHT_SOURCE_ATTRIBUTION = """
query PreflightSourceAttribution {
  __type(name: "UpsertReportDatapointsInput") {
    inputFields { name }
  }
}
"""

# ── Situation analysis ─────────────────────────────────────────────

_GET_SITUATION_ANALYSIS = """
query SituationAnalysis(
  $countryLocationId: String!,
  $windowKind: String,
  $windowStart: DateTime,
  $schemaVersion: String,
) {
  situationAnalysis(
    countryLocationId: $countryLocationId,
    windowKind: $windowKind,
    windowStart: $windowStart,
    schemaVersion: $schemaVersion,
  ) {
    data
    generatedAt
    windowKind
    windowStart
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
    estimatedCurrentTotals {
      displacement { total stock flowsSince t0 flowCount }
      returns { total stock flowsSince t0 flowCount }
    }
  }
}
"""

_GET_PIPELINE_COUNTRIES = """
query PipelineCountriesForSituation {
  pipelineCountries { name iso3 pcode bbox }
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
# lookups. Situation-analysis needs the reverse - a specific country
# location by name - so we reuse that resolver by passing name only.

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
      request; retrying just amplifies the damage - see
      clear-pipeline's populationDisplaced incident).
    - 5xx / connection errors → exponential backoff, up to
      ``retries`` attempts, then re-raise the last error.
    - ``errors`` in the JSON body → treated as a hard failure via
      ``RuntimeError`` (retryable - sometimes reflects transient
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
                errs = result["errors"]
                err_text = str(errs)
                # A schema/version mismatch — e.g. the signal-drain endpoints from
                # clear-api PR #127 not yet deployed — is PERMANENT, not transient.
                # Raise a clear, non-retryable error instead of retrying every
                # sensor tick with a generic message.
                if any(m in err_text for m in ("Cannot query field", "Unknown argument", "Unknown type")):
                    raise ClearApiError(
                        "clear-api schema mismatch — is clear-api PR #127 (signal/crisis/"
                        f"translation drain + eventsPendingAlert) deployed? {err_text[:300]}"
                    )
                logger.error("clear-api GraphQL errors: %s", errs)
                raise RuntimeError(f"clear-api GraphQL errors: {errs}")

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
# Public API - one function per operation
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


def resolve_data_source(*, name: str, homepage: str | None = None) -> str | None:
    """Resolve an organisation/source name to a ``data_sources`` id via
    clear-api's ``resolveDataSource`` mutation, creating an ungraded
    ``organisation`` row on miss.

    Used for both a figure's cited source (``source_name`` -> ``source_id``)
    and a report's publisher (ReliefWeb ``report.source`` -> the report's
    ``sourceId``). Returns the id, or ``None`` on empty input. See ADR-0004.
    """
    if not name or not name.strip():
        return None
    data = _execute(
        _RESOLVE_DATA_SOURCE,
        {"name": name.strip(), "homepage": homepage},
    )
    return data.get("resolveDataSource")


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
    source_id: str | None = None,
) -> dict[str, Any]:
    """Replace the ``report_datapoints`` row for ``report_id``.

    The `data` blob follows the Pydantic sub-schema layout defined in
    ``datapoints_schemas.py`` - one top-level key per domain, each
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
        "sourceId": source_id,
    }
    result = _execute(_UPSERT_REPORT_DATAPOINTS, {"input": payload})
    return result["upsertReportDatapoints"]


def has_aggregated_datapoints(
    schema_version: str, *, country_location_id: str | None = None,
) -> bool:
    """Cheap existence check - is there at least one current
    aggregated_datapoints row for this schema version (and, when
    ``country_location_id`` is given, for THAT country)?

    Used by the aggregation asset to distinguish a first-run backfill
    (needs a wide lookback window to catch existing history) from a
    routine weekly refresh (narrow window is enough). Scoping it to the
    country makes a newly-onboarded country's first run use the initial
    window even after other countries are already established.
    """
    data = _execute(
        _HAS_AGGREGATED_DATAPOINTS,
        {"schemaVersion": schema_version, "countryLocationId": country_location_id},
    )
    return bool(data.get("hasAggregatedDatapoints"))


def report_datapoints_exist(report_id: str, *, schema_version: str) -> bool:
    """True only when this report has an extracted ``report_datapoints`` row AT
    ``schema_version``.

    Lets the datapoint asset skip the 6 LLM extraction calls for a report a
    prior run already finished - but the version must match, or a schema bump
    could never re-extract: a v1 row would count as "done" under v2 forever,
    leaving the v2 aggregation buckets empty and the situation snapshots null.
    (`evals/assets.py` already keys its cache on schema_version the same way.)

    The DB is the source of truth on purpose - the S3 debug snapshot is written
    BEFORE the upsert, so it can't confirm the write actually landed.
    """
    data = _execute(_REPORT_DATAPOINT_EXISTS, {"reportId": report_id})
    row = data.get("reportDatapoint")
    return bool(row) and row.get("schemaVersion") == schema_version


def supports_source_attribution() -> bool:
    """True when the deployed clear-api understands source attribution — i.e. its
    ``UpsertReportDatapointsInput`` exposes ``sourceId`` (clear-api PR #110).

    A read-only introspection probe, NOT ``resolve_data_source`` (a mutation that
    would seed a junk ``data_sources`` row on every check). Used as a deploy-order
    preflight: schema v2 sends ``sourceId`` unconditionally, so running against an
    undeployed clear-api would 400 every report AFTER paying for its 6 LLM calls
    and finish the run green with zero data.
    """
    data = _execute(_PREFLIGHT_SOURCE_ATTRIBUTION)
    type_ = data.get("__type") or {}
    fields = type_.get("inputFields") or []
    return any(f.get("name") == "sourceId" for f in fields)


def refresh_aggregated_datapoints(
    *,
    from_iso: str,
    to_iso: str,
    schema_version: str,
    country_location_id: str | None = None,
) -> dict[str, Any]:
    """Trigger clear-api's four-tier aggregation refresh for every
    report whose ``reportingPeriodEnd`` falls in ``[from_iso, to_iso]``.
    When ``country_location_id`` is given the refresh is SCOPED to that
    country's subtree, so a per-country partition run recomputes only its
    own buckets instead of a redundant global pass.

    Returns the server-side summary: ``{ computedBuckets,
    supersededBuckets, schemaVersion }``. clear-api walks the
    hierarchy (A2 → A1 → A0) internally so a single call refreshes
    weekly-A2, monthly-A1, yearly-country, and all-time-country
    tiers atomically per bucket.
    """
    data = _execute(
        _REFRESH_AGGREGATED_DATAPOINTS,
        {
            "from": from_iso,
            "to": to_iso,
            "schemaVersion": schema_version,
            "countryLocationId": country_location_id,
        },
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
    window_kind: str,
    window_start: str,
    schema_version: str | None = None,
) -> dict[str, Any] | None:
    """Fetch the current situation-analysis snapshot for one bucket.

    Buckets are keyed (country, window_kind, window_start), so both are
    required here - clear-api only derives a start for the yearly kind and
    rejects a finer kind without one. Used by the generator to read the
    PRECEDING period's snapshot so it can diff against it for the "what
    changed" notes. Returns None when that bucket has no snapshot.
    """
    data = _execute(
        _GET_SITUATION_ANALYSIS,
        {
            "countryLocationId": country_location_id,
            "windowKind": window_kind,
            "windowStart": window_start,
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
    for. Each row is ``{name, iso3, bbox}`` - ``iso3`` scopes external-API
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
# Postgres is reached over an SSH tunnel - so the cost is dominated by BYTES
# moved (payload in + the unchanged-guard reading open rows + reading the result
# back), not row count. A fixed row count is wrong: 50 humanitarian-needs blobs
# (~100 KB each, finely disaggregated PIN) is ~5 MB/chunk and times out, while 50
# tiny funding blobs is nothing. So chunk by cumulative payload BYTES, with a row
# cap as a backstop. Tune with LOCATION_METADATA_UPSERT_MAX_BYTES /
# LOCATION_METADATA_UPSERT_CHUNK.
#
# Read lazily (not at import): every defs/ module calls load_dotenv AFTER
# importing this provider, so import-time os.environ.get would miss .env - the
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
        logger.warning("%s=%r is not a positive integer - using default %d", name, raw, default)
        return default


def _upsert_max_bytes() -> int:
    return _int_env("LOCATION_METADATA_UPSERT_MAX_BYTES", _DEFAULT_UPSERT_MAX_BYTES)


def _upsert_max_rows() -> int:
    return _int_env("LOCATION_METADATA_UPSERT_CHUNK", _DEFAULT_UPSERT_MAX_ROWS)


def _size_chunks(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Split rows into chunks bounded by cumulative byte size (and a row cap). A
    single row larger than the byte cap still goes in its own chunk - we never
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


def resolve_country_location_id(
    country_name: str, *, pcode: str | None = None,
) -> str | None:
    """Reverse-lookup a country's admin-0 `locations.id`. Resolves by PCODE first
    (the strong, name-independent key — `resolveKnowledgebaseLocation` prefers
    pcode over name), falling back to the name at admin_level=0. Prefer passing
    the pcode: the backfilled A0 name is often the long official form
    ("Venezuela (Bolivarian Republic of)") that an exact-name lookup misses."""
    return resolve_location(pcode=pcode, name=country_name, admin_level=0)


def resolve_country_location_id_by_iso3(iso3: str) -> str | None:
    """A country's admin-0 `locations.id` from its ISO3 — the reliefweb partition
    key. Looks the country up in `pipelineCountries` (by iso3) for its pcode +
    name, then resolves by PCODE (name fallback). Returns None when the iso3 isn't
    a configured pipeline country or nothing resolves — the aggregation asset then
    forces the wider initial window rather than silently under-refreshing."""
    row = next(
        (c for c in get_pipeline_countries() if (c.get("iso3") or "").lower() == iso3.lower()),
        None,
    )
    if row is None:
        return None
    return resolve_country_location_id(row["name"], pcode=row.get("pcode"))


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

    Filters mirror `KnowledgebaseFilters` on the GraphQL side -
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
    transaction on the clear-api side - no half-written state on
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


# ═══════════════════════════════════════════════════════════════════════════
# Signal / event / alert / crisis operations (ported from clear-pipeline).
# Ported callers catch GraphQLClientError; it is the same as ClearApiError.
# ═══════════════════════════════════════════════════════════════════════════

GraphQLClientError = ClearApiError

# ─── Mutations ────────────────────────────────────────────────────────────────

CREATE_SIGNAL = """
mutation CreateSignal($input: CreateSignalInput!) {
  createSignal(input: $input) {
    id
    title
    severity
    casualties
    externalId
    publishedAt
    originLocation { id name level ancestorIds }
    destinationLocation { id name level ancestorIds }
    generalLocation { id name level ancestorIds }
    # If the API returned an existing row (idempotent ingest), it may
    # already be linked to an event from a prior run. group_signal uses
    # this to short-circuit — a signal that already has an event must not
    # spawn another one.
    events { id title types severity casualties populationAffected }
  }
}
"""

UPDATE_SIGNAL_SEVERITY = """
mutation UpdateSignalSeverity($id: String!, $severity: Int!) {
  updateSignalSeverity(id: $id, severity: $severity) {
    id
    severity
  }
}
"""

UPDATE_SIGNAL_GEOPARSED_DATA = """
mutation UpdateSignalGeoparsedData($id: String!, $geoparsedData: JSON!) {
  updateSignalGeoparsedData(id: $id, geoparsedData: $geoparsedData) {
    id
  }
}
"""

UPDATE_SIGNAL_LOCATION = """
mutation UpdateSignalLocation($id: String!, $locationId: String!) {
  updateSignalLocation(id: $id, locationId: $locationId) {
    id
  }
}
"""

CREATE_EVENT = """
mutation CreateEvent($input: CreateEventInput!) {
  createEvent(input: $input) {
    id
    title
    types
  }
}
"""

UPDATE_EVENT = """
mutation UpdateEvent($id: String!, $input: UpdateEventInput!) {
  updateEvent(id: $id, input: $input) {
    id
    title
  }
}
"""

CREATE_ALERT = """
mutation CreateAlert($input: CreateAlertInput!) {
  createAlert(input: $input) {
    id
    status
  }
}
"""

ESCALATE_EVENT = """
mutation EscalateEvent($eventId: String!, $userId: String!) {
  escalateEvent(eventId: $eventId, userId: $userId) {
    id
    isCrisis
    validFrom
    validTo
  }
}
"""

NOTIFY_ALERT_SUBSCRIBERS = """
mutation NotifyAlertSubscribers($input: AlertNotifyInput!) {
  notifyAlertSubscribers(input: $input)
}
"""

NOTIFY_ALERT_DIGEST = """
mutation NotifyAlertDigest($input: AlertDigestInput!) {
  notifyAlertDigest(input: $input)
}
"""

UPDATE_LOCATION_GEOMETRY = """
mutation UpdateLocationGeometry($id: String!, $geometry: GeoJSON!) {
  updateLocationGeometry(id: $id, geometry: $geometry) { id }
}
"""

UPDATE_LOCATION_POPULATION = """
mutation UpdateLocationPopulation($id: String!, $population: String!) {
  updateLocationPopulation(id: $id, population: $population) { id population }
}
"""

UPDATE_LOCATION = """
mutation UpdateLocation($id: String!, $input: UpdateLocationInput!) {
  updateLocation(id: $id, input: $input) { id pCode name }
}
"""

CREATE_LOCATION = """
mutation CreateLocation($input: CreateLocationInput!) {
  createLocation(input: $input) { id name level pCode }
}
"""

ARCHIVE_STALE_ALERTS = """
mutation ArchiveStaleAlerts($olderThanDays: Int) {
  archiveStaleAlerts(olderThanDays: $olderThanDays) { alertsArchived }
}
"""

UPDATE_CRISIS_POPULATION = """
mutation UpdateCrisisPopulation($id: String!, $input: UpdateCrisisPopulationInput!) {
  updateCrisisPopulation(id: $id, input: $input) {
    id
    populationAffected
    populationInArea
  }
}
"""

SET_CRISIS_NEEDS_ANALYSIS = """
mutation SetCrisisNeedsAnalysis(
  $id: String!,
  $generalSummary: [String!]!,
  $sector: JSON!,
) {
  setCrisisNeedsAnalysis(
    id: $id,
    generalSummary: $generalSummary,
    sector: $sector,
  ) {
    id
  }
}
"""

GET_LOCATION_WITH_GEOMETRY = """
query LocationWithGeometry($id: String!) {
  location(id: $id) {
    id
    name
    level
    population
    geometry
    parent { id name level }
  }
}
"""

GET_LOCATIONS_BY_LEVEL = """
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

GET_EVENT_FOR_CRISIS = """
query EventForCrisis($id: String!) {
  event(id: $id) {
    id
    title
    description
    types
    severity
    populationAffected
    originLocation { name metadata { type data } }
    destinationLocation { name metadata { type data } }
    generalLocation { name metadata { type data } }
  }
}
"""

GET_EVENT_WITH_SIGNALS = """
query EventWithSignals($id: String!) {
  event(id: $id) {
    id
    title
    description
    types
    severity
    casualties
    populationAffected
    signals {
      id
      title
      description
      severity
      casualties
      publishedAt
      source { id name type }
    }
  }
}
"""

GET_LOCATION_METADATA = """
query LocationMetadata($locationId: String!, $type: String) {
  locationMetadata(locationId: $locationId, type: $type) {
    id
    type
    data
    validFrom
    validTo
  }
}
"""

UPSERT_LOCATION_METADATA = """
mutation UpsertLocationMetadata($input: UpsertLocationMetadataInput!) {
  upsertLocationMetadata(input: $input) { id type data updatedAt }
}
"""

UPSERT_LOCATION_METADATA_BATCH = """
mutation UpsertLocationMetadataBatch($inputs: [UpsertLocationMetadataInput!]!) {
  upsertLocationMetadataBatch(inputs: $inputs) { id type }
}
"""

ALL_LOCATION_METADATA = """
query AllLocationMetadata($type: String!) {
  allLocationMetadata(type: $type) {
    id
    type
    data
    location { id name pCode }
  }
}
"""

GET_RECENT_ALERTS = """
query RecentAlerts {
  alerts(status: published) {
    id
    status
    event {
      id
      firstSignalCreatedAt
    }
  }
}
"""

# ─── Queries ──────────────────────────────────────────────────────────────────

GET_SIGNAL = """
query Signal($id: String!) {
  signal(id: $id) {
    id
    title
    severity
    casualties
    externalId
    publishedAt
    # Resolved location objects with the fields resolve_signal_admin2
    # needs (id / level / ancestorIds). The manual-signal pipeline uses
    # this to look up the location the user picked in the UI so
    # event_grouping_v2 can key on the correct admin-2 district instead
    # of falling through to isolated-event behaviour.
    originLocation { id name level ancestorIds }
    destinationLocation { id name level ancestorIds }
    generalLocation { id name level ancestorIds }
    # Same short-circuit rationale as CREATE_SIGNAL: if the signal is
    # already linked to an event from a prior run, group_signal
    # returns that event instead of re-clustering.
    events { id title types severity casualties populationAffected }
  }
}
"""

GET_LATEST_SIGNAL = """
query LatestSignal {
  signals {
    id
    publishedAt
  }
}
"""

GET_EVENTS = """
query Events {
  events {
    id
    title
    description
    types
    severity
    casualties
    populationAffected
    rank
    validFrom
    validTo
    firstSignalCreatedAt
    lastSignalCreatedAt
    originLocation { id name level ancestorIds }
    destinationLocation { id name level ancestorIds }
    generalLocation { id name level ancestorIds }
    alerts { id status }
  }
}
"""

GET_LOCATIONS = """
query Locations {
  locations {
    id
    name
    level
    parent { id name }
  }
}
"""

GET_DATA_SOURCES = """
query DataSources {
  dataSources {
    id
    name
  }
}
"""

GET_DISASTER_TYPES = """
query DisasterTypes {
  disasterTypes {
    id
    disasterType
    disasterClass
    glideNumber
    level1
    level2
    idType
  }
}
"""


# GraphQLClientError is re-exported from providers.clear_api (ClearApiError) — see
# the header import. The shared _execute raises it on 4xx (non-retryable).


# ─── Dagster drain (clear-api #467 — durable status markers) ──────────────────

# Signals awaiting downstream processing (status = NEW), oldest-first. The
# selection mirrors CREATE_SIGNAL so the drain feeds the SAME classify→group→
# alert code the Celery path uses. Raw payload stays in S3 (rawS3Key).
PENDING_SIGNALS = """
query PendingSignals($first: Int, $source: String) {
  pendingSignals(first: $first, source: $source) {
    id
    externalId
    title
    description
    severity
    casualties
    publishedAt
    status
    rawS3Key
    source { id name }
    originLocation { id name level ancestorIds }
    destinationLocation { id name level ancestorIds }
    generalLocation { id name level ancestorIds }
    events { id title types severity casualties populationAffected }
  }
}
"""

MARK_SIGNALS_PROCESSED = """
mutation MarkSignalsProcessed($ids: [String!]!, $status: SignalStatus) {
  markSignalsProcessed(ids: $ids, status: $status)
}
"""


# ─── Public API ───────────────────────────────────────────────────────────────


def create_signal(input_data: dict) -> dict:
    result = _execute(CREATE_SIGNAL, {"input": input_data})
    return result["createSignal"]


def pending_signals(first: int = 100, source: str | None = None) -> list[dict]:
    """Signals awaiting downstream processing (status = NEW), oldest-first — the
    Dagster event-driven drain (clear-api #467). ``source`` filters by DataSource
    name (e.g. "dataminr"). Returns [] when nothing is pending."""
    variables: dict = {"first": first}
    if source is not None:
        variables["source"] = source
    result = _execute(PENDING_SIGNALS, variables)
    return result["pendingSignals"]


def mark_signals_processed(ids: list[str], status: str = "PROCESSED") -> int:
    """Mark signals done for the drain — PROCESSED (default) or FAILED. Returns
    the number of rows updated. Idempotent (clear-api #467)."""
    if not ids:
        return 0
    result = _execute(MARK_SIGNALS_PROCESSED, {"ids": ids, "status": status})
    return result["markSignalsProcessed"]


def get_signal(signal_id: str) -> dict | None:
    """Fetch an existing signal with its resolved origin/general/destination
    locations. Used by the manual-signal pipeline path where the signal
    is created API-side (with the location the user picked in the UI)
    before the Celery task runs — event_grouping_v2 needs the location
    to key on the admin-2 district."""
    result = _execute(GET_SIGNAL, {"id": signal_id})
    return result.get("signal")


def update_signal_severity(signal_id: str, severity: int) -> dict:
    """Update a signal's severity score (1-5)."""
    result = _execute(UPDATE_SIGNAL_SEVERITY, {"id": signal_id, "severity": severity})
    return result["updateSignalSeverity"]


def update_signal_geoparsed_data(signal_id: str, geoparsed_data: dict) -> dict:
    """Attach the geoparser's structured result to an existing signal.
    Used by the manual-signal pipeline path, where the signal is created
    before the geoparser has run."""
    result = _execute(
        UPDATE_SIGNAL_GEOPARSED_DATA,
        {"id": signal_id, "geoparsedData": geoparsed_data},
    )
    return result["updateSignalGeoparsedData"]


def update_signal_location(signal_id: str, location_id: str) -> dict:
    """Set an existing signal's `generalLocation`. Used by the manual-signal
    pipeline path when the user didn't pick a location and the geoparser
    resolved a landmark — we promote the landmark to an L4 and wire the
    signal to it so downstream event grouping can key on the correct
    admin-2 district."""
    result = _execute(
        UPDATE_SIGNAL_LOCATION,
        {"id": signal_id, "locationId": location_id},
    )
    return result["updateSignalLocation"]


def create_event(input_data: dict) -> dict:
    # retries=1: createEvent has no idempotency key (unlike createSignal's
    # (sourceId, externalId)), so a retry after a timeout that actually committed
    # would mint a duplicate event. Fail fast instead — the drain re-processes the
    # signal on the next run, and group_signal's "already linked" short-circuit
    # dedups once the link is visible.
    result = _execute(CREATE_EVENT, {"input": input_data}, retries=1)
    return result["createEvent"]


def update_event(event_id: str, input_data: dict) -> dict:
    result = _execute(UPDATE_EVENT, {"id": event_id, "input": input_data})
    return result["updateEvent"]


def escalate_event(event_id: str, user_id: str) -> dict:
    """Escalate an event to an alert and record the user escalation."""
    result = _execute(ESCALATE_EVENT, {"eventId": event_id, "userId": user_id})
    return result["escalateEvent"]


def create_alert(input_data: dict) -> dict:
    result = _execute(CREATE_ALERT, {"input": input_data})
    return result["createAlert"]


def notify_alert_subscribers(alert_id: str) -> int:
    """Notify immediate subscribers of an alert. Returns notification count."""
    result = _execute(NOTIFY_ALERT_SUBSCRIBERS, {"input": {"alertId": alert_id}})
    return result["notifyAlertSubscribers"]


def notify_alert_digest(alert_ids: list[str], frequency: str) -> int:
    """Send digest notifications for alerts. Returns notification count."""
    result = _execute(NOTIFY_ALERT_DIGEST, {"input": {"alertIds": alert_ids, "frequency": frequency}})
    return result["notifyAlertDigest"]


def get_published_alerts() -> list[dict]:
    """Get all published alerts."""
    result = _execute(GET_RECENT_ALERTS)
    return result.get("alerts", [])


def get_latest_signal_timestamp() -> str | None:
    """Get the publishedAt of the most recent signal, or None if no signals exist."""
    result = _execute(GET_LATEST_SIGNAL)
    signals = result.get("signals", [])
    if not signals:
        return None
    # Find the most recent by publishedAt
    return max(signals, key=lambda s: s["publishedAt"])["publishedAt"]


def get_events() -> list[dict]:
    result = _execute(GET_EVENTS)
    return result.get("events", [])


def get_locations() -> list[dict]:
    result = _execute(GET_LOCATIONS)
    return result.get("locations", [])


def get_data_sources() -> list[dict]:
    result = _execute(GET_DATA_SOURCES)
    return result.get("dataSources", [])


def get_disaster_types() -> list[dict]:
    result = _execute(GET_DISASTER_TYPES)
    return result.get("disasterTypes", [])


_source_id_cache: dict[str, str] = {}


def get_source_id_by_name(name: str) -> str:
    """Resolve a clear-api ``DataSource`` id by name (cached per process).

    Raises if the source row is absent — every connector's ingest depends on
    the matching ``data_sources`` row existing (same contract the Celery
    ``_get_<source>_source_id`` helpers enforced)."""
    cached = _source_id_cache.get(name)
    if cached is not None:
        return cached
    for src in get_data_sources():
        if src["name"] == name:
            _source_id_cache[name] = src["id"]
            return src["id"]
    raise RuntimeError(
        f"Data source '{name}' not found in CLEAR API. "
        "Ensure it exists in the data_sources table."
    )


def get_dataminr_source_id() -> str:
    """Find the dataminr data source ID from the CLEAR API."""
    return get_source_id_by_name(os.environ.get("DATAMINR_SOURCE_NAME", "dataminr"))


# ─── Population / Geometry helpers ────────────────────────────────────────────


def get_location_with_geometry(location_id: str) -> dict | None:
    result = _execute(GET_LOCATION_WITH_GEOMETRY, {"id": location_id})
    return result.get("location")


def update_location_geometry(location_id: str, geometry: dict) -> dict:
    result = _execute(
        UPDATE_LOCATION_GEOMETRY,
        {"id": location_id, "geometry": geometry},
    )
    return result["updateLocationGeometry"]


def update_location_population(location_id: str, population: int) -> dict:
    result = _execute(
        UPDATE_LOCATION_POPULATION,
        {"id": location_id, "population": str(population)},
    )
    return result["updateLocationPopulation"]


def update_location(location_id: str, **fields) -> dict:
    """Update a location's scalar fields (pCode, name, geoId, osmId, level, parentId).
    Only fields passed are changed."""
    result = _execute(
        UPDATE_LOCATION,
        {"id": location_id, "input": fields},
    )
    return result["updateLocation"]


def create_location(name: str, level: int, **fields) -> dict:
    """Create a new location (geometry defaults to POINT(0 0); set via
    update_location_geometry afterwards)."""
    payload = {"name": name, "level": level, **fields}
    result = _execute(CREATE_LOCATION, {"input": payload})
    return result["createLocation"]


def archive_stale_alerts(older_than_days: int = 14) -> int:
    """Archive alerts whose event.lastSignalCreatedAt is older than N days.
    Returns the number of rows affected."""
    result = _execute(ARCHIVE_STALE_ALERTS, {"olderThanDays": older_than_days})
    return int(result["archiveStaleAlerts"]["alertsArchived"])


def set_crisis_needs_analysis(
    crisis_id: str,
    *,
    general_summary: list[str],
    sector: dict,
) -> dict:
    """Merge an LLM-generated SAF needs analysis into the crisis's `needs`
    JSONB. Server-side JSONB `||` merge overwrites `generalSummary` and
    `sector` keys only — other keys on `needs` stay intact.

    `general_summary` is the 4-bullet array produced by the
    `CrisisNeedsAnalysis` Pydantic model; the GraphQL mutation accepts
    `[String!]!`."""
    result = _execute(
        SET_CRISIS_NEEDS_ANALYSIS,
        {
            "id": crisis_id,
            "generalSummary": general_summary,
            "sector": sector,
        },
    )
    return result["setCrisisNeedsAnalysis"]


def update_crisis_population(
    crisis_id: str,
    population_affected: int | None = None,
    population_in_area: int | None = None,
    title: str | None = None,
    summary: str | None = None,
    scenarios: dict | None = None,
) -> dict:
    input_data: dict = {}
    if population_affected is not None:
        input_data["populationAffected"] = str(population_affected)
    if population_in_area is not None:
        input_data["populationInArea"] = str(population_in_area)
    if title is not None:
        input_data["title"] = title
    if summary is not None:
        input_data["summary"] = summary
    if scenarios is not None:
        input_data["scenarios"] = scenarios
    result = _execute(
        UPDATE_CRISIS_POPULATION,
        {"id": crisis_id, "input": input_data},
    )
    return result["updateCrisisPopulation"]


def get_event_for_crisis(event_id: str) -> dict | None:
    result = _execute(GET_EVENT_FOR_CRISIS, {"id": event_id})
    return result.get("event")


def get_event_with_signals(event_id: str) -> dict | None:
    """Fetch an event plus all its linked signals. Used by the rewrite pass
    of the new grouping algorithm."""
    result = _execute(GET_EVENT_WITH_SIGNALS, {"id": event_id})
    return result.get("event")


def get_location_metadata(location_id: str, type_: str | None = None) -> list[dict]:
    """Return all locationMetadata rows for a location, optionally filtered by type."""
    variables: dict = {"locationId": location_id}
    if type_ is not None:
        variables["type"] = type_
    result = _execute(GET_LOCATION_METADATA, variables)
    return result.get("locationMetadata", []) or []


def upsert_location_metadata(location_id: str, type_: str, data: dict) -> dict:
    """Create or update a location's metadata entry for a given type."""
    result = _execute(
        UPSERT_LOCATION_METADATA,
        {"input": {"locationId": location_id, "type": type_, "data": data}},
    )
    return result["upsertLocationMetadata"]


def get_all_location_metadata(type_: str) -> list[dict]:
    """Return every locationMetadata row of a given type across all locations."""
    result = _execute(ALL_LOCATION_METADATA, {"type": type_})
    return result.get("allLocationMetadata", []) or []


# ─── Nominatim geocoder cache ─────────────────────────────────────────────

GET_NOMINATIM_CACHE_ENTRY = """
query NominatimCacheEntry($queryHash: String!) {
  nominatimCacheEntry(queryHash: $queryHash) {
    id
    queryHash
    query
    endpoint
    responseJson
    status
    fetchedAt
    expiresAt
  }
}
"""

UPSERT_NOMINATIM_CACHE = """
mutation UpsertNominatimCache($input: UpsertNominatimCacheInput!) {
  upsertNominatimCache(input: $input) {
    id
    queryHash
    status
    expiresAt
  }
}
"""


def get_nominatim_cache_entry(query_hash: str) -> dict | None:
    """Read a cached Nominatim response by query hash. Returns None when the
    entry is missing or expired (the API filters expired rows server-side)."""
    result = _execute(GET_NOMINATIM_CACHE_ENTRY, {"queryHash": query_hash})
    return result.get("nominatimCacheEntry")


def upsert_nominatim_cache(
    *,
    query_hash: str,
    query: str,
    endpoint: str,
    response_json: dict | list,
    status: str,
    ttl_seconds: int,
) -> dict:
    """Write a Nominatim response to the cache. `status` is one of
    'ok' / 'no_result' / 'error'. The server computes expires_at from
    ttl_seconds."""
    result = _execute(
        UPSERT_NOMINATIM_CACHE,
        {
            "input": {
                "queryHash": query_hash,
                "query": query,
                "endpoint": endpoint,
                "responseJson": response_json,
                "status": status,
                "ttlSeconds": ttl_seconds,
            }
        },
    )
    return result["upsertNominatimCache"]


# ─── Geoparser L4 promotion ────────────────────────────────────────────────

FIND_OR_CREATE_LANDMARK_L4 = """
mutation FindOrCreateLandmarkL4($input: FindOrCreateLandmarkL4Input!) {
  findOrCreateLandmarkL4(input: $input) {
    locationId
    reused
    pointType
    abortedReason
  }
}
"""


def find_or_create_landmark_l4(
    *,
    name: str,
    lat: float,
    lng: float,
    kind: str,
    source_lat: float | None = None,
    source_lng: float | None = None,
) -> dict:
    """Promote a geoparsed candidate into a reusable L4 location.

    Returns the resolver result:
      { locationId: str|None, reused: bool, pointType: str|None,
        abortedReason: "different_a2"|None }

    When abortedReason is set, the caller should fall back to source coords
    (no L4 promotion) — the candidate's A2 didn't match the source's A2 and
    promoting would mis-attribute the signal.
    """
    payload: dict = {
        "name": name,
        "lat": lat,
        "lng": lng,
        "kind": kind,
    }
    if source_lat is not None:
        payload["sourceLat"] = source_lat
    if source_lng is not None:
        payload["sourceLng"] = source_lng
    result = _execute(FIND_OR_CREATE_LANDMARK_L4, {"input": payload})
    return result["findOrCreateLandmarkL4"]


RESOLVE_GAZETTEER_LOCATION = """
query ResolveGazetteerLocation($name: String!, $countryCode: String, $minSimilarity: Float) {
  resolveGazetteerLocation(name: $name, countryCode: $countryCode, minSimilarity: $minSimilarity) {
    geonamesId
    name
    latitude
    longitude
    featureClass
    featureCode
    countryCode
    population
    score
    exact
  }
}
"""


def resolve_gazetteer_location(
    name: str,
    *,
    country_code: str | None = None,
    min_similarity: float | None = None,
) -> dict | None:
    """Resolve a place name against clear-api's offline GeoNames gazetteer.

    Returns the GazetteerHit dict (latitude/longitude, featureClass, score,
    exact, …) or None when nothing matches. The hybrid geoparser's first,
    offline tier; LocationIQ is the fallback for landmarks/POIs it lacks.
    """
    variables: dict = {"name": name}
    if country_code is not None:
        variables["countryCode"] = country_code
    if min_similarity is not None:
        variables["minSimilarity"] = min_similarity
    # retries=1 (single attempt, no backoff): this tier is best-effort with a
    # LocationIQ fallback, so a transient clear-api error must not stall the
    # Celery worker through ~6s of retry sleeps before the fallback runs.
    data = _execute(RESOLVE_GAZETTEER_LOCATION, variables, retries=1)
    return data.get("resolveGazetteerLocation")


GET_CRISIS_CANONICAL = """
query CrisisCanonical($id: String!) {
  crisis(id: $id) {
    id
    title
    summary
    scenarios
    needs
  }
}
"""

GET_EVENT_CANONICAL = """
query EventCanonical($id: String!) {
  event(id: $id) {
    id
    title
    description
  }
}
"""

GET_LOCATION_CANONICAL = """
query LocationCanonical($id: String!) {
  location(id: $id) {
    id
    name
  }
}
"""


def get_crisis_canonical(crisis_id: str) -> dict | None:
    """Fetch only the four translatable fields of a crisis. Used by the
    translation step in tasks/crisis.py to feed Claude the current
    canonical English text after all in-task writes have committed.

    Relies on the pipeline user's language being 'en' so the resolver
    overlay short-circuits and returns canonical values — if that ever
    changes, swap to an explicit `Accept-Language: en` header in
    `_execute`.
    """
    result = _execute(GET_CRISIS_CANONICAL, {"id": crisis_id})
    return result.get("crisis")


def get_event_canonical(event_id: str) -> dict | None:
    """Fetch the two translatable fields of an event (title,
    description). Same pipeline-language invariant as
    get_crisis_canonical above."""
    result = _execute(GET_EVENT_CANONICAL, {"id": event_id})
    return result.get("event")


def get_location_canonical(location_id: str) -> dict | None:
    """Fetch the one translatable field of a location (name). Same
    pipeline-language invariant as get_crisis_canonical above."""
    result = _execute(GET_LOCATION_CANONICAL, {"id": location_id})
    return result.get("location")


# ─── Translations ─────────────────────────────────────────────────────────────

GET_TRANSLATIONS = """
query Translations($entityType: String!, $entityId: String!) {
  translations(entityType: $entityType, entityId: $entityId) {
    locale
    data
    sourceHashes
  }
}
"""

UPSERT_TRANSLATIONS = """
mutation UpsertTranslations($input: UpsertTranslationsInput!) {
  upsertTranslations(input: $input) {
    entityType
    entityId
    locales
  }
}
"""

GET_ENTITIES_MISSING_TRANSLATION = """
query EntitiesMissingTranslation($entityType: String!, $locale: String!) {
  entitiesMissingTranslation(entityType: $entityType, locale: $locale)
}
"""


def get_translations(entity_type: str, entity_id: str) -> list[dict]:
    """Fetch every translation row currently stored for the entity.
    Returns an empty list when nothing has been written yet (cold start).
    Admin/pipeline auth required at the API.
    """
    result = _execute(
        GET_TRANSLATIONS,
        {"entityType": entity_type, "entityId": entity_id},
    )
    return result.get("translations") or []


def get_entities_missing_translation(
    entity_type: str,
    locale: str,
) -> list[str]:
    """IDs of entities (of `entity_type`) that have no translation row
    for `locale`. Lets the backfill driver dispatch only entities the
    worker would actually translate, skipping the noisy "all current"
    path inside translate_and_upsert. Stale rows (row exists with
    out-of-date hashes) are NOT returned — they're rare and handled
    by per-entity enrichment hooks.
    """
    result = _execute(
        GET_ENTITIES_MISSING_TRANSLATION,
        {"entityType": entity_type, "locale": locale},
    )
    return result.get("entitiesMissingTranslation") or []


# ─── Ground intel (WhatsApp signal pipeline) ──────────────────────────────
# Staging-tier surface owned by clear-api (groundSources / groundThreads /
# groundMessages). Message text arrives already redacted (phone numbers
# stripped at persistence); senderRef is pseudonymous.

GROUND_MESSAGES_FOR_CLASSIFICATION = """
query GroundMessagesForClassification($groundSourceId: String!, $limit: Int) {
  groundMessagesForClassification(groundSourceId: $groundSourceId, limit: $limit) {
    id
    text
    sentAt
    senderRef
    hasMedia
    classification
    threadId
  }
}
"""

UPSERT_GROUND_MESSAGE_CLASSIFICATIONS = """
mutation UpsertGroundMessageClassifications(
  $inputs: [GroundMessageClassificationInput!]!
) {
  upsertGroundMessageClassifications(inputs: $inputs)
}
"""


def ground_messages_for_classification(
    ground_source_id: str,
    limit: int | None = None,
) -> list[dict]:
    """Fetch a ground source's messages awaiting classification/threading.

    The server scopes the result to the source and orders by sentAt; rows
    carry `classification` / `threadId` as null until this pipeline fills
    them in.
    """
    variables: dict = {"groundSourceId": ground_source_id}
    if limit is not None:
        variables["limit"] = limit
    result = _execute(GROUND_MESSAGES_FOR_CLASSIFICATION, variables)
    return result.get("groundMessagesForClassification") or []


def upsert_ground_message_classifications(inputs: list[dict]) -> int:
    """Write classifications back to clear-api. Each input row must shape as
    {messageId, classification, uncertaintyMarker} — uncertaintyMarker may
    be None when the contributor attached no uncertainty tag. The server
    returns a scalar count of upserted rows."""
    if not inputs:
        return 0
    result = _execute(UPSERT_GROUND_MESSAGE_CLASSIFICATIONS, {"inputs": inputs})
    return result.get("upsertGroundMessageClassifications") or 0


GROUND_THREADS_FOR_SOURCE = """
query GroundThreadsForSource($groundSourceId: String!, $states: [String!]) {
  groundThreadsForSource(groundSourceId: $groundSourceId, states: $states) {
    id
    title
    lifecycleState
    reviewState
    messageIds
  }
}
"""


def ground_threads_for_source(
    ground_source_id: str,
    states: list[str] | None = None,
) -> list[dict]:
    """Fetch a source's existing incident threads.

    Used before the threading stage so a later run can APPEND to a thread
    created by an earlier run (a correction or retraction that arrives
    after its incident was threaded) instead of minting an orphan thread.
    `states` optionally filters by lifecycle state.
    """
    variables: dict = {"groundSourceId": ground_source_id}
    if states is not None:
        variables["states"] = states
    result = _execute(GROUND_THREADS_FOR_SOURCE, variables)
    return result.get("groundThreadsForSource") or []


UPSERT_GROUND_THREADS = """
mutation UpsertGroundThreads($inputs: [GroundThreadUpsertInput!]!) {
  upsertGroundThreads(inputs: $inputs)
}
"""


def upsert_ground_threads(inputs: list[dict]) -> list[str | None]:
    """Create/update incident threads and attach their messages. Each input
    row shapes as {groundSourceId, title, lifecycleState, messageIds} plus
    an optional threadId — when threadId is set, the server APPENDS the
    messageIds to that existing (non-promoted) thread and updates its
    lifecycleState/title instead of creating a new thread. Returns thread
    ids index-aligned with `inputs` (entries may be null for rejected rows).
    """
    if not inputs:
        return []
    result = _execute(UPSERT_GROUND_THREADS, {"inputs": inputs})
    return result.get("upsertGroundThreads") or []


def upsert_translations(
    entity_type: str,
    entity_id: str,
    translations: list[dict],
) -> dict:
    """Write/replace per-locale translation rows. Each entry in
    `translations` must shape as:
        {"locale": "ar", "data": {...}, "sourceHashes": {field: "sha256:..."}}

    Mirrors clear-api's UpsertTranslationsInput exactly. The mutation
    runs the per-locale upserts in a single DB transaction so a partial
    failure can't leave the entity with some locales written and others
    missing.
    """
    result = _execute(
        UPSERT_TRANSLATIONS,
        {
            "input": {
                "entityType": entity_type,
                "entityId": entity_id,
                "translations": translations,
            }
        },
    )
    return result["upsertTranslations"]


# ─── Translation queue (the durable stage between entity creation & translate) ─

PENDING_TRANSLATIONS = """
query PendingTranslations($first: Int, $entityType: String, $locale: String) {
  pendingTranslations(first: $first, entityType: $entityType, locale: $locale) {
    entityType
    entityId
    locale
  }
}
"""

ENQUEUE_TRANSLATION = """
mutation EnqueueTranslation($entityType: String!, $entityId: String!, $locale: String!) {
  enqueueTranslation(entityType: $entityType, entityId: $entityId, locale: $locale) {
    entityType
    entityId
    locale
  }
}
"""

MARK_TRANSLATED = """
mutation MarkTranslated($entityType: String!, $entityId: String!, $locale: String!) {
  markTranslated(entityType: $entityType, entityId: $entityId, locale: $locale)
}
"""


def pending_translations(
    first: int = 200, entity_type: str | None = None, locale: str | None = None
) -> list[dict]:
    """Drain the translation queue oldest-first (optionally filtered by
    entityType/locale). Each row is {entityType, entityId, locale}."""
    result = _execute(
        PENDING_TRANSLATIONS,
        {"first": first, "entityType": entity_type, "locale": locale},
    )
    return result.get("pendingTranslations") or []


def enqueue_translation(entity_type: str, entity_id: str, locale: str) -> dict:
    """Enqueue an entity for (re)translation at a locale (idempotent per
    entityType/entityId/locale)."""
    return _execute(
        ENQUEUE_TRANSLATION,
        {"entityType": entity_type, "entityId": entity_id, "locale": locale},
    )["enqueueTranslation"]


def mark_translated(entity_type: str, entity_id: str, locale: str) -> bool:
    """Remove an entity/locale from the queue. ``upsert_translations`` already
    clears the row on write, so this is only needed for explicit drops."""
    return _execute(
        MARK_TRANSLATED,
        {"entityType": entity_type, "entityId": entity_id, "locale": locale},
    )["markTranslated"]


# ─── Events pending alert (the durable stage between grouping & alert) ─────────

EVENTS_PENDING_ALERT = """
query EventsPendingAlert($first: Int, $minSeverity: Int, $maxAgeHours: Int) {
  eventsPendingAlert(first: $first, minSeverity: $minSeverity, maxAgeHours: $maxAgeHours) {
    id
    title
    description
    types
    severity
    casualties
    populationAffected
    validFrom
    validTo
    lastSignalCreatedAt
    originLocation { id name level ancestorIds }
    destinationLocation { id name level ancestorIds }
    generalLocation { id name level ancestorIds }
  }
}
"""


def events_pending_alert(
    first: int = 100, min_severity: int = 4, max_age_hours: int = 48
) -> list[dict]:
    """Events with severity >= ``min_severity``, no alert yet, and a signal within
    the last ``max_age_hours`` (real-world time) — the alert stage's queue. The age
    bound keeps historical backlog / backdated backfill out of alerting; 0 disables it."""
    result = _execute(
        EVENTS_PENDING_ALERT,
        {"first": first, "minSeverity": min_severity, "maxAgeHours": max_age_hours},
    )
    return result.get("eventsPendingAlert") or []
