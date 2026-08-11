"""Country partitioning + S3 layout for the ReliefWeb knowledgebase pipeline.

The whole ReliefWeb chain (ingest → PDF text → chunks → enrich → extract →
aggregate) is partitioned by **country ISO3** so each country ingests,
extracts, and aggregates independently — its own first-run lookback, its own
retries, its own backfills. This module is the single source of truth for:

  - the partition set (``country_partitions``), driven at runtime by clear-api's
    ``pipelineCountries`` via the sync sensor, so onboarding a country in
    clear-api is all it takes to add a partition; and
  - the S3 key scheme, keyed by ``iso3`` (previously a duplicated
    ``COUNTRY_ISO3 = "sdn"`` constant in six modules).

The prefix strings are byte-for-byte what the pre-partition constants produced
for ``iso3="sdn"``, so Sudan's existing S3 artefacts are adopted unchanged (no
migration): the ``sdn`` partition simply finds its history already in place.
"""

import dagster as dg

from clear_context_pipeline.providers import clear_api

# Report format scope, shared across the chain. ReliefWeb's `format` taxonomy
# distinguishes "Situation Report" (the operational sitreps we want) from "Map",
# "News and Press Release", etc. `FORMAT_SLUG` is the S3 path segment; keeping it
# a separate segment from the country means a future format switch won't
# overwrite this archive.
FORMAT_NAME = "Situation Report"
FORMAT_SLUG = "situation-report"

# One partition per country ISO3. DYNAMIC (not static) so the set is data-driven:
# the sync sensor reconciles it against `pipelineCountries` at runtime, and the
# weekly schedule fans out one run per partition. A fresh Dagster instance starts
# with an empty set — the sensor's first tick seeds it (Sudan included).
country_partitions = dg.DynamicPartitionsDefinition(name="reliefweb_country")


def list_pipeline_iso3s() -> list[str]:
    """Lowercased ISO3s of every country clear-api is configured for
    (``pipelineCountries``) — the single source of truth the partition sensor
    reconciles the dynamic partition set against. Runtime call (network); never
    invoked at import time."""
    return [
        c["iso3"].lower()
        for c in clear_api.get_pipeline_countries()
        if c.get("iso3")
    ]


# ── S3 layout, keyed by iso3 ──────────────────────────────────────────────
# Each helper reproduces the exact string the old per-module constant built for
# `sdn`, so keys stay stable. `reports_prefix` keeps its trailing slash; the
# `kb/*` prefixes have none (callers append `/<report_id>/…`), matching the
# originals.

def reports_prefix(iso3: str) -> str:
    return f"reliefweb/reports/{iso3}/{FORMAT_SLUG}/"


def pdf_prefix(iso3: str) -> str:
    return f"reliefweb/pdfs/{iso3}/{FORMAT_SLUG}/"


def pdf_key(iso3: str, report_id: str, filename: str) -> str:
    """Stable S3 key for one report's PDF attachment."""
    return f"{pdf_prefix(iso3)}{report_id}/{filename}"


def text_prefix(iso3: str) -> str:
    return f"reliefweb/kb/text/{iso3}/{FORMAT_SLUG}"


def chunks_prefix(iso3: str) -> str:
    return f"reliefweb/kb/chunks/{iso3}/{FORMAT_SLUG}"


def enriched_prefix(iso3: str) -> str:
    return f"reliefweb/kb/enriched/{iso3}/{FORMAT_SLUG}"


def datapoints_prefix(iso3: str) -> str:
    return f"reliefweb/kb/datapoints/{iso3}/{FORMAT_SLUG}"
