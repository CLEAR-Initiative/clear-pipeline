"""``analysis_numbers_extract`` — in-lane numbers extraction (doc §8, §10).

Per country, hoists the frame's KB-aggregated figures (the yearly
``aggregated_datapoints`` bucket) into the analysis numbers artifact on S3, so
the gap decision knows what the DB already answers. Deterministic; no LLM.

The DB write to ``analysis_datapoints`` (clear-api ``upsertAnalysisDatapoints``)
happens in the later assembly/publish stage — that stage creates the
``analyses`` row the non-null ``analysis_id`` FK requires, then links these
numbers. Here we only compute + persist.

NOTE: no ``from __future__ import annotations`` — Dagster introspects the
``context`` parameter annotation at definition load.
"""

import os
from datetime import datetime, timezone

import dagster as dg

from clear_pipeline.defs.analysis import paths
from clear_pipeline.defs.analysis.numbers import build_frame_numbers
from clear_pipeline.defs.analysis.registry import PROMPT_VERSION, SCHEMA_VERSION
from clear_pipeline.defs.analysis.store import save_json
from clear_pipeline.defs.knowledgebase.datapoints_schemas import (
    SCHEMA_VERSION as AGGREGATION_SCHEMA_VERSION,
)
from clear_pipeline.defs.reliefweb_partitions import country_partitions
from clear_pipeline.providers import clear_api

_WINDOW_KIND = "yearly"


def _s3():
    from clear_pipeline.providers.s3 import s3_client

    return s3_client()


def _calendar_year_window(year: int) -> tuple[str, str]:
    start = datetime(year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    return start.isoformat(), end.isoformat()


@dg.asset(
    group_name="analysis",
    deps=["analysis_sub"],
    partitions_def=country_partitions,
)
def analysis_numbers_extract(context: dg.AssetExecutionContext) -> dict:
    """Extract + aggregate THIS country's figures from the KB aggregation bucket
    into the numbers artifact (pre-gap). No-op-ish when no bucket exists yet."""
    iso3 = context.partition_key
    row = {
        (c.get("iso3") or "").lower(): c for c in clear_api.get_pipeline_countries()
    }.get(iso3.lower())
    if row is None:
        raise dg.Failure(description=f"partition {iso3!r} is not in clear-api pipelineCountries")
    country_id = clear_api.resolve_country_location_id(row["name"], pcode=row.get("pcode"))
    if not country_id:
        raise dg.Failure(description=f"{row['name']} ({iso3}): no A0 location resolved")

    year = datetime.now(timezone.utc).year
    window_start, window_end = _calendar_year_window(year)

    aggregated = None
    try:
        aggregated = clear_api.get_aggregated_datapoint(
            location_id=country_id,
            window_start=window_start,
            window_end=window_end,
            window_kind=_WINDOW_KIND,
            schema_version=AGGREGATION_SCHEMA_VERSION,
        )
    except Exception as exc:  # noqa: BLE001
        context.log.warning("[analysis_numbers_extract] %s: aggregated fetch failed (%s)", iso3, exc)

    numbers = build_frame_numbers(aggregated)
    save_json(
        _s3(), os.environ["S3_BUCKET"],
        paths.numbers_key(iso3, _WINDOW_KIND, PROMPT_VERSION, SCHEMA_VERSION),
        {"iso3": iso3, "window_kind": _WINDOW_KIND, **numbers},
    )

    context.add_output_metadata({
        "country": dg.MetadataValue.text(iso3),
        "report_count": dg.MetadataValue.int(int(numbers["report_count"])),
        "origin": dg.MetadataValue.text(numbers["origin"] or "none"),
        "empty": dg.MetadataValue.bool(numbers["empty"]),
    })
    return {"iso3": iso3, "report_count": numbers["report_count"], "empty": numbers["empty"]}
