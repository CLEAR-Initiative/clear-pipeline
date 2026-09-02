"""GX-backed `@dg.asset_check`s for the IDMC medallion POC, one per layer
in docs/data-quality-ingestion-design.md §5.

Two check styles, matching §6a's failure policy exactly. Verified against
real Dagster 1.13 behavior, not assumed from docs: a `blocking=True` check
only actually halts downstream materialization when it returns
`passed=False` at the default ERROR severity. `passed=False` at WARN
severity is visible in the UI but does not halt.

  - **Blocking** (bronze, silver, gold): `blocking=True` on the decorator.
    A GX failure below `gx_utils.BLOCK_THRESHOLD` reports `passed=False,
    severity=WARN` (visible, batch proceeds). Above threshold, or any
    structural failure, reports `passed=False, severity=ERROR` (the run
    halts before the next layer promotes).
  - **Observational** (classify, geo, temporal): `blocking=False`
    (the default), `passed` reflects the real GX result, severity is fixed
    at WARN so a failure is visible without ever halting. Matches the
    doc's own words for substep 3 ("statistical, not per-record: watch the
    ratio, don't gate on it"), extended here to substeps 1-2 too, which the
    doc frames as quality signals, not hard gates.
"""

import great_expectations as gx
import pandas as pd

import dagster as dg
from clear_context_pipeline.defs.dq_poc_idmc.assets import (
    idmc_poc_bronze,
    idmc_poc_classify,
    idmc_poc_geo,
    idmc_poc_gold,
    idmc_poc_silver,
    idmc_poc_temporal,
)
from clear_context_pipeline.defs.dq_poc_idmc.gx_utils import validate_dataframe


def _blocking_result(result, *, error_metadata: dict | None = None) -> dg.AssetCheckResult:
    metadata = {**result.check_metadata(), **(error_metadata or {})}
    if result.blocked:
        return dg.AssetCheckResult(
            passed=False, severity=dg.AssetCheckSeverity.ERROR, metadata=metadata
        )
    if not result.success:
        return dg.AssetCheckResult(
            passed=False, severity=dg.AssetCheckSeverity.WARN, metadata=metadata
        )
    return dg.AssetCheckResult(passed=True, metadata=metadata)


def _observational_result(result, *, extra_metadata: dict | None = None) -> dg.AssetCheckResult:
    return dg.AssetCheckResult(
        passed=result.success,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={**result.check_metadata(), **(extra_metadata or {})},
    )


# ══════════════════════════════════════════════════════════════════════════
# Bronze: shape of the raw payload (doc §5 "Bronze")
# ══════════════════════════════════════════════════════════════════════════
@dg.asset_check(asset=idmc_poc_bronze, blocking=True, name="bronze_shape")
def idmc_poc_bronze_check(idmc_poc_bronze: pd.DataFrame) -> dg.AssetCheckResult:
    result = validate_dataframe(
        idmc_poc_bronze,
        suite_name="idmc_poc_bronze",
        expectations=[
            gx.expectations.ExpectColumnValuesToNotBeNull(column="id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="created_at"),
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        ],
    )
    return _blocking_result(result)


# ══════════════════════════════════════════════════════════════════════════
# Bronze to Silver: completeness, ranges, geo-validity (doc §5 "Silver")
# ══════════════════════════════════════════════════════════════════════════
@dg.asset_check(asset=idmc_poc_silver, blocking=True, name="silver_completeness")
def idmc_poc_silver_check(idmc_poc_silver: pd.DataFrame) -> dg.AssetCheckResult:
    result = validate_dataframe(
        idmc_poc_silver,
        suite_name="idmc_poc_silver",
        expectations=[
            gx.expectations.ExpectColumnValuesToNotBeNull(column="title", mostly=0.95),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="description", mostly=0.95),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="severity", min_value=1, max_value=5
            ),
            gx.expectations.ExpectColumnValuesToBeUnique(column="idu_id"),
            # Global sane-range check, not the doc's per-country bbox. See
            # the module docstring's simplifications note: a real bbox check
            # needs either a custom expectation or a per-country partition.
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="lat", min_value=-90, max_value=90, mostly=0.99
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="lng", min_value=-180, max_value=180, mostly=0.99
            ),
        ],
    )
    return _blocking_result(result)


# ══════════════════════════════════════════════════════════════════════════
# Silver to Gold business logic, §3a substeps. Observational, never blocks.
# ══════════════════════════════════════════════════════════════════════════
@dg.asset_check(asset=idmc_poc_classify, name="classify_populated")
def idmc_poc_classify_check(idmc_poc_classify: pd.DataFrame) -> dg.AssetCheckResult:
    result = validate_dataframe(
        idmc_poc_classify,
        suite_name="idmc_poc_classify",
        expectations=[
            gx.expectations.ExpectColumnValuesToNotBeNull(column="relevance_score"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="event_type"),
        ],
    )
    return _observational_result(result)


@dg.asset_check(asset=idmc_poc_geo, name="geo_resolution_rate")
def idmc_poc_geo_check(idmc_poc_geo: pd.DataFrame) -> dg.AssetCheckResult:
    result = validate_dataframe(
        idmc_poc_geo,
        suite_name="idmc_poc_geo",
        expectations=[
            gx.expectations.ExpectColumnValuesToNotBeNull(column="district_id", mostly=0.9),
        ],
    )
    return _observational_result(result)


@dg.asset_check(asset=idmc_poc_temporal, name="temporal_match_ratio")
def idmc_poc_temporal_check(idmc_poc_temporal: pd.DataFrame) -> dg.AssetCheckResult:
    result = validate_dataframe(
        idmc_poc_temporal,
        suite_name="idmc_poc_temporal",
        expectations=[
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="match_outcome", value_set=["new", "merged"]
            ),
        ],
    )
    # This is a drift signal to graph over time (doc §5: "track the ratio,
    # don't gate on it"), reported alongside the structural in-set check
    # rather than replacing it.
    merged_ratio = (
        (idmc_poc_temporal["match_outcome"] == "merged").mean() if len(idmc_poc_temporal) else 0.0
    )
    return _observational_result(result, extra_metadata={"merged_ratio": round(float(merged_ratio), 3)})


# ══════════════════════════════════════════════════════════════════════════
# Gold: referential integrity + aggregate bounds (doc §5 "Gold"). Blocking:
# a failure here means the stubbed push never fires.
# ══════════════════════════════════════════════════════════════════════════
@dg.asset_check(asset=idmc_poc_gold, blocking=True, name="gold_integrity")
def idmc_poc_gold_check(idmc_poc_gold: pd.DataFrame) -> dg.AssetCheckResult:
    result = validate_dataframe(
        idmc_poc_gold,
        suite_name="idmc_poc_gold",
        expectations=[
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="population_affected", min_value=0, max_value=1_000_000_000
            ),
            # Referential integrity stand-in: every Event must aggregate at
            # least one real silver signal.
            gx.expectations.ExpectColumnValuesToBeBetween(column="signal_count", min_value=1),
        ],
    )
    return _blocking_result(result)
