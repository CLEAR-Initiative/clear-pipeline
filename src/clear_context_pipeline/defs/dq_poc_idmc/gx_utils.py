"""Great Expectations Core helper for the medallion POC.

GX Core only (no Cloud): every check builds a fresh, in-process "ephemeral"
context, defines a suite, validates one pandas DataFrame, and throws the
context away. Nothing is persisted to disk or to a GX-hosted service. This
matches `docs/data-quality-ingestion-design.md`'s recommendation (§4: "runs
in-process against data the connector already holds in memory, no new
service"), and sidesteps the framework's own 0.x -> 1.x "Fluent" API
churn by pinning `great-expectations>=1.22,<2` (verified against the
Fluent API directly, not against 0.x docs, see pyproject.toml).

Implements the doc's §6a failure policy: per-record isolation is the
caller's job (an asset already isolates one bad batch from crashing the
whole run); this module's job is the *suite-level* warn/block call. A
structural failure (row count, uniqueness at the table level) always
blocks: there's no "unexpected proportion" to weigh for those. A
column-level failure blocks only once its unexpected proportion crosses
`block_threshold`; below that it's a warning and the batch proceeds.
"""

import logging
from dataclasses import dataclass, field

import great_expectations as gx
import pandas as pd

logger = logging.getLogger(__name__)

# "More than half a batch failing a critical expectation": the doc's own
# example threshold (§6a). A domain call, not decided unilaterally there;
# fixed here as the POC default rather than left unconfigurable.
BLOCK_THRESHOLD = 0.5


@dataclass
class ExpectationOutcome:
    expectation_type: str
    column: str | None
    success: bool
    unexpected_percent: float | None


@dataclass
class GXCheckResult:
    suite_name: str
    row_count: int
    success: bool
    blocked: bool
    outcomes: list[ExpectationOutcome] = field(default_factory=list)

    @property
    def failed(self) -> list[ExpectationOutcome]:
        return [o for o in self.outcomes if not o.success]

    def check_metadata(self) -> dict:
        """Shaped for `dg.AssetCheckResult(metadata=...)`. Lists every
        expectation the suite ran, not just the failures, so the Dagster UI
        shows the actual suite rather than an empty list on a passing run."""
        return {
            "row_count": self.row_count,
            "success": self.success,
            "blocked": self.blocked,
            "expectations": [self._describe(o) for o in self.outcomes],
            "failed_expectations": [self._describe(o) for o in self.failed],
        }

    @staticmethod
    def _describe(o: "ExpectationOutcome") -> str:
        mark = "✓" if o.success else "✗"
        if o.unexpected_percent is not None:
            return f"{mark} {o.expectation_type}({o.column}): {o.unexpected_percent:.1f}% unexpected"
        return f"{mark} {o.expectation_type}({o.column})"


def validate_dataframe(
    df: pd.DataFrame,
    *,
    suite_name: str,
    expectations: list,
    block_threshold: float = BLOCK_THRESHOLD,
) -> GXCheckResult:
    """Validate `df` against `expectations` in a throwaway GX context.

    `expectations` is a list of already-constructed `gx.expectations.Expect*`
    instances, built by the caller next to the checkpoint it belongs to (see
    `checks.py`) so the suite definition sits beside the pipeline stage it
    gates, matching the doc's per-layer suite examples (§5).
    """
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("poc_pandas")
    data_asset = data_source.add_dataframe_asset(name=f"{suite_name}_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{suite_name}_batch")

    suite = gx.ExpectationSuite(name=suite_name)
    for exp in expectations:
        suite.add_expectation(exp)

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    result = batch.validate(suite)

    outcomes: list[ExpectationOutcome] = []
    blocked = False
    for r in result.results:
        unexpected_percent = r.result.get("unexpected_percent")
        outcome = ExpectationOutcome(
            expectation_type=r.expectation_config.type,
            column=r.expectation_config.kwargs.get("column"),
            success=r.success,
            unexpected_percent=unexpected_percent,
        )
        outcomes.append(outcome)
        if not r.success:
            # No unexpected_percent means a structural/table-level check
            # (row count, table-wide uniqueness). Those don't have a
            # partial-credit reading: a failure blocks outright. A
            # column-level failure only blocks past the threshold.
            if unexpected_percent is None or (unexpected_percent / 100) > block_threshold:
                blocked = True

    check_result = GXCheckResult(
        suite_name=suite_name,
        row_count=len(df),
        success=result.success,
        blocked=blocked,
        outcomes=outcomes,
    )
    logger.info(
        "[gx:%s] rows=%d success=%s blocked=%s failed=%s",
        suite_name,
        check_result.row_count,
        check_result.success,
        check_result.blocked,
        [o.expectation_type for o in check_result.failed],
    )
    return check_result
