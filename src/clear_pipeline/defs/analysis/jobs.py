"""Job for the analysis module's descriptive→publish DAG.

A SEPARATE automation from the weekly knowledge-base enrichment pipeline — the
analysis DAG is not chained to, or triggered by, the KB run. It reads the
current KB state via clear-api at run time instead.

Selects only the new agentic-DAG assets; the two queue-drain assets
(``drain_analysis_requests`` / ``drain_analysis_automations``) stay on their own
sensor automations and are intentionally excluded.
"""

import dagster as dg

# Respect the heavy-run queue limit (same tag the KB runs use) so analysis runs
# share the concurrency ceiling rather than piling on.
_HEAVY_RUN_TAGS = {"clear/weight": "heavy"}

# The descriptive → numbers → gaps/eval → (web gap-fill) → publish chain.
ANALYSIS_ASSET_KEYS = [
    "analysis_sub",
    "analysis_numbers_extract",
    "analysis_gaps",
    "analysis_evaluation",
    "analysis_web_gapfill",
    "analysis_overview",
]

analysis_job = dg.define_asset_job(
    name="analysis_job",
    selection=dg.AssetSelection.assets(*ANALYSIS_ASSET_KEYS),
    tags=_HEAVY_RUN_TAGS,
)
