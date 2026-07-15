---
status: accepted
---

# Population Affected is extracted from reports, not sourced from `events`

The situation dashboard needs Population Affected — the widest circle of crisis impact (see `CONTEXT.md`). Two places in the system could supply it:

1. **`events.populationAffected`** in clear-api — already exists, already populated.
2. **The datapoints pipeline** — `report_datapoints` → `aggregated_datapoints`. Has no affected field today; the design doc specifies one (`Max` / `max_within_report_then_latest`) that was never built.

We chose (2), and will add an `affected` field to the extraction schema with `Max` aggregation.

## Why not `events`

**The primary reason is incomparability, not data quality.** `events` is built from **event-driven** sources — ACLED, GDACS, Dataminr, signals — where each record is a discrete occurrence. `report_datapoints` is built from **analytical** reports, where each figure is a total already aggregated over a Figure Scope (see `CONTEXT.md`). The event types clustered into an `events` row may not correspond to the event types an analytical report covers, so `events.populationAffected` and a report-derived affected figure describe different populations, measured different ways, over different partitions of reality. They are not two estimates of one quantity; putting them in one tile would imply a comparison that does not hold. Incorporating `events` later is plausible and worth revisiting — it is a separate decision, not a corollary of this one.

A secondary reason reinforces it: `events.populationAffected` mixes measured and **imputed** values in one column, with no field recording which is which. `clear-pipeline` resolves it in tiers:

1. Raw signal extraction — GDACS exposure data, Dataminr/manual regex.
2. Per-event-type lookup — median `population_1km` from `acled_event_type_stats.json`, which holds real percentile distributions per ACLED event type.
3. Last-resort constant — `default_population_affected = 33_000`, the mean across all events over a 5-year span. (`default_population_displaced = 1670` is the same statistic for displacement.)

None of that is arbitrary, and this ADR should not be read as criticising it. Tier 3 is a principled prior — the mean is what minimises squared error absent any signal — and tier 2 is a genuine per-event-type distribution. For the events system's actual job, ranking alerts, every event needs a magnitude and a well-chosen imputation beats a null that sorts arbitrarily. It is the right design for that consumer.

It is the wrong input for a dashboard, for a reason that is the opposite of "the numbers are bad": **a well-chosen imputation is more dangerous here than an obvious placeholder.** 33,000 reads as a finding. A sentinel like `-1` would announce itself and never reach a user. Because tiers 1–3 all land in the same column, a reader cannot tell an extracted figure from a 5-year mean, and neither can a downstream aggregator. Note also that tier 2 imputes *population near the event* — an exposure proxy, closer to "who was in range" than "who was impacted", which is a third concept again.

The two consumers hold different standards of evidence. Ranking may impute; a published humanitarian figure may not. One column cannot serve both.

The datapoints pipeline holds the opposite contract: every numeric carries a Quality Envelope (`value`, `unit`, `confidence`, `source_quote`, `chunk_index`, `page_number`), and absence stays null. A country-year with no reported affected figure renders as "no data", never as a default. That contract is what makes the number defensible on a humanitarian dashboard.

## Consequences

- **Two representations of the same concept now coexist deliberately.** `events.populationAffected` (alert-ranking; may be imputed from a distribution or a 5-year mean) and `aggregated_datapoints.affected` (dashboard; always evidenced, null when unknown). Anyone comparing them will find disagreement — that is expected, not a bug, and the imputed side is not the "wrong" one. Do not "reconcile" them without revisiting this ADR.
- Requires a schema bump and re-extraction of every report. The extraction schema currently has no field the LLM could populate.
- `Max` aggregation is not robust to a single bad extraction: one hallucinated high figure pins the yearly bucket, since Max never decreases. Quality-weighting mitigates but does not eliminate this. Revisit if it bites.
- **Interim, already applied**: the situation payload's `population_affected` field was hoisting `overall_pin` — People in Need — and was renamed to `population_in_need` (2026-07-15, pre-merge on `feat/situation-analysis`). The field now says what it holds. Done before the branch merged, while `SCHEMA_VERSION = "v1"` was unpublished and no consumer read the payload (`data` is an opaque `JSON!` scalar in GraphQL), so it cost nothing; after merge it would have been a version bump plus regeneration of every country-year.
- `population_in_need` is sparse by nature: `overall_pin` only populates when a report headlines a country/appeal-wide figure, so it is driven by HNO/HRP/appeal documents and is null for most field reports. Expect a thin series.
- When affected extraction ships, it lands as a **new** `population_affected` field alongside `population_in_need`, not as a rename back. They are different numbers and the dashboard should be able to show both.
