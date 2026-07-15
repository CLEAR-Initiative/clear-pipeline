---
status: accepted
---

# Deduplicate at figure scope — per-incident extraction is not possible

## What the source data actually is

ReliefWeb reports are **analytical**, not incident logs. A report states a figure that is *already aggregated at source* over a scope:

> "1,000 people were affected in Kordofan State, Sudan, between X and Y, across ~100 incidents."

There are no per-incident numbers in the document. The 1,000 is a total over `(location = Kordofan, period = X–Y, event types = {…})`. This is the defining property of the corpus and it constrains everything downstream.

§6.4's worked example (case C) shows the opposite shape:

```
Report A: { event: "attack-on-health", location: SD0201, date: 2026-07-02, killed: 3 }
```

That is an incident-log record — the shape of ACLED or a signals feed, not of a ReliefWeb analytical PDF. **The doc's incident key `(event, location, time_bucket)` describes data this pipeline's sources do not contain.** An earlier draft of this ADR proposed adding `incidents: list[Incident]` to extraction to close the gap; that was wrong. You cannot extract per-incident figures from a document that reports only totals — the LLM would have to invent the split.

## The real gap: figure scope vs mentioned locations

`TimingAndScope.locations` is "every distinct place the report discusses", and `LocationRef.admin_level` is a resolver hint that narrows name lookup. Nothing records **which location a figure is scoped to**.

So for "1,000 affected in Kordofan", extraction yields `affected: 1000` alongside `locations: [Sudan, Kordofan, El Obeid]` — Kordofan is the scope, Sudan is context, El Obeid is a passing mention, and the three are indistinguishable. `extractNumericMentions` therefore fans the figure across all three, and at country scope `additive_count` sums the copies (clear-api ADR-0001: one report, 10 killed, 3 places → country-wide 30).

## Decision

Deduplicate at **figure scope**, and extract the scope to make that possible:

1. **Extract each figure's scope** — the `(location, admin_level)` the number covers, distinct from the locations the report merely mentions. A figure scoped to Kordofan (A1) contributes to Kordofan's bucket, not to El Obeid's.
2. **Include event type in the incident key.** The key becomes `(figure_scope_location, time_bucket, event_type_set)` — dedup on totals, as intended. The implemented key today is only `(location, time_bucket)`; the event dimension was specified in §6.4.1 and never built, which silently discards distinct co-located events (see Consequences).
3. **Treat the event-type set atomically.** A report totalling across `{conflict, flood}` cannot be split between them — the split does not exist in the source. Fanning across event types would repeat the location bug in a second dimension (3 locations × 2 types = 6 copies of one figure).

## Consequences

- **Cross-level reconciliation does not arise.** Deduplication is for *competing observations* — two reports describing the same `(location, time range, event type)`, where one wins and the rest are dropped. It is not a mechanism for reconciling an A0 figure against the sum of its A1s, and must not be pressed into that role. Since `aggregated_datapoints` is keyed one row per `(window, level, location)`, each bucket consumes only figures scoped to it: Sudan's bucket takes Sudan-scoped figures, Kordofan's takes Kordofan-scoped ones. Nothing sums across levels, so a parent and its child are never added together. Figure Scope is what makes this hold — without it, a Kordofan figure reaches Sudan's bucket by accident, which is exactly the fan-out defect.
- **Within a bucket, the three key dimensions behave differently, and all three are needed.** Same `(location, time_bucket, event_type)` → competing observations → dedup, pick one. Different `time_bucket` (Kordofan Jan–Mar 600, Apr–Jun 400) → genuinely different periods → sum to 1,000 across the year. Different `event_type` (Kordofan Jan–Mar conflict 600, flood 200) → different phenomena → sum to 800. Dropping `event_type` from the key collapses that last case into the first and silently discards one of the two figures — the proven undercount below.
- **The missing event dimension is a real, proven defect.** Clash (5 killed) and flood (3 killed), same place, same day, currently collapse to one group: `killed_total = 3`, with the clash's report absent from `contributing_report_ids` — while `event_types` still unions to `["conflict","flood"]`, so the payload contradicts itself. Under the intended `(time, location, event type)` key these are distinct keys and sum to 8.
- **§6.4.4's "same-report multi-mention collapse" remains unimplementable** and should be struck from the doc. The schema holds one `NumericField` per field per report; there are never multiple mentions to collapse. The LLM does it implicitly at extraction.
- clear-api ADR-0001's report-scope dedup at country level is a **stopgap** that stops the overcounting today. It is superseded once figure scope exists: country buckets should then consume A0-scoped figures rather than dedup-by-report.
- Requires a schema bump, prompt work, and re-extraction. Existing aggregated buckets are wrong for the seven `additive_count` fields and need regeneration — a regeneration, not a migration, given bitemporal supersede-and-insert.
- Both defects were invisible to all 23 passing aggregation tests. Regression tests for the multi-location and same-day-distinct-event cases are required.

## Relationship to `events.populationAffected`

Out of scope here, and deliberately so — see ADR-0001. `events` is **event-driven** data (ACLED/GDACS/signals) whose event types may not correspond to those a report covers, so the two are not comparable today. Incorporating it later is plausible but is its own decision, not a corollary of this one.
