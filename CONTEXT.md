# CLEAR Context Pipeline

Builds the CLEAR knowledge base from humanitarian reports (ReliefWeb PDFs + manual uploads), extracting structured datapoints and rolling them up into aggregations and situation analyses.

## Language

### Population figures

The four headline population numbers are distinct concepts and must never be substituted for one another. From widest to narrowest circle: **Population Affected** ⊃ **People in Need** ⊃ **Casualties**, with **Population Displaced** cutting across.

**Population Affected**:
Everyone whose life the crisis touched — displaced, injured, bereaved, cut off from services, or stripped of home or livelihood. The widest circle.
_Avoid_: Casualties, victims, impacted population, people in need

**Casualties**:
People killed, injured, or missing as a direct result of the crisis. A small subset of Population Affected.
_Avoid_: Affected, losses, victims

**People in Need (PIN)**:
The population assessed as requiring humanitarian assistance. Narrower than Population Affected — being affected does not automatically make someone in need of assistance.
_Avoid_: Affected, beneficiaries, caseload

**Population Displaced**:
People forced from their homes. Cuts across the other three rather than nesting cleanly inside them.
_Avoid_: Refugees (a specific legal status — a displaced person who crossed an international border), IDPs (a displaced person who did not)

### Severity

A 1-5 signal/event magnitude. It is **nullable**: `null` means *unknown* (the source gave none), which is NOT the scale floor. Unknown values stay null — we never invent a default (no `?? 1`, `?? 3`, or per-source floor). Event severity averages only the signals that have one; null-severity signals still belong to the event but aren't counted. See [ADR-0010](docs/adr/0010-unknown-values-stay-null.md) for how null severity is treated in the event mean, range filters, alert matching, and sorting.

### Reports and incidents

**Report**:
One published document (a ReliefWeb PDF or a manual upload). **Analytical, not an incident log**: it states figures already aggregated at source over a scope — "1,000 affected in Kordofan between X and Y across ~100 incidents". The unit of ingestion and of provenance, never the unit of what happened.
_Avoid_: Source (reserve for the dashboard's citation list), document, article

**Figure Scope**:
The `(location, admin_level, time range, event type set)` a reported figure covers. The load-bearing concept for aggregation: a figure scoped to Kordofan belongs to Kordofan's bucket, regardless of which other places the Report happens to mention. Distinct from the Report's mentioned locations, which include context and passing references.
_Avoid_: Location (ambiguous — a Report has many; a figure has one scope), coverage

**Incident**:
One real-world occurrence. Reports count them ("across ~100 incidents") but do not break figures down by them, so an Incident is **not** a unit this pipeline can extract, aggregate, or deduplicate at. Deduplication happens at Figure Scope instead.
_Avoid_: using it as a synonym for the dedup key — that is Figure Scope

**Event Type**:
The category of an Incident (conflict, flood, disease outbreak), canonicalised through the `disaster_types` glide-code taxonomy. A classification, never a thing that happened — "flood" is an Event Type; "the July flood in El Fasher" is an Incident.
_Avoid_: Event, disaster, hazard (reserve Hazard for the forward-looking risk component)

**Event**:
Ambiguous across the CLEAR system — avoid it unqualified. In the alerts/signals system it is a ranked, clustered record built from **event-driven** sources (ACLED, GDACS, Dataminr), carrying its own `populationAffected`. That is a different population, measured a different way, over event types that may not correspond to those an analytical Report covers. It has no counterpart in this pipeline: Reports give totals over a Figure Scope, not per-event records. The two are not comparable today — see `docs/adr/0001-affected-extracted-not-sourced-from-events.md`.
_Avoid_: using bare "event" in any field name, prompt, or dashboard label

### Aggregation

**Additive Count**:
A field whose values sum across reports, deduplicated by (event, location, date bucket). Applies to things that genuinely accumulate — `killed`, `new_displacements`, `funding_received`.
_Avoid_: Sum, total

**Latest State**:
A field representing a stock measured at a point in time, where the most recently published report wins rather than summing. Applies to `idp_stock`, `PIN`, `IPC phase`.
_Avoid_: Current, snapshot

**Max (Upper-Bound)**:
A field where the largest quality-adjusted value across reports wins. Used where reports observe overlapping populations and summing would double-count, but a stock reading understates. Applies to `population_affected`.
_Avoid_: Peak, highest

**Quality Envelope**:
The per-field provenance wrapper on every extracted numeric — `{ value, unit, confidence, source_quote, chunk_index, page_number, value_low/value_high (magnitude range, always finite — ADR-0007), qualifier (per-figure bias direction: exact/at_least/at_most/approx — ADR-0007), measure_type (stock_as_of/period_flow/cumulative_to_date — ADR-0007), basis_period_start/basis_period_end (the figure's own period when stated distinctly — ADR-0007), scope_location_name/scope_location_id (Figure Scope), source_name/source_id (source attribution, ADR-0004), credibility (per-figure credibility overrides, ADR-0004 §4) }`. `value` stays the headline point (`value_low`/`value_high` default to it for an exact figure); `confidence` is the Directness credibility criterion; the other credibility criteria + source reliability combine into the read-time **data_quality** score (ADR-0005).
_Avoid_: Metadata, provenance blob

### Situation analysis

**Situation Analysis**:
A per-country, per-year snapshot combining deterministic headline numbers with LLM-generated narrative, written as one bitemporal row per country and consumed by the dashboard.
_Avoid_: Report, summary, briefing

**SAF Sector**:
One of the six NRC Sector Analysis Framework sectors: education, food security, health, shelter, WASH, protection. A fixed taxonomy — not an open list.
_Avoid_: Cluster (the OCHA coordination concept, which is a different taxonomy)

### Tasks and Workers

**Task**:
One unit of work of a named kind about one subject, kept by clear-api in its generic Task
queue (clear-api ADR-0010; vocabulary owned by clear-api's CONTEXT.md). This pipeline is a
**Task Worker** on that queue, not its owner.
_Avoid_: job, request, drain item

**Task Worker**:
The `drain_tasks` asset acting on clear-api's Worker protocol — claim, heartbeat, complete,
fail — as the `worker` service user. A new kind of work is a **handler** registered by kind.
_Avoid_: agent, bot, enricher

**Handler**:
The function that turns one claimed Task into a `TaskOutcome` (raw result, and usage if it
ran a model). Handlers never write to clear-api themselves; the Worker loop does. None is
registered today (see **ImpactPrior**).
_Avoid_: processor, job runner

**ImpactPrior**:
What has typically happened before given a hazard type and a country, with its evidence
basis (vocabulary owned by clear-api's CONTEXT.md). No longer this pipeline's concern: its
`event.impact_prior.clear` handler — the first **Handler** — was retired on 2026-10-08,
because clear-api now computes the ImpactPrior from accepted history and the web Worker (a
Claude routine) proposes individual cases for analysts to accept. The generic **Task
Worker** stays for future kinds.
_Avoid_: precedent, history, prior events
