---
status: accepted
---

# Unknown values stay null — we never invent a default

## Context

Across the pipeline and API we kept substituting invented constants when a real
value was unknown: population affected defaulted to 33,000, population displaced
to 1,670, signal/event severity to a floor (3, or per-source 1/2), and an event's
end date to `start + 7 days`. Each made "we don't know" indistinguishable from a
real measurement — a fabricated 33,000 affected reads exactly like a sourced one,
a floored severity silently clears alert gates, an invented end date expires a
still-active event.

Three changes removed those inventions:

- **clear-pipeline #95** — population affected/displaced emit `None` (constants
  dropped); connector severity floors (GDACS, ACLED, Darfur24, gx) emit `None`;
  `SignalClassification.severity` is `int | None`; event `validTo` is left null
  instead of `start + 7 days`.
- **clear-api #201** — `events.validTo` made nullable; severity range filters keep
  null-severity rows; alert fan-out stopped inventing a severity.
- **clear-api #205** — the alert-severity matching rule was consolidated into one
  shared helper (`src/utils/alert-severity.ts`) so the fan-out paths can't drift.

## Decision

**If a value is unknown, store `null`. Never invent a default to stand in for it.**

Null means "unknown", which is not the same as zero, the scale floor, or "now +
N days". Code that consumes a possibly-null value must decide, explicitly and
visibly, how to treat unknown — it must not paper over it with `?? <default>`.

## What null severity means in each place

Severity is the field most tempting to re-default, so the treatment is pinned
here. A `null` severity is a real, expected state — do **not** reintroduce `?? 1`
(or `?? 3`, or a per-source floor) anywhere:

| Context | Treatment of a null severity |
|---|---|
| **Event-severity mean** (`providers/event.py` `_compute_event_severity`) | **Excluded** from the average. Null-severity signals still belong to the event; they just aren't counted. The mean is over the signals that *have* a severity; the Claude estimate is used only when **none** do. |
| **Severity range filters** (clear-api list queries) | **Included.** A `severityMin`/`severityMax` range keeps null rows — unknown is "not ranked", not "below the floor", so filtering must not silently drop it. |
| **Alert matching** (clear-api `src/utils/alert-severity.ts`) | Treated as the floor (`ALL_SEVERITIES_FLOOR = 1`) **for matching only** — so a null-severity event reaches subscribers who opted into *every* severity, but not subscribers who raised their minimum. The event's stored severity stays null. |
| **Sorting** | **Always last** — a null severity sorts below every ranked value, never as a 0 or a 1. |

## Consequences

- "Unknown" is now honestly null end-to-end; dashboards and consumers can show
  "—" instead of a fabricated number, and analysts can trust that a figure is
  sourced, not imputed.
- Every consumer of a nullable domain value owns an explicit unknown-handling
  decision. The table above is the reference for severity; follow the same
  principle (decide, don't default) for population, dates, and disaster type.
- **Do not reintroduce invented defaults.** A `?? 1` / `?? 3` / `|| 33_000` on
  severity or population is a regression of this decision, not a convenience.

See also ADR-0001 (population affected is extracted, not sourced — its tier-3
invented defaults were removed in #95).
