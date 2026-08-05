# Location Metadata Reconciliation — Design for Sign-off

**Status:** Proposed — for review and sign-off · **Owner:** CLEAR data pipeline team

This document explains, in full and without requiring access to the system, how CLEAR will
combine two streams of humanitarian figures into one reconciled, trustworthy set of
aggregated numbers:

- **Report figures** — numbers *read from reports* (situation reports and bulletins) by an AI.
- **Location metadata** — authoritative reference figures *pulled directly from the source
  organisations that produce them*, tied to specific locations and kept current
  automatically. "Location metadata" is the name of this reference layer throughout CLEAR;
  this document uses that name consistently.

It is a companion to the *Data Quality & Source Attribution* sign-off document; where that
one grades a single figure, this one decides how figures from these two streams are
**cross-checked, gap-filled, and combined**.

---

## 1. Purpose

CLEAR builds its aggregated humanitarian picture (people displaced, people in need, funding,
and so on) largely from **reports** — situation reports and bulletins published on
humanitarian platforms such as ReliefWeb, from which an AI reads the figures. This has two
limits:

1. **Reports can lag or misread.** A report may be weeks old, or the AI may misread a
   figure, and today nothing corrects it.
2. **We already hold better versions of many of these numbers.** CLEAR separately maintains
   **location metadata** — authoritative figures pulled *directly* from the organisations
   that produce them (the displacement-tracking body, the UN humanitarian-needs and funding
   systems, the refugee agency, the food-security classification, and more), tied to each
   area and refreshed automatically every day or month. Until now this layer has not been
   used to check or complete the report-derived aggregates.

This design connects the two: **location metadata** becomes the **anchor and cross-check**
for the report figures.

## 2. Executive summary

- CLEAR maintains **location metadata** — a layer of authoritative structured figures,
  pulled straight from source organisations and kept current automatically (§4).
- The report-derived **aggregated figures** cover the same metrics, but are read from
  documents of varying freshness and reliability (§5).
- The two are **reconciled per metric, location, and time period** (§6): the location-metadata
  figure **anchors** the number, **fills gaps** where reports are silent, and **overrides**
  reports that are stale or wildly divergent — while fresh reports still lead where they
  legitimately catch a development first.
- A specific long-standing error — **over-counting returnees** by mixing running totals
  with per-period movements — is fixed with a clean **stock-and-flow** model (§7).
- Safeguards prevent the opposite error, **double-counting**, when a report simply repeats a
  figure the location metadata already holds (§8).

**What needs sign-off is domain judgment:** the disagreement threshold at which location
metadata overrides a report (§6), and confirmation of the reconciliation rules for
displacement (§7).

---

## 3. The two streams, at a glance

| | **Report figures** | **Location metadata** |
|---|---|---|
| Where from | Reports/bulletins on humanitarian platforms | Pulled directly from source organisations' data services |
| How read | By an AI reading the document text | Structured data — no interpretation needed |
| Freshness | As fresh as the report; can lag | Refreshed automatically (daily or monthly) |
| Coverage | Wherever a report mentions a figure | Wherever the source organisation publishes |
| Reliability | Varies by publisher and document | Consistently high (official producers) |
| Detail | A number, a place, a period | A number, a precise place, an exact reference period, and a full history |

Neither stream alone is sufficient: location metadata is more reliable but updates less often
and only where its producers operate; the reports are more frequent and wider, but noisier.
Reconciliation takes the best of both.

## 4. Location metadata — what we hold

CLEAR already ingests and keeps current the following authoritative sources as location
metadata, each tied to specific administrative areas (country / region / district) and kept
as a **full dated history** (so any past state can be reconstructed):

**Sources that directly reconcile with our headline metrics**

| Location-metadata source | Provided by | What it gives | Reconciles with |
|---|---|---|---|
| Displacement tracking | IOM DTM | Number of internally displaced people present, by area | **People displaced (IDPs)** |
| Refugees & persons of concern | UNHCR | Refugees by country of origin | **Refugees** |
| Returnees | UNHCR | Returnees by country of origin | **Returnees** |
| Humanitarian needs | UN OCHA (needs system) | People in need, by sector and area | **People in need** (per sector + overall) |
| Funding | UN OCHA (funding system) | Appeal funding required and received | **Funding required / received** |
| Food security | IPC / Cadre Harmonisé | Food-insecure population by phase and area | **Food-security needs** |

**Context layers — also held as location metadata — that enrich the picture but are not
summed into headline figures**

| Context source | Provided by | What it gives |
|---|---|---|
| Operational presence | UN OCHA "who-does-what-where", and NRC's own presence | Which organisations work where |
| Market prices | WFP | Food-commodity prices by market |
| Poverty | Oxford (multidimensional poverty) | Poverty rates by region |
| Seasonal calendar | ACAPS | Seasonal events (lean seasons, harvests, rains) by area |
| Crisis severity | ACAPS (INFORM) | Standing severity indicators |
| Access & infrastructure | Road/bridge datasets | Physical access constraints |

The context layers feed the **written situation analysis** and help decide how long a figure
stays "fresh" (e.g. a lean season lengthens the window for food-security needs); they are
never added into the headline numbers, so they carry no double-counting risk.

## 5. The report-derived aggregated figures — what we compute

From reports, CLEAR extracts and aggregates a fixed set of humanitarian metrics per area and
time period. They fall into three shapes, which matters for how location metadata reconciles
them:

| Shape | Meaning | Metrics | How combined |
|---|---|---|---|
| **State** (point-in-time) | A total that is true *as of* a date | People displaced (IDPs), refugees, people in need (all sectors + overall), funding required | The **latest** figure wins |
| **Flow** (accumulating) | Events counted over a period | New displacements, returns, people killed/injured, security incidents, aid workers killed, funding received | **Summed** across non-overlapping periods |
| **Reach** (widest) | The largest credible affected count | People affected | The **highest** credible figure |

Location metadata maps cleanly onto the **State** metrics (displacement, refugees, needs,
funding required) and the two displacement **Flow** metrics — which is exactly where
reconciliation applies.

## 6. How reconciliation works

For every *(metric, area, time period)*, the two streams are combined by three rules:

1. **Anchor & gap-fill.** Where location metadata has a figure, it enters as a
   high-reliability contributor. Where reports say *nothing*, the location-metadata figure
   **fills the gap** on its own.
2. **Freshness leads — usually.** By default the **freshest** figure wins, whether it comes
   from a report or from location metadata. Because location metadata refreshes daily, it
   naturally wins the common "the report is old" case. But a genuinely fresher report can
   still lead where it legitimately catches a development first.
3. **Location metadata overrides on large disagreement.** When a report and the
   location-metadata figure disagree by **more than 25 %** *(the threshold requiring
   sign-off)*, the **location-metadata figure wins** — a wildly divergent report is treated
   as a misread or outlier. The disagreement is **not discarded**: it is surfaced as an
   **early-warning signal** ("this report claims far more than the official baseline"), so a
   real emerging crisis is flagged even though the headline figure stays anchored.

This keeps the aggregates anchored to authoritative data without going blind to the leading
edge that frequent reporting provides.

## 7. Fixing displacement: stock vs. flow

**The problem.** "Returnees" has been counted incorrectly: a **running total** of everyone
who has returned (a *stock*) was added together with **per-period return counts** (a *flow*),
producing inflated, meaningless totals. The refugee/returnee source also updates
infrequently, so between its updates the picture would otherwise freeze.

**The fix — a stock-and-flow model.** We separate the two ideas and combine them properly:

> **Estimated current total = latest official running total + movements reported since that total's date**

- The **running total** (stock) comes from location metadata and is dated. Because a running
  total already includes everything up to its date, **only movements reported *after* that
  date are added** — never movements from before, which are already counted. This single rule
  removes the over-count.
- We keep the **running total** and the **new movements** as **separate figures**, so it is
  always clear what the last official number was and what has been added on top of it.
- This applies both to **returns** (official returnee total + new returns since) and to
  **displacement** (official displaced total + new displacements since), letting an
  infrequently-updated official total stay current through more frequent reporting.
- If two reports describe the **same** period of movements for the same area, they are **not
  added together** — we take the **higher** of the two (displacement is typically
  under-counted, so the higher figure is the better estimate), breaking ties by quality.

*Simplification for the first version:* displacement and returns are each anchored on their
own official total independently; we do not yet net returns against the displaced total
(i.e. a return does not automatically reduce the displaced count). This is a deliberate,
noted starting point.

## 8. Preventing the opposite error — double-counting

Reports frequently **quote** the very figures location metadata already holds (a UN sitrep
citing the displacement-tracking number). Adding both would count one observation twice.
Safeguards:

- Figures are grouped by *(source, metric, area, period)*. A report that merely **echoes** a
  location-metadata figure lands in the **same group** and is collapsed to one — the
  location-metadata version is kept.
- When the same source publishes the figure at **different times with slightly different
  values**, we keep the **latest** — matching by source and period, not by exact value.
- These safeguards matter most for the **added-up** metrics; for the point-in-time metrics
  only one figure is ever chosen anyway, so there is nothing to double-count.

## 9. What needs sign-off

| # | Decision | Where |
|--:|---|---|
| 1 | The **disagreement threshold** (start at 25 %) at which location metadata overrides a report | §6 |
| 2 | Whether that override applies when the report is **higher, lower, or both** | §6 |
| 3 | Confirmation of the **stock-and-flow** reconciliation for displacement and returns | §7 |

The source **reliability grades** these figures inherit are covered in the companion *Data
Quality & Source Attribution* sign-off document.

## 10. Assumptions & scope (to confirm during build)

- Each location-metadata figure carries a precise area and reference period, so it can be
  matched to the right report figure — this holds for the sources listed in §4.
- Location metadata is refreshed on schedule and an unchanged refresh is recognised as
  unchanged (so "freshness" reflects when a value truly last changed, not when we last
  checked).
- **Not in scope:** reconciliation only compares metrics both streams report. Where location
  metadata does not cover a metric (e.g. casualties, security incidents), the report stream
  remains the sole source and is unaffected by this design.
- The first version anchors each displacement metric independently, without full cross-metric
  displacement accounting (§7).

---

## Related materials (held by the engineering team)

- The *Data Quality & Source Attribution* sign-off document — how each individual figure is
  graded and attributed to a source.
- Two detailed engineering decision records — one for source attribution and quality scoring,
  one for this location-metadata reconciliation.
- The underlying datapoint extraction & aggregation design that both extend.
