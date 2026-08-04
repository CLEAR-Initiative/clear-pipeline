# Data Quality & Source Attribution — Design for Sign-off

**Status:** Proposed — for review and sign-off · **Owner:** CLEAR data pipeline team

This document describes, in full and without requiring access to the system, two linked
improvements to how CLEAR grades the humanitarian figures it ingests. It lists in one
place every table that requires **domain / management sign-off** before we build. The
engineering team holds detailed decision records that mirror this document.

---

## 1. Purpose

Two gaps limit how far a humanitarian figure in CLEAR can be trusted:

1. **We don't record where a figure actually came from.** The reports CLEAR ingests
   (from humanitarian reporting platforms such as ReliefWeb) are *published* by one
   organisation — say OCHA or UNICEF — but a single report routinely quotes figures that
   *originate* elsewhere: an OCHA situation report carries IOM DTM displacement numbers,
   WHO health numbers, WFP market prices. Today none of that origin is captured.
2. **"Quality" is one-dimensional.** Every figure currently carries only a *confidence*
   rating — essentially, how direct the observation was. Nothing captures the **standing
   reliability of the source** or the **credibility of the specific document**.

This design introduces **source attribution** and a composite **data-quality score** so
every aggregated number can be trusted, ranked, and explained.

## 2. Executive summary

- Each figure is attributed to a **source** — the originating organisation, not just the
  platform publisher — held in a central **source registry**.
- Each source carries a manually-assigned **reliability grade** (1–4).
- Each figure receives an **information-credibility score** (0–10), assessed
  automatically by an AI model across eight criteria (recency, directness, attribution,
  consistency, plausibility, and so on).
- These combine into a single **data-quality score** (1–10) that **replaces** the old
  confidence-only measure and decides which figures win when reports disagree.
- Aggregation becomes **bias-aware**: because weak figures *over-report* some metrics
  (e.g. casualties) and *under-report* others (e.g. displacement, needs), the scoring now
  respects the likely **direction** of the error rather than treating all errors alike.

**What needs sign-off is domain judgment, not engineering:** the source reliability
grades (§5.2), the credibility weights (§6.1), and the per-metric bias map (§7.1).

## 3. The data-quality score

> **Data quality  =  ( Source reliability × 2.5 )  ×  Information credibility  ÷  10**

| Term | Range | Where it comes from |
|---|---|---|
| Source reliability | 1–4 (unknown → 1) | a manual grade held against each source |
| × 2.5 | → 2.5–10 | rescales the 1–4 grade onto a 10-point axis |
| Information credibility | 0–10 | an automated (AI) assessment of the specific document |
| **Data quality** | 0–10 | the headline score; higher = more trustworthy |

Source reliability is used **only** as this outer multiplier — not also inside the
credibility score — so a weak source is not penalised twice.

**How the two combine, step by step:**

1. Rescale the source's **1–4 reliability** onto a 0–10 axis (× 2.5).
2. Multiply by the document's **0–10 credibility**.
3. Divide by 10 to bring the result back onto the 0–10 scale.

Because it is a *product*, a high score needs **both** a trustworthy source **and** a
credible document — a strong source carrying a weak document, or a weak source carrying a
strong document, both land in the middle or below. (One consequence: a "usually reliable"
grade-3 source tops out at 7.5 even on a flawless document; only a grade-4 source can reach 10.)

**Worked examples:**

| Source (reliability) | Document credibility | Calculation | Data quality |
|---|--:|---|--:|
| UN agency (3) | 8 / 10 | (3 × 2.5) × 8 ÷ 10 | **6.0** |
| Local media, known bias (1) | 8 / 10 | (1 × 2.5) × 8 ÷ 10 | **2.0** |
| UN agency (3) | 4 / 10 | (3 × 2.5) × 4 ÷ 10 | **3.0** |
| Curated dataset, e.g. ACLED (3) | 10 / 10 | (3 × 2.5) × 10 ÷ 10 | **7.5** |

The *same* credible document scores three times higher from a UN agency than from biased
media (6.0 vs 2.0) — that gap is exactly the source-reliability signal this design adds.

---

## 4. Source attribution

### 4.1 The source registry

Sources live in a central **source registry** (which already lists our automated data
feeds — IOM DTM, ACLED, GDACS, Dataminr — and manual field-officer entries). We extend
each registry entry with:

| Added to each source | Purpose |
|---|---|
| **Alias list** | so name variants ("IOM DTM", "Displacement Tracking Matrix", "DTM") resolve to one entry |
| **Reliability grade** | the 1–4 grade (or "ungraded") |
| **Organisation marker** | separates report publishers / cited organisations from the automated feeds |

### 4.2 How a figure gets a source

- The **publisher** of a report (known directly from the report's own metadata, no
  guessing) is recorded against that report. If a report lists several publishers (rare),
  we take the primary, otherwise the first.
- The **originating source** of a specific figure is read from the text by the AI
  ("according to IOM DTM…"). If none is stated, the figure **inherits the publisher**. So
  every figure always resolves to a reliability grade.
- Source is also captured as part of each passage's metadata, alongside location,
  timeline, and needs.

### 4.3 Keeping the registry clean

A source name is matched to a registry entry by, in order: an exact or alias match, then
a match on the organisation's website address, then a fuzzy text match — and only if all
fail is a **new entry created** (ungraded, awaiting a grade). This prevents the same
organisation being recorded under many spellings.

---

## 5. Source reliability

### 5.1 The scale (NATO Admiralty-style, from the underlying specification)

| Grade | Level | Indicative sources |
|--:|---|---|
| 4 | Completely reliable | Peer-reviewed top journals (Lancet, Nature) — rare |
| 3 | Usually reliable | UN agencies, major INGOs, curated datasets (ACLED) |
| 2 | Fairly reliable | Reputable national press, smaller established NGOs |
| 1 | Not usually reliable | Media with known bias / inconsistent accuracy |
| ungraded | Cannot be judged | New/unknown sources (treated as **1** by the score) |

### 5.2 Seed grades — **requires sign-off**

The initial grades to load into the registry. Editable at any time; the registry is the
source of truth.

| Reliability | Sources |
|--:|---|
| **4** | *(reserve — Lancet/Nature-tier; likely none in our corpus yet)* |
| **3** | OCHA · UNHCR · UNICEF · WHO · WFP · FAO · IOM / DTM · IPC / Cadre Harmonisé · FEWS NET · ACAPS · MSF · ICRC · NRC · Save the Children · ACLED · GDACS · NRC field officer |
| **2** | Reputable national newspapers · smaller established NGOs · Dataminr |
| **1** | Media with known bias / inconsistent accuracy |
| ungraded → 1 | New / unresolved sources pending grading |

*Open question for sign-off:* is the **NRC field officer** a 3 (our own trained staff) or a 2?

---

## 6. Information credibility (0–10, AI-assessed per figure)

Assessed **per figure**, falling back to a single **document-level** assessment where a
figure gives no signal for a criterion. Each criterion is rated **met (1) / partial
(0.5) / unmet (0)**; the weighted sum is the 0–10 score.

### 6.1 The eight criteria and their weights — **requires sign-off**

| # | Criterion | Weight | What it asks |
|--:|---|--:|---|
| 1 | Directness of observation | 2.0 | Firsthand vs. secondhand vs. aggregation of unnamed sources *(this is today's confidence rating, kept on its five-level scale)* |
| 2 | Recency | 1.5 | Is the figure fresh relative to its category's validity window? *(applied live — see §8)* |
| 3 | Attribution quality | 1.5 | Are claims attributed to identifiable sources, or anonymous/absent? |
| 4 | Internal consistency | 1.5 | Do figures and claims within the document agree with each other? |
| 5 | Plausibility in context | 1.5 | Are the claims plausible given a short country/crisis brief? |
| 6 | Geographic & temporal specificity | 1.0 | Is it located and dated precisely enough to act on? |
| 7 | Methodology transparency | 0.5 | Is the collection method / sample / coverage stated? |
| 8 | Representativeness | 0.5 | Does the stated scope match the claim (no over-generalising)? |
| | **Total** | **10.0** | |

Directness keeps five levels rather than three: **verified 1.0 · reported 0.8 ·
estimated 0.5 · media 0.3 · unverified 0.1** (multiplied by its weight).

### 6.2 When each criterion is measured

Seven of the eight are intrinsic to the document, so they are assessed once when a report
is first processed and stored. **Recency depends on today's date**, so it is applied at
the moment the data is read (see §8). This means quality never silently ages, and never
needs a scheduled recomputation just to stay current.

---

## 7. Aggregation: quality-weighted and bias-aware

The data-quality score replaces the old confidence-only measure everywhere and decides
which figure wins when reports disagree. Crucially, weak figures do not err symmetrically.

### 7.1 Per-metric quality bias — **requires sign-off**

| Metric(s) | Bias | Rationale |
|---|---|---|
| Population affected | **over-report** | widest-reach / round-up claims |
| People killed / injured, aid workers killed | **over-report** | media inflation of tolls |
| Displacement — IDPs, new displacements, returnees, refugees | **under-report** | incomplete movement capture |
| People in need (all sectors + overall) | **under-report** | access-limited undercount |
| Security incidents | **under-report** | under-recorded incidents |
| Funding required / received | **neutral** | reported precisely |

### 7.2 Metric → validity window & override divisor — **requires sign-off**

The window sets how long a figure stays "recent" (§8); the divisor controls how far back
a higher-quality figure may reach to override a fresher, weaker one.

| Metric(s) | Category | Window | Divisor |
|---|---|--:|--:|
| Killed / injured, security incidents, aid workers killed | Conflict events | 7 days | 2 |
| Displacement (IDPs / new / returnees / refugees) | Displacement | 30 days | 3 |
| Population affected | Operational updates | 30 days | 3 |
| Funding required / received | Operational updates¹ | 30 days | 3 |
| People in need (sectors + overall) | Needs assessments | 90 days | 3 |
| Food-security needs | Food security (IPC / Cadre Harmonisé) | ~120 days | 2 |
| Event types / clusters (labels) | — | n/a | — |

¹ Funding follows appeal cycles (quarterly-ish); 30 days is conservative.

### 7.3 Which figure wins

- The **freshest** figure wins, **unless** another figure has meaningfully higher data
  quality **and** is recent enough to be relevant (within the window ÷ divisor of the
  freshest).
- When two figures are of **comparable quality**, the tiebreak goes toward truth: the
  **lower** value for over-reported metrics, the **higher** value for under-reported ones.
- For **population affected** — a "highest wins" metric — we **discard the weakest quarter**
  of figures by quality first, then take the highest of the rest, so a single weak outlier
  can't inflate the ceiling.
- Point-in-time totals (current IDPs, people in need, funding required) keep "latest
  wins" — freshness is definitional there.

---

## 8. Freshness & retrospective corrections

- **Recency** is scored live: fully met if within the metric's validity window, partial
  if within twice it, unmet beyond. A freshly-published report about an *older* period
  still scores high (it's a fresh, reconciled account) and is filed against the period it
  describes — so old *content* is never penalised, only stale *publication*.
- A previously-planned "staleness flag" is **dropped**: low recency now expresses "no
  fresh reporting on this" as a continuous quality signal that flows into the number,
  rather than a separate badge. (Plain "as of N days ago" dates remain available.)
- **Retrospective corrections flow through automatically.** When a newly-ingested report
  revises an older period, the affected figure — and every roll-up above it (monthly →
  yearly → national) — is recomputed. The **figures** in any affected situation analysis
  are refreshed from the corrected numbers; its **written narrative is not** rewritten for
  small corrections (it remains a point-in-time account, optionally marked "figures
  revised").

---

## 9. What needs sign-off

| # | Decision | Where |
|--:|---|---|
| 1 | Source **reliability grades** (incl. field-officer 3 vs 2) | §5.2 |
| 2 | Credibility **criteria weights** | §6.1 |
| 3 | Per-metric **quality-bias** map | §7.1 |
| 4 | Metric **validity windows** & divisors | §7.2 |

All four are settings we can adjust after launch as the corpus teaches us more.

## 10. Rollout phases

1. **Source attribution** — the registry and matching; attach publisher + originating
   source to every figure and passage.
2. **Credibility scoring** — the eight-criterion AI assessment at ingest; store per figure.
3. **Data-quality aggregation** — the composite score, bias-aware winner selection,
   live recency, and retrospective corrections.

## 11. Assumptions & scope (to confirm during build)

- A report's primary publisher can be identified reliably (else we use the first listed).
- The eight credibility checks can be produced within our existing per-report AI
  processing, without a costly new step per figure.
- A situation analysis can refresh its **figures** independently of its **written
  narrative**, so small corrections don't trigger an expensive narrative rewrite.
- Introducing these scores re-processes the existing back-catalogue of figures once, so
  old and new figures are graded on the same basis.
- **Not in scope:** for under-reported metrics, quality-weighting corrects figures that
  are *too low*, but it cannot recover an observation a source omitted entirely (e.g. a
  district it never covered).

---

## Related materials (held by the engineering team)

- Two detailed engineering decision records — one for the source-attribution model, one
  for the quality-weighted, bias-aware aggregation.
- The underlying source-reliability & information-credibility specification this design
  implements.
- The existing datapoint extraction & aggregation design this design extends.
