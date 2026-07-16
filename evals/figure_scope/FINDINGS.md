# SPIKE #271 — Can the LLM reliably identify a figure's Figure Scope?

**Answer: Yes, with one caveat — trust it for the *place*, not the *admin level*.** Recommend proceeding with #272 (Extract Figure Scope), with the division of labour below.

## Method

Controlled eval, 16 authored figures with known-correct scope labels, covering the failure modes the ticket names. Run through the pipeline's own provider (`make_llm_provider("extraction")` → `complete_structured`) on the configured model, **claude-sonnet-4-6**. Each case gives the model report text + one named figure and asks for that figure's single geographic scope, or `unresolvable`. Run **3×** to test stability.

Controlled rather than real-corpus because the question is capability, and authored ground truth measures a hit rate without a human labeller. Harness is rerunnable for a real-corpus pass later.

## Results — stable across all 3 runs (identical every time)

| Metric | Value |
|---|---|
| Acceptable (correct + defensible) | **94%** (15/16) |
| Strict (exact name + admin level) | **81%** (13/16) |
| scope-vs-mention (the load-bearing case) | **5/5** |
| unresolvable (correct abstention) | **2/2** |
| implied-by-framing | **2/2** |
| multi-area | 2/2 (parent when one exists, else unresolvable) |

Zero variance between runs — same verdicts, same single failure, all three times.

## What this establishes

1. **The core discriminator works reliably.** Every scope-vs-mention case passed all 3 runs: the model separates "the place the figure is a total *for*" from places the report merely mentions. A figure stated for El Fasher inside a Sudan-framed report → El Fasher, not Sudan. A killed figure for El Geneina while Port Sudan is named for aid → El Geneina. This is the exact capability the whole Figure Scope fix depends on, and it's the thing the original bug got wrong by fanning across every mentioned place.

2. **`unresolvable` must be a first-class outcome, and the model uses it correctly.** It abstained on "thousands affected" (no place) and "across the region" (vague), and — unprompted by any single-run fluke — chose unresolvable on the multi-state case with no common parent. It did **not** default to the country or the first-mentioned place, which is the failure the ticket worried about. Answering the ticket's open question directly: yes, add `unresolvable`; the model will reach for it rather than hallucinate.

3. **The one consistent weakness is admin-level precision, not place identification.** The only failure, all 3 runs, was Zamzam *camp* labelled L3 where the gazetteer would call it L2 — a genuinely debatable boundary. The model got the place right and only the level wrong. Across the set, wrong-level was the sole error mode; it never invented a place or mis-attributed a figure to the wrong location.

## Recommendation for #272

**Figure Scope is a `locations` table id — same as `locationIds` already is.**
The location ids on `report_datapoints` and `aggregated_datapoints` are real
`locations` rows (the pipeline stores resolved ids, not pcodes or free text),
and each row carries `level` and `ancestorIds`. So Figure Scope isn't a new
kind of value — it's one resolved location id per figure, and aggregation keys
on it directly, consistent with the existing pattern.

**Split the responsibility accordingly:**

- **LLM emits `{ location_name, unresolvable, reasoning }`** — what it's reliably good at (naming the scope, or abstaining). It does NOT emit an admin level.
- **The existing location resolver turns `location_name` into a `locations` id.** `admin_level` and `ancestorIds` then come **free from the resolved row** — they are properties of the location record, not LLM outputs.

This **removes the one weakness the eval found outright.** The only miss across all runs was admin-level precision (Zamzam camp L2/L3). Because level is read from the `locations` row rather than guessed by the model, that error mode cannot occur once the name resolves — the LLM's job shrinks to the thing it did at ~100%: identifying the place, or abstaining.

Concretely, the extraction schema carries `location_name` + `unresolvable` per figure; the resolve step (already built for the vector path) yields the id, level, and ancestor chain. An unresolvable figure — or a name the resolver can't match — is excluded from cross-report dedup and never rolled up (matching §6.4.1's rule for unresolved locations). Storing the id also makes descendant-aware bucketing a plain `ancestorIds` lookup (see the related #275 fix), rather than anything the aggregator has to infer.

**Residual risk to watch in #272:** the load-bearing step is now the resolver matching the LLM's free-text name to a `locations` row. An unmatched name fails safe (treated as unresolvable, no silent wrong bucket), but its frequency on the real corpus is the thing to measure next — that's the resolver-match rate, not an LLM-accuracy question. A real-corpus validation pass (needs clear-api/S3 up) would quantify it; the controlled eval can't.

## Not done / out of scope

- No real-corpus run (needs local services + data). Harness is ready for it.
- No production schema or prompt merged (per the spike's own criteria).
- Glide-code / synonym canonicalisation of event types is a separate concern (#270 follow-up), untouched here.
