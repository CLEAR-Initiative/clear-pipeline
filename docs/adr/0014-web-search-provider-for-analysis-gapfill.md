---
status: accepted
---

# Web search for analysis gap-fill: Serper (Google SERP) as the default, Brave as the swappable alternative

## Context

The analysis module's `analysis_web_gapfill` asset runs a web search to fill
evidence gaps the judge flags before revision (the descriptive fan-out →
`analysis_gaps` → `analysis_evaluation` (judge) → `analysis_web_gapfill` →
`analysis_revision` chain). The search provider sits behind a seam —
`resolve_web_search()` in `providers/web_search.py` returns `(client,
api_key_env, name)`, selected by the `WEB_SEARCH_PROVIDER` env var — so the
provider is swappable without touching the asset.

We evaluated three candidates for what backs that seam:

- **Brave Search API (LLM Context endpoint)** — Brave operates its **own
  independent web index** (not a Google/Bing proxy). Self-contained, one key.
- **Serper (serper.dev)** — a thin, fast API over **Google's real SERP**
  (`google.serper.dev/search`), with `gl`/`hl` locale controls and full-content
  scraping via `scrape.serper.dev`. Google recall, which matters for
  multilingual humanitarian signals where Google's coverage of local-language
  and regional sources is strongest.
- **Jina (`s.jina.ai`)** — a hosted search-and-read endpoint.

The head-to-head (Tavily was also compared) lives in
`docs/web-search-stack-comparison.md`. This ADR records the decision and,
specifically, why **Jina was evaluated and not adopted** — the finding that
settled it.

### Why Jina is not a provider alternative

Reading Jina's own open-source backend (`jina-ai/reader`, the branch Jina
publishes as the code behind `s.jina.ai`), its `iterProviders()` selection in
`src/api/searcher.ts` is explicit: the `engine` param accepts only
`google | bing | reader`, and the **default path prefers `serperGoogle`
(serper.dev's Google API) whenever a `SERPER_SEARCH_API_KEY` is set**, falling
back to Jina's own Puppeteer-based Google scraping, then Bing. Jina runs **no
independent web index** for live SERP (its `reader` engine queries only Jina's
private crawl cache — narrower than live Google). `architecture.md` confirms it:
"Reader primarily relies on external SERP providers for web search results."

So Jina's hosted search is, in its primary path, **a Serper → Google call with a
content-extraction (Reader) layer on top** — the same stack we already run
directly (Serper for recall + `scrape.serper.dev` for content). Self-hosting
Jina (Apache-2.0 OSS image) does **not** escape this: the self-host build still
needs an upstream SERP key (`SERPER_SEARCH_API_KEY`, or BrightData/Thordata/
Jina's hosted SERP) and has no SearXNG/own-index option. Adopting Jina would
mean paying Jina to call the Serper/Google we already call, for the marginal
benefit of its Reader extraction — which competes with `scrape.serper.dev`,
already wired and cheaper to operate.

## Decision

**Default the analysis web gap-fill to Serper; keep Brave as the one explicit
alternative behind the same seam; do not adopt Jina (or Tavily) as a provider.**

1. **Serper is the default.** `resolve_web_search()` resolves to Serper unless
   `WEB_SEARCH_PROVIDER` is set otherwise; an unknown value falls back to Serper
   rather than erroring. Google recall + locale controls (`gl`/`hl`, defaulted
   from `SERPER_GL`/`SERPER_HL`) best fit multilingual, regional humanitarian
   sources.
2. **Brave stays as the swappable alternative.** Set `WEB_SEARCH_PROVIDER=brave`
   to route to Brave's independent index — the one genuinely *different* recall
   source, kept for when we want index diversity or to A/B against Google recall.
3. **Content enrichment is Serper's own scrape.** `serper_search.py` enriches
   SERP hits with `scrape.serper.dev` (gated by `SERPER_SCRAPE`, best-effort —
   a scrape failure degrades to the snippet, never fails the gap-fill). This is
   the extraction layer Jina's Reader would otherwise provide, without the extra
   hosted dependency.
4. **The seam is the contract, not the vendor.** Any future provider is a new
   `*_search.py` client + a branch in `resolve_web_search()` + its key env. The
   asset consumes `(client, name)` and surfaces the provider in run metadata;
   `WebSearchError` is the common failure type the asset catches.
5. **Jina/Tavily are recorded as evaluated-not-adopted** (this ADR +
   `web-search-stack-comparison.md`). Revisit Jina's *Reader* (not its search)
   only if concrete extraction-quality failures appear that `scrape.serper.dev`
   cannot handle — and even then as a parsing upgrade, not a search provider.

## Consequences

- **One default, one alternative, one seam.** Operators flip Brave↔Serper via
  `WEB_SEARCH_PROVIDER` with no deploy; the pipeline fetches only the selected
  provider's key.
- **Infra fetches only what's used.** In `clear-infrastructure`, the analysis
  secret group pulls `serper_api_key` **or** `brave_search_api_key` based on
  `dagster_web_search_provider` (default `serper`), gated by `analysis_enabled`,
  so an env that hasn't provisioned a key still plans/applies cleanly (infra
  PR #87; dagster.env wiring PR #86).
- **No independent-index default.** Serper is Google-backed, so our default
  recall is Google's. If Google coverage ever proves a blind spot for a locale,
  Brave (independent index) is already one env var away — that's the reason it's
  retained rather than removed.
- **Jina/Tavily dependencies avoided.** No second hosted search vendor, no extra
  key to rotate, no Dockerized Chrome service in the Dagster VM for a self-hosted
  Reader we decided we don't need.

## Alternatives considered

- **Brave as the default** — rejected: its independent index trades away Google's
  stronger local-language/regional recall, which is the gap-fill's whole point.
  Kept as the alternative for exactly the cases where index diversity helps.
- **Jina (`s.jina.ai`), hosted or self-hosted** — rejected: it resells
  Serper/Google (its default path prefers serper.dev) and runs no own index, so
  it adds a vendor without adding recall; self-hosting still needs a SERP key and
  a Chrome service. Its Reader may return later as an *extraction* upgrade, not a
  search provider.
- **Tavily** — compared in `web-search-stack-comparison.md`; not adopted (see
  that doc for the head-to-head).

## Related

- `docs/web-search-stack-comparison.md` — the Brave/Serper/Tavily/Jina
  head-to-head this ADR draws its conclusion from.
- `src/clear_pipeline/providers/web_search.py` — `resolve_web_search()` seam,
  `WebSearchError`, default/override logic.
- `src/clear_pipeline/providers/serper_search.py` — Serper client +
  `scrape.serper.dev` enrichment.
- `src/clear_pipeline/providers/brave_search.py` — Brave client
  (`BraveSearchError(WebSearchError)`).
- `src/clear_pipeline/defs/analysis/assets_gaps.py` — `analysis_web_gapfill`
  wired to the resolver.
- ADR-0003 — the LLM model-selection-by-run-scale decision for the KB pipeline's
  cheap steps (same "config, not code branches" posture for provider choice).
