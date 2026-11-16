# Web search backends for gap-fill

> Decision record for the search/fetch stack behind `analysis_web_gapfill` — the
> conditional step (doc §9 in `analysis-module-agentic-dag.md`) that reaches the
> open web only for the gaps the knowledge base can't answer.
>
> **Decision: Brave Search API — LLM Context endpoint (provisional).** Companion
> to the `analysis-web-gapfill-brave` memory. Brave is the default, but two of
> its three selling points here — ZDR privacy and low self-serve cost — depend on
> facts that only hold on an **enterprise** plan, and the source-control claim is
> partial, not full. So this is "Brave, pending the §5 open items": confirm
> ZDR + storage rights with Brave sales, run a multilingual bake-off, and ship a
> fallback provider from day one. Pricing is list-rate (Nov 2026, verified against
> brave.com/search/api) and drifts — treat as order-of-magnitude.

## 1. What the gap-fill actually needs

The web agent is not "ask the web a question." It must return material our own
code can **classify, sanitise, and cite** (§9). That shapes the whole
comparison — several popular options fail not on quality but on *control*.

- **Returns URLs + the page content we act on** — we classify each source into
  the gov › orgs › news › social › unknown tiers and run the injection-guard
  ourselves. An API that only returns a synthesised answer removes that control.
- **Search + extraction in one hop (ideally)** — avoids standing up a separate
  fetch / boilerplate-strip / dedupe pipeline.
- **Independence + data privacy** — building for NRC: minimise Big-Tech
  dependency and query-data exposure (retention + reseller ToS risk both count).
- **Bounded, predictable cost** — gap-fill is conditional + capped (top-N gaps ×
  budget), so per-query cost and a free dev tier matter more than bulk throughput.
- **Freshness + coverage** — humanitarian situations move weekly; the index must
  be current and broad (gov/UN/NGO/news).

## 2. At a glance

"Source control" = do we get the raw URLs + content *before* any synthesis,
enough to run §9 tiering.

| Option | Type | Own index | Extracted content | §9 source control | Privacy | ~ Price | Self-host | Fit |
|---|---|---|---|---|---|---|---|---|
| **Brave (LLM Context)** | Search + extract | ✅ independent | ◑ pre-ranked chunks | ◑ chunks, not full page | ◑ ZDR enterprise-only; std DPA on self-serve | ~1k free ($5 credit/mo) · $5/1k | ❌ | **Best fit (provisional)** |
| **Tavily** | Search + extract | ❌ aggregates | ✅ | ✅ Full | ◑ standard retention | Free 1k/mo · ~$8/1k adv | ❌ | Strong alt |
| **Exa** | Neural search + contents | ◑ own crawl (semantic) | ✅ | ✅ Full | ◑ standard | ~$7/1k + $1/1k contents | ❌ | Good (discovery) |
| **Serper / SerpAPI** | Google SERP reseller | ❌ Google | ◑ scrape endpoint (2nd call) | ✅ raw SERP + position | ❌ Google-dependent | Serper ~$0.3–1/1k · SerpAPI ~$15/1k | ❌ | **Recall provider (integrating)** |
| **Perplexity Sonar** | Answer API (LLM) | ❌ | ◑ answer + citations | ❌ pre-synthesised | ❌ retention | tokens + $5–14/1k search | ❌ | Poor (control) |
| **Bing Web Search** | Search | (was independent) | ❌ | — | — | — | ❌ | Retired (Aug 2025) |
| **Google PSE / Vertex grounding** | Search / grounding | ❌ Google | ◑ grounding only | ◑ limited | ❌ Google-dependent | PSE capped · Vertex metered | ❌ | Weak |
| **SearXNG (self-host)** | Meta-search | ❌ aggregates | ❌ | ◑ URLs only | ✅ full (self-run) | infra only | ✅ | Niche |
| **Jina Reader/Search** | Extract (+ search) | ❌ | ✅ reader | ✅ Full | ◑ standard | cheap / free tier | ◑ | Complement |
| **Model-native web** (OpenRouter `:online`, Anthropic `web_search`) | LLM + web plugin | ❌ | ❌ ingested by model | ❌ none | ❌ varies | per-token + per-search | ❌ | Poor (control) |

## 3. Option by option

### Brave — LLM Context  (search + extract · independent index)
`GET /res/v1/llm/context` · Search plan **$5/1k** (no free tier; a **$5/mo
credit ≈ 1k reqs** stands in) · Answers plan $4/1k + $5/M tokens · enterprise
custom

The only at-scale search API backed by its **own** web index (~35B pages,
crawled daily) rather than reselling Google/Bing. The **LLM Context** endpoint
does search *and* returns pre-extracted, relevance-ranked chunks per source with
metadata (`site_name`, dates) in a single sub-second call — URLs + content to
tier and sanitise. Two caveats the §2 table now reflects:

- **It returns chunks Brave has already chosen and ranked, not the full page.**
  Our injection-guard and source-tier classifier only ever see Brave's excerpt,
  so a chunk can be cited missing the sentence that qualifies it, and the
  freshness/`site_name` metadata is Brave's extraction, not ours. §9 control is
  therefore **partial**; for high-stakes claims, fetch the full source page
  through the seam.
- **Zero Data Retention is enterprise-only.** Brave's [ZDR
  announcement](https://brave.com/blog/search-api-zero-data-retention/) is
  explicit: ZDR is "available to all Brave Search API customers **on a custom
  enterprise plan**," enabled by contacting support — *not* the self-serve
  default. On a self-serve key we have only Brave's standard DPA, which is close
  to what Tavily offers (and Tavily was set aside partly on privacy). Storage
  rights — needed because CLEAR keeps and cites the extracted chunks — are
  likewise tier-gated ("a plan that includes storage rights").

**Verdict:** Best fit *for the independent-index and single-hop shape*, but the
privacy and cost edges hold only on an enterprise contract. *Watch:* raw ranking
can trail Google on long-tail, local-language and regional sources — the exact
humanitarian gap case, and the reason for the §5 bake-off; one vendor, no
self-host.

### Tavily  (search + extract · AI-agent oriented)
free 1k credits/mo · $0.008/credit (basic 1, advanced 2) → ~$8/1k advanced

The most popular agent-search API: purpose-built for LLM retrieval, clean
results + extracted content, easy to integrate. But it **aggregates other
engines** rather than running its own index, and retains queries under standard
terms — so it inherits upstream dependencies and offers less privacy
independence than Brave.

**Verdict:** Strongest drop-in alternative; the natural second provider behind a
fallback. Passed on only for the independence/privacy axis that matters for NRC.

### Exa  (neural / embeddings search + contents)
~$7/1k search · agentic ~$12/1k · contents +$1/1k (first 10 full-text now free)

Semantic/neural search over its own crawl — excellent for "find pages *like*
this" discovery and research-style queries where keyword SERP is weak. Returns
contents too. Different ranking philosophy (embeddings vs keyword), a strength
for fuzzy gaps and a weakness for precise named-entity lookups.

**Verdict:** Good, especially exploratory gaps; a credible alternative/complement.
Keyword precision + independence favour Brave for the default.

### Serper / SerpAPI  (Google SERP resellers)
Serper ~$0.3–1/1k (cheapest here) · SerpAPI ~$15/1k

Relay **Google** results — the best raw ranking and, critically, **Google's
multilingual index**: `gl`/`hl` give real Google recall on regional queries
(e.g. `gl=sa&hl=ar` for Arabic/Sudan) that Brave's and Tavily's own indexes
underserve. Serper returns in 1–2s at 1 credit/query, with raw `organic[]`
(title/link/snippet/position) — ideal for §9: we tier Google's own candidate set.
The old "snippets only" knock is now softened — Serper has a separate
**`scrape.serper.dev`** endpoint (full page → markdown + metadata), so richer
content is a second call, not a bespoke pipeline. Residual costs: Google
dependency + ToS risk, no privacy independence, and two calls for full content.

**Tavily vs Serper (the head-to-head, 2026-11-16).** Both beat Brave on raw
content (Tavily Extract / Serper scrape) and both let us bring our own source
ranking. The decider is **recall**: Serper is the only one serving *real Google*,
so for the Arabic/regional gap that drove this search it closes the gap, while
Tavily — its own index, no Google, no Arabic claim — is a lateral move from
Brave on that axis. Tavily wins on single-vendor integration + a more mature
Extract; Serper wins on the one axis that matters here.

**Verdict:** Now the chosen **recall provider** — integrating behind the
`providers/` seam (`WEB_SEARCH_PROVIDER=serper`), with Brave still the default and
Serper opt-in for the Arabic/regional bake-off. We accept the Google dependency
*for recall* precisely because that recall is the gap; the independence argument
stays for the default. (`scrape.serper.dev` maturity + exact locale params still
to confirm against Serper's live docs.)

### Perplexity Sonar  (answer API — its own LLM)
model tokens + per-request search fee ~$5–14/1k

Returns a **synthesised answer + citations**, not a controllable result set.
That breaks §9: we can't classify/tier or sanitise sources *before* a model
ingests them — the trust judgement happens inside Perplexity. It's also itself
an LLM, duplicating our DeepSeek synthesis.

**Verdict:** Poor fit for this design — an answer engine, when we want raw,
tiered evidence. Fine for a human research surface, not our gap-fill.

### Bing Web Search API  (retired Aug 2025)
Microsoft **retired every Bing Search sub-API on 11 Aug 2025**, steering users to
an Azure AI Foundry agent ("Grounding with Bing Search") — a full platform
commitment, not a key swap, and a grounding tool rather than a raw search API.
This retirement is a big part of *why* the independent-index options now matter.

**Verdict:** Unavailable as a plain API; the Azure path is heavy and
Microsoft-locked. Not considered.

### SearXNG (self-hosted meta-search)  (max data control)
infra cost only · open source

Self-hosted meta-search aggregating other engines — the only option giving
**full data control** (queries never leave our infra) at zero per-query cost.
But **no index of its own** (quality = whatever upstreams it scrapes, with
ToS/breakage risk), results-only (we add extraction), and ours to operate.

**Verdict:** Niche — compelling only if data sovereignty becomes a hard
requirement and we accept the ops + quality variance. The "full control" fallback.

### Jina Reader / Search  (extraction-first)
cheap · free tier (`r.jina.ai` reader, `s.jina.ai` search)

Best known for its **reader** (clean URL → markdown extraction) and a
lightweight search. More a high-quality *fetch/extract layer* than a primary
index. Useful **paired** with a results-only SERP — but that's two vendors doing
what Brave LLM Context does in one.

**Verdict:** A good complement, not a primary. Redundant given Brave extracts.

### Model-native web  (OpenRouter `:online`, Anthropic `web_search`, Vertex grounding)
per-token + per-search, varies

Append web to the model so it searches + reads inline — lowest friction, reuses
the existing OpenRouter key. But the model **ingests results directly**: no
URLs/content for us to classify, so §9's source-tiering + per-source
sanitisation can't run. Also couples retrieval to one model vendor.

**Verdict:** Poor fit given we want the source ranking. The low-friction option
we explicitly declined for losing control of the tiering.

## 4. Why Brave for CLEAR specifically

- **Independence** — after Bing's API retirement, Brave is **one of the few**
  at-scale search APIs not reselling a Big-Tech SERP (Exa runs its own crawl too;
  Mojeek, Linkup and You.com aren't compared here). For a humanitarian org that's
  a resilience and neutrality argument — no Google/Microsoft dependency in the
  evidence path. *Caveat:* independence from Big Tech is **not** independence from
  US jurisdiction — Brave is a US company, so sending potentially sensitive
  crisis queries still carries a GDPR data-transfer question for NRC (Norwegian).
- **Privacy** — a **potential** edge, not a given. True Zero Data Retention is
  **enterprise-only** (see §3); on a self-serve key we have only the standard
  DPA, roughly on par with Tavily. ZDR for the country/crisis queries we send is
  worth having, but it means an enterprise contract and written confirmation, not
  a self-serve API key. **Open — confirm with Brave sales before relying on it.**
- **§9 fit** — LLM Context hands back URLs + ranked extracted chunks + `site_name`
  in one call (no separate scrape pipeline) — but the chunks are **Brave's
  selection**, not the full page, so source-tiering and the injection-guard run on
  excerpts. Good enough for most gaps; **partial**, not full, control (see §3).
- **Cost shape** — gap-fill is conditional + capped, so volume is low and cost is
  a non-issue on paper: **$5/1k** on the Search plan, with the **$5/mo credit
  (~1k reqs)** covering dev. The caveat is that if ZDR or storage rights push us
  to enterprise, "cost" becomes a negotiated contract, not a metered key.

## 5. Open items — do these before committing

The review of this record (2026-11-06) turned up enough stale/overstated facts
that Brave is the *default*, not a settled choice. Four things to close first:

1. **Confirm ZDR + storage rights with Brave sales** (`api-sales@brave.com`) —
   in writing, for the plan we'd actually buy. This decides whether Brave is a
   self-serve key or an enterprise contract, and changes both cost and
   procurement. Until then, don't claim ZDR in anything NRC-facing.
2. **Run a multilingual bake-off** — 30–50 real gap queries across 2–3 current
   crises in local languages (Arabic/Sudan, French/Sahel, …), through Brave,
   **Serper** (`gl`/`hl` per locale), Tavily and Exa, scored on whether the right
   sources come back and how fresh they are. The long-tail/regional ranking risk
   is the biggest quality unknown and it can't be settled by desk research —
   Serper (real Google) is the hypothesis to beat for Arabic recall.
3. **Build the fallback / recall provider now, not "later."** — *In progress
   (2026-11-16):* **Serper** is being integrated behind the `providers/` seam
   (`WEB_SEARCH_PROVIDER=brave|serper`) as the Google-recall provider for the
   Arabic/regional gap. Brave stays the default; Serper is opt-in until the
   bake-off decides. This also gives gap-fill a second vendor so a Brave plan
   change or outage no longer fails it quietly.
4. **Keep this record honest** — the pricing, privacy and source-control claims
   above were corrected on 2026-11-06; re-verify before the next reader relies
   on them.

## When we'd switch the default away from Brave

The choice is behind a small client (`providers/brave_search.py`) and a
source-classifier seam, so switching is cheap. Triggers:

- **The bake-off shows weak long-tail/regional recall** — promote **Exa**
  (semantic) or **Serper** (Google ranking) from fallback to primary, keeping our
  own extraction/tiering.
- **Data sovereignty becomes mandatory** (or the US-jurisdiction concern bites) —
  stand up **SearXNG** and own the extraction layer.
- **§9 control stops mattering** — if the model can be trusted to self-tier, the
  OpenRouter `:online` route becomes the zero-infra option.

## Sources

Accessed Nov 2026; Brave pricing + ZDR re-verified against brave.com on
2026-11-06 (no free tier; $5/1k Search; ZDR enterprise-only). Pricing is
list-rate and drifts — re-check before relying on it.

- [Brave Search API](https://brave.com/search/api/) · [LLM Context docs](https://api-dashboard.search.brave.com/documentation/services/llm-context) · [Zero Data Retention](https://brave.com/blog/search-api-zero-data-retention/) · [best search APIs 2026](https://brave.com/learn/best-search-api-2026/)
- Bing API sunset (Aug 2025): [WebProNews](https://www.webpronews.com/microsoft-to-sunset-bing-search-apis-by-august-11-2025-pushing-developers-toward-azure-ai-integration/) · [Linkup — alternatives](https://www.linkup.so/blog/what-s-the-best-alternative-to-the-bing-search-api)
- Pricing/comparisons: [AI search API pricing 2026](https://www.buildmvpfast.com/api-costs/ai-search) · [Tavily vs Exa vs Serper](https://unknownproxies.com/blog/guides/tavily-vs-exa-vs-serper) · [Perplexity Sonar pricing](https://www.cloudzero.com/blog/perplexity-api-pricing/) · [web search APIs for agents](https://you.com/resources/best-web-search-apis-for-ai-agents)
