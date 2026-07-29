---
status: accepted
---

# Route the cheap LLM steps by run scale: Claude for weekly, OpenRouter + Claude fallback for backfills

## Context

The knowledge-base pipeline makes three "cheap" LLM calls per report — **context**
(per-chunk contextual-retrieval prefix), **extraction** (per-chunk structured
tags), and **datapoints** (six-domain numeric extraction). Two very different
workloads run them:

- **Weekly run** — 5–10 new ReliefWeb reports. Latency-tolerant, tiny volume.
- **Backfill run** — 1,000+ reports at once (new country onboarding, schema-version
  re-extraction). Cost and reliability dominate.

We ran a Claude-parity eval (`src/clear_context_pipeline/defs/evals/`, results in
`evals/results/leaderboard.md` + `evals/results/COST_STRATEGY.md`) scoring cheap
OpenRouter models against Claude as the oracle, per step. This ADR records what we
decided from it.

Key cost facts (1,000 reports, ~25 chunks/report; see COST_STRATEGY.md):

- **Context is ~78% of all input tokens** — it re-sends the whole document on
  every chunk. Extraction sees only the chunk (cheap); datapoints is 6 whole-doc
  calls/report.
- A full backfill is a **low-hundreds-of-dollars, one-off** cost, not thousands.
- The weekly run costs **~$1** regardless of model.

## Decision

### 1. Route by run scale

- **Weekly run → all Claude.** At 5–10 reports the spend is ~$1; the marginal
  saving from a cheap model is cents and not worth any quality or reliability
  risk. Claude is the reference the whole design targets. Keep it simple.
- **Backfill run → OpenRouter cheap models + Claude fallback.** Here cost scales
  and a cheap tier saves ~$200–400 vs all-Claude-Sonnet. Reliability is handled by
  the fallback + timeout below, so a flaky cheap model can never stall the batch.

### 2. Per-step model choice (backfill)

Chosen on the eval's Claude-parity scores, gated on `validity`:

| step | model | evidence | why |
|---|---|---|---|
| **context** | `nvidia/nemotron-3-nano-30b-a3b:free` (free) or `qwen/qwen3-next-80b-a3b-instruct` (paid) | semantic cosine to Claude **0.824 / 0.911**, 100% valid | context is 78% of cost and the easy task; a free model this close makes it ~$0 |
| **extraction** | `qwen/qwen3-235b-a22b-2507` | F1 **0.542**, 92% valid, $0.09/$0.55 | best value; extraction is only ~$15/1k reports so the choice is low-stakes |
| **datapoints** | Claude Haiku 4.5 (default) or `moonshotai/kimi-k2-0905` | kimi figF1 **0.580**, value-agree **0.790** | numbers feed the dashboard — wrong figures are the costly error; keep on Claude unless a spot-check clears a cheap model |

**Explicitly rejected: `qwen3-235b` for context.** Under the old *lexical* context
metric it looked average (0.120) like everything else. The *semantic* re-score
exposed it at **0.438** — roughly half the leaders (0.91) and below a free model
(0.82). It is a good *extraction* model and a weak *context* one; do not conflate.

### 3. Context step is plain text, not structured output

`enrich._run_context` now uses `complete_text` (a single free-text prefix), not a
`ChunkContext` JSON schema. Rationale: the output is one string, so the schema
bought nothing but a parse step that models fond of ` ```json ` fences or empty
bodies failed on — and it needlessly required structured-output support for the
pipeline's cheapest, highest-volume step. Consequences: any chat model can now do
context (the model pool for it is unconstrained), and the JSON-parse failures that
plagued cheap models on this step are gone. Extraction and datapoints keep
structured output — they have genuinely multi-field / nested schemas.

### 4. Resilience is mandatory for any non-Claude model

A flaky model must never stall or drop work. Three mechanisms in
`providers/llm.py`:

- **Fast-fail timeout — 90s** (`LLM_TIMEOUT_SECONDS`, was 600s). A hang fails in
  90s, not 10 minutes. This alone was the root cause of eval runs stalling.
- **Claude fallback** (`FallbackProvider`). On any primary failure/hang the call
  is transparently re-served by a reliable provider — the pipeline never stalls,
  no chunk/report is lost. Configured with
  `LLM_<ROLE>_FALLBACK_{PROVIDER,MODEL,API_KEY,BASE_URL}`.
- **Circuit breaker** — after 2 consecutive primary failures, route straight to
  the fallback for a 5-minute cooldown, so a persistently-down model stops costing
  a per-call timeout and the batch continues at fallback speed.

Production config for a cheap-primary step is therefore: cheap model as primary +
Claude Haiku as `*_FALLBACK_*`. Cheap when it works, Claude the instant it doesn't.

### 5. Cost levers for backfill

- **Prompt caching** on context + datapoints (they re-send the doc) — the single
  biggest lever, ~5× input reduction. Free with Claude (`cache_control`); add it to
  the OpenRouter provider for cache-capable routes before relying on it there.
- **Batch API** (50% off) for any Claude step in a backfill — it isn't
  latency-sensitive.
- **Guardrail** every backfill with `KB_MAX_COST_USD_PER_RUN` + the skip switch.

### 6. Scoring methodology (for future re-runs)

The eval measures **Claude-parity**, not ground truth (a candidate that copies
Claude's mistakes scores well; pair datapoints with a human spot-check). Per step:

- **context → semantic cosine** over the RAG embedding model (its real downstream
  job). Lexical difflib is retained only as a fallback when no embedding key is set
  — it systematically under-scores paraphrases and must not be the basis for a
  decision (`EVAL_CONTEXT_SCORER`).
- **extraction → set-F1** over event-types / sectors / locations + exact time.
- **datapoints → figure P/R/F1 + value-agreement at 1% tolerance**
  (`EVAL_DP_REL_TOL`) so rounding/re-quoting isn't counted as disagreement.

Read `validity` (fraction of calls that succeeded) first — a high similarity over
few valid calls is noise.

## Consequences

- **Cost expectations.** Weekly ≈ $1. Backfill (1,000 reports): cheap tier
  ~$70–100 · quality-tiered (Claude datapoints) ~$120–170 · all-Claude-Sonnet
  cached+batch ~$400. Spend the quality budget on datapoints; make context cheap;
  extraction is nearly free either way.
- **Two model sets to maintain** (weekly Claude vs backfill cheap+fallback). Both
  are pure env config — no code branches — so an operator swaps them per run via
  `LLM_<ROLE>_*` without a deploy.
- **The eval harness is kept** (`defs/evals/`) and is cache-aware, so re-running
  when a model or price changes is cheap. Re-score existing candidates from cache;
  only new models generate.
- **Free-tier OpenRouter is not dependable.** Every *free structured* model we
  tried hung or emitted invalid JSON — gemma-4:free, gpt-oss-20b:free,
  nemotron-nano-9b:free (hangs), plus kimi-k2.5 / qwen3.5-35b (empty JSON). They
  are commented out in `candidates.py` with reasons. Free models are usable *only*
  behind the fallback + timeout, and only the plain-text context step tolerated
  them (nemotron-3-nano-30b:free scored 0.824 there). For anything you need
  *scored reliably*, use the paid route.
- **Cache staleness caveat when the context prompt/path changes.** Existing context
  outputs cached under the old structured path are re-scored as-is; a low-validity
  row may reflect that stale failure, not plain-text quality (the *paid* nemotron
  reads 0.008 while its *free* plain-text sibling reads 0.824). Regenerate context
  with `EVAL_FORCE=1` for a strict apples-to-apples comparison.

## Related

- `evals/results/leaderboard.md` — the scored comparison this ADR draws on.
- `evals/results/COST_STRATEGY.md` — the token/cost model and per-step breakdown.
- `src/clear_context_pipeline/defs/evals/candidates.py` — the candidate set +
  drop reasons.
- `src/clear_context_pipeline/providers/llm.py` — `FallbackProvider`, timeout,
  circuit breaker.
- ADR-0001 / ADR-0002 — the datapoints extraction + aggregation decisions this
  pipeline's numbers depend on.
