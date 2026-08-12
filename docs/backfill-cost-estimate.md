# Backfill cost estimate — 5,000-report re-extraction

**Status:** estimate · **Scope:** one-off backfill of ~5,000 reports through the
knowledge-base pipeline (context → extraction → datapoints), plus the downstream
Claude aggregation/narrative step.

Token volumes are the pipeline's own tracked model from
[`evals/results/COST_STRATEGY.md`](../evals/results/COST_STRATEGY.md) (**±30%**).
Prices as of 2026-08-10 (Claude Sonnet 5 intro $2/$10 through 2026-08-31). Qwen
OpenRouter prices from [ADR-0003](./adr/0003-llm-model-selection-and-resilience.md);
Azure GPU prices from the Azure retail-prices API (US East, Linux) — approximate,
region/commitment move them.

---

## TL;DR

The **OpenRouter-vs-Azure answer flips on model size**:

- **Big models** (ADR-0003 picks: qwen3-235b, qwen3-next-80b, full precision) →
  **OpenRouter wins.** Self-hosting a 235B needs A100-class nodes (~$27/hr for
  8×A100); days of runtime = **$500–1,000+**. OpenRouter is **~$295**.
- **Your small local models** (9B / 35B-A3B / 26B-A4B, Q4/Q8 GGUF) → **self-deploy
  wins on dollars.** They run on cheap T4/A10 GPUs, so you rent compute-time
  (~$40–100 PAYG / ~$15–30 Spot) instead of paying OpenRouter's per-token bill —
  **but** it's days of wall-clock + you own the inference ops, and fidelity is
  lower than the big full-precision models.

Datapoints stay on **Claude Haiku 4.5 (~$190)** in every scenario — small Q4
models are too risky for dashboard numbers (ADR-0003's core reason). "Aggregation
on Sonnet" is **~$0** (it's deterministic TypeScript; narrative generation is a
fixed ~$10–30, per-country not per-report).

**All-in for 5,000 reports (context+ext by route, + Haiku datapoints):**

| Route (context + extraction) | Cloud $ | Wall-clock | Ops | Model fidelity |
|---|--:|---|---|---|
| OpenRouter, uncached | ~$485 | **hours** | none | ADR-tested (big, FP) |
| OpenRouter, context caching on | ~$300 | hours | none | ADR-tested |
| **Azure single A100 self-host** | **~$235** | ~½–1 day | moderate | your models |
| **Azure T4 self-host (PAYG)** | **~$210–290** | 3–8 days | you run vLLM | your models |
| **Azure T4/A10 self-host (Spot)** | **~$205–250** | 3–8 days* | + checkpointing | your models |
| **Owned T4** (already have it) | **~$195** | 3–8 days | already running | your models |

*Spot instances are preemptible — risky for a multi-day job without checkpointing.

---

## Per-report token model (COST_STRATEGY.md)

~25 chunks/report, ~18k tokens/doc. Per **1,000** reports:

| Step | Calls | Input tok | Output tok | Notes |
|---|--:|--:|--:|---|
| Context | 25k | 450M | 5M | whole doc re-sent per chunk → **78% of all input** |
| Extraction | 25k | 20M | 12M | sees only the chunk |
| Datapoints | 6k | 108M (→~27M cached) | 9M | 6 whole-doc domain calls/report |

**×5 for 5,000 reports:** Context 2,250M/25M · Extraction 100M/60M · Datapoints
540M gross (~135M cached) / 45M.

Context + extraction, **with** prompt caching (context ~5×) + batching ≈ **655M
tokens** to process; **without** either ≈ 2,435M (weeks on one card — both are
mandatory for self-host).

---

## Option A — OpenRouter (hosted API, ADR-0003 models)

| Stage | Model | Price ($/M in→out) | Cost (5k) |
|---|---|---|--:|
| Context | qwen3-next-80b | $0.10→$1.10, uncached | ~$253 |
| Extraction | qwen3-235b-2507 | $0.09→$0.55 | ~$42 |
| Datapoints | Claude Haiku 4.5 | $1→$5, cached + Batch | ~$190 |
| **Total** | | | **≈ $485** |

Context caching wired for the Qwen route (~5× input cut) → context ~$253→$70 →
**total ≈ $300**. Fastest, zero ops, no VRAM limits — you pay a premium for it.

---

## Option B — Azure self-deploy (rent GPU, run the models yourself)

Rent an Azure GPU VM, serve the GGUF models via vLLM/llama.cpp, run context +
extraction on it (datapoints still Claude Haiku). You pay **GPU-hours**, not
per-token — so for small models this undercuts OpenRouter.

### Azure GPU prices (US East, Linux, retail API)

| SKU | GPU / VRAM | PAYG $/hr | Spot $/hr | Fits your models? |
|---|---|--:|--:|---|
| NC4as_T4_v3 | 1× T4 / 16 GB | $0.526 | $0.153 | 9B yes; MoE (22/17 GB) needs CPU offload |
| NC8as_T4_v3 | 1× T4 / 16 GB, 56 GB RAM | $0.752 | $0.218 | same, better offload headroom |
| NV36ads_A10_v5 | 1× A10 / **24 GB** | $3.20 | $0.591 | **all fit in VRAM** (incl. 22 GB MoE) |
| NC24ads_A100_v4 | 1× A100 / 80 GB | ~$3.67 | ~$1.10 | everything, fastest |

### Throughput → GPU-hours (context + extraction, cached + batched, ~655M tok)

Blended prefill+decode, batched. **Estimates — validate on a 100-report sample.**

| GPU | ~tok/s | Runtime | PAYG $ | Spot $ |
|---|--:|---|--:|--:|
| T4 (16 GB) | ~1,500 | ~5 days (3–8) | **~$63** ($38–101) | **~$18** ($11–29) |
| A10 (24 GB) | ~3,500 | ~2.2 days (1.5–3) | ~$166 ($115–307) | ~$31 ($21–57) |
| A100 (80 GB) | ~15,000 | ~12 hrs (8–18) | **~$44** ($29–66) | ~$13 ($9–20) |

Notes:
- **T4 PAYG (~$63) beats A10 PAYG (~$166)** — the A10's 6× hourly outweighs its
  ~2.4× speedup. A10 only wins on **Spot** or when you need the MoE models to fit
  fully in VRAM (no offload) / want the shorter runtime.
- **Single A100 (~$44, ~½ day) is the self-host sweet spot** — cheaper than
  OpenRouter, fits every model, done in hours not days. Needs A100 quota.
- Spot is cheapest but **preemptible** — only viable with checkpointing on a
  multi-day run.

---

## Option C — Owned Tesla T4 (models already on disk)

You already run a **T4 (16 GB, ~320 GB/s, 70 W)** serving Qwen3.5-9B + `all-mpnet`
embeddings. Marginal cost of the backfill ≈ **electricity only** (~120–200 GPU-hr
× ~0.2 kW × ~$0.15 ≈ **$4–6**). Effectively free, but:

- **16 GB VRAM:** 9B-Q8 (9.5 GB) fits; **35B-A3B-Q4 (22.1 GB) and 26B-A4B-Q4
  (16.9 GB) exceed 16 GB** → CPU offload (slower). Can't hold two large LLMs +
  embeddings resident — run stages sequentially.
- **Contends with live serving** — the T4 already runs weekly + RAG. A multi-day
  backfill competes; run off-hours or throttle concurrency.
- **Same ~3–8 day runtime** as the Azure T4 (same card).

---

## Fidelity caveat (applies to every self-host route)

Your local models are **much smaller and more quantized** than ADR-0003's picks.
ADR-0003 found even the *larger* full-precision qwen3-235b reproduces only ~54% of
Claude's extraction and needs a spot-check. A local **Q4 9B / 35B-A3B** is weaker
— fine for **context** (easy, plain-text), tolerable for **extraction** (RAG
tags), **not** for datapoints. Note also: OpenRouter serves its own catalog, so a
truly *identical* model on both hosts only exists for the big ADR models (where
Azure needs the expensive A100 nodes) — your exact Q4 GGUFs can only be run
self-hosted.

---

## Recommendation

1. **Datapoints → Claude Haiku 4.5** (cached + Batch, **~$190**), always. Numbers
   feed the dashboard; run the ADR-0003 §6 spot-check on a Haiku sample first.
2. **Context + extraction → pick by urgency and whether you run inference infra:**
   - **Have the owned T4 idle / not time-sensitive →** run locally, **~$5**,
     accept days. Cheapest.
   - **Want it self-hosted but faster / T4 must keep serving →** rent a **single
     Azure A100** (~$44, ~½ day, fits everything) — the self-host sweet spot.
   - **Cheapest cloud, tolerant of days + Spot preemption →** Azure T4 Spot
     (~$18) with checkpointing.
   - **Urgent / no appetite for inference ops →** **OpenRouter ~$295** (or ~$112
     for context+ext with caching). You pay for speed and zero ops.
3. **Enable llama.cpp/vLLM prompt caching** for context — mandatory for any
   self-host route (without it the run is weeks).
4. **Guardrail** with `KB_MAX_COST_USD_PER_RUN` (~$700 headroom for ±30% variance
   + Claude fallback spillover).

### Decision rule

- **Small models (what you run)** → self-deploy wins on dollars; owned T4 ≈ $195
  all-in, single A100 ≈ $235, vs OpenRouter $300–485. Trade: days + ops.
- **Big ADR models (full precision)** → OpenRouter wins; Azure needs 8×A100
  ($27/hr) → $500–1,000+.
- **Urgency or no inference ops** → OpenRouter regardless.

*Bottom line: a low-hundreds one-off. On your small local models, self-deploy
(owned T4 or a single Azure A100) is cheaper than OpenRouter; OpenRouter's premium
buys speed and zero ops. Keep datapoints on Claude Haiku in all cases.*
