"""Candidate model registry for the model-replacement eval.

We're replacing Claude on the cheap LLM steps (context, extraction, datapoints)
with the model that stays closest to Claude while being far cheaper. Hard
requirement (from earlier runs): the model must reliably emit
**structured / schema-constrained JSON** — models that can't (no
structured-outputs support, or reasoning leaking into the answer) are useless
here regardless of raw quality.

Every slug below was verified against OpenRouter's live catalog
(https://openrouter.ai/api/v1/models) for: exact id, `structured_outputs`
support, price, and context. Prices are USD per 1M tokens (in / out) at
verification time — RE-VERIFY before a run, the catalog moves.

Tiers: one `:free` baseline (rate-limited, `free=True`), the rest **cheap
paid** (Qwen / GLM / DeepSeek / Kimi) — pennies for the whole eval (~50
calls/model over 3 short PDFs). All send strict `json_schema`
(`json_schema_mode=True`) since every model here supports it.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Candidate:
    """One model under test.

    key: filesystem-safe id (Dagster partition key + results filename).
    slug: OpenRouter model slug. params_b/active_b: total / MoE-active params (B).
    context: context window (tokens). train_tokens_t: training tokens (T) or None.
    json_schema_mode: True → strict json_schema; False → json_object + repair.
    free: True → free tier (paced by throttle); False → paid.
    in_price/out_price: USD per 1M tokens (0.0 for free). notes: family + caveats.
    structured_ok: True → run all three steps. False → CONTEXT-ONLY: the model
        can't reliably do structured output, so only the context step (plain
        text, no schema) is run/scored; extraction + datapoints are skipped and
        show `—` on the leaderboard. Lets cheap/free models that fail JSON
        compete for the highest-volume step without grinding on the others.
    """

    key: str
    slug: str
    params_b: float
    active_b: float | None
    context: int
    train_tokens_t: float | None
    json_schema_mode: bool
    free: bool
    in_price: float
    out_price: float
    notes: str
    structured_ok: bool = True


# Ordered small → large by TOTAL params (the scale axis). Structured-output
# capability varies (see `structured_ok`); context runs on all. Prices USD / 1M
# tokens (in / out).
CANDIDATES: list[Candidate] = [
    # gemma4-26b-a4b:free — DROPPED at runtime (2026-07-28 eval run): NOT a
    # bad-output problem (the fence fix let its context chunks parse fine).
    # The :free endpoint became unresponsive mid-run — calls hung the full
    # 600s client timeout × retries, ~40 min/chunk, and two consecutive runs
    # stalled at the same context chunk. Free-tier availability, not model
    # quality. Retry when the free endpoint is healthy, or use the paid route
    # google/gemma-4-26b-a4b-it ($0.14/$0.42).
    # Candidate(
    #     key="gemma4-26b-a4b",
    #     slug="google/gemma-4-26b-a4b-it:free",
    #     params_b=26.0, active_b=4.0, context=262_144, train_tokens_t=None,
    #     json_schema_mode=True, free=True, in_price=0.0, out_price=0.0,
    #     notes="Google Gemma 4, MoE 26B/A4B. Free baseline (paid route "
    #           "google/gemma-4-26b-a4b-it is $0.14/$0.42 if the free tier throttles).",
    # ),
    Candidate(
        key="nemotron3-nano-30b",
        slug="nvidia/nemotron-3-nano-30b-a3b",
        params_b=30.0, active_b=3.0, context=262_144, train_tokens_t=None,
        json_schema_mode=True, free=False, in_price=0.05, out_price=0.20,
        notes="NVIDIA Nemotron 3 Nano, MoE 30B/A3B. Non-reasoning variant "
              "(supports structured outputs, unlike the :free reasoning one). Cheapest.",
    ),
    Candidate(
        key="qwen3-30b-a3b-2507",
        slug="qwen/qwen3-30b-a3b-instruct-2507",
        params_b=30.0, active_b=3.0, context=262_144, train_tokens_t=None,
        json_schema_mode=True, free=False, in_price=0.05, out_price=0.19,
        notes="Qwen3 MoE 30B/A3B (2507 instruct). Dirt cheap, long context.",
    ),
    # qwen3.5-35b-a3b — DROPPED at runtime (2026-07-28 eval run): same failure
    # as kimi-k2.5. Advertises json_schema_mode but does NOT reliably emit valid
    # JSON for the extraction step — it returned empty strings, looping the
    # structured-output parser on "EOF while parsing" and stalling at 2/5
    # reports before we cut it. Effectively unusable here.
    # Candidate(
    #     key="qwen3.5-35b-a3b",
    #     slug="qwen/qwen3.5-35b-a3b",
    #     params_b=35.0, active_b=3.0, context=262_144, train_tokens_t=None,
    #     json_schema_mode=True, free=False, in_price=0.14, out_price=1.00,
    #     notes="Qwen3.5 MoE 35B/A3B — newest Qwen family, tiny/cheap.",
    # ),
    Candidate(
        key="qwen3-next-80b-a3b",
        slug="qwen/qwen3-next-80b-a3b-instruct",
        params_b=80.0, active_b=3.0, context=262_144, train_tokens_t=None,
        json_schema_mode=True, free=False, in_price=0.10, out_price=1.10,
        notes="Qwen3-Next MoE 80B/A3B — newer arch, fills the 80B gap. Cheap.",
    ),
    Candidate(
        key="deepseek-v4-flash",
        slug="deepseek/deepseek-v4-flash",
        params_b=100.0, active_b=None, context=1_048_576, train_tokens_t=None,
        json_schema_mode=True, free=False, in_price=0.14, out_price=0.28,
        notes="DeepSeek V4 Flash — newer/cheaper than v3.2, 1M context. "
              "params_b APPROX (V4 sizes not public).",
    ),
    Candidate(
        key="qwen3-235b-a22b-2507",
        slug="qwen/qwen3-235b-a22b-2507",
        params_b=235.0, active_b=22.0, context=262_144, train_tokens_t=None,
        json_schema_mode=True, free=False, in_price=0.09, out_price=0.55,
        notes="Qwen3 flagship MoE 235B/A22B (2507). Big but very cheap.",
    ),
    Candidate(
        key="glm-4.6",
        slug="z-ai/glm-4.6",
        params_b=355.0, active_b=32.0, context=204_800, train_tokens_t=None,
        # PAID — free=True here ONLY to enable the throttle: Zhipu's endpoint
        # 429'd "upstream" when we fired unpaced (15/38 last run). Price column
        # still shows $0.50/$2.00 (driven by in/out_price, not this flag).
        json_schema_mode=True, free=True, in_price=0.50, out_price=2.00,
        notes="Zhipu GLM-4.6, MoE ~355B/A32B (paid, but throttled to dodge the "
              "upstream 429). If 429s persist it's provider saturation — retry later.",
    ),
    Candidate(
        key="glm-4.7",
        slug="z-ai/glm-4.7",
        params_b=355.0, active_b=32.0, context=204_800, train_tokens_t=None,
        json_schema_mode=True, free=False, in_price=0.40, out_price=1.75,
        notes="Zhipu GLM-4.7 — newer/cheaper than 4.6; may route to a less "
              "contended provider. params_b APPROX (~4.6 family).",
    ),
    Candidate(
        key="deepseek-v3.2",
        slug="deepseek/deepseek-v3.2",
        params_b=671.0, active_b=37.0, context=163_840, train_tokens_t=None,
        json_schema_mode=True, free=False, in_price=0.27, out_price=0.40,
        notes="DeepSeek V3.2, MoE ~671B/A37B. Cheap output. (deepseek-v4-flash "
              "$0.14/$0.28 @ 1M ctx is a newer option if you want it.)",
    ),
    Candidate(
        key="kimi-k2-0905",
        slug="moonshotai/kimi-k2-0905",
        params_b=1000.0, active_b=32.0, context=262_144, train_tokens_t=None,
        json_schema_mode=True, free=False, in_price=0.60, out_price=2.50,
        notes="Moonshot Kimi K2 (0905), MoE ~1T/A32B. (base moonshotai/kimi-k2 "
              "does NOT support structured outputs — using the 0905 snapshot.)",
    ),

    # ── FREE tier (2026-07-28) ────────────────────────────────────────
    # Now that the context step is plain text (no schema), models WITHOUT
    # structured-output support can still compete for it — the pipeline's
    # cheapest, highest-volume step. `structured_ok=False` → context-only
    # (extraction + datapoints skipped, shown as `—`). All slugs + context
    # windows verified against OpenRouter's live catalog; params are the
    # model-card figures (approx where unpublished). Free tier shares a
    # 20/min (and, without credit, 50/day) cap — expect a slow run.

    # -- structured-output-capable free models: DROPPED (2026-07-29 run) --
    # openai/gpt-oss-20b:free and nvidia/nemotron-nano-9b-v2:free both HUNG
    # running the full 3-step set and had to be terminated. Same free-tier
    # availability problem as gemma4:free — running extraction + datapoints
    # (~286 calls) over the free route hits hangs / the 50/day cap. Use the
    # PAID routes (openai/gpt-oss-20b, nvidia/nemotron-nano-9b-v2) if you want
    # these evaluated. Context-only free models (below) run far fewer calls.
    # Candidate(
    #     key="gpt-oss-20b-free",
    #     slug="openai/gpt-oss-20b:free",
    #     params_b=21.0, active_b=3.6, context=131_072, train_tokens_t=None,
    #     json_schema_mode=True, free=True, in_price=0.0, out_price=0.0,
    #     notes="OpenAI gpt-oss 20B (MoE ~21B/A3.6B), free, structured_outputs=YES.",
    # ),
    # Candidate(
    #     key="nemotron-nano-9b-v2-free",
    #     slug="nvidia/nemotron-nano-9b-v2:free",
    #     params_b=9.0, active_b=None, context=128_000, train_tokens_t=None,
    #     json_schema_mode=True, free=True, in_price=0.0, out_price=0.0,
    #     notes="NVIDIA Nemotron Nano 9B v2, free, structured_outputs=YES. Smallest here.",
    # ),

    # -- context-only free models (structured_outputs=NO on the free route) --
    Candidate(
        key="ling-3.0-flash-free",
        slug="inclusionai/ling-3.0-flash:free",
        params_b=100.0, active_b=6.0, context=262_144, train_tokens_t=None,
        json_schema_mode=False, free=True, in_price=0.0, out_price=0.0,
        structured_ok=False,
        notes="InclusionAI Ling 3.0 Flash (MoE; sizes approx). Context-only.",
    ),
    Candidate(
        key="nemotron-3-nano-30b-free",
        slug="nvidia/nemotron-3-nano-30b-a3b:free",
        params_b=30.0, active_b=3.0, context=256_000, train_tokens_t=None,
        json_schema_mode=False, free=True, in_price=0.0, out_price=0.0,
        structured_ok=False,
        notes="Free nemotron-3-nano route (30B/A3B); struct=NO here. Context-only. "
              "(Paid nvidia/nemotron-3-nano-30b-a3b DOES do structured output.)",
    ),
    Candidate(
        key="nemotron-3-ultra-550b-free",
        slug="nvidia/nemotron-3-ultra-550b-a55b:free",
        params_b=550.0, active_b=55.0, context=1_000_000, train_tokens_t=None,
        json_schema_mode=False, free=True, in_price=0.0, out_price=0.0,
        structured_ok=False,
        notes="NVIDIA Nemotron-3 Ultra 550B/A55B, 1M ctx, FREE. Context-only.",
    ),
    # kimi-k2.5 — DROPPED at runtime (2026-07-28 eval run): despite advertising
    # json_schema_mode, moonshotai/kimi-k2.5 does NOT reliably emit valid JSON
    # for the extraction step — it returned empty strings and ```json```-fenced
    # bodies, causing 91 structured-output parse failures and only 2/5 reports
    # extracted before we cut it. Effectively unusable here; use kimi-k2-0905.
    # Candidate(
    #     key="kimi-k2.5",
    #     slug="moonshotai/kimi-k2.5",
    #     params_b=1000.0, active_b=32.0, context=262_144, train_tokens_t=None,
    #     json_schema_mode=True, free=False, in_price=0.57, out_price=2.85,
    #     notes="Moonshot Kimi K2.5 — newer than k2-0905, MoE ~1T/A32B.",
    # ),
]

# Excluded after verification (do NOT re-add without checking structured_outputs):
#  - inclusionai/ling-3.0-flash:free, nvidia/nemotron-3-*-super/ultra,
#    nvidia/nemotron-3-nano-omni-*:free, z-ai/glm-4.5-air, base qwen3-235b-a22b,
#    base moonshotai/kimi-k2 — each either lacks structured-outputs support or
#    leaked reasoning into the answer. gemma-4-31b:free = upstream-rate-limited.
#  - moonshotai/kimi-k2.5 — advertises json_schema_mode but emits empty/fenced
#    JSON at runtime; 91 parse failures in the 2026-07-28 run (see block above).
#  - qwen/qwen3.5-35b-a3b — same failure as kimi-k2.5: advertises json_schema_mode
#    but emits empty JSON on extraction, stalled at 2/5 reports (2026-07-28 run).
#  - openai/gpt-oss-20b:free, nvidia/nemotron-nano-9b-v2:free — hung on the full
#    3-step run (free-tier availability), terminated 2026-07-29. Use paid routes.
#  - google/gemma-4-26b-a4b-it:free — output was FINE (parses with the fence
#    fix); dropped for :free endpoint availability — calls hung the 600s timeout,
#    two runs stalled at the same context chunk (2026-07-28). Retry when healthy.


def candidate_by_key(key: str) -> Candidate:
    for c in CANDIDATES:
        if c.key == key:
            return c
    raise KeyError(f"unknown candidate key {key!r}; known: {[c.key for c in CANDIDATES]}")


CANDIDATE_KEYS: list[str] = [c.key for c in CANDIDATES]
