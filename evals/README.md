# Model-replacement eval

Find the **cheap** OpenRouter model that stays closest to Claude on the three
cheap LLM steps — **context**, **extraction**, **datapoints** — so we can swap
Claude out on those steps for large backfills. Narrative is intentionally out
of scope (it needs cross-report aggregation a handful of standalone PDFs don't
provide).

**Reference = Claude** (whatever the production `LLM_<ROLE>_*` env points at).
Candidates = mostly **cheap paid** OpenRouter slugs (Qwen / GLM / DeepSeek /
Kimi — pennies for the whole eval), plus a few `:free` baselines that run
**context-only** where they can't emit structured JSON (`structured_ok=False`
in `candidates.py`). The metric is *closeness to Claude*, i.e. Claude-parity —
the right target for a drop-in replacement. For the datapoints step, pair the
score with a human spot-check before trusting it; wrong numbers are the costly
error.

This harness measures **quality (closeness) + reliability (validity)** only — it
does **not** compute a cost model. The token/cost reasoning and the final
per-step routing decision live in `docs/adr/0003-llm-model-selection-and-resilience.md`
(with the price columns below as one input).

Only the PDFs you place here leave for OpenRouter — keep them non-sensitive.

## 1. Add PDFs

Drop non-sensitive report PDFs into `evals/reports/`. The `report_id` is the
filename stem.

## 2. Configure

- `src/clear_context_pipeline/defs/evals/candidates.py` — the candidate list,
  ordered by parameter size. **Re-verify each slug** (id, `structured_outputs`
  support, price) against OpenRouter's live catalog before a run — the catalog
  moves: <https://openrouter.ai/models>.
- Env (`.env`):
  - `OPENROUTER_API_KEY` — for the candidates (paid + free routes alike).
  - The existing `LLM_CONTEXT_*`, `LLM_EXTRACTION_*`, `LLM_DATAPOINTS_*` — used
    to build the Claude reference (same providers as production).

## 3. Run

Materialise the `evals` asset group (Dagster UI, or the `model_replacement_eval`
job). Order: `eval_corpus → eval_reference → eval_candidate_outputs (per model)
→ eval_scores (per model) → eval_leaderboard`.

`eval_candidate_outputs` and `eval_scores` are **partitioned by model** — run
all partitions to compare the whole field, or a single partition to test one
model. Outputs are cached by `report_id` on disk (`evals/cache/`), so re-runs
are cheap and resumable, and you can re-score without re-calling any model.

## 4. Read results

- `evals/results/leaderboard.md` — ranked table (price/M, context similarity,
  extraction F1, datapoints figure-F1 + value-agreement, per-step validity),
  ordered by model scale. A `†` on a context score means it was scored
  *lexically* (no embedding key at score time) and isn't comparable to the
  semantic rows.
- `evals/results/scores/<model>.json` — full per-step metrics per model
  (including `similarity_method` for the context step).

## Notes

- **Nothing here touches production S3 or clear-api** — the datapoints step
  runs the domain-extraction loop only (no upsert), and all artefacts stay
  under `evals/`.
- The decision axis is **quality (closeness) → reliability → cost**. This
  harness scores the first two; watch the `validity` columns — weak models lean
  on the provider's JSON-repair retry, and small-context models fail outright on
  long reports (both show up as low validity). Cost is weighed separately in
  ADR-0003 (the leaderboard's price column is one input, not a computed ranking).
- `evals/cache/` and `evals/results/` are run artefacts — git-ignored. The
  decision's evidence is the results table embedded in ADR-0003.
