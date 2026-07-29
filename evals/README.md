# Model-replacement eval

Find the **free** OpenRouter model that stays closest to Claude on the three
cheap LLM steps — **context**, **extraction**, **datapoints** — so we can swap
Claude out on those steps. Narrative is intentionally out of scope (it needs
cross-report aggregation a handful of standalone PDFs don't provide).

**Reference = Claude** (whatever the production `LLM_<ROLE>_*` env points at).
Candidates = free `:free` OpenRouter slugs. The metric is *closeness to
Claude*, i.e. Claude-parity — the right target for a drop-in replacement. For
the datapoints step, pair the score with a human spot-check before trusting
it; wrong numbers are the costly error.

Only the PDFs you place here leave for OpenRouter — keep them non-sensitive.

## 1. Add PDFs

Drop non-sensitive report PDFs into `evals/reports/`. The `report_id` is the
filename stem.

## 2. Configure

- `src/clear_context_pipeline/defs/evals/candidates.py` — the candidate list,
  ordered by parameter size. **Verify each `:free` slug** against the current
  free tier: <https://openrouter.ai/models?max_price=0>.
- Env (`.env`):
  - `OPENROUTER_API_KEY` — for the candidates.
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

- `evals/results/leaderboard.md` — ranked table (context similarity,
  extraction F1, datapoints figure-F1 + value-agreement, per-step validity),
  ordered by model scale.
- `evals/results/scores/<model>.json` — full per-step metrics per model.

## Notes

- **Nothing here touches production S3 or clear-api** — the datapoints step
  runs the domain-extraction loop only (no upsert), and all artefacts stay
  under `evals/`.
- Free models are ~$0 marginal cost, so the decision axis is **quality
  (closeness) then reliability** — watch the `validity` columns: weak models
  lean on the provider's JSON-repair retry, and small-context models fail
  outright on long reports (both show up as low validity).
- `evals/cache/` and `evals/results/` are run artefacts — git-ignored.
