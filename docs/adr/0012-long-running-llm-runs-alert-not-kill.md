---
status: proposed
---

# Long-running LLM-generation runs alert, they are not auto-killed

## Context

Dagster's run-monitoring daemon cancels any run that exceeds a max-runtime cap.
The instance-wide default is `run_monitoring.max_runtime_seconds` = 6 h
(clear-infra `dagster.yaml`); a per-job `dagster/max_runtime` tag overrides it
per run (`defs/run_limits.py`, `max_runtime_tags(seconds)`). The analysis
automation drain carried a tight **900 s (15 min)** cap
(`drain_analysis_automations_job`, and `analysis` stages).

That cap was sized for the *old monolithic* analysis generation — one LLM call
per frame. The move to modular, per-leaf selective regeneration (ADR-0007 /
the situation `build_leaf_components` path) made a single frame run **~23 leaf
LLM calls sequentially**; the drain then processes **many frames in one run**
(`_BATCH_SIZE = 100`, grouped by frame). A drain over several frames, or even a
single wide frame (e.g. a 7-location Sudan frame), now routinely exceeds 900 s.

Observed failure (2026-10-28): a drain was cancelled mid-Claude-call inside
`build_leaf_components` with
`DagsterExecutionInterruptedError: Exceeded maximum runtime of 900 seconds`.
The kill:

- **Wastes the frame's partial LLM spend** — a frame upserts only at its end, so
  an interrupt discards the Claude calls already made for that frame.
- **Can wedge a frame permanently.** `_process_frame` advances a frame's
  `nextRunAt` (`_mark_automations_ran`) only *after* the generation completes.
  A frame that legitimately needs more than the cap is cancelled at the same
  point on every drain, never completes, never advances, and is re-selected and
  re-killed every run.
- **Fires the failure sensors** (`run_failure_metrics_sensor`,
  `run_failure_glitchtip_sensor`) on every such kill — a FAILURE that is not a
  failure, just a slow-but-healthy LLM run.

The original reason for a tight cap was a different problem: the 2026-10-20
incident where zombie runs (worker died, Dagster still thought them STARTED)
held `max_concurrent_runs` slots and wedged the queue. That failure mode — a
run whose **worker is gone** — is now handled directly by `defs/orphaned_runs.py`
(it reaps runs whose container died), and the 6 h instance cap remains as a
backstop for a run that is genuinely hung but still alive. A tight per-job
kill is the wrong tool for a run that is simply doing a lot of real LLM work.

## Decision

**A long-running analysis-generation run is NOT auto-terminated. It is allowed
to run to completion; when it crosses a soft budget it raises a GlitchTip alert
for visibility, and keeps running.**

Concretely:

- The analysis-generation jobs (`drain_analysis_automations`, the on-demand
  analysis generation, and the situation `build_leaf_components` path they
  share) carry **no tight per-job `dagster/max_runtime` kill**. The only hard
  stop is the instance-wide 6 h `run_monitoring.max_runtime_seconds` backstop,
  which exists to catch a genuinely hung — not merely slow — run.
- Exceeding a **soft budget** (a threshold well below 6 h) emits a **GlitchTip
  alert** so a slow run is visible and investigable, **without cancelling it**.
  The alert is informational, not a FAILURE: a slow run must not look like a
  broken one.
- `orphaned_runs` continues to reap runs whose worker/container died — that is
  the real "stuck slot" remedy, independent of runtime length.

This is a deliberate trade: we accept that a legitimately slow run holds its
queue slot for longer, in exchange for never discarding in-progress LLM work,
never wedging a frame that needs more than a fixed cap, and not drowning the
error channel in false FAILUREs.

## Scope

This decision governs runs whose wall-clock time is dominated by **legitimate,
unbounded LLM work** — the analysis-generation jobs above. It is not a blanket
removal of all run caps:

- Jobs with a bounded, non-LLM workload keep their caps as cheap stuck-run
  detectors (they *should* finish fast, so a long one is anomalous).
- The 6 h instance backstop stays for everything.

If another job's runtime becomes LLM-dominated, it inherits this policy.

## Why not just raise the cap

A higher fixed number (say 3600 s) does not solve it: the drain's batch of up to
100 frames is unbounded in aggregate, and a single pathologically wide frame can
still exceed any fixed cap and wedge. The problem is that a *time limit* is the
wrong control for *legitimate variable-length work*; alert-and-continue is.

## Why not keep killing and rely on the drain self-healing

Completed frames do advance, so a too-large batch self-heals across drains as
finished frames drop out of "due". But that does not save the **in-flight**
frame's work, and it does not help a **single** frame that needs more than the
cap — that frame is re-killed forever. Self-healing addresses batch size, not
per-frame duration; this decision addresses both by not killing.

## Consequences

- A slow-but-healthy analysis run now **completes** instead of being cancelled
  mid-call; its LLM spend is not wasted and its frames advance normally.
- A long run is surfaced by a **GlitchTip alert on a soft budget**, not by a
  FAILURE. The alerting is informational; it names the run and how long it has
  been going so an operator can decide whether to intervene.
- A genuinely hung-but-alive run is still caught, now at the 6 h backstop rather
  than 15 min; a dead-worker run is still reaped promptly by `orphaned_runs`.
- A long legitimate run holds a `max_concurrent_runs` slot for its full
  duration. Bounding the drain's per-run work (a smaller `_BATCH_SIZE` or a
  time-boxed frame loop) and parallelising a frame's independent leaf calls are
  **separate follow-ups** to keep runs naturally short — they reduce how often
  the soft budget is crossed, but they are not the kill this ADR removes.
- Removing a per-job cap is a one-line change where `max_runtime_tags(...)` is
  applied; the soft-budget GlitchTip alert is a monitoring addition (a sensor /
  run-age check that reports without cancelling), distinct from the existing
  `run_failure_*` sensors which fire only on terminal FAILURE.
