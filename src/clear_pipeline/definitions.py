from pathlib import Path

from dagster import Definitions, definitions, in_process_executor, load_from_defs_folder

from clear_pipeline.providers.observability import init_sentry

# Initialise GlitchTip reporting as the code location loads — runs in the daemon,
# the code server, and every run/step process (no-op without SENTRY_DSN).
init_sentry()


@definitions
def defs():
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)
    # Run every job with the in-process executor: one process per run (no separate
    # per-step subprocess), steps execute sequentially. The multiprocess executor
    # was forking a step worker that re-imported the full definitions (~690 MB each)
    # — so 4 concurrent runs = ~8 heavy processes, which OOM-killed the dagster-code
    # cgroup and stranded runs in STARTED (2026-10-12 incident). These jobs are short
    # drains / linear asset chains, so in-process loses no meaningful parallelism and
    # roughly halves per-run memory, letting the concurrent runs fit the cgroup.
    return Definitions.merge(loaded, Definitions(executor=in_process_executor))
