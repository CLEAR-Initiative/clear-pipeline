"""Dagster eval job: free-OpenRouter-model vs Claude, on the non-sensitive
PDFs in ``evals/reports/``.

Asset graph (group ``evals``)::

  eval_corpus ─┬─> eval_reference ────────────────┐
               └─> eval_candidate_outputs ─┐       │
                     (partition: model)    ├─> eval_scores ──> eval_leaderboard
                                           │   (partition: model)
                                           └───────┘

- ``eval_corpus``            extract + chunk every PDF once (model-independent).
- ``eval_reference``         run context/extraction/datapoints with the
                             production Claude providers; cache per report.
- ``eval_candidate_outputs`` same three steps with ONE free model
                             (static-partitioned by candidate key); cached.
- ``eval_scores``            closeness-to-Claude per step + latency/validity
                             for the partition's model; writes results/scores/<key>.json.
- ``eval_leaderboard``       collate every model's scores into a leaderboard
                             (markdown + JSON) under evals/results/.

Caching is by report_id on disk, so re-runs are cheap and a run can resume;
scoring reads cached outputs, so you can re-score without re-calling any model.
Nothing here touches production S3 or clear-api.
"""

import dataclasses
import json
import os
from pathlib import Path
from typing import Any, Callable

import dagster as dg
from dagster import AssetExecutionContext

from clear_context_pipeline.defs.evals.candidates import (
    CANDIDATE_KEYS,
    candidate_by_key,
)
from clear_context_pipeline.defs.evals.runner import (
    CACHE_DIR,
    RESULTS_DIR,
    STEPS,
    EvalReport,
    build_corpus,
    candidate_provider,
    reference_provider,
)
from clear_context_pipeline.defs.evals.scoring import SCORERS, _mean
from clear_context_pipeline.defs.knowledgebase.datapoints_schemas import (
    SCHEMA_VERSION,
)

candidate_partitions = dg.StaticPartitionsDefinition(CANDIDATE_KEYS)

# Steps whose cached output depends on the datapoint extraction schema: a
# SCHEMA_VERSION bump makes an old cache entry stale (missing/renamed fields),
# so it must be regenerated even without EVAL_FORCE. `context`/`extraction`
# don't carry the datapoint schema, so a bump leaves their cache valid and
# they aren't listed here. See ADR-0004 / ADR-0005.
_STEP_SCHEMA_VERSION: dict[str, str] = {"datapoints": SCHEMA_VERSION}


# ────────────────────────────────────────────────────────────────────
# Generation helper (cache-aware) shared by reference + candidates
# ────────────────────────────────────────────────────────────────────

def _generate(
    base_dir: Path,
    corpus: list[EvalReport],
    provider_factory: Callable[[str], Any],
    *,
    log,
    force: bool = False,
    only_steps: set[str] | None = None,
) -> dict[str, Any]:
    """Run every step over the corpus, caching {output, stats} per report.

    ``provider_factory(role)`` builds the LLM provider for a step — called
    lazily and memoised, so a fully-cached run needs no API key at all.
    ``only_steps`` restricts which steps run (e.g. context-only models); None
    runs all of ``STEPS``.
    """
    providers: dict[str, Any] = {}

    def get_provider(role: str):
        if role not in providers:
            providers[role] = provider_factory(role)
        return providers[role]

    summary: dict[str, Any] = {}
    for step, (role, runner) in STEPS.items():
        if only_steps is not None and step not in only_steps:
            continue
        step_dir = base_dir / step
        step_dir.mkdir(parents=True, exist_ok=True)
        ok = failed = reused = 0
        seconds = 0.0
        first_error: str | None = None
        expected_sv = _STEP_SCHEMA_VERSION.get(step)
        for report in corpus:
            cache_file = step_dir / f"{report.report_id}.json"
            if cache_file.exists() and not force:
                cached = json.loads(cache_file.read_text())
                # Reuse only when the cache matches the current schema (steps
                # without a schema version always reuse). A bump regenerates
                # this step; a pre-versioning cache (no `schema_version` key)
                # reads as None and so regenerates too.
                if expected_sv is None or cached.get("schema_version") == expected_sv:
                    reused += 1
                    ok += cached["stats"]["ok"]
                    failed += cached["stats"]["failed"]
                    seconds += cached["stats"]["seconds"]
                    first_error = first_error or next(iter(cached["stats"].get("errors") or []), None)
                    continue
                log.info(
                    "%s / %s: cached schema_version=%s != %s — regenerating",
                    step, report.report_id, cached.get("schema_version"), expected_sv,
                )
            output, stats = runner(get_provider(role), report)
            cache_file.write_text(json.dumps(
                {
                    "output": output,
                    "stats": dataclasses.asdict(stats),
                    "schema_version": expected_sv,
                },
                ensure_ascii=False,
            ))
            ok += stats.ok
            failed += stats.failed
            seconds += stats.seconds
            if stats.errors:
                first_error = first_error or stats.errors[0]
                log.warning(
                    "%s / %s: %d/%d calls failed — first: %s",
                    step, report.report_id, stats.failed, stats.ok + stats.failed,
                    stats.errors[0],
                )
        summary[step] = {
            "reports": len(corpus), "reused": reused,
            "calls_ok": ok, "calls_failed": failed, "seconds": round(seconds, 1),
            **({"first_error": first_error} if first_error else {}),
        }
    return summary


def _force() -> bool:
    """Re-run even cached reports. Cache is written for failed reports too (so
    the validity metric survives), which means a plain re-run after fixing
    config (throttle, json mode, …) would reuse the stale failure. Set
    EVAL_FORCE=1 — or delete evals/cache/candidates/<key>/ — to actually retry."""
    return os.environ.get("EVAL_FORCE") == "1"


def _load_outputs(base_dir: Path, step: str, report_id: str) -> tuple[dict, dict]:
    """Return (output, stats) for one report+step from the cache."""
    payload = json.loads((base_dir / step / f"{report_id}.json").read_text())
    return payload["output"], payload["stats"]


# ────────────────────────────────────────────────────────────────────
# Assets
# ────────────────────────────────────────────────────────────────────

@dg.asset(group_name="evals")
def eval_corpus(context: AssetExecutionContext) -> list[EvalReport]:
    """Extract + chunk every PDF in ``evals/reports/`` — done once, shared."""
    corpus = build_corpus()
    if not corpus:
        raise dg.Failure(
            description="No PDFs found in evals/reports/ (or none had extractable text).",
        )
    context.add_output_metadata({
        "reports": dg.MetadataValue.int(len(corpus)),
        "chunks_total": dg.MetadataValue.int(sum(len(r.chunks) for r in corpus)),
        "report_ids": dg.MetadataValue.json([r.report_id for r in corpus]),
    })
    return corpus


@dg.asset(group_name="evals")
def eval_reference(
    context: AssetExecutionContext, eval_corpus: list[EvalReport],
) -> dict:
    """Claude's outputs — the oracle. Regenerated (cache-aware) from the
    production providers so it always reflects what Claude ships today."""
    summary = _generate(
        CACHE_DIR / "reference", eval_corpus, reference_provider,
        log=context.log, force=_force(),
    )
    context.add_output_metadata({k: dg.MetadataValue.json(v) for k, v in summary.items()})

    # Fail loud if the oracle is empty — otherwise every candidate is scored
    # against nothing and the leaderboard is silently meaningless (e.g. an
    # Anthropic usage-limit 400 on LLM_*_API_KEY, or a bad key).
    total_ok = sum(v["calls_ok"] for v in summary.values())
    total_failed = sum(v["calls_failed"] for v in summary.values())
    if total_ok == 0 and total_failed > 0:
        first = next((v["first_error"] for v in summary.values() if v.get("first_error")), "unknown")
        raise dg.Failure(
            description=(
                f"Claude reference produced 0 valid outputs across {total_failed} calls — "
                f"no oracle to compare against. Check the Anthropic key/usage limit on "
                f"LLM_*_API_KEY. First error: {first}"
            ),
        )
    return summary


@dg.asset(group_name="evals", partitions_def=candidate_partitions)
def eval_candidate_outputs(
    context: AssetExecutionContext, eval_corpus: list[EvalReport],
) -> dict:
    """Run the three steps with ONE free model (the partition key).

    Context-only models (``structured_ok=False``) run just the context step;
    extraction + datapoints are skipped (they'd only fail on the schema)."""
    candidate = candidate_by_key(context.partition_key)
    only_steps = None if candidate.structured_ok else {"context"}
    summary = _generate(
        CACHE_DIR / "candidates" / candidate.key,
        eval_corpus,
        lambda role: candidate_provider(role, candidate),
        log=context.log, force=_force(), only_steps=only_steps,
    )
    context.add_output_metadata({
        "model": dg.MetadataValue.text(candidate.slug),
        "params_b": dg.MetadataValue.float(candidate.params_b),
        **{k: dg.MetadataValue.json(v) for k, v in summary.items()},
    })
    return summary


@dg.asset(group_name="evals", partitions_def=candidate_partitions)
def eval_scores(
    context: AssetExecutionContext,
    eval_corpus: list[EvalReport],
    eval_reference: dict,
    eval_candidate_outputs: dict,
) -> dict:
    """Closeness-to-Claude per step for the partition's model, plus
    latency/validity. Writes results/scores/<key>.json for the leaderboard."""
    candidate = candidate_by_key(context.partition_key)
    ref_dir = CACHE_DIR / "reference"
    cand_dir = CACHE_DIR / "candidates" / candidate.key

    # Context-only models are scored on context alone; the other steps were
    # never run, so leave them out of `steps` entirely → they render as `—`.
    steps_to_score = STEPS if candidate.structured_ok else {"context": STEPS["context"]}
    per_step: dict[str, Any] = {}
    for step in steps_to_score:
        scorer = SCORERS[step]
        per_report_scores: list[dict] = []
        ok = failed = 0
        seconds = 0.0
        for report in eval_corpus:
            try:
                ref_out, _ = _load_outputs(ref_dir, step, report.report_id)
                cand_out, cand_stats = _load_outputs(cand_dir, step, report.report_id)
            except FileNotFoundError:
                continue
            per_report_scores.append(scorer(ref_out, cand_out))
            ok += cand_stats["ok"]
            failed += cand_stats["failed"]
            seconds += cand_stats["seconds"]

        # Average each numeric metric across reports.
        agg: dict[str, Any] = {}
        if per_report_scores:
            for metric in per_report_scores[0]:
                vals = [s[metric] for s in per_report_scores if isinstance(s.get(metric), (int, float))]
                if vals:
                    agg[metric] = round(_mean(vals), 4)
            # Carry through non-numeric provenance the numeric loop drops — chiefly
            # the context scorer's `similarity_method`. A lexical-fallback score
            # (no embedding key) systematically under-scores paraphrase and is NOT
            # comparable to a semantic-cosine one, so the leaderboard must be able
            # to flag which was used rather than silently mixing the two.
            methods = sorted({
                s["similarity_method"] for s in per_report_scores
                if isinstance(s.get("similarity_method"), str)
            })
            if methods:
                agg["similarity_method"] = methods[0] if len(methods) == 1 else "mixed:" + ",".join(methods)
        agg["calls_ok"] = ok
        agg["calls_failed"] = failed
        agg["validity"] = round(ok / (ok + failed), 4) if (ok + failed) else 0.0
        agg["seconds"] = round(seconds, 1)
        per_step[step] = agg

    result = {
        "key": candidate.key,
        "model": candidate.slug,
        "params_b": candidate.params_b,
        "active_b": candidate.active_b,
        "context": candidate.context,
        "in_price": candidate.in_price,
        "out_price": candidate.out_price,
        "steps": per_step,
    }
    scores_dir = RESULTS_DIR / "scores"
    scores_dir.mkdir(parents=True, exist_ok=True)
    (scores_dir / f"{candidate.key}.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False),
    )
    context.add_output_metadata(
        {step: dg.MetadataValue.json(v) for step, v in per_step.items()},
    )
    return result


@dg.asset(group_name="evals", deps=[eval_scores])
def eval_leaderboard(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Collate every model's scores (from results/scores/) into a leaderboard.

    Uses a filesystem glob rather than a cross-partition asset input, so it
    works with however many candidate partitions you've materialised so far.
    """
    scores_dir = RESULTS_DIR / "scores"
    rows = [
        json.loads(p.read_text())
        for p in sorted(scores_dir.glob("*.json"))
    ] if scores_dir.exists() else []
    if not rows:
        raise dg.Failure(description="No scored models yet — materialise eval_scores partitions first.")

    def _fmt(x: Any) -> str:
        # "—" for missing/non-informative metrics (e.g. no reports with figures)
        # so they read as "no signal", not a real 0.000.
        return f"{x:.3f}" if isinstance(x, (int, float)) else "—"

    rows.sort(key=lambda r: r["params_b"])  # scale curve order
    lines = [
        "# Model-replacement eval — closeness to Claude",
        "",
        "**Read `validity` first** (fraction of calls that succeeded, ctx/ext/dp): "
        "if it's low, the other columns are computed over very few successful "
        "calls and mean little. `—` = the metric had no informative reports "
        "(e.g. Claude produced no figures to match).",
        "",
        "| model | params(B) | active(B) | $/M in→out | context sim | extraction F1 | datapoints figF1 | dp value-agree | validity (ctx/ext/dp) |",
        "|---|--:|--:|--|--:|--:|--:|--:|--|",
    ]
    lexical_seen = False  # any context score scored lexically, not semantically?
    for r in rows:
        s = r["steps"]
        ctx = s.get("context", {})
        ctx_sim = ctx.get("mean_similarity")
        # Flag a lexical(-fallback) context score — it under-scores paraphrase and
        # isn't comparable to a semantic-cosine one (see similarity_method).
        ctx_method = ctx.get("similarity_method")
        ctx_cell = _fmt(ctx_sim)
        if ctx_sim is not None and isinstance(ctx_method, str) and ctx_method != "embedding_cosine":
            ctx_cell += " †"
            lexical_seen = True
        ext_f1 = s.get("extraction", {}).get("overall")
        dp_f1 = s.get("datapoints", {}).get("figure_f1")
        dp_val = s.get("datapoints", {}).get("value_agreement")
        val = "/".join(
            (str(s[step]["validity"]) if step in s else "—")
            for step in ("context", "extraction", "datapoints")
        )
        active = f"{r['active_b']:g}" if r.get("active_b") is not None else "—"
        price = "free" if not (r.get("in_price") or r.get("out_price")) else (
            f"${r['in_price']:g}→${r['out_price']:g}"
        )
        lines.append(
            f"| {r['model']} | {r['params_b']:g} | {active} | {price} | {ctx_cell} | {_fmt(ext_f1)} "
            f"| {_fmt(dp_f1)} | {_fmt(dp_val)} | {val} |",
        )
    if lexical_seen:
        lines += [
            "",
            "† context sim scored by **lexical** difflib, not semantic cosine (no "
            "embedding key at score time). Lexical under-scores paraphrase — not "
            "comparable to the semantic rows; re-score with an embedding key set.",
        ]
    md = "\n".join(lines) + "\n"

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "leaderboard.md").write_text(md)
    (RESULTS_DIR / "leaderboard.json").write_text(json.dumps(rows, indent=2, ensure_ascii=False))

    return dg.MaterializeResult(metadata={
        "models_scored": dg.MetadataValue.int(len(rows)),
        "leaderboard": dg.MetadataValue.md(md),
        "path": dg.MetadataValue.path(str(RESULTS_DIR / "leaderboard.md")),
    })


# One-shot job: corpus → reference → candidates → scores → leaderboard.
eval_job = dg.define_asset_job(
    name="model_replacement_eval",
    selection=dg.AssetSelection.groups("evals"),
)
