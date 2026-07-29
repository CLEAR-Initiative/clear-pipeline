"""Closeness-to-Claude scorers, one per eval step.

The reference is Claude's own output; each candidate is scored on how closely
it reproduces it. This measures *Claude-parity* (the right target for a
drop-in replacement), not ground-truth correctness — a candidate that copies
Claude's mistakes scores well. For the datapoints step, pair the score with a
human spot-check before trusting it, since wrong numbers are the costly error.

Every scorer returns a plain dict of metrics in [0, 1] (plus counts), so the
leaderboard can rank on them and the JSON is diff-friendly across runs.
"""

from __future__ import annotations

import math
import os
from difflib import SequenceMatcher
from typing import Any

# ── shared set-F1 ────────────────────────────────────────────────────

def _prf(reference: set, candidate: set) -> dict[str, float]:
    if not reference and not candidate:
        return {"precision": 1.0, "recall": 1.0, "f1": 1.0}
    tp = len(reference & candidate)
    precision = tp / len(candidate) if candidate else 0.0
    recall = tp / len(reference) if reference else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1}


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


# ── context: prefix similarity ───────────────────────────────────────
# The context prefix is a free-text summary; two good prefixes can share
# almost no words yet mean the same thing, so lexical overlap (difflib)
# systematically UNDER-scores paraphrases and is a poor closeness signal.
# Default to **semantic cosine** over the same embedding model the pipeline
# uses for RAG (the prefix's actual downstream job), which rewards meaning
# not wording. Falls back to lexical when no embedding key is configured, so
# offline/CI runs still work. Force either via EVAL_CONTEXT_SCORER=lexical|semantic.

_embedder: Any = None
_embedder_tried = False


def _get_embedder() -> Any:
    """Lazily build the embedding provider once. Returns None (and stays None)
    if it can't be constructed — no key, missing dep — so scoring degrades to
    lexical instead of failing the whole leaderboard."""
    global _embedder, _embedder_tried
    if not _embedder_tried:
        _embedder_tried = True
        try:
            from clear_context_pipeline.providers import make_embedding_provider
            _embedder = make_embedding_provider()
        except Exception:  # noqa: BLE001 — degrade to lexical
            _embedder = None
    return _embedder


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return (dot / (na * nb)) if (na and nb) else 0.0


def _embed_all(embedder: Any, texts: list[str], *, batch: int = 64) -> list[list[float]]:
    """Embed a list of prefixes, slicing to the provider's per-batch limit."""
    vectors: list[list[float]] = []
    for i in range(0, len(texts), batch):
        results = embedder.embed(texts[i:i + batch], input_type="document")
        vectors.extend(r.embedding for r in results)
    return vectors


def score_context(reference: dict, candidate: dict) -> dict[str, Any]:
    keys = sorted(set(reference) & set(candidate))
    mode = os.environ.get("EVAL_CONTEXT_SCORER", "semantic").lower()

    sims: list[float] | None = None
    method = "lexical"
    if keys and mode != "lexical":
        embedder = _get_embedder()
        if embedder is not None:
            try:
                ref_vecs = _embed_all(embedder, [reference[k] or "" for k in keys])
                cand_vecs = _embed_all(embedder, [candidate[k] or "" for k in keys])
                sims = [_cosine(r, c) for r, c in zip(ref_vecs, cand_vecs)]
                method = "embedding_cosine"
            except Exception:  # noqa: BLE001 — fall back to lexical on any embed error
                sims = None
    if sims is None:
        sims = [
            SequenceMatcher(None, reference[k] or "", candidate[k] or "").ratio()
            for k in keys
        ]
        method = "lexical" if mode == "lexical" else "lexical_fallback"

    return {
        "mean_similarity": _mean(sims),
        "similarity_method": method,
        "chunks_scored": len(keys),
        "chunks_reference": len(reference),
        "chunks_candidate": len(candidate),
    }


# ── extraction: set-F1 over the tag fields ───────────────────────────

def _loc_key(ref: dict) -> str:
    return (ref.get("pcode") or ref.get("name") or "").strip().lower()


def score_extraction(reference: dict, candidate: dict) -> dict[str, Any]:
    keys = set(reference) & set(candidate)
    events, sectors, locs, time_hits = [], [], [], []
    for k in keys:
        r, c = reference[k], candidate[k]
        events.append(_prf(set(r.get("event_types") or []), set(c.get("event_types") or []))["f1"])
        sectors.append(_prf(set(r.get("need_sectors") or []), set(c.get("need_sectors") or []))["f1"])
        locs.append(_prf(
            {_loc_key(x) for x in (r.get("locations") or [])} - {""},
            {_loc_key(x) for x in (c.get("locations") or [])} - {""},
        )["f1"])
        time_hits.append(1.0 if (
            r.get("time_range_start") == c.get("time_range_start")
            and r.get("time_range_end") == c.get("time_range_end")
        ) else 0.0)
    fields = {
        "event_types_f1": _mean(events),
        "need_sectors_f1": _mean(sectors),
        "locations_f1": _mean(locs),
        "time_range_exact": _mean(time_hits),
    }
    fields["overall"] = _mean(list(fields.values()))
    fields["chunks_scored"] = len(keys)
    return fields


# ── datapoints: numeric-leaf agreement ───────────────────────────────

def _numeric_leaves(obj: Any, path: str, out: dict[str, float]) -> None:
    """Collect every numeric figure as {dotted-path: value}. A figure is a
    QualityEnvelope-shaped dict carrying a numeric ``value`` (the shape the
    datapoints schema uses); bare numbers are collected too."""
    if isinstance(obj, dict):
        if isinstance(obj.get("value"), (int, float)) and not isinstance(obj.get("value"), bool):
            out[path] = float(obj["value"])
        for k, v in obj.items():
            if k == "value":
                continue
            _numeric_leaves(v, f"{path}.{k}" if path else k, out)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            _numeric_leaves(v, f"{path}[{i}]", out)
    elif isinstance(obj, (int, float)) and not isinstance(obj, bool):
        out[path] = float(obj)


def _close(a: float, b: float, rel_tol: float) -> bool:
    return math.isclose(a, b, rel_tol=rel_tol, abs_tol=1e-9)


# Default figure-agreement tolerance. Humanitarian figures are routinely
# rounded/re-quoted (45,000 vs 44,800 vs "~45k"), so exact-match under-counts
# genuine agreement. 1% is tight enough to still catch a real disagreement.
# Override with EVAL_DP_REL_TOL (0 = exact).
_DP_REL_TOL = float(os.environ.get("EVAL_DP_REL_TOL", "0.01"))


def score_datapoints(
    reference: dict, candidate: dict, *, rel_tol: float | None = None,
) -> dict[str, Any]:
    """``rel_tol`` is the relative tolerance for two figures to 'agree'.
    Defaults to ``_DP_REL_TOL`` (1%, env-overridable); pass 0.0 for exact."""
    if rel_tol is None:
        rel_tol = _DP_REL_TOL
    ref_leaves: dict[str, float] = {}
    cand_leaves: dict[str, float] = {}
    _numeric_leaves(reference, "", ref_leaves)
    _numeric_leaves(candidate, "", cand_leaves)

    shared = set(ref_leaves) & set(cand_leaves)
    agree = sum(1 for k in shared if _close(ref_leaves[k], cand_leaves[k], rel_tol))

    # Domain-presence parity: did the candidate populate the same domains?
    # Meaningful even for figure-less reports, so it's always scored.
    domains = set(reference) | set(candidate)
    domain_hits = [
        1.0 for d in domains
        if (reference.get(d) is not None) == (candidate.get(d) is not None)
    ]

    if ref_leaves:
        prf = _prf(set(ref_leaves), set(cand_leaves))
        fp, fr, ff = prf["precision"], prf["recall"], prf["f1"]
        va = (agree / len(shared)) if shared else 0.0
    else:
        # Claude produced NO numeric figures for this report → there's nothing
        # to match against, so figure metrics are non-informative. Return None
        # (the aggregator skips it) rather than a vacuous 1.0 for empty-vs-empty.
        fp = fr = ff = va = None

    return {
        "figure_precision": fp,   # candidate figures Claude also produced (None if no ref figures)
        "figure_recall": fr,      # Claude figures the candidate reproduced
        "figure_f1": ff,
        "value_agreement": va,
        "domain_presence_parity": _mean(domain_hits),
        "reference_figures": len(ref_leaves),
        "candidate_figures": len(cand_leaves),
        "matched_figures": len(shared),
        "rel_tol": rel_tol,
    }


SCORERS = {
    "context": score_context,
    "extraction": score_extraction,
    "datapoints": score_datapoints,
}
