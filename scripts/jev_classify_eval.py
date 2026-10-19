#!/usr/bin/env python
"""Prototype + eval: Jev (TypeSafe System One, via OpenRouter) for GLIDE
disaster-type classification, scored against a gold set. (The old MiniLM +
keyword classifier it used to be compared against has been removed — Jev is now
the only classifier.)

Jev is served through OpenRouter's *Decisions* API (NOT chat-completions): you
send `state` + typed `questions` and get back typed answers with calibrated
probabilities. This eval issues the SAME single Choice question the production
classifier does (providers/jev.py) — reusing its instructions + criteria so the
eval can't drift from production:

  * `glide` — a Choice over all 52 GLIDE codes (+ an explicit "none") → the primary
              disaster type + a calibrated confidence + the full probability
              distribution.

Relevance is NOT a separate question anymore: the production gate keys on
`1 - P("none")` from the glide distribution (`settings.relevance_threshold`). A
live run here (a) exercises the exact production request shape and (b) produces the
relevance distribution + a threshold sweep you need to tune `relevance_threshold`.

Run (needs an OpenRouter key with Jev access — this calls a paid API):
    OPENROUTER_API_KEY=sk-or-... .venv/bin/python scripts/jev_classify_eval.py

Flags:
    --dry-run        Build + print the Jev request for each example; no API call,
                     no key, no cost. Use to inspect the prompt/criteria.
    --gold PATH      JSON file of real labelled signals: [{"text": "...",
                     "code": "<glide>", "relevant": true}, ...]. `relevant` is
                     optional (see the threshold sweep below). Defaults to the
                     built-in seed set — a SMOKE TEST, not a real benchmark.
                     Point this at a few hundred real signals for a decision-grade
                     eval (and a trustworthy threshold).
    --list           Print the full code -> L1 > L2 > L3 taxonomy and exit.

Threshold sweep: for each candidate gate the eval reports how many signals would
be kept vs. dropped. If gold rows carry a `relevant` bool it also reports
precision/recall of the gate; rows WITHOUT the field are assumed relevant=true
(the built-in set is incidents-by-construction), so a sweep on it mostly measures
how many real incidents a given gate would wrongly drop.

The Decisions endpoint is https://openrouter.ai/api/alpha/decisions; the API
reference also documents https://openrouter.ai/api/v1/systemone — override with
JEV_URL=... if one 404s for your account.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import httpx

# Taxonomy ships inside the package; read it directly so this stays decoupled
# from the classifier internals.
from clear_pipeline.providers import classify as C

# Reuse the PRODUCTION request constants so the eval can't drift from what the
# pipeline actually sends.
from clear_pipeline.providers import jev

MODEL = jev.JEV_MODEL
JEV_URL = jev.JEV_URL

_THRESHOLDS = (0.3, 0.4, 0.5, 0.6, 0.7)


def build_body(text: str) -> dict:
    """The exact single-Choice Decisions request the production classifier sends."""
    return {
        "model": MODEL,
        "state": {"text": text},
        "questions": {
            "glide": {
                "type": "choice",
                "instructions": jev._GLIDE_INSTRUCTIONS,
                "criteria": jev._glide_criteria(),
            },
        },
    }


def classify_jev(text: str, api_key: str) -> dict:
    """One Jev Choice call. Returns
    {choice, confidence, relevance, _cost, _latency_ms}, where relevance is the
    production gate value 1 - P("none") from the glide distribution."""
    body = build_body(text)
    t0 = time.monotonic()
    resp = httpx.post(
        JEV_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=body,
        timeout=60,
    )
    latency_ms = (time.monotonic() - t0) * 1000
    resp.raise_for_status()
    data = resp.json()
    answers = data["answers"]
    glide = answers["glide"]
    probabilities = glide.get("probabilities") or {}
    if probabilities:
        relevance = max(0.0, 1.0 - float(probabilities.get(jev.NONE_CHOICE) or 0.0))
    else:
        relevance = 0.0 if glide.get("choice") == jev.NONE_CHOICE else float(glide.get("confidence") or 0.0)
    return {
        "choice": glide.get("choice"),
        "confidence": float(glide.get("confidence") or 0.0),
        "relevance": relevance,
        "_cost": (data.get("usage") or {}).get("cost", 0.0),
        "_latency_ms": latency_ms,
    }


def seed_gold(taxonomy: list[dict]) -> list[dict]:
    """Built-in SMOKE gold set. Canonical cases are auto-built from the taxonomy
    (text = a key phrase, label = that code). A few messy / non-English probes
    test disambiguation + multilingual."""
    by_code = {r["id"]: r for r in taxonomy if r.get("id")}
    gold: list[dict] = []
    for code, row in by_code.items():
        phrases = row.get("key_phrases") or []
        if phrases:
            gold.append({"text": phrases[0], "code": code, "kind": "canonical"})
    probes = [
        {"text": "Thousands marched peacefully through the capital demanding reform", "code": "pp"},
        {"text": "Manifestation pacifique de milliers de personnes dans la capitale", "code": "pp"},
        {"text": "Ola de frío extremo deja temperaturas bajo cero en la región", "code": "cw"},
        {"text": "Rivers burst their banks, submerging dozens of villages overnight", "code": "fl"},
    ]
    gold += [{**p, "kind": "messy"} for p in probes if p["code"] in by_code]
    return gold


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = max(0, min(len(sorted_vals) - 1, int(round((pct / 100) * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


def _report_code_confidence(confs: list[float]) -> None:
    """Print the glide-code confidence distribution — the data needed to pick
    `signal_jev_min_code_confidence` (below it a specific code is demoted to the
    'other' catch-all). Watch the low tail: set the floor under the bulk of CORRECT
    answers, or correct-but-split classifications get demoted to 'other'."""
    if not confs:
        return
    s = sorted(confs)
    n = len(s)
    print("\n" + "=" * 60)
    print("Code-confidence distribution — floor (signal_jev_min_code_confidence) demotes below this")
    print(f"  min {s[0]:.2f}  p10 {_percentile(s, 10):.2f}  p25 {_percentile(s, 25):.2f}  "
          f"median {_percentile(s, 50):.2f}  mean {sum(s)/n:.2f}  max {s[-1]:.2f}")
    buckets = [0] * 10
    for v in s:
        buckets[min(9, int(v * 10))] += 1
    print("  histogram (0.0→1.0): " + " ".join(f"{b}" for b in buckets))


def _report_relevance(gold: list[dict], relevances: list[float]) -> None:
    """Print the relevance distribution, the lowest-relevance rows, and a gate
    sweep — the data needed to pick `relevance_threshold`. Relevance is
    1 - P("none") from the glide distribution (the production gate value)."""
    if not relevances:
        return
    s = sorted(relevances)
    n = len(s)
    print("\n" + "=" * 60)
    print("Relevance distribution — gate = 1 - P(\"none\") from the glide distribution")
    print(f"  min {s[0]:.2f}  p10 {_percentile(s, 10):.2f}  median {_percentile(s, 50):.2f}  "
          f"mean {sum(s)/n:.2f}  p90 {_percentile(s, 90):.2f}  max {s[-1]:.2f}")
    # 10-bucket histogram.
    buckets = [0] * 10
    for v in s:
        buckets[min(9, int(v * 10))] += 1
    print("  histogram (0.0→1.0): " + " ".join(f"{b}" for b in buckets))

    # Lowest-relevance rows — eyeball whether the low tail is the ambiguous /
    # non-event probes (good) or real incidents (gate would wrongly drop them).
    paired = sorted(zip(relevances, gold), key=lambda x: x[0])
    print("\nLowest-relevance rows (inspect — are these the non-events?):")
    for rel_score, g in paired[:8]:
        rel = g.get("relevant")
        tag = "" if rel is None else f" [labelled relevant={rel}]"
        print(f"  rel={rel_score:.2f}  gold={g['code']:<4}{tag}  | {g['text'][:56]}")

    # Threshold sweep.
    labelled = [(rl, g.get("relevant")) for rl, g in zip(relevances, gold)]
    print("\nThreshold sweep (rows w/o a `relevant` label assumed relevant=true):")
    for thr in _THRESHOLDS:
        kept = sum(1 for rl in relevances if rl >= thr)
        dropped = n - kept
        # Treat unlabelled as relevant=true (this set is incidents-by-construction).
        tp = sum(1 for rl, r in labelled if rl >= thr and (r is True or r is None))
        fp = sum(1 for rl, r in labelled if rl >= thr and r is False)
        fn = sum(1 for rl, r in labelled if rl < thr and (r is True or r is None))
        prec = tp / (tp + fp) if (tp + fp) else 1.0
        rec = tp / (tp + fn) if (tp + fn) else 1.0
        print(f"  thr={thr:.2f}  kept {kept:>3}/{n}  dropped {dropped:>3}   "
              f"precision {prec:.2f}  recall {rec:.2f}")
    neg = sum(1 for _, r in labelled if r is False)
    print(f"  ({neg} rows labelled relevant=false; add more non-event rows for a sharper gate)")
    print("  NOTE: relevance is 'is it a real disaster TYPE' (1 - P(none)), not 'is it a")
    print("  CURRENT incident' — a confident classification of a retrospective/forecast")
    print("  about a real hazard scores HIGH here and is NOT filtered by this gate.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--gold")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()

    taxonomy = C._load_taxonomy()
    criteria = jev._glide_criteria()
    code_to_l1 = C.code_to_level1_map()

    if args.list:
        for code, desc in sorted(criteria.items()):
            print(f"{code:>4}  {desc}")
        return

    if args.gold:
        gold = json.loads(Path(args.gold).read_text())
    else:
        gold = seed_gold(taxonomy)

    # Drop any gold rows whose code isn't in the taxonomy.
    valid = set(criteria)
    dropped = [g for g in gold if g["code"] not in valid]
    gold = [g for g in gold if g["code"] in valid]
    if dropped:
        print(f"⚠️  dropped {len(dropped)} gold rows with unknown codes: "
              f"{sorted({g['code'] for g in dropped})}\n")

    if args.dry_run:
        example = gold[0]
        body = build_body(example["text"])
        trimmed = dict(list(criteria.items())[:4] + [("…", f"+{len(criteria)-4} more codes")])
        body["questions"]["glide"]["criteria"] = trimmed
        print("=== DRY RUN — sample Jev request (no API call) ===")
        print(json.dumps(body, indent=2, ensure_ascii=False))
        print(f"\ngold examples: {len(gold)}  |  criteria/codes: {len(criteria)}  "
              f"|  endpoint: {JEV_URL}  |  model: {MODEL}")
        return

    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise SystemExit("Set OPENROUTER_API_KEY (or use --dry-run).")

    jev_code_hits = jev_l1_hits = 0
    total_cost = 0.0
    total_latency = 0.0
    conf_sum = 0.0
    relevances: list[float] = []
    confs: list[float] = []
    rows: list[str] = []

    for g in gold:
        text, gold_code = g["text"], g["code"]
        gold_l1 = code_to_l1.get(gold_code)

        ans = classify_jev(text, api_key)
        jc = ans["choice"]
        conf = ans["confidence"]
        relevance = ans["relevance"]
        relevances.append(relevance)
        confs.append(conf)
        total_cost += ans["_cost"]
        total_latency += ans["_latency_ms"]
        conf_sum += conf
        jev_code_hits += jc == gold_code
        jev_l1_hits += code_to_l1.get(jc) == gold_l1

        mark = "✓" if jc == gold_code else ("~" if code_to_l1.get(jc) == gold_l1 else "✗")
        rows.append(
            f"{mark} gold={gold_code:<4} jev={str(jc):<4} conf={conf:.2f} rel={relevance:.2f}"
            + f"  | {text[:52]}"
        )

    n = len(gold)
    print("\n".join(rows))
    print("\n" + "=" * 60)
    print(f"N={n}   (✓ exact code · ~ right L1 only · ✗ wrong L1)")
    print(f"Jev      code-acc {jev_code_hits/n:.1%}   L1-acc {jev_l1_hits/n:.1%}   "
          f"mean-conf {conf_sum/n:.2f}")
    print(f"Jev cost ${total_cost:.5f} total (${total_cost/n:.6f}/signal)   "
          f"avg latency {total_latency/n:.0f} ms")

    _report_code_confidence(confs)
    _report_relevance(gold, relevances)


if __name__ == "__main__":
    main()
