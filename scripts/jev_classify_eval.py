#!/usr/bin/env python
"""Prototype + eval: Jev (TypeSafe System One, via OpenRouter) for GLIDE
disaster-type classification, head-to-head against the current MiniLM + keyword
classifier (`classify_locally`).

Jev is served through OpenRouter's *Decisions* API (NOT chat-completions): you
send `state` + typed `questions` and get back typed answers with calibrated
probabilities. This eval issues the SAME two parallel questions the production
classifier does (providers/jev.py) — reusing its instructions + criteria so the
eval can't drift from production:

  * `glide`    — a Choice over all 52 GLIDE codes → the disaster type + a
                 calibrated confidence + the full probability distribution.
  * `relevant` — a Noul "is this an actual, current incident?" → the probability
                 the production relevance-gate keys on (`settings.relevance_threshold`).

Because it sends both questions, a live run here (a) exercises the exact
production request shape (confirming the live Noul parse) and (b) produces the
Noul distribution + a threshold sweep you need to tune `relevance_threshold`.

Run (needs an OpenRouter key with Jev access — this calls a paid API):
    OPENROUTER_API_KEY=sk-or-... .venv/bin/python scripts/jev_classify_eval.py

Flags:
    --dry-run        Build + print the Jev request for each example; no API call,
                     no key, no cost. Use to inspect the prompt/criteria.
    --no-baseline    Skip the MiniLM baseline (avoids the torch/sentence-
                     transformers import + model load).
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
# pipeline actually sends (the review's J8: the eval used to send only `glide`).
from clear_pipeline.providers import jev

MODEL = jev.JEV_MODEL
JEV_URL = jev.JEV_URL

_THRESHOLDS = (0.3, 0.4, 0.5, 0.6, 0.7)


def build_body(text: str) -> dict:
    """The exact two-question Decisions request the production classifier sends."""
    return {
        "model": MODEL,
        "state": {"text": text},
        "questions": {
            "glide": {
                "type": "choice",
                "instructions": jev._GLIDE_INSTRUCTIONS,
                "criteria": jev._glide_criteria(),
            },
            "relevant": {
                "type": "noul",
                "instructions": jev._RELEVANT_INSTRUCTIONS,
                "criteria": jev._RELEVANT_CRITERIA,
            },
        },
    }


def classify_jev(text: str, api_key: str) -> dict:
    """One Jev call issuing both questions. Returns
    {choice, confidence, noul, _cost, _latency_ms}."""
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
    relevant = answers["relevant"]
    return {
        "choice": glide.get("choice"),
        "confidence": float(glide.get("confidence") or 0.0),
        "noul": float(relevant.get("noul") or 0.0),
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


def _report_noul(gold: list[dict], nouls: list[float]) -> None:
    """Print the Noul distribution, the lowest-Noul rows, and a gate sweep — the
    data needed to pick `relevance_threshold`."""
    if not nouls:
        return
    s = sorted(nouls)
    n = len(s)
    print("\n" + "=" * 60)
    print("Noul (relevance) distribution — gate keys on this, NOT the code confidence")
    print(f"  min {s[0]:.2f}  p10 {_percentile(s, 10):.2f}  median {_percentile(s, 50):.2f}  "
          f"mean {sum(s)/n:.2f}  p90 {_percentile(s, 90):.2f}  max {s[-1]:.2f}")
    # 10-bucket histogram.
    buckets = [0] * 10
    for v in s:
        buckets[min(9, int(v * 10))] += 1
    print("  histogram (0.0→1.0): " + " ".join(f"{b}" for b in buckets))

    # Lowest-Noul rows — eyeball whether the low tail is the ambiguous / non-event
    # probes (good) or real incidents (gate would wrongly drop them).
    paired = sorted(zip(nouls, gold), key=lambda x: x[0])
    print("\nLowest-Noul rows (inspect — are these the non-events?):")
    for noul, g in paired[:8]:
        rel = g.get("relevant")
        tag = "" if rel is None else f" [labelled relevant={rel}]"
        print(f"  noul={noul:.2f}  gold={g['code']:<4}{tag}  | {g['text'][:56]}")

    # Threshold sweep.
    labelled = [(nl, g.get("relevant")) for nl, g in zip(nouls, gold)]
    print("\nThreshold sweep (rows w/o a `relevant` label assumed relevant=true):")
    for thr in _THRESHOLDS:
        kept = sum(1 for nl in nouls if nl >= thr)
        dropped = n - kept
        # Treat unlabelled as relevant=true (this set is incidents-by-construction).
        tp = sum(1 for nl, r in labelled if nl >= thr and (r is True or r is None))
        fp = sum(1 for nl, r in labelled if nl >= thr and r is False)
        fn = sum(1 for nl, r in labelled if nl < thr and (r is True or r is None))
        prec = tp / (tp + fp) if (tp + fp) else 1.0
        rec = tp / (tp + fn) if (tp + fn) else 1.0
        print(f"  thr={thr:.2f}  kept {kept:>3}/{n}  dropped {dropped:>3}   "
              f"precision {prec:.2f}  recall {rec:.2f}")
    neg = sum(1 for _, r in labelled if r is False)
    print(f"  ({neg} rows labelled relevant=false; add more non-event rows for a sharper gate)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-baseline", action="store_true")
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

    baseline = None
    if not args.no_baseline:
        baseline = C.classify_locally  # lazy — loads MiniLM on first call

    jev_code_hits = jev_l1_hits = 0
    base_code_hits = base_l1_hits = 0
    total_cost = 0.0
    total_latency = 0.0
    conf_sum = 0.0
    nouls: list[float] = []
    rows: list[str] = []

    for g in gold:
        text, gold_code = g["text"], g["code"]
        gold_l1 = code_to_l1.get(gold_code)

        ans = classify_jev(text, api_key)
        jc = ans["choice"]
        conf = ans["confidence"]
        noul = ans["noul"]
        nouls.append(noul)
        total_cost += ans["_cost"]
        total_latency += ans["_latency_ms"]
        conf_sum += conf
        jev_code_hits += jc == gold_code
        jev_l1_hits += code_to_l1.get(jc) == gold_l1

        bc = None
        if baseline is not None:
            bres = baseline(title=text, description=None)
            bc = (bres.disaster_types or [None])[0]
            base_code_hits += bc == gold_code
            base_l1_hits += code_to_l1.get(bc) == gold_l1

        mark = "✓" if jc == gold_code else ("~" if code_to_l1.get(jc) == gold_l1 else "✗")
        rows.append(
            f"{mark} gold={gold_code:<4} jev={str(jc):<4} conf={conf:.2f} noul={noul:.2f}"
            + (f"  base={str(bc):<4}" if baseline else "")
            + f"  | {text[:52]}"
        )

    n = len(gold)
    print("\n".join(rows))
    print("\n" + "=" * 60)
    print(f"N={n}   (✓ exact code · ~ right L1 only · ✗ wrong L1)")
    print(f"Jev      code-acc {jev_code_hits/n:.1%}   L1-acc {jev_l1_hits/n:.1%}   "
          f"mean-conf {conf_sum/n:.2f}")
    if baseline:
        print(f"MiniLM   code-acc {base_code_hits/n:.1%}   L1-acc {base_l1_hits/n:.1%}   (baseline)")
    print(f"Jev cost ${total_cost:.5f} total (${total_cost/n:.6f}/signal)   "
          f"avg latency {total_latency/n:.0f} ms")

    _report_noul(gold, nouls)


if __name__ == "__main__":
    main()
