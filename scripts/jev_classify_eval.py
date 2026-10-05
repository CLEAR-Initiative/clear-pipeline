#!/usr/bin/env python
"""Prototype + eval: Jev (TypeSafe System One, via OpenRouter) for GLIDE
disaster-type classification, head-to-head against the current MiniLM + keyword
classifier (`classify_locally`).

Jev is served through OpenRouter's *Decisions* API (NOT chat-completions): you
send `state` + typed `questions` (here one Choice over the GLIDE codes) and get
back the chosen code + a calibrated confidence + the full probability
distribution. That calibrated confidence is what a production confidence-gate
(auto-accept high; escalate the low-confidence tail) would key on.

Run (needs an OpenRouter key with Jev access — this calls a paid API):
    OPENROUTER_API_KEY=sk-or-... .venv/bin/python scripts/jev_classify_eval.py

Flags:
    --dry-run        Build + print the Jev request for each example; no API call,
                     no key, no cost. Use to inspect the prompt/criteria.
    --no-baseline    Skip the MiniLM baseline (avoids the torch/sentence-
                     transformers import + model load).
    --gold PATH      JSON file of real labelled signals: [{"text": "...",
                     "code": "<glide>"}, ...]. Defaults to the built-in seed set
                     (canonical taxonomy phrasings + a few messy/multilingual
                     probes) — a SMOKE TEST, not a real benchmark. Point this at
                     a few hundred real signals for a decision-grade eval.
    --list           Print the full code -> L1 > L2 > L3 taxonomy and exit
                     (handy when authoring a --gold file).

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

import requests

# Taxonomy ships inside the package; read it directly so this stays decoupled
# from the classifier internals.
from clear_pipeline.providers import classify as C

MODEL = os.environ.get("JEV_MODEL", "typesafe/jev-1.13")
JEV_URL = os.environ.get("JEV_URL", "https://openrouter.ai/api/alpha/decisions")

INSTRUCTIONS = (
    "Classify the emergency/disaster signal in `text` into exactly one GLIDE "
    "disaster-type code. Pick the single code whose category best matches the "
    "PRIMARY hazard or event described. If several apply, choose the dominant one."
)


def build_criteria(taxonomy: list[dict]) -> dict[str, str]:
    """GLIDE code -> a concise criterion the model chooses among: the
    L1 > L2 > L3 path plus a couple of canonical phrasings as cues."""
    criteria: dict[str, str] = {}
    for row in taxonomy:
        code = row.get("id")
        if not code:
            continue
        path = " > ".join(
            p for p in (row.get("type_level_1"), row.get("type_level_2"), row.get("type_level_3")) if p
        )
        cues = "; ".join((row.get("key_phrases") or [])[:3])
        criteria[code] = f"{path} (e.g. {cues})" if cues else path
    return criteria


def classify_jev(text: str, criteria: dict[str, str], api_key: str) -> dict:
    """One Jev Choice over all GLIDE codes. Returns the parsed answer dict:
    {choice, confidence, probabilities} plus _cost / _latency_ms."""
    body = {
        "model": MODEL,
        "state": {"text": text},
        "questions": {
            "glide": {"type": "choice", "instructions": INSTRUCTIONS, "criteria": criteria}
        },
    }
    t0 = time.monotonic()
    resp = requests.post(
        JEV_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=body,
        timeout=60,
    )
    latency_ms = (time.monotonic() - t0) * 1000
    resp.raise_for_status()
    data = resp.json()
    ans = data["answers"]["glide"]
    ans["_cost"] = (data.get("usage") or {}).get("cost", 0.0)
    ans["_latency_ms"] = latency_ms
    return ans


def seed_gold(taxonomy: list[dict]) -> list[dict]:
    """Built-in SMOKE gold set. Canonical cases are auto-built from the taxonomy
    (text = a key phrase, label = that code — so every label is a real code from
    the file, nothing hand-guessed). A few deliberately messy / non-English
    probes test disambiguation + multilingual, using codes confirmed present."""
    by_code = {r["id"]: r for r in taxonomy if r.get("id")}
    gold: list[dict] = []
    # One canonical phrasing per code that has key_phrases — a spread across the
    # whole taxonomy (6 L1s / 33 L2s / 52 codes).
    for code, row in by_code.items():
        phrases = row.get("key_phrases") or []
        if phrases:
            gold.append({"text": phrases[0], "code": code, "kind": "canonical"})
    # Messy / multilingual probes (only kept if the code exists in this taxonomy).
    probes = [
        {"text": "Thousands marched peacefully through the capital demanding reform", "code": "pp"},
        {"text": "Manifestation pacifique de milliers de personnes dans la capitale", "code": "pp"},
        {"text": "Ola de frío extremo deja temperaturas bajo cero en la región", "code": "cw"},
        {"text": "Rivers burst their banks, submerging dozens of villages overnight", "code": "fl"},
    ]
    gold += [{**p, "kind": "messy"} for p in probes if p["code"] in by_code]
    return gold


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-baseline", action="store_true")
    ap.add_argument("--gold")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()

    taxonomy = C._load_taxonomy()
    criteria = build_criteria(taxonomy)
    code_to_l1 = C.code_to_level1_map()

    if args.list:
        for code, desc in sorted(criteria.items()):
            print(f"{code:>4}  {desc}")
        return

    if args.gold:
        gold = json.loads(Path(args.gold).read_text())
    else:
        gold = seed_gold(taxonomy)

    # Drop any gold rows whose code isn't in the taxonomy (don't score against a
    # label the classifier can never produce).
    valid = set(criteria)
    dropped = [g for g in gold if g["code"] not in valid]
    gold = [g for g in gold if g["code"] in valid]
    if dropped:
        print(f"⚠️  dropped {len(dropped)} gold rows with unknown codes: "
              f"{sorted({g['code'] for g in dropped})}\n")

    if args.dry_run:
        example = gold[0]
        print("=== DRY RUN — sample Jev request (no API call) ===")
        print(json.dumps({
            "model": MODEL,
            "state": {"text": example["text"]},
            "questions": {"glide": {"type": "choice", "instructions": INSTRUCTIONS,
                                    "criteria": dict(list(criteria.items())[:4] + [("…", f"+{len(criteria)-4} more codes")])}},
        }, indent=2, ensure_ascii=False))
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
    rows: list[str] = []

    for g in gold:
        text, gold_code = g["text"], g["code"]
        gold_l1 = code_to_l1.get(gold_code)

        ans = classify_jev(text, criteria, api_key)
        jc = ans.get("choice")
        conf = float(ans.get("confidence") or 0.0)
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
            f"{mark} gold={gold_code:<4} jev={str(jc):<4} conf={conf:.2f}"
            + (f"  base={str(bc):<4}" if baseline else "")
            + f"  | {text[:60]}"
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


if __name__ == "__main__":
    main()
