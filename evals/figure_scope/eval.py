"""SPIKE #271 — can the LLM reliably identify a figure's Figure Scope?

Controlled eval: authored cases with known-correct scope labels, covering
the failure modes the ticket names. Runs the configured extraction model
through the pipeline's own provider path. Investigation tooling — NOT
production code, and intentionally not wired into any Dagster asset.

Run from the repo root:
    .venv/bin/python evals/figure_scope/eval.py

Results (claude-sonnet-4-6, stable across 3 runs) and the recommendation
for #272 are in FINDINGS.md alongside this file.
"""

import json
import sys
from pathlib import Path
from typing import Literal, Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field

# Repo root = two levels up from evals/figure_scope/. Load .env + make the
# package importable without any absolute paths baked in.
REPO = Path(__file__).resolve().parents[2]
load_dotenv(REPO / ".env")
sys.path.insert(0, str(REPO / "src"))

from clear_pipeline.providers.llm import make_llm_provider  # noqa: E402


# ── The model's answer shape ────────────────────────────────────────
class FigureScope(BaseModel):
    """The single geographic scope a stated figure is a total FOR."""
    location_name: Optional[str] = Field(
        default=None,
        description="The ONE place this figure counts — the area the number "
        "is a total for, NOT every place the text mentions. Null if unresolvable.",
    )
    admin_level: Optional[Literal[0, 1, 2, 3]] = Field(
        default=None,
        description="0=country, 1=state/province, 2=district/locality, 3=sub-locality. "
        "Null if unresolvable.",
    )
    unresolvable: bool = Field(
        default=False,
        description="True if the figure cannot be pinned to one geographic scope.",
    )
    reasoning: str = Field(description="One sentence: why this scope.")


SYSTEM = """You extract the GEOGRAPHIC SCOPE of a specific humanitarian figure from report text.

A report is analytical: a figure (e.g. "50,000 displaced") is a total already aggregated over ONE geographic area — its Figure Scope. Your job: identify that ONE area.

Critical distinctions:
- The scope is the area the figure is a total FOR, NOT every place the report mentions. A report about Sudan may state a figure for one town; the scope is the town.
- If a figure is explicitly a total across several named areas, the scope is their common parent (e.g. three states in Darfur -> "Darfur" region) — or mark unresolvable if no single parent fits.
- If no geographic area can be pinned to the figure, set unresolvable=true. Do NOT default to the country or to the first place mentioned.
- admin_level: 0=country, 1=state, 2=district/locality, 3=sub-locality.

Return the scope of the SPECIFIC figure named in the request, nothing else."""


# ── Controlled eval set — (report_text, figure, ground truth) ───────
# gt_names: set of acceptable location names (lowercased), or None for
# unresolvable. gt_unresolvable: True (must abstain), False (must resolve),
# or None (either the named parent OR abstention is acceptable).
CASES = [
    dict(id="explicit-a1", category="explicit",
         text="As of 30 June, an estimated 1,000 people were affected by flooding in Kordofan State over the past week.",
         figure="people affected (1,000)",
         gt_level=1, gt_names={"kordofan", "kordofan state"}, gt_unresolvable=False),
    dict(id="subadmin-in-country-report", category="scope-vs-mention",
         text="SUDAN HUMANITARIAN UPDATE. The crisis continues nationwide. In El Fasher, 200 people were killed during clashes on 2 July.",
         figure="people killed (200)",
         gt_level=2, gt_names={"el fasher", "al fashir", "el-fasher"}, gt_unresolvable=False),
    dict(id="country-headline", category="explicit",
         text="Nationwide, 8.2 million people across Sudan are assessed to be in need of humanitarian assistance in 2026.",
         figure="people in need (8.2 million)",
         gt_level=0, gt_names={"sudan"}, gt_unresolvable=False),
    dict(id="multi-state-total", category="multi-area",
         text="Across North Darfur, South Darfur and West Darfur, a combined 50,000 people were newly displaced in June.",
         figure="newly displaced (50,000)",
         gt_level=1, gt_names={"darfur"}, gt_unresolvable=None),
    dict(id="implied-by-framing", category="implied",
         text="KASSALA STATE FLOOD RESPONSE. Following heavy rains, 3,000 households now lack access to clean water.",
         figure="households lacking clean water (3,000)",
         gt_level=1, gt_names={"kassala", "kassala state"}, gt_unresolvable=False),
    dict(id="unresolvable-noplace", category="unresolvable",
         text="Thousands of people have been affected by the ongoing crisis, with needs rising sharply.",
         figure="people affected (thousands)",
         gt_level=None, gt_names=None, gt_unresolvable=True),
    dict(id="distractor-two-towns", category="scope-vs-mention",
         text="Aid convoys reached Port Sudan this week. Meanwhile in El Geneina, 400 people were killed in renewed fighting.",
         figure="people killed (400)",
         gt_level=2, gt_names={"el geneina", "el-geneina", "geneina", "al-junaynah"}, gt_unresolvable=False),
    dict(id="explicit-a2", category="explicit",
         text="In Zalingei locality, 12,500 internally displaced persons are sheltering in three gathering sites.",
         figure="IDPs (12,500)",
         gt_level=2, gt_names={"zalingei"}, gt_unresolvable=False),
    dict(id="country-fig-state-example", category="scope-vs-mention",
         text="Sudan-wide, 4.9 million children are out of school; the situation is especially acute in Khartoum.",
         figure="children out of school (4.9 million)",
         gt_level=0, gt_names={"sudan"}, gt_unresolvable=False),
    dict(id="unresolvable-vague-region", category="unresolvable",
         text="Across the region, an estimated 200,000 people require food assistance amid deteriorating conditions.",
         figure="people requiring food assistance (200,000)",
         gt_level=None, gt_names=None, gt_unresolvable=True),
    dict(id="state-fig-localities-named", category="scope-vs-mention",
         text="In South Kordofan, 15,000 people were displaced, with movements recorded in Kadugli, Dilling and Abu Jubayha.",
         figure="people displaced (15,000)",
         gt_level=1, gt_names={"south kordofan"}, gt_unresolvable=False),
    dict(id="two-figures-pick-second", category="scope-vs-mention",
         text="Sudan hosts 6 million IDPs nationally. In Nyala specifically, 800 households were affected by a market fire.",
         figure="households affected by market fire (800)",
         gt_level=2, gt_names={"nyala"}, gt_unresolvable=False),
    dict(id="explicit-a1-blue-nile", category="explicit",
         text="Blue Nile State reported 5,600 new arrivals from across the border during the reporting period.",
         figure="new arrivals (5,600)",
         gt_level=1, gt_names={"blue nile", "blue nile state"}, gt_unresolvable=False),
    dict(id="multi-state-no-parent", category="multi-area",
         text="Combined figures for Kassala, Gedaref and River Nile show 22,000 people reached with assistance.",
         figure="people reached (22,000)",
         gt_level=None, gt_names=None, gt_unresolvable=None),
    dict(id="country-implied", category="implied",
         text="REPUBLIC OF SUDAN — NATIONAL NUTRITION SURVEY 2026. Global acute malnutrition affects 1.3 million children under five.",
         figure="children with GAM (1.3 million)",
         gt_level=0, gt_names={"sudan"}, gt_unresolvable=False),
    dict(id="camp-level", category="explicit",
         text="At Zamzam camp, 45,000 residents face critical food shortages following the latest displacement wave.",
         figure="camp residents facing shortages (45,000)",
         gt_level=2, gt_names={"zamzam", "zamzam camp"}, gt_unresolvable=False),
]


def normalise(s):
    return (s or "").strip().lower()


def judge(case, ans: "FigureScope"):
    """Return (verdict, detail). verdict in {correct, acceptable, wrong}."""
    if case["gt_unresolvable"] is None:
        if ans.unresolvable:
            return "acceptable", "model chose unresolvable (accepted)"
        if case["gt_names"] and normalise(ans.location_name) in case["gt_names"]:
            return "acceptable", f"model chose {ans.location_name} (accepted parent)"
        return "wrong", f"expected the common parent or unresolvable; got {ans.location_name!r} L{ans.admin_level}"
    if case["gt_unresolvable"]:
        return ("correct", "correctly unresolvable") if ans.unresolvable else \
            ("wrong", f"should be unresolvable; got {ans.location_name!r} L{ans.admin_level}")
    if ans.unresolvable:
        return "wrong", f"marked unresolvable; expected {sorted(case['gt_names'])} L{case['gt_level']}"
    name_ok = normalise(ans.location_name) in case["gt_names"]
    level_ok = ans.admin_level == case["gt_level"]
    if name_ok and level_ok:
        return "correct", "name+level match"
    if name_ok and not level_ok:
        return "wrong", f"right place, wrong level (got L{ans.admin_level}, want L{case['gt_level']})"
    return "wrong", f"got {ans.location_name!r} L{ans.admin_level}; want {sorted(case['gt_names'])} L{case['gt_level']}"


def main():
    llm = make_llm_provider("extraction")
    print(f"model: {llm.provider_name}/{llm.model}\n" + "=" * 72)
    results = []
    for c in CASES:
        user = f"REPORT TEXT:\n{c['text']}\n\nFIGURE: {c['figure']}\n\nWhat is this figure's geographic scope?"
        try:
            ans = llm.complete_structured(system=SYSTEM, user=user, schema=FigureScope, max_tokens=400)
        except Exception as e:  # noqa: BLE001
            results.append((c, None, "error", str(e)[:80]))
            print(f"[ERR ] {c['id']:<28} {str(e)[:60]}")
            continue
        verdict, detail = judge(c, ans)
        results.append((c, ans, verdict, detail))
        mark = {"correct": "PASS", "acceptable": "ACC ", "wrong": "FAIL"}[verdict]
        got = "UNRESOLVABLE" if ans.unresolvable else f"{ans.location_name} (L{ans.admin_level})"
        print(f"[{mark}] {c['id']:<28} {c['category']:<16} -> {got}")
        if verdict == "wrong":
            print(f"        {detail}")

    print("=" * 72)
    n = len(results)
    correct = sum(1 for _, _, v, _ in results if v == "correct")
    acc = sum(1 for _, _, v, _ in results if v == "acceptable")
    wrong = sum(1 for _, _, v, _ in results if v == "wrong")
    err = sum(1 for _, _, v, _ in results if v == "error")
    print(f"correct={correct}  acceptable={acc}  wrong={wrong}  error={err}  n={n}")
    if n:
        print(f"hit rate (correct+acceptable): {(correct + acc) / n:.0%}   strict: {correct / n:.0%}")
    cats: dict = {}
    for c, _, v, _ in results:
        cats.setdefault(c["category"], []).append(v)
    print("\nby category:")
    for cat, vs in sorted(cats.items()):
        ok = sum(1 for v in vs if v in ("correct", "acceptable"))
        print(f"  {cat:<16} {ok}/{len(vs)}")
    outp = Path(__file__).parent / "results.json"
    payload = [dict(id=c["id"], category=c["category"], verdict=v, detail=d,
                    answer=(ans.model_dump() if ans else None))
               for c, ans, v, d in results]
    outp.write_text(json.dumps(payload, indent=2))
    print(f"\nfull results -> {outp.relative_to(REPO)}")


if __name__ == "__main__":
    main()
