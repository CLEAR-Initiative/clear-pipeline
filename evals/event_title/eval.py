"""#1258: does the event title keep the places the signals name?

Runs the real event-rewrite prompt (`rewrite-v4`) and the code check
(`clean_event_title`) over a few cases, through the pipeline's own provider
path for the `signal` role. The first two cases are real sudan-war-x signals
from dev, as they are. Investigation tooling: not wired into any asset.

Run from the repo root, with the `signal` role pointed at a model:

    LLM_SIGNAL_PROVIDER=... LLM_SIGNAL_MODEL=... LLM_SIGNAL_API_KEY=... \\
        uv run --frozen python evals/event_title/eval.py

Results are in FINDINGS.md beside this file.
"""

import re
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO = Path(__file__).resolve().parents[2]
load_dotenv(REPO / ".env")
sys.path.insert(0, str(REPO / "src"))

from clear_pipeline.providers.event import REWRITE_SYSTEM, EventRewrite  # noqa: E402
from clear_pipeline.providers.llm import make_llm_provider  # noqa: E402
from clear_pipeline.providers.prompts.rewrite import (  # noqa: E402
    REWRITE_PROMPT_VERSION,
    TITLE_MAX_CHARS,
    build_rewrite_prompt,
    clean_event_title,
)

ARABIC = re.compile(r"[ء-ي]")
ARABIC_PARENS = re.compile(r"\(([^()]*[ء-ي][^()]*)\)")


def signal(title: str, description: str, published: str, source: str = "sudan-war-x") -> dict:
    return {"title": title, "description": description, "publishedAt": published, "source": {"name": source}}


# must: every one of these (case-insensitive regex) is in the title.
# never: none of these is. all_or_none: all of these are, or none is. arabic: the Arabic spellings the title must carry
# in parentheses ([] = no Arabic at all).
CASES = [
    dict(
        id="three-villages",
        note="dev event cmvuv8hya00051jogybdegcfn, signal cmvuv6vp000001jogfswh0wo7",
        location="North Darfur", type="battles",
        signals=[signal(
            "أعلنت قوات الدعم السريع، في بيان، سيطرتها يوم الخميس على مناطق مزبد وعروري وكامو في ولاية شمال…",
            "أعلنت قوات الدعم السريع، في بيان، سيطرتها يوم الخميس على مناطق مزبد وعروري وكامو في ولاية شمال دارفور، "
            "واصفةً إياها بالمناطق ذات الأهمية الاستراتيجية، وذلك عقب معارك قالت إنها كانت حاسمة.\n"
            "وأضاف البيان أن القوات استولت على مركبات قتالية بكامل عتادها وكميات كبيرة من الأسلحة",
            "2026-10-30T10:29:57.000Z")],
        must=[r"ma[sz]b[uo]d", r"[ao]r[uo]+ri", r"kam[ou]", r"north darfur"],
        never=[r"el fasher", r"al[- ]fashir"],
        arabic=["مزبد", "عروري", "كامو"],  # they fit: 71 characters with all three
    ),
    dict(
        id="al-kanabi",
        note="dev event cmvzgvxui00i31js5wz9xln9a, signal cmvzgd3yd00f81js5cdeow0tl",
        location="Northern", type="explosions / remote violence",
        signals=[signal(
            "عقب القصف الأخير الذي استهدف عدة مواقع في الولايات الشمالية للسودان، أقدمت السلطات المحلية على…",
            "عقب القصف الأخير الذي استهدف عدة مواقع في الولايات الشمالية للسودان، أقدمت السلطات المحلية على إزالة "
            "وتدمير #الكنابي وتهجير قاطنيها قسراً متهمة إياهم برفع إحداثيات للمسيّرات.\n"
            " وفي هذا السياق صرح والي الولاية الشمالية علانية ضد أهالي الكنابي مستهدفاً إياهم على خلفية أصولهم",
            "2026-11-02T15:43:55.000Z")],
        must=[r"k[ae]nabi", r"northern"],
        never=[],
        arabic=["الكنابي"],
    ),
    dict(
        id="state-only",
        note="authored: the text names a state and nothing finer",
        location="North Kordofan", type="disease outbreak",
        signals=[signal(
            "ارتفاع حالات الإصابة بالكوليرا في ولاية شمال كردفان",
            "أعلنت وزارة الصحة ارتفاع حالات الإصابة بالكوليرا في ولاية شمال كردفان إلى 240 حالة خلال أسبوع، "
            "مع تسجيل 9 وفيات، وسط نقص حاد في المحاليل الوريدية.",
            "2026-10-28T08:00:00.000Z")],
        must=[r"north kordofan"],
        never=[r"el[- ]obeid", r"al[- ]ubayyid", r"\("],
        arabic=[],
    ),
    dict(
        id="english-only",
        note="authored: an English report that names a town",
        location="White Nile", type="flood", 
        signals=[signal(
            "Flash floods destroy homes in Tendalti",
            "Flash floods after heavy rain destroyed about 300 homes in Tendalti, White Nile State, on Monday. "
            "Local officials said 1,800 people need shelter.",
            "2026-09-14T12:00:00.000Z", source="darfur24")],
        must=[r"tendalti", r"white nile"],
        never=[r"\("],
        arabic=[],
    ),
    # The cases below are the grouping path: an event with a district holds
    # signals from several places, and `group_signal` passes the DISTRICT as
    # Location (`_event_area_name`), never the place one signal names.
    dict(
        id="two-towns-one-district",
        note="authored: two signals in one district event name different villages and share none",
        location="Um Rawaba", type="battles",
        signals=[
            signal("اشتباكات عنيفة في منطقة ود عشانا بشمال كردفان",
                   "اندلعت اشتباكات عنيفة بين الجيش وقوات الدعم السريع في منطقة ود عشانا بمحلية أم روابة.",
                   "2026-10-21T09:00:00.000Z"),
            signal("قصف مدفعي على قرية السميح",
                   "تعرضت قرية السميح بمحلية أم روابة بولاية شمال كردفان لقصف مدفعي عنيف صباح اليوم.",
                   "2026-10-21T13:00:00.000Z"),
        ],
        must=[r"um+ r[au]wab[ae]"],
        never=[],
        # Both villages or neither: never the event titled after one of them.
        all_or_none=[r"wad[- ][aeiou]?sh+a'?a?na", r"s[aeiu]?m[aeiu]+h"],
        arabic=None,  # follows from which places the title names
    ),
    dict(
        id="five-villages-one-district",
        note="authored: five signals in one district event, each names a different village",
        location="Um Rawaba", type="battles",
        signals=[
            signal("اشتباكات عنيفة في منطقة ود عشانا بشمال كردفان",
                   "اندلعت اشتباكات عنيفة بين الجيش وقوات الدعم السريع في منطقة ود عشانا بمحلية أم روابة.",
                   "2026-10-21T09:00:00.000Z"),
            signal("قصف مدفعي على قرية السميح",
                   "تعرضت قرية السميح بمحلية أم روابة بولاية شمال كردفان لقصف مدفعي عنيف صباح اليوم.",
                   "2026-10-21T13:00:00.000Z"),
            signal("هجوم على قرية شركيلا",
                   "هاجمت قوة مسلحة قرية شركيلا بمحلية أم روابة وأحرقت عدداً من المنازل.",
                   "2026-10-22T07:30:00.000Z"),
            signal("معارك في منطقة الله كريم",
                   "دارت معارك بين الجيش وقوات الدعم السريع في منطقة الله كريم شرق أم روابة.",
                   "2026-10-22T11:00:00.000Z"),
            signal("اشتباكات في الغبشة",
                   "تجددت الاشتباكات في قرية الغبشة بمحلية أم روابة مساء اليوم.",
                   "2026-10-22T18:00:00.000Z"),
        ],
        must=[r"um+ r[au]wab[ae]"],
        # Five places: the district alone, not the latest village.
        never=[r"wad", r"sh+ana", r"s[aeiu]?m[aeiu]+h", r"sh[aie]rk", r"kar[ie]+m", r"gh?[aeiu]b+[aei]?sh"],
        arabic=None,
    ),
    dict(
        id="one-town-in-a-district",
        note="authored: the first signal of a district event names a village",
        location="Um Rawaba", type="battles",
        signals=[
            signal("اشتباكات عنيفة في منطقة ود عشانا بشمال كردفان",
                   "اندلعت اشتباكات عنيفة بين الجيش وقوات الدعم السريع في منطقة ود عشانا بمحلية أم روابة.",
                   "2026-10-21T09:00:00.000Z"),
        ],
        must=[r"wad[- ][aeiou]?sh+a'?a?na", r"um+ r[au]wab[ae]"],
        never=[],
        arabic=["ود عشانا"],
    ),
    dict(
        id="two-signals-same-town",
        note="authored: two signals in one district event both name the same village",
        location="Um Rawaba", type="battles",
        signals=[
            signal("قصف مدفعي على قرية السميح",
                   "تعرضت قرية السميح بمحلية أم روابة بولاية شمال كردفان لقصف مدفعي عنيف صباح اليوم.",
                   "2026-10-21T13:00:00.000Z"),
            signal("مقتل 6 مدنيين في السميح",
                   "قُتل 6 مدنيين وأصيب آخرون جراء تجدد القصف على قرية السميح.",
                   "2026-10-21T17:00:00.000Z"),
        ],
        must=[r"s[aeiu]?m[aeiu]+h", r"um+ r[au]wab[ae]"],
        never=[],
        arabic=["السميح"],
    ),
    dict(
        id="town-with-the-name-of-its-district",
        note="authored: both signals name the town the district is named after",
        location="Dilling", type="battles",
        signals=[
            signal("قصف مدفعي على مدينة الدلنج",
                   "تعرضت مدينة الدلنج بولاية جنوب كردفان لقصف مدفعي عنيف صباح اليوم.",
                   "2026-10-21T13:00:00.000Z"),
            signal("مقتل 6 مدنيين في الدلنج",
                   "قُتل 6 مدنيين وأصيب آخرون جراء تجدد القصف على أحياء مدينة الدلنج.",
                   "2026-10-21T17:00:00.000Z"),
        ],
        must=[r"dilling"],
        never=[r"dilling.*dilling"],  # the name once, not "Dilling, Dilling"
        arabic=None,  # the place is Location itself: the spelling is optional
    ),
]


def check(case: dict, title: str) -> list[str]:
    problems = []
    text = " ".join(f"{s['title']} {s['description']}" for s in case["signals"])
    for pattern in case["must"]:
        if not re.search(pattern, title, re.I):
            problems.append(f"missing /{pattern}/")
    for pattern in case["never"]:
        if re.search(pattern, title, re.I):
            problems.append(f"has /{pattern}/")
    named = [bool(re.search(pattern, title, re.I)) for pattern in case.get("all_or_none", [])]
    if any(named) and not all(named):
        problems.append("names the place of one signal and not of the other")
    spellings = [m.strip() for m in ARABIC_PARENS.findall(title)]
    for spelling in spellings:
        if spelling not in text:
            problems.append(f"Arabic spelling not in the text: {spelling}")
    if ARABIC.search(ARABIC_PARENS.sub("", title)):
        problems.append("Arabic outside parentheses")
    if case["arabic"] is not None and sorted(spellings) != sorted(case["arabic"]):
        problems.append(f"Arabic spellings {spellings}, wanted {case['arabic']}")
    if len(title) > TITLE_MAX_CHARS:
        problems.append(f"{len(title)} characters")
    return problems


def main() -> int:
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    llm = make_llm_provider("signal")
    failed = 0
    print(f"{REWRITE_PROMPT_VERSION} · {runs} run(s) per case\n")
    for case in CASES:
        prompt = build_rewrite_prompt(case["location"], case["type"], case["signals"])
        for run in range(1, runs + 1):
            raw = llm.complete_structured(system=REWRITE_SYSTEM, user=prompt, schema=EventRewrite).title
            title = clean_event_title(raw, case["signals"], case["location"])
            problems = check(case, title)
            failed += bool(problems)
            cleaned = "" if raw == title else f"   (model wrote: {raw})"
            print(f"{'FAIL' if problems else 'ok  '} {case['id']} #{run}: {title}{cleaned}")
            for problem in problems:
                print(f"       - {problem}")
    print(f"\n{failed} failed of {len(CASES) * runs}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
