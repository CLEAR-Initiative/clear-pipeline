"""#1258: the event title keeps the places the signals name.

The prompt (`rewrite-v4`) asks for the places and their Arabic spellings; a
code check after the model call removes what must not be stored. The prompt's
effect on real posts is measured in `evals/event_title/`, not here.
"""

from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.providers import event
from clear_pipeline.providers.prompts import rewrite
from clear_pipeline.providers.prompts.rewrite import clean_event_title

KANABI = {
    "title": "عقب القصف الأخير الذي استهدف عدة مواقع في الولايات الشمالية للسودان…",
    "description": "أقدمت السلطات المحلية على إزالة وتدمير #الكنابي وتهجير قاطنيها قسراً",
}
VILLAGES = {
    "title": "سيطرتها على مناطق مزبد وعروري وكامو في ولاية شمال دارفور",
    "description": None,
}


def test_the_prompt_asks_for_the_places_the_signals_name_and_their_arabic_spelling():
    assert rewrite.REWRITE_PROMPT_VERSION == "rewrite-v4"
    prompt = rewrite.build_rewrite_prompt("North Darfur", "battles", [VILLAGES])
    assert "name that place, then" in prompt
    assert "Use Location alone only when the signals name nothing more specific" in prompt
    assert "Arabic spelling in parentheses" in prompt
    assert "Never add a place" in prompt
    assert "only when every signal names it" in prompt
    # The two rules that blocked this are gone.
    assert "no quotes/brackets" not in prompt
    assert "Lead with\n  the event type and location" not in prompt
    # The model sees the Arabic text.
    assert "مزبد وعروري وكامو" in prompt


def test_an_arabic_spelling_the_signals_write_is_kept():
    title = "Explosions and forced displacement in Al-Kanabi (الكنابي), Northern"
    assert clean_event_title(title, [KANABI]) == title
    three = "Battles in Mazbud (مزبد), Aruri (عروري) and Kamo (كامو), North Darfur"
    assert clean_event_title(three, [VILLAGES]) == three


@pytest.mark.parametrize(("title", "cleaned"), [
    # A spelling the model made up, or "corrected".
    ("Explosions in Al-Kanabi (الكنابى), Northern", "Explosions in Al-Kanabi, Northern"),
    ("Explosions in Al-Kanabi (كنابي الشمالية), Northern", "Explosions in Al-Kanabi, Northern"),
    # One right, one wrong: only the wrong one goes.
    ("Battles in Mazbud (مزبد) and Aruri (أروري), North Darfur", "Battles in Mazbud (مزبد) and Aruri, North Darfur"),
])
def test_an_arabic_spelling_that_is_in_no_signal_is_removed(title, cleaned):
    assert clean_event_title(title, [KANABI, VILLAGES]) == cleaned


def test_vowel_marks_do_not_hide_a_spelling_the_signal_writes():
    marked = {"title": "اشتباكات في مَزْبَد", "description": None}
    assert clean_event_title("Clashes in Mazbud (مزبد), North Darfur", [marked]) == "Clashes in Mazbud (مزبد), North Darfur"


def test_a_title_too_long_with_its_arabic_spellings_loses_them_all():
    long_title = (
        "Explosions, forced displacement and demolition of homes in Mazbud (مزبد), "
        "Aruri (عروري) and Kamo (كامو), North Darfur"
    )
    assert len(long_title) > rewrite.TITLE_MAX_CHARS
    assert clean_event_title(long_title, [VILLAGES]) == (
        "Explosions, forced displacement and demolition of homes in Mazbud, Aruri and Kamo, North Darfur"
    )


def test_invisible_direction_marks_are_never_stored():
    title = "Explosions in Al-Kanabi ⁧(الكنابي)⁩‏, Northern"
    assert clean_event_title(title, [KANABI]) == "Explosions in Al-Kanabi (الكنابي), Northern"


@pytest.mark.parametrize("title", [
    "Cholera outbreak in North Kordofan",
    "Flash floods in Tendalti, White Nile",
    "Clashes near the market (two dead) in Nyala",  # Latin parentheses are not ours to judge
])
def test_a_title_with_no_arabic_is_left_as_it_is(title):
    assert clean_event_title(title, [KANABI]) == title


def test_the_rewrite_cleans_the_title_before_it_reaches_the_event():
    made_up = event.EventRewrite(title="Explosions in Al-Kanabi (الكنابى), Northern", description="d")
    provider = MagicMock(**{"complete_structured.return_value": made_up})
    with (
        patch.object(event.graphql, "event_members", return_value=[KANABI]),
        patch.object(event, "make_llm_provider", return_value=provider),
    ):
        result, members = event._rewrite_event("evt-1", "Northern", "explosions")
    assert result.title == "Explosions in Al-Kanabi, Northern"
    assert members == [KANABI]
