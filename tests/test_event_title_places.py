"""#1258: the event title keeps the places the signals name.

The prompt (`rewrite-v4`) asks for the places and their Arabic spellings; a
code check after the model call removes what must not be stored. The prompt's
effect on real posts is measured in `evals/event_title/`, not here.
"""

from contextlib import contextmanager
from types import SimpleNamespace
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
    assert "name that place, then\n      the area" in prompt
    assert "Use Location alone when the signals name nothing more specific" in prompt
    assert "Arabic spelling in parentheses" in prompt
    assert "Never add a place" in prompt
    assert "Never name the\n      place of one signal and leave out the place of another" in prompt
    assert "The last place in the title is the area on the Location line" in prompt
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
    title = "Explosions in Al-Kanabi \u2067(الكنابي)\u2069\u200f, Northern"
    assert clean_event_title(title, [KANABI]) == "Explosions in Al-Kanabi (الكنابي), Northern"


@pytest.mark.parametrize("mark", [
    "\u061c",  # Arabic letter mark
    "\u200e", "\u200f",  # left-to-right and right-to-left marks
    "\u202a", "\u202b", "\u202c", "\u202d", "\u202e",  # embeddings and overrides
    "\u2066", "\u2067", "\u2068", "\u2069",  # isolates
])
def test_every_direction_mark_is_removed(mark):
    title = f"Explosions in Al-Kanabi {mark}(الكنابي){mark}, Northern"
    assert clean_event_title(title, [KANABI]) == "Explosions in Al-Kanabi (الكنابي), Northern"


def test_the_prompt_label_is_never_stored_in_the_title():
    semeih = {"title": "قصف مدفعي على قرية السميح", "description": None}
    assert clean_event_title(
        "Battles in Al-Semeih (السميح), Um Rawaba Location", [semeih], "Um Rawaba",
    ) == "Battles in Al-Semeih (السميح), Um Rawaba"


@pytest.mark.parametrize(("title", "cleaned"), [
    # More than three places: the event is in many places, the district says so.
    ("Battles in Wad Ashana, Al-Sumayh, Sharkila, Allah Karim and Al-Ghubsha, Um Rawaba", "Battles in Um Rawaba"),
    ("Battles in Wad Ushana, Al Kharim, Sherkila and other locations, Um Rawaba (أم روابة)", "Battles in Um Rawaba"),
    ("Battles in Mazbud (مزبد), Aruri (عروري), Kamo (كامو) and Kutum, Um Rawaba", "Battles in Um Rawaba"),
    # The prompt's label at the end does not hide a long list, or a spelling after Location.
    ("Battles in Wad Ashana, Al-Sumayh, Sharkila and Allah Karim, Um Rawaba Location", "Battles in Um Rawaba"),
    ("Battles in Mazbud (مزبد), Um Rawaba (أم روابة) Location", "Battles in Mazbud (مزبد), Um Rawaba"),
    # Three or fewer stay.
    ("Battles in Mazbud (مزبد), Aruri (عروري) and Kamo (كامو), Um Rawaba", "Battles in Mazbud (مزبد), Aruri (عروري) and Kamo (كامو), Um Rawaba"),
    ("Battles in Wad Ashana and Al-Sumaih, Um Rawaba", "Battles in Wad Ashana and Al-Sumaih, Um Rawaba"),
    # "and" in what happened is not a list of places.
    ("Explosions, looting and forced displacement in Mazbud, Um Rawaba", "Explosions, looting and forced displacement in Mazbud, Um Rawaba"),
    # A title of another shape is left as it is.
    ("Battles across Um Rawaba", "Battles across Um Rawaba"),
])
def test_a_title_names_three_places_at_most(title, cleaned):
    assert clean_event_title(title, [VILLAGES], "Um Rawaba") == cleaned


def test_location_carries_no_arabic_spelling():
    # Location is CLEAR's own name for the area: nothing to check it against.
    state = {"title": "ارتفاع حالات الكوليرا في ولاية شمال كردفان", "description": None}
    assert clean_event_title(
        "Cholera outbreak in North Kordofan (شمال كردفان)", [state], "North Kordofan",
    ) == "Cholera outbreak in North Kordofan"
    # Only at the end of the title, where Location goes: a town with the name
    # of its district is a place the signals name, and keeps its spelling.
    dilling = {"title": "قصف مدفعي على مدينة الدلنج", "description": None}
    assert clean_event_title(
        "Battles in Dilling (الدلنج), South Kordofan", [dilling], "Dilling",
    ) == "Battles in Dilling (الدلنج), South Kordofan"
    assert clean_event_title(
        "Battles in Dilling (الدلنج), South Kordofan", [dilling], None,
    ) == "Battles in Dilling (الدلنج), South Kordofan"


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


# ── The Location the prompt gets is the event's district ─────────────────────

DISTRICT = {"id": "d-um-rawaba", "name": "Um Rawaba", "level": 2, "ancestorIds": ["c-sd", "s-nk"]}
WAD_ASHANA = {"id": "l-wad-ashana", "name": "Wad Ashana", "level": 4, "ancestorIds": ["c-sd", "s-nk", "d-um-rawaba"]}
STATE = {"id": "s-nk", "name": "North Kordofan", "level": 1, "ancestorIds": ["c-sd"]}


@pytest.fixture(autouse=True)
def _no_cached_area_names():
    event._area_name_cache.clear()
    yield
    event._area_name_cache.clear()


def test_the_area_of_an_event_is_its_district_not_the_place_one_signal_names():
    with patch.object(event.graphql, "get_location_canonical", return_value={"id": "d-um-rawaba", "name": "Um Rawaba"}) as read:
        assert event._event_area_name("d-um-rawaba", WAD_ASHANA) == "Um Rawaba"
        assert event._event_area_name("d-um-rawaba", WAD_ASHANA) == "Um Rawaba"
    read.assert_called_once_with("d-um-rawaba")  # read once, then cached


def test_a_signal_placed_at_the_district_needs_no_lookup():
    with patch.object(event.graphql, "get_location_canonical") as read:
        assert event._event_area_name("d-um-rawaba", DISTRICT) == "Um Rawaba"
    read.assert_not_called()


def test_without_a_district_the_event_takes_the_signal_location():
    assert event._event_area_name(None, STATE) == "North Kordofan"
    assert event._event_area_name(None, None) is None


def test_a_district_name_that_cannot_be_read_is_unknown_and_never_the_town():
    with patch.object(event.graphql, "get_location_canonical", side_effect=RuntimeError("down")):
        assert event._event_area_name("d-um-rawaba", WAD_ASHANA) is None
    # A failure is not cached: the next signal reads the name again.
    with patch.object(event.graphql, "get_location_canonical", return_value={"name": "Um Rawaba"}):
        assert event._event_area_name("d-um-rawaba", WAD_ASHANA) == "Um Rawaba"


def test_grouping_gives_the_rewrite_the_district_when_the_signal_names_a_town(monkeypatch):
    """The caller path: a signal geocoded to a town joins its district's event.
    The rewrite must get the district as Location, or it titles the whole
    event after the latest town."""
    seen: dict = {}

    @contextmanager
    def lock(*args, **kwargs):
        yield True

    monkeypatch.setattr(event.graphql, "set_signal_glide_code", lambda *a: None)
    monkeypatch.setattr(event, "resolve_signal_admin2", lambda signal: "d-um-rawaba")
    monkeypatch.setattr(event.graphql, "get_location_canonical", lambda lid: {"id": lid, "name": "Um Rawaba"})
    monkeypatch.setattr(event, "redis_lock", lock)
    monkeypatch.setattr(event, "_match_and_act", lambda **kwargs: seen.update(kwargs) or {"id": "evt-1"})

    event.group_signal(
        signal_id="sig-1",
        signal_title="اشتباكات في ود عشانا",
        signal_description=None,
        signal_timestamp="2026-10-21T09:00:00+00:00",
        classification=SimpleNamespace(
            disaster_types=["ac"], type_level_1="conflict", type_level_2="battles",
            relevance=0.9, summary="Clashes",
        ),
        created_signal={"id": "sig-1", "generalLocation": WAD_ASHANA},
    )

    assert seen["location_name"] == "Um Rawaba"
    assert seen["primary"] == WAD_ASHANA
