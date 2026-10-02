"""Opt-in relative / movement phrasing in the geoparser's candidate
extraction (`relative_phrases=True`, used by the hotline enrichment).

Extraction only: no network. The texts are the dev hotline messages that
got no location because only in/at/near/around/outside/on were recognised.
"""

from unittest.mock import patch

import pytest

from clear_pipeline.defs.ground import stages
from clear_pipeline.providers import geoparser as g


def names(text, *, relative_phrases):
    return [c.name for c in g._extract_from_text(text, "body", relative_phrases=relative_phrases)]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Checkpoint on the road out of Mukjar is stopping vehicles.", ["Mukjar"]),
        ("Families from the villages east of Um Dukhun are arriving.", ["Um Dukhun"]),
        ("Shelling north-west of Kutum overnight.", ["Kutum"]),
        ("Families arriving from Kutum and moving towards El Fasher.", ["Kutum", "El Fasher"]),
        ("Trucks heading into Nyala this morning.", ["Nyala"]),
    ],
)
def test_relative_phrases_find_the_place(text, expected):
    assert names(text, relative_phrases=True) == expected


@pytest.mark.parametrize(
    "text",
    [
        "Checkpoint on the road out of Mukjar is stopping vehicles.",
        "Families from the villages east of Um Dukhun are arriving.",
    ],
)
def test_off_by_default_so_signal_geoparsing_is_unchanged(text):
    assert names(text, relative_phrases=False) == []


def test_lowercase_words_and_stopwords_are_not_places():
    assert names("Water to the school from Monday, towards evening.", relative_phrases=True) == []


def test_existing_prepositions_still_work_alongside():
    assert names("Flooding in Nyala, water coming from Kas.", relative_phrases=True) == ["Nyala", "Kas"]


def test_geoparse_signal_passes_the_flag_to_both_fields():
    with patch.object(g, "_extract_from_text", return_value=[]) as extract:
        g.geoparse_signal("title", "body", relative_phrases=True)
    assert [c.kwargs["relative_phrases"] for c in extract.call_args_list] == [True, True]


def test_hotline_enrichment_turns_it_on():
    with (
        patch.object(stages, "geoparse_signal", return_value=None) as geoparse,
    ):
        stages._geoparse_one_message("the road out of Mukjar")
    assert geoparse.call_args.kwargs["relative_phrases"] is True
