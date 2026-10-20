"""Unit tests for translate's JSON recovery (providers/translate._parse_json).
Pure parsing — no network, no LLM. Covers the thinking/OSS-model failure mode
where the JSON arrives wrapped in prose or after a reasoning preamble."""

from clear_pipeline.providers.translate import _parse_json


def test_plain_json():
    assert _parse_json('{"fr": {"title": "Bonjour"}}') == {"fr": {"title": "Bonjour"}}


def test_json_code_fence():
    assert _parse_json('```json\n{"fr": {"title": "Bonjour"}}\n```') == {"fr": {"title": "Bonjour"}}


def test_prose_wrapped_json():
    text = 'Here is the translation:\n{"fr": {"title": "Bonjour"}}\nHope this helps!'
    assert _parse_json(text) == {"fr": {"title": "Bonjour"}}


def test_reasoning_preamble_then_json():
    text = 'Let me think... locale fr, field title. {"fr": {"title": "Bonjour"}}'
    assert _parse_json(text) == {"fr": {"title": "Bonjour"}}


def test_braces_inside_string_values_do_not_close_early():
    # A '}' / '{' inside a string value must not terminate the object prematurely.
    text = 'noise {"fr": {"title": "a } b { c"}} trailing'
    assert _parse_json(text) == {"fr": {"title": "a } b { c"}}


def test_non_object_json_rejected():
    assert _parse_json('["not", "an", "object"]') is None


def test_unparseable_returns_none():
    assert _parse_json("the model refused and said nothing useful") is None


def test_empty_returns_none():
    assert _parse_json("") is None
