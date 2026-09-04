"""Unit tests for the upsert chunker + lazy env knobs (no network).

``_size_chunks`` is the fix for a production upsert timeout over the DB tunnel;
the env knobs it reads are lazy (read at call time, not import) so a value set in
.env — loaded AFTER this module is imported — actually applies.
"""

from clear_pipeline.providers import clear_api


def _row(nbytes: int) -> dict:
    # data blob padded to ~nbytes so we can drive the byte budget deterministically.
    return {"locationId": "L", "type": "t", "data": {"pad": "x" * nbytes}}


def test_chunks_split_on_byte_budget(monkeypatch):
    monkeypatch.setenv("LOCATION_METADATA_UPSERT_MAX_BYTES", "1000")
    monkeypatch.setenv("LOCATION_METADATA_UPSERT_CHUNK", "1000")  # row cap out of the way
    rows = [_row(400) for _ in range(5)]  # ~400B each → ~2 per 1000B chunk
    chunks = clear_api._size_chunks(rows)
    assert len(chunks) >= 3
    assert all(len(c) <= 3 for c in chunks)
    assert sum(len(c) for c in chunks) == 5  # nothing dropped


def test_row_cap_applies_when_rows_are_tiny(monkeypatch):
    monkeypatch.setenv("LOCATION_METADATA_UPSERT_MAX_BYTES", "100000000")  # never the limit
    monkeypatch.setenv("LOCATION_METADATA_UPSERT_CHUNK", "10")
    chunks = clear_api._size_chunks([_row(1) for _ in range(25)])
    assert [len(c) for c in chunks] == [10, 10, 5]


def test_single_oversized_row_gets_its_own_chunk(monkeypatch):
    monkeypatch.setenv("LOCATION_METADATA_UPSERT_MAX_BYTES", "500")
    monkeypatch.setenv("LOCATION_METADATA_UPSERT_CHUNK", "50")
    rows = [_row(50), _row(5000), _row(50)]  # middle row alone exceeds the budget
    chunks = clear_api._size_chunks(rows)
    # It must not be dropped, split, or loop forever — it lands in its own chunk.
    assert [len(c) for c in chunks] == [1, 1, 1]
    assert sum(len(c) for c in chunks) == 3


def test_empty_input_returns_empty():
    assert clear_api._size_chunks([]) == []


def test_bad_env_value_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("LOCATION_METADATA_UPSERT_MAX_BYTES", "not-a-number")
    assert clear_api._upsert_max_bytes() == clear_api._DEFAULT_UPSERT_MAX_BYTES
    monkeypatch.setenv("LOCATION_METADATA_UPSERT_CHUNK", "-5")
    assert clear_api._upsert_max_rows() == clear_api._DEFAULT_UPSERT_MAX_ROWS


def test_env_override_takes_effect(monkeypatch):
    monkeypatch.setenv("LOCATION_METADATA_UPSERT_MAX_BYTES", "12345")
    monkeypatch.setenv("LOCATION_METADATA_UPSERT_CHUNK", "7")
    assert clear_api._upsert_max_bytes() == 12345
    assert clear_api._upsert_max_rows() == 7
