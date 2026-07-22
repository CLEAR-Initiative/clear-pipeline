"""Tests for the chunk-slicing algorithm in chunks.py.

The chunker is deterministic given (pages, chunk_tokens,
overlap_tokens). Correctness matters because every knowledge-base
row keys off the chunk_index / page_start / page_end it emits — a
regression that shifts boundaries silently mis-cites the source
paragraph on every retrieval hit.

The tokenizer (tiktoken cl100k_base) is a real dependency here, not
mocked — the algorithm depends on its stable behaviour to make chunk
boundaries reproducible across the pipeline.
"""

import pytest

from clear_context_pipeline.defs.knowledgebase.chunks import (
    _slice_into_chunks,
    _encoding,
)


def _pages(*texts: str) -> list[dict]:
    """Convenience — turns a positional list of page texts into the
    {page_num, text} shape the chunker expects. 1-indexed page nums to
    match PDF convention."""
    return [{"page_num": i + 1, "text": t} for i, t in enumerate(texts)]


class TestSliceIntoChunks:
    def test_short_document_yields_single_chunk(self):
        # Total tokens well under chunk_tokens → one chunk covering
        # everything, page range = 1..1.
        pages = _pages("This is a short document about Sudan sitrep.")
        chunks = _slice_into_chunks(pages, chunk_tokens=800, overlap_tokens=100)
        assert len(chunks) == 1
        assert chunks[0]["chunk_index"] == 0
        assert chunks[0]["page_start"] == 1
        assert chunks[0]["page_end"] == 1
        assert "Sudan" in chunks[0]["text"]

    def test_empty_document_yields_no_chunks(self):
        # Empty page list → empty output; must not crash.
        assert _slice_into_chunks([], chunk_tokens=800, overlap_tokens=100) == []

    def test_chunk_index_is_zero_indexed_and_contiguous(self):
        # Force a multi-chunk split by shrinking the window to
        # something smaller than the actual content.
        big_text = "word " * 500  # ~500 tokens
        pages = _pages(big_text)
        chunks = _slice_into_chunks(pages, chunk_tokens=100, overlap_tokens=20)
        assert len(chunks) >= 2
        indices = [c["chunk_index"] for c in chunks]
        assert indices == list(range(len(chunks)))

    def test_chunks_respect_overlap(self):
        # With chunk=100 / overlap=20, consecutive chunks advance by
        # step = 80 tokens. Verify the token-level shift is consistent.
        text = " ".join(f"tok{i}" for i in range(500))
        pages = _pages(text)
        chunks = _slice_into_chunks(pages, chunk_tokens=100, overlap_tokens=20)
        assert len(chunks) >= 2
        # Re-tokenise each chunk and check they overlap on their
        # last/first token windows. This catches an off-by-one on
        # `step = chunk_tokens - overlap_tokens` that would silently
        # halve retrieval recall on split-across-boundary passages.
        first_ids = _encoding().encode(chunks[0]["text"])
        second_ids = _encoding().encode(chunks[1]["text"])
        # The tail of chunk 0 should share a prefix with the head of
        # chunk 1 (allowing some tokenizer-boundary jitter, so we
        # check for a non-trivial overlap).
        assert len(set(first_ids[-30:]) & set(second_ids[:30])) > 5

    def test_page_range_is_preserved_across_page_boundaries(self):
        # A chunk that spans two pages should report page_start=1,
        # page_end=2. Content-heavy pages ensure the chunker actually
        # walks across the boundary.
        pages = _pages(
            "Page one " + ("alpha " * 100),
            "Page two " + ("beta " * 100),
        )
        chunks = _slice_into_chunks(pages, chunk_tokens=150, overlap_tokens=30)
        # At least one chunk must straddle both pages given the sizes.
        straddling = [c for c in chunks if c["page_start"] != c["page_end"]]
        assert len(straddling) >= 1
        for c in straddling:
            assert c["page_start"] == 1
            assert c["page_end"] == 2

    def test_last_chunk_terminates_at_end_of_document(self):
        # The chunker's break condition (`end == len(token_ids)`)
        # must stop after emitting the tail; otherwise the loop keeps
        # producing chunks that shrink to nothing.
        text = "word " * 300
        pages = _pages(text)
        chunks = _slice_into_chunks(pages, chunk_tokens=100, overlap_tokens=20)
        last_text_ids = _encoding().encode(chunks[-1]["text"])
        assert len(last_text_ids) <= 100

    def test_overlap_larger_than_chunk_is_rejected(self):
        # step = chunk - overlap would go <= 0 → infinite loop. The
        # chunker must raise, not spin.
        with pytest.raises(ValueError):
            _slice_into_chunks(_pages("x"), chunk_tokens=100, overlap_tokens=100)
        with pytest.raises(ValueError):
            _slice_into_chunks(_pages("x"), chunk_tokens=100, overlap_tokens=200)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
