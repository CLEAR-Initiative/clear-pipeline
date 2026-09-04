"""Tests for `_embedding_batches` in upsert.py.

The embedding batcher must respect BOTH the provider's input-count cap and
its per-batch token limit. Voyage rejects any call over 120k tokens
regardless of count, so a fixed 128-slice fails on large backfills — the bug
these tests pin.
"""

from clear_pipeline.defs.knowledgebase.upsert import (
    EMBED_BATCH_SIZE,
    _embedding_batches,
)

_BUDGET = int(120_000 * 0.95)  # matches the batcher's margin


class _TokenLimitedEmbedder:
    """Fake Voyage-like provider: fixed tokens per text, exact counter."""

    MAX_TOKENS_PER_BATCH = 120_000

    def __init__(self, per_text_tokens: int):
        self._n = per_text_tokens

    def count_tokens(self, texts: list[str]) -> int:
        return self._n * len(texts)


class _CountOnlyEmbedder:
    """Fake provider with no token limit (e.g. Together/TEI)."""


def _enriched(n: int) -> list[dict]:
    return [{"embedded_text": f"chunk {i}"} for i in range(n)]


def test_splits_on_token_budget_before_count():
    # 1000 tokens/chunk → the 114k-token budget binds before the 128 count.
    emb = _TokenLimitedEmbedder(per_text_tokens=1000)
    batches = list(_embedding_batches(_enriched(300), emb))
    assert [len(b) for b in batches] == [114, 114, 72]
    for b in batches:
        assert len(b) <= EMBED_BATCH_SIZE
        assert emb.count_tokens([e["embedded_text"] for e in b]) <= _BUDGET
    assert sum(len(b) for b in batches) == 300


def test_splits_on_count_when_tokens_are_small():
    # Tiny chunks → the 128 count cap binds, not the token budget.
    emb = _TokenLimitedEmbedder(per_text_tokens=5)
    batches = list(_embedding_batches(_enriched(300), emb))
    assert [len(b) for b in batches] == [128, 128, 44]


def test_count_only_fallback_when_provider_has_no_token_limit():
    batches = list(_embedding_batches(_enriched(300), _CountOnlyEmbedder()))
    assert [len(b) for b in batches] == [128, 128, 44]


def test_oversized_chunk_gets_its_own_batch():
    # A single chunk over the budget can't be split further — it goes alone
    # (Voyage truncates it on send) rather than wedging the batcher.
    emb = _TokenLimitedEmbedder(per_text_tokens=200_000)
    batches = list(_embedding_batches(_enriched(3), emb))
    assert [len(b) for b in batches] == [1, 1, 1]


def test_empty_input_yields_nothing():
    assert list(_embedding_batches([], _TokenLimitedEmbedder(1000))) == []
