import pytest


@pytest.fixture(autouse=True)
def _disable_event_extraction_llm(monkeypatch):
    """Neutralize the unified event extractor's LLM call by default so no test
    accidentally hits a real model / the network (the ingest builders and the
    drain now call `extract_event`). With the provider unavailable,
    `extract_event` returns None — the pre-feature behaviour — keeping existing
    builder/drain tests deterministic. Tests that exercise the extractor patch
    `make_llm_provider` themselves, which overrides this within their scope."""
    import clear_pipeline.providers.event_extraction as _ee

    def _unavailable(_role):
        raise RuntimeError("LLM disabled in tests (see conftest._disable_event_extraction_llm)")

    monkeypatch.setattr(_ee, "make_llm_provider", _unavailable)


@pytest.fixture(autouse=True)
def _block_llm_requests(monkeypatch):
    """Fail any request the OpenAI/Anthropic SDKs would send, so a test that forgets
    to mock its provider can't spend credits (embeddings and OpenRouter included).
    Mocked providers and mocked `_client`s never reach this."""
    import anthropic._base_client
    import openai._base_client

    def _blocked(*_args, **_kwargs):
        raise RuntimeError("real LLM request blocked in tests (see conftest._block_llm_requests)")

    for module in (openai._base_client, anthropic._base_client):
        monkeypatch.setattr(module.SyncAPIClient, "request", _blocked)
        monkeypatch.setattr(module.AsyncAPIClient, "request", _blocked)


class FakeRedis:
    """In-memory stand-in for the Redis commands the ground drains' attempt
    counters (defs/ground/attempts.py) and the newsletter cursor use. Values
    are stored as given; TTLs are recorded, not enforced."""

    def __init__(self):
        self.store: dict[str, int | str] = {}
        self.ttls: dict[str, int] = {}

    def get(self, key):
        return self.store.get(key)

    def mget(self, keys):
        return [None if k not in self.store else str(self.store[k]).encode() for k in keys]

    def incr(self, key):
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    def expire(self, key, seconds):
        self.ttls[key] = seconds

    def set(self, key, value, ex=None):
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = ex

    def delete(self, *keys):
        for key in keys:
            self.store.pop(key, None)
            self.ttls.pop(key, None)


@pytest.fixture
def fake_redis():
    return FakeRedis()
