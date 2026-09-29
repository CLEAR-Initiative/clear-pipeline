import pytest


class FakeRedis:
    """In-memory stand-in for the Redis commands the ground drains' attempt
    counters use (defs/ground/attempts.py). TTLs are recorded, not enforced."""

    def __init__(self):
        self.store: dict[str, int] = {}
        self.ttls: dict[str, int] = {}

    def mget(self, keys):
        return [None if k not in self.store else str(self.store[k]).encode() for k in keys]

    def incr(self, key):
        self.store[key] = self.store.get(key, 0) + 1
        return self.store[key]

    def expire(self, key, seconds):
        self.ttls[key] = seconds

    def set(self, key, value, ex=None):
        self.store[key] = int(value)
        if ex is not None:
            self.ttls[key] = ex


@pytest.fixture
def fake_redis():
    return FakeRedis()
