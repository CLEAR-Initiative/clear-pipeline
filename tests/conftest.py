import pytest


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
