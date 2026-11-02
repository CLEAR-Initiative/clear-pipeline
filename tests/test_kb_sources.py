"""Tests for the shared knowledge-base source contract (providers/kb_sources).

Contract-only — no network / S3 / Dagster. Covers the KbRawDocument invariants
and the connector registry that the (future) fan-in drain enumerates."""

import pytest

from clear_pipeline.providers.kb_sources import base
from clear_pipeline.providers.kb_sources import (
    KbRawDocument,
    all_connectors,
    get_connector,
    register_connector,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    saved = dict(base._REGISTRY)
    base._REGISTRY.clear()
    try:
        yield
    finally:
        base._REGISTRY.clear()
        base._REGISTRY.update(saved)


def _doc(**over):
    base_kwargs = dict(
        source="reliefweb", report_id="r1", pdf_s3_key="pdfs/reliefweb/sdn/r1/a.pdf",
        title="Sudan sitrep", published_at="2026-11-01T00:00:00Z",
    )
    base_kwargs.update(over)
    return KbRawDocument(**base_kwargs)


class TestKbRawDocument:
    def test_optional_fields_default_safely(self):
        d = _doc()
        assert d.source_url is None and d.publisher_name is None
        assert d.country_iso3 is None and d.extra == {}

    def test_carries_publisher_and_scope(self):
        d = _doc(publisher_name="UNHCR", publisher_homepage="https://unhcr.org", country_iso3="sdn")
        assert (d.publisher_name, d.publisher_homepage, d.country_iso3) == (
            "UNHCR", "https://unhcr.org", "sdn",
        )

    @pytest.mark.parametrize("field_name", ["source", "report_id", "pdf_s3_key", "title", "published_at"])
    def test_required_fields_rejected_when_blank(self, field_name):
        with pytest.raises(ValueError, match=field_name):
            _doc(**{field_name: "  "})

    def test_is_frozen(self):
        d = _doc()
        with pytest.raises(Exception):
            d.source = "other"  # type: ignore[misc]


class _StubConnector:
    def __init__(self, key):
        self.key = key

    def fetch(self, *, country_iso3, window_start, window_end):
        return []


class _PushOnlyConnector:
    """A push source (manual upload) has no fetch."""

    key = "manual"


class TestRegistry:
    def test_register_and_get(self):
        c = _StubConnector("reliefweb")
        register_connector(c)
        assert get_connector("reliefweb") is c
        assert get_connector("nope") is None

    def test_all_connectors_sorted_by_key(self):
        register_connector(_StubConnector("zeta"))
        register_connector(_StubConnector("alpha"))
        assert [c.key for c in all_connectors()] == ["alpha", "zeta"]

    def test_duplicate_key_rejected(self):
        register_connector(_StubConnector("dup"))
        with pytest.raises(ValueError, match="duplicate"):
            register_connector(_StubConnector("dup"))

    def test_reregistering_same_instance_is_ok(self):
        c = _StubConnector("same")
        register_connector(c)
        register_connector(c)  # idempotent, no raise
        assert get_connector("same") is c

    def test_has_fetch_distinguishes_pull_from_push(self):
        assert base.has_fetch(_StubConnector("x")) is True
        assert base.has_fetch(_PushOnlyConnector()) is False
