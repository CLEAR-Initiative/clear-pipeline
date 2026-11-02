"""Source-parameterized S3 path builders + the ReliefWeb connector.

The load-bearing test here is `test_reliefweb_paths_byte_identical_to_legacy`:
it proves that `source="reliefweb"` reproduces `reliefweb_partitions.py`'s exact
keys. That equivalence is what guarantees generalizing the downstream to be
multi-source causes **no re-ingest / no re-materialization** — ReliefWeb's
artifacts keep landing at the same S3 keys, so every skip-if-exists still hits.
"""

import pytest

from clear_pipeline.defs import reliefweb_partitions as legacy
from clear_pipeline.providers.kb_sources import get_connector, paths
from clear_pipeline.providers.kb_sources.reliefweb import SOURCE, ReliefWebConnector

SLUG = legacy.FORMAT_SLUG


@pytest.mark.parametrize("iso3", ["sdn", "ssd", "tcd", "ve", "af"])
def test_reliefweb_paths_byte_identical_to_legacy(iso3):
    assert paths.reports_prefix(SOURCE, iso3, SLUG) == legacy.reports_prefix(iso3)
    assert paths.pdf_prefix(SOURCE, iso3, SLUG) == legacy.pdf_prefix(iso3)
    assert paths.pdf_key(SOURCE, iso3, SLUG, "r1", "a.pdf") == legacy.pdf_key(iso3, "r1", "a.pdf")
    assert paths.text_prefix(SOURCE, iso3, SLUG) == legacy.text_prefix(iso3)
    assert paths.chunks_prefix(SOURCE, iso3, SLUG) == legacy.chunks_prefix(iso3)
    assert paths.enriched_prefix(SOURCE, iso3, SLUG) == legacy.enriched_prefix(iso3)
    assert paths.datapoints_prefix(SOURCE, iso3, SLUG) == legacy.datapoints_prefix(iso3)
    assert paths.figures_prefix(SOURCE, iso3, SLUG) == legacy.figures_prefix(iso3)


def test_trailing_slash_convention_matches_legacy():
    # reports/pdf prefixes end with "/"; kb/* prefixes do not — same as legacy.
    assert paths.reports_prefix(SOURCE, "sdn", SLUG).endswith("/")
    assert paths.pdf_prefix(SOURCE, "sdn", SLUG).endswith("/")
    assert not paths.text_prefix(SOURCE, "sdn", SLUG).endswith("/")


def test_new_source_is_namespaced_and_country_scoped():
    p = paths.text_prefix("acme", "sdn", "sitrep")
    assert p == "acme/kb/text/sdn/sitrep"
    assert p.startswith("acme/")  # own namespace — can't collide with reliefweb/
    assert "/sdn/" in p           # country folder retained for every source


class TestReliefWebConnector:
    def test_registered_under_its_key(self):
        assert isinstance(get_connector("reliefweb"), ReliefWebConnector)

    def test_bound_path_helpers_match_legacy(self):
        c = ReliefWebConnector()
        assert c.text_prefix("sdn") == legacy.text_prefix("sdn")
        assert c.pdf_key("sdn", "r1", "a.pdf") == legacy.pdf_key("sdn", "r1", "a.pdf")
        assert c.datapoints_prefix("sdn") == legacy.datapoints_prefix("sdn")

    def test_fetch_is_deferred_to_phase_2b(self):
        with pytest.raises(NotImplementedError, match="phase 2b"):
            ReliefWebConnector().fetch(
                country_iso3="sdn", window_start="2026-01-01", window_end="2026-01-07"
            )
