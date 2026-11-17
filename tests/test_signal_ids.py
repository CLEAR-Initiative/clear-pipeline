"""Pipeline-minted Signal ids: the id function and the per-source prefix registry."""

import uuid

import pytest

from clear_pipeline.defs.gx_pipeline import signal_ids
from clear_pipeline.defs.gx_pipeline.sources import GX_SOURCES


def test_signal_id_is_pinned():
    # A changed namespace or input encoding silently re-ids every Signal.
    assert signal_ids.signal_id("idmc:123") == "fabe38b8-405f-5b28-b44b-9edb76f4bbd8"


def test_signal_id_is_a_lowercase_v5():
    sid = signal_ids.signal_id("inso:INSO-AF-2026-001:1")
    assert sid == sid.lower()
    assert uuid.UUID(sid).version == 5


def test_mint_checks_the_source_prefix():
    assert signal_ids.mint("idmc:123", "idmc:") == signal_ids.signal_id("idmc:123")


@pytest.mark.parametrize("external_id", [None, "", "123", "acled:123", "idmc:", "IDMC:123"])
def test_mint_refuses_a_missing_or_unprefixed_external_id(external_id):
    with pytest.raises(signal_ids.InvalidExternalId):
        signal_ids.mint(external_id, "idmc:")


def test_prefixes_are_pairwise_distinct_and_disjoint_from_non_pipeline_ones():
    prefixes = [*(s.id_prefix for s in GX_SOURCES), *signal_ids.NON_PIPELINE_PREFIXES]
    assert len(set(prefixes)) == len(prefixes)
    # No prefix may start another, or two sources could mint the same id.
    for a in prefixes:
        for b in prefixes:
            assert a == b or not b.startswith(a), (a, b)


def test_gx_source_prefixes_are_pinned():
    # A renamed prefix silently re-ids its source and breaks clear-api's dedupe.
    assert {s.source: s.id_prefix for s in GX_SOURCES} == {
        "dataminr": "dataminr:", "acled": "acled:", "darfur24": "darfur24:",
        "idmc": "idmc:", "sudan_witness": "suw:",
        "iom_dtm_flash_alerts": "iom_dtm_flash_alert:", "inso": "inso:",
    }
