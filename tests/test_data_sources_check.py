"""The deploy-time data_sources check (clear #1523).

Covers the three outcomes the release workflow distinguishes: every row
present, a row missing (the 2026-10-20 / 2026-11-13 incidents), and clear-api
unreachable — which must not be reported as a missing row.
"""

from clear_pipeline.checks import data_sources as check

# The source names a deploy of this build requires a clear-api data_sources row
# for: every source that resolves its id with get_source_id_by_name, and so
# cannot ingest without the row. Pinned deliberately — adding a connector to
# CONNECTORS or GX_SOURCES fails this test with the new name in the diff, which
# is the reminder that the source also needs a clear-api migration seeding its
# row. Update this list in the same PR as the connector, and write the
# migration into that PR's "Manual deploy steps".
EXPECTED_REQUIRED = {
    "acled",
    "darfur24",
    "dataminr",
    "gdacs",
    "idmc",
    "inso",
    "iom_dtm_flash_alerts",
    "sudan_witness",
}

# Registered, but never resolved here: clear-api creates these signals (an
# analyst, or POST /api/x/ingest), so a missing row breaks a clear-api mutation
# rather than a deploy of this repo. See required_source_names' docstring —
# requiring `manual` would red every dev deploy today over a row clear-api's
# createManualSignal does not even accept.
EXPECTED_EXCLUDED = {"manual", "field_officer", "partner", "government", "sudan-war-x"}


def test_required_source_names_matches_the_registries():
    assert check.required_source_names() == EXPECTED_REQUIRED


def test_analyst_facing_sources_are_excluded():
    """Not an oversight: these names are registered and deliberately unchecked.
    If one ever starts resolving an id here, `required_source_names` picks it up
    and the pinned set above fails — which is the point."""
    required = check.required_source_names()
    assert required.isdisjoint(EXPECTED_EXCLUDED)
    # And the exclusions really are registered, so this test can't rot into a
    # list of names nothing mentions any more.
    from clear_pipeline.defs.signals.connectors import CONNECTORS

    assert EXPECTED_EXCLUDED <= {c.source for c in CONNECTORS}


def test_required_names_union_both_registries(monkeypatch):
    """CONNECTORS currently names every gx source too, so the GX_SOURCES half of
    the union is redundant *today* — dropping it would not change the pinned set
    above, and would not fail a test. It is not redundant by design: a gx-only
    source (no `defs/signals` connector, which is where that package is headed —
    see the GX_SOURCES docstring) would then go unchecked. Stub both registries
    so each contributes a name the other does not."""

    class _Src:
        def __init__(self, name):
            self.source = name

        def api_source_id(self):  # what marks a source as needing the row
            return "id"

    import clear_pipeline.defs.gx_pipeline.sources as gx
    import clear_pipeline.defs.signals.connectors as conn

    monkeypatch.setattr(conn, "CONNECTORS", [_Src("connector_only"), _Src("both")])
    monkeypatch.setattr(gx, "GX_SOURCES", [_Src("gx_only"), _Src("both")])
    assert check.required_source_names() == {"connector_only", "gx_only", "both"}


def test_required_names_cover_every_scheduled_gx_source():
    """The four sources whose rows were missing on prod on 2026-11-21, all of
    them polled by a gx schedule — the case the check exists for."""
    required = check.required_source_names()
    for name in ("idmc", "sudan_witness", "iom_dtm_flash_alerts", "inso"):
        assert name in required


def test_missing_lists_every_missing_name_sorted():
    missing = check.missing_source_names(
        present=["acled", "gdacs"],
        required=["gdacs", "sudan_witness", "acled", "idmc", "inso"],
    )
    assert missing == ["idmc", "inso", "sudan_witness"]


def test_extra_rows_in_the_environment_are_not_an_error():
    """The check is one-directional: a `whatsapp` row nothing registers is a
    leftover, not a deploy blocker."""
    assert check.missing_source_names(present=["acled", "whatsapp"], required=["acled"]) == []


def test_main_is_green_when_every_required_row_exists(capsys):
    required = sorted(check.required_source_names())
    code = check.main(fetch=lambda: [*required, "whatsapp"])
    assert code == check.EXIT_OK
    assert capsys.readouterr().err == ""


def test_main_fails_naming_every_missing_source(capsys):
    """DoD: a missing row fails the deploy with the source name in the error.
    Every missing name, not just the first — five deploy failures in a row,
    one name each, is the shape that lets the fourth get missed."""
    present = sorted(check.required_source_names() - {"idmc", "inso", "sudan_witness"})
    code = check.main(fetch=lambda: present)
    err = capsys.readouterr().err
    assert code == check.EXIT_MISSING
    for name in ("idmc", "inso", "sudan_witness"):
        assert f"- {name}" in err
    assert "missing 3 of" in err
    # Names that DO have a row must not be reported as missing.
    assert "- acled" not in err


def test_main_separates_unreachable_from_missing(capsys):
    """A clear-api outage is not a verdict on the rows. Distinct exit code and
    a message that says so, so nobody writes a migration for a network blip."""

    def _boom():
        raise ConnectionError("connection refused")

    code = check.main(fetch=_boom)
    err = capsys.readouterr().err
    assert code == check.EXIT_UNREACHABLE
    assert code != check.EXIT_MISSING
    assert "could not run" in err
    assert "connection refused" in err
    assert "migration" not in err
