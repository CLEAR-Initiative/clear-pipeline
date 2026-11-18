"""scripts/signal_aliases_import.py: the one-off alias export (ADR-0015 deploy step 4)."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from clear_pipeline.defs.gx_pipeline import iceberg_signals
from clear_pipeline.defs.gx_pipeline.signal_ids import signal_id

_PATH = Path(__file__).parents[1] / "scripts" / "signal_aliases_import.py"
_spec = importlib.util.spec_from_file_location("signal_aliases_import", _PATH)
script = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(script)

CUID_A, CUID_B = "c" + "a" * 24, "c" + "b" * 24


def _row(id_, external_id, source="idmc"):
    return {"id": id_, "external_id": external_id, "source": source}


def test_a_pre_switch_gx_row_gets_an_alias_to_its_postgres_id():
    aliases, counts, conflicts = script.plan([_row(CUID_A, "idmc:123")], {})
    assert conflicts == []
    assert aliases == [{"signalId": signal_id("idmc:123"), "externalId": "idmc:123", "legacyId": CUID_A}]
    assert counts["source: idmc"] == 1


@pytest.mark.parametrize("external_id", ["x:1", "whatsapp:1", "url:https://a", "IDMC:1", "idmc:", "123"])
def test_rows_without_a_gx_prefix_are_skipped(external_id):
    aliases, counts, conflicts = script.plan([_row(CUID_A, external_id)], {})
    assert (aliases, conflicts) == ([], [])
    assert counts["skipped: no gx prefix"] == 1


def test_a_row_already_holding_its_minted_id_needs_no_alias():
    aliases, counts, _ = script.plan([_row(signal_id("inso:9"), "inso:9", "inso")], {})
    assert aliases == []
    assert counts["skipped: already the minted id"] == 1


def test_a_stored_identical_alias_is_not_rewritten():
    sid = signal_id("idmc:123")
    aliases, counts, conflicts = script.plan([_row(CUID_A, "idmc:123")], {sid: CUID_A})
    assert (aliases, conflicts) == ([], [])
    assert counts["unchanged: alias already stored"] == 1


def test_two_postgres_rows_for_one_minted_id_is_a_conflict():
    # (sourceId, externalId) is unique in Postgres, so one externalId under two
    # DataSources would give one signalId two legacy ids.
    rows = [_row(CUID_A, "idmc:123", "idmc"), _row(CUID_B, "idmc:123", "idmc_legacy")]
    aliases, _, conflicts = script.plan(rows, {})
    assert aliases == []
    assert len(conflicts) == 1 and "idmc:123" in conflicts[0]


def test_a_stored_alias_to_another_legacy_id_is_a_conflict():
    sid = signal_id("idmc:123")
    aliases, _, conflicts = script.plan([_row(CUID_A, "idmc:123")], {sid: CUID_B})
    assert aliases == []
    assert len(conflicts) == 1


def _run(tmp_path, csv_text, *argv):
    csv_path = tmp_path / "aliases.csv"
    csv_path.write_text(csv_text)
    with (
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_warehouse", f"file://{tmp_path / 'wh'}"),
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_catalog_uri", f"sqlite:///{tmp_path / 'cat.db'}"),
        patch.object(sys, "argv", ["signal_aliases_import.py", str(csv_path), *argv]),
    ):
        code = script.main()
        stored = dict(iceberg_signals.aliases_df()[["signalId", "legacyId"]].itertuples(index=False))
    return code, stored


def test_dry_run_writes_nothing_and_apply_writes_the_aliases(tmp_path):
    csv_text = f"id,external_id,source\n{CUID_A},idmc:123,idmc\n{CUID_B},x:9,x\n"
    assert _run(tmp_path, csv_text) == (0, {})
    assert _run(tmp_path, csv_text, "--apply") == (0, {signal_id("idmc:123"): CUID_A})
    # Re-running is a no-op.
    assert _run(tmp_path, csv_text, "--apply") == (0, {signal_id("idmc:123"): CUID_A})


def test_a_csv_with_a_byte_order_mark_is_read(tmp_path):
    csv_text = f"﻿id,external_id,source\n{CUID_A},idmc:123,idmc\n"
    assert _run(tmp_path, csv_text, "--apply") == (0, {signal_id("idmc:123"): CUID_A})


def test_a_conflict_aborts_without_writing(tmp_path):
    csv_text = f"id,external_id,source\n{CUID_A},idmc:123,idmc\n{CUID_B},idmc:123,other\n"
    assert _run(tmp_path, csv_text, "--apply") == (1, {})
