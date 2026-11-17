"""The full gx job mints each Signal's id, sends it, and records the Postgres
id when clear-api keeps an older one (`gold.signal_id_aliases`)."""

import json
from unittest.mock import patch

import pytest

from clear_pipeline.defs.gx_pipeline import iceberg_signals
from clear_pipeline.defs.gx_pipeline.signal_ids import signal_id
from clear_pipeline.providers.clear_api import ClearApiError
from tests.test_gx_sync import R, T, Harness, HookedSource, _rec

# uuid5(signal_ids.NAMESPACE, "fakesrc:a"), computed outside the code under test.
A_ID = "462cf688-6fbb-5d0d-be27-b38afe56bac7"


@pytest.fixture
def h(tmp_path):
    return Harness(tmp_path, HookedSource([]))


def _aliases(h):
    w, c = h._iceberg()
    with w, c:
        return iceberg_signals.aliases_df().to_dict("records")


def _view(h):
    w, c = h._iceberg()
    with w, c:
        return iceberg_signals.postgres_ids("fakesrc")


def test_a_fixed_record_gets_a_literal_id_in_gold_and_on_the_wire(h):
    h.poll([_rec("a")])
    assert h.gold["a"]["signalId"] == A_ID
    [(op, sent)] = h.pg.calls
    assert (op, sent["id"]) == ("create", A_ID)
    assert h.pg_row("a")["id"] == A_ID
    silver = next(v for k, v in h.s3.objects.items() if k.startswith("silver/"))
    assert json.loads(silver)["id"] == A_ID
    assert "id" not in json.loads(h.gold["a"]["signalInputJson"])  # the column is gold's only copy
    assert _aliases(h) == []


def test_a_legacy_row_returned_by_clear_api_becomes_an_alias(h):
    h.pg.rows[("source-fake-123", "fakesrc:a")] = {
        "id": "clegacy0000000000000000aa", "contentHash": "h1", "retracted": False, "rawS3Key": None,
    }
    h.poll([_rec("a"), _rec("b")])
    assert _aliases(h) == [{"signalId": A_ID, "externalId": "fakesrc:a", "legacyId": "clegacy0000000000000000aa"}]
    assert h.gold["a"]["pushedAt"] is not None
    assert _view(h) == {A_ID: "clegacy0000000000000000aa", signal_id("fakesrc:b"): signal_id("fakesrc:b")}


def test_a_re_created_legacy_row_keeps_one_alias(h):
    h.pg.rows[("source-fake-123", "fakesrc:a")] = {
        "id": "clegacy0000000000000000aa", "contentHash": "h1", "retracted": False, "rawS3Key": None,
    }
    h.poll([_rec("a")])
    # Gold loses the push (e.g. the signals commit failed after the aliases one).
    w, c = h._iceberg()
    with w, c:
        table = iceberg_signals.get_signals_table("fakesrc")
        row = iceberg_signals._from_column_dict(table.scan().to_pandas().to_dict("records")[0])
        iceberg_signals.upsert_signals(table, [{**row, "pushedAt": None, "pushedState": None}])
    h.poll([])
    assert len(_aliases(h)) == 1


def test_an_update_that_reaches_a_legacy_row_records_its_alias_once(h):
    h.poll([_rec("a", h="h1")])
    # Pushed before the switch: Postgres holds its Prisma id, and no alias exists.
    h.pg_row("a")["id"] = "clegacy0000000000000000aa"
    h.poll([_rec("a", h="h2")])
    assert h.pg.ops()[-1] == "update"
    assert _aliases(h) == [{"signalId": A_ID, "externalId": "fakesrc:a", "legacyId": "clegacy0000000000000000aa"}]
    snapshots = _alias_snapshots(h)
    result = h.poll([_rec("a", h="h3")])  # same alias again: nothing written
    assert h.metadata(result, "push")["aliased"] == 0
    assert _alias_snapshots(h) == snapshots  # no Iceberg commit on the shared table


def _alias_snapshots(h):
    w, c = h._iceberg()
    with w, c:
        return len(iceberg_signals.get_aliases_table().metadata.snapshots)


class _BadIdSource(HookedSource):
    def to_silver_input(self, record, source_id):
        data = super().to_silver_input(record, source_id)
        if record["id"] == "bad":
            data["externalId"] = "bad"  # no `fakesrc:` prefix
        return data


def test_an_unprefixed_external_id_fails_only_its_row(tmp_path):
    h = Harness(tmp_path, _BadIdSource([]))
    result = h.poll([_rec("a"), _rec("bad")])
    assert set(h.gold) == {"a"}
    assert [sent["externalId"] for _, sent in h.pg.calls] == ["fakesrc:a"]
    assert h.metadata(result, "silver")["id_failed"] == 1


def test_a_conflict_leaves_the_row_unpushed_without_an_alias_and_retries_it(h):
    h.pg.fail_next["create"] = ClearApiError("CONFLICT: id held by source-other / fakesrc:a")
    h.poll([_rec("a")])
    assert h.gold["a"]["pushedAt"] is None
    assert _aliases(h) == []
    h.poll([])
    assert h.pg.ops() == ["create"]
    assert h.gold["a"]["pushedAt"] is not None


def test_a_failed_follow_up_update_writes_no_alias_yet(h):
    h.pg.rows[("source-fake-123", "fakesrc:a")] = {
        "id": "clegacy0000000000000000aa", "contentHash": "old", "retracted": False, "rawS3Key": None,
    }
    h.pg.fail_next["update"] = ClearApiError("boom")
    h.poll([_rec("a")])
    assert h.gold["a"]["pushedAt"] is None
    assert _aliases(h) == []


def test_event_bundles_list_signal_ids(h):
    from clear_pipeline.defs.gx_pipeline import iceberg_events

    merged = []
    real = iceberg_events.merge_event
    with patch("clear_pipeline.defs.gx_pipeline.factory.iceberg_events.merge_event",
               side_effect=lambda t, e: merged.append(dict(e)) or real(t, e)):
        h.poll([_rec("a")])
    assert [e["signalIds"] for e in merged] == [[A_ID]]


def test_gold_rows_from_before_the_column_get_their_id_from_the_stored_external_id(tmp_path):
    """No DB call: the id is a pure function of the stored signalInput.externalId."""
    import pyarrow as pa
    from pyiceberg.schema import Schema

    from clear_pipeline.defs.gx_pipeline import iceberg_catalog

    h = Harness(tmp_path, HookedSource([]))
    old_schema = Schema(*[f for f in iceberg_signals._SCHEMA.fields if f.name != "signalId"],
                        identifier_field_ids=[1])
    w, c = h._iceberg()
    with w, c:
        cat = iceberg_catalog.catalog()
        iceberg_catalog.ensure_namespace(cat)
        table = cat.create_table(f"{iceberg_catalog.NAMESPACE}.fakesrc_signals", schema=old_schema)
        signal_input = {"sourceId": "source-fake-123", "externalId": "fakesrc:a", "title": "t", "contentHash": "h1"}
        table.append(pa.Table.from_pylist(
            [{"externalId": "a", "eventId": "e1", "signalInputJson": json.dumps(signal_input), "retracted": False}],
            schema=old_schema.as_arrow(),
        ))
    h.poll([])
    assert h.gold["a"]["signalId"] == A_ID
    [(op, sent)] = h.pg.calls
    assert (op, sent["id"]) == ("create", A_ID)


def _legacy_row_behind_a_failed_follow_up_update_then_superseded(h, *, aliases_fail=False):
    h.pg.rows[("source-fake-123", "fakesrc:a")] = {
        "id": "clegacy0000000000000000aa", "contentHash": "old", "retracted": False, "rawS3Key": None,
    }
    h.pg.fail_next["update"] = ClearApiError("boom")
    h.poll([_rec("a", event_id="e1", role=T)])
    real = iceberg_signals.upsert_aliases

    def upsert_aliases(rows):
        if aliases_fail and rows:
            raise ClearApiError("aliases commit failed")
        real(rows)

    with patch.object(iceberg_signals, "upsert_aliases", side_effect=upsert_aliases):
        h.poll([_rec("b", event_id="e1", role=R)])  # b supersedes a: a is probed


def test_a_probe_that_reaches_a_legacy_row_records_its_alias(h):
    _legacy_row_behind_a_failed_follow_up_update_then_superseded(h)
    assert h.pg_row("a")["retracted"] is True
    assert h.gold["a"]["pushedState"] == "h1|1"
    assert _aliases(h) == [{"signalId": A_ID, "externalId": "fakesrc:a", "legacyId": "clegacy0000000000000000aa"}]
    assert _view(h)[A_ID] == "clegacy0000000000000000aa"


def test_a_probe_alias_is_stored_before_the_row_is_marked_probed(h):
    with pytest.raises(ClearApiError):
        _legacy_row_behind_a_failed_follow_up_update_then_superseded(h, aliases_fail=True)
    assert h.gold["a"]["pushedState"] is None
