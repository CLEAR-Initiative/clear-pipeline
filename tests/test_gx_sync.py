"""gx -> clear-api propagation: bronze skip, rawS3Key, and `_push`'s create /
update / probe for sources with sync hooks. Runs the real Dagster graph against
a fake S3, a local Iceberg warehouse and an in-memory, naturally keyed clear-api.
"""

from unittest.mock import patch

import dagster as dg
import pytest

from clear_pipeline.defs.gx_pipeline import iceberg_signals
from clear_pipeline.defs.gx_pipeline.factory import (
    _already_synced,
    build_gx_source_assets,
)
from clear_pipeline.defs.gx_pipeline.sources import (
    ACLEDGXSource,
    Darfur24GXSource,
    DataminrGXSource,
    IDMCGXSource,
)
from clear_pipeline.providers import idmc
from clear_pipeline.providers.clear_api import ClearApiNotFound
from tests.test_gx_pipeline import FakeS3, GroupingFakeSource, _gold_rows

# ── fakes ────────────────────────────────────────────────────────────────────


class CountingFakeS3(FakeS3):
    def __init__(self):
        super().__init__()
        self.puts: list[tuple[str, bytes]] = []

    def put_object(self, Bucket, Key, Body, **kw):
        super().put_object(Bucket, Key, Body, **kw)
        if Key.startswith("raw/"):
            self.puts.append((Key, self.objects[Key]))


class FakePostgres:
    """clear-api's signal table as gx sees it: get-or-create on create, and
    updateSignalContent keyed by (sourceId, externalId) raising NOT_FOUND."""

    def __init__(self):
        self.rows: dict[tuple[str, str], dict] = {}
        self.calls: list[tuple[str, dict]] = []
        self.fail_next: dict[str, Exception] = {}  # "create"/"update" -> error, once

    def _maybe_fail(self, op):
        if op in self.fail_next:
            raise self.fail_next.pop(op)

    def create(self, data):
        self.calls.append(("create", data))
        self._maybe_fail("create")
        key = (data["sourceId"], data["externalId"])
        if key not in self.rows:  # get-or-create returns an existing row unchanged
            self.rows[key] = {"contentHash": data.get("contentHash"), "retracted": False,
                              "rawS3Key": data.get("rawS3Key"), "title": data.get("title")}
        return {"id": f"sig-{key[1]}", "externalId": key[1], **self.rows[key]}

    def update(self, data):
        self.calls.append(("update", data))
        self._maybe_fail("update")
        key = (data["sourceId"], data["externalId"])
        if key not in self.rows:
            raise ClearApiNotFound([{"message": "Signal not found", "extensions": {"code": "NOT_FOUND"}}])
        row = self.rows[key]
        row["contentHash"] = data["contentHash"]
        row["title"] = data.get("title")
        if data.get("retracted") is not None:
            row["retracted"] = data["retracted"]
        if data.get("rawS3Key"):
            row["rawS3Key"] = data["rawS3Key"]
        return {"id": f"sig-{key[1]}", **row}

    def ops(self):
        return [op for op, _ in self.calls]


def _rec(rec_id, *, h="h1", event_id=None, role="Recommended figure", created_at="2026-09-01T00:00:00Z"):
    return {
        "id": rec_id, "ts": created_at, "title": f"Displacement {rec_id} {h}",
        "lat": 12.0, "lng": 30.0, "hash": h,
        "event_id": event_id, "role": role, "created_at": created_at,
    }


class HookedSource(GroupingFakeSource):
    """IDMC-shaped: group hooks plus the sync hooks. Each record carries its
    content `hash`; `content_update_input` delegates to the real IDMC builder."""

    def to_silver_input(self, record, source_id):
        data = super().to_silver_input(record, source_id)
        data["contentHash"] = record["hash"]
        return data

    def content_hash(self, record):
        return record["hash"]

    def content_update_input(self, signal_input, *, retracted):
        return idmc.build_signal_content_update(signal_input, retracted=retracted)


class Harness:
    """Successive polls against one S3 + Iceberg warehouse + FakePostgres."""

    def __init__(self, tmp_path, source):
        self.warehouse = f"file://{tmp_path / 'warehouse'}"
        self.catalog_uri = f"sqlite:///{tmp_path / 'catalog.db'}"
        self.s3 = CountingFakeS3()
        self.pg = FakePostgres()
        self.source = source
        defs_list = build_gx_source_assets(source)
        self.assets = [d for d in defs_list if isinstance(d, dg.AssetsDefinition)]
        self.checks = [d for d in defs_list if isinstance(d, dg.AssetChecksDefinition)]

    def _iceberg(self):
        return (
            patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_warehouse", self.warehouse),
            patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_catalog_uri", self.catalog_uri),
        )

    def poll(self, batch):
        self.source._batches = [batch]
        self.source._poll_count = 0
        self.pg.calls.clear()
        self.s3.puts.clear()
        w, c = self._iceberg()
        with (
            w, c,
            patch("clear_pipeline.defs.gx_pipeline.factory.lake.s3_client", return_value=self.s3),
            patch("clear_pipeline.defs.gx_pipeline.factory.settings.s3_bucket", "test-bucket"),
            patch("clear_pipeline.defs.gx_pipeline.factory.create_signal_for_sync", side_effect=self.pg.create),
            patch("clear_pipeline.defs.gx_pipeline.factory.update_signal_content", side_effect=self.pg.update),
            patch("clear_pipeline.defs.gx_pipeline.factory.classify_signal") as classify,
        ):
            classify.return_value.relevance = 0.9
            classify.return_value.type_level_2 = "conflict"
            classify.return_value.disaster_types = ["cv"]
            result = dg.materialize(self.assets + self.checks)
        assert result.success
        return result

    def metadata(self, result, asset):
        for event in result.get_asset_materialization_events():
            if event.asset_key.to_user_string() == f"fakesrc_{asset}":
                return {k: v.value for k, v in event.materialization.metadata.items()}
        return {}

    @property
    def gold(self):
        return _gold_rows(self.warehouse, self.catalog_uri)

    def pg_row(self, rec_id):
        return self.pg.rows.get(("source-fake-123", f"fakesrc:{rec_id}"))

    def snapshot_count(self):
        w, c = self._iceberg()
        with w, c:
            return len(iceberg_signals.get_signals_table("fakesrc").metadata.snapshots)


@pytest.fixture
def h(tmp_path):
    return Harness(tmp_path, HookedSource([]))


# ── create ───────────────────────────────────────────────────────────────────


def test_create_carries_the_bronze_key_as_raw_s3_key(h):
    h.poll([_rec("a")])
    assert h.pg.ops() == ["create"]
    assert h.pg_row("a")["rawS3Key"] == "raw/fakesrc/2026-09-01/a.json"
    assert h.gold["a"]["pushedState"] == "h1|0"


def test_hookless_source_create_also_carries_raw_s3_key(tmp_path):
    hookless = Harness(tmp_path, GroupingFakeSource([]))
    hookless.poll([_rec("a")])
    assert hookless.pg.ops() == ["create"]
    assert hookless.pg_row("a")["rawS3Key"] == "raw/fakesrc/2026-09-01/a.json"


def test_create_returning_an_older_row_is_followed_by_one_update(h):
    # Postgres already holds the row with another hash (earlier half-failed
    # push, or production created it).
    h.pg.rows[("source-fake-123", "fakesrc:a")] = {"contentHash": "old", "retracted": False, "rawS3Key": None}
    h.poll([_rec("a")])
    assert h.pg.ops() == ["create", "update"]
    assert h.pg_row("a")["contentHash"] == "h1"
    assert h.pg_row("a")["rawS3Key"] == "raw/fakesrc/2026-09-01/a.json"


def test_create_returning_an_identical_row_sends_no_update(h):
    h.pg.rows[("source-fake-123", "fakesrc:a")] = {"contentHash": "h1", "retracted": False, "rawS3Key": None}
    h.poll([_rec("a")])
    assert h.pg.ops() == ["create"]


def test_create_returning_a_retracted_row_unretracts_it(h):
    h.pg.rows[("source-fake-123", "fakesrc:a")] = {"contentHash": "h1", "retracted": True, "rawS3Key": None}
    h.poll([_rec("a")])
    assert h.pg.ops() == ["create", "update"]
    assert h.pg_row("a")["retracted"] is False


# ── skip + quiet ─────────────────────────────────────────────────────────────


def test_unchanged_repoll_writes_the_blob_but_makes_no_calls_and_no_snapshot(h):
    h.poll([_rec("a"), _rec("b")])
    snapshots = h.snapshot_count()

    result = h.poll([_rec("a"), _rec("b")])

    assert h.pg.calls == []
    assert len(h.s3.puts) == 2  # blob always rewritten with the latest payload
    assert h.metadata(result, "bronze")["skipped_unchanged"] == 2
    assert "rows" not in h.metadata(result, "silver")  # empty bronze -> silver returns early
    assert h.snapshot_count() == snapshots


@pytest.mark.parametrize(
    ("polled", "gold_hash", "pushed_state", "skip"),
    [
        ("h1", "h1", "h1|0", True),    # (a) in sync
        ("h1", "h1", "h1|1", True),    # (b) in sync, retracted
        ("h1", "h1", None, False),     # (c) legacy row, never stamped
        ("h1", "h2", "h1|0", False),   # (d) revert after a failed push
        ("h2", "h1", "h1|0", False),   # (e) revised upstream
        ("h1", None, None, False),     # new row
        ("h1", "h1", "h12|0", False),  # prefix trap
        (None, None, None, False),     # no hash
    ],
)
def test_already_synced(polled, gold_hash, pushed_state, skip):
    assert _already_synced(polled, gold_hash, pushed_state) is skip


# ── update ───────────────────────────────────────────────────────────────────


def test_revision_after_push_is_one_natural_key_update_then_quiet(h):
    h.poll([_rec("a", h="h1")])
    h.poll([_rec("a", h="h2")])

    assert h.pg.ops() == ["update"]
    sent = h.pg.calls[0][1]
    assert (sent["sourceId"], sent["externalId"]) == ("source-fake-123", "fakesrc:a")
    assert "id" not in sent
    assert sent["contentHash"] == "h2"
    assert sent["retracted"] is False
    assert sent["rawS3Key"] == "raw/fakesrc/2026-09-01/a.json"
    assert h.gold["a"]["pushedState"] == "h2|0"

    h.poll([_rec("a", h="h2")])
    assert h.pg.calls == []


def test_revision_overwrites_the_blob_at_the_same_key(h):
    h.poll([_rec("a", h="h1")])
    h.poll([_rec("a", h="h2")])
    key = "raw/fakesrc/2026-09-01/a.json"
    assert [k for k, _ in h.s3.puts] == [key]
    assert b'"hash": "h2"' in h.s3.objects[key]
    assert h.pg_row("a")["rawS3Key"] == key


def test_key_moves_when_the_created_at_day_changes(h):
    # Known limitation: the blob key follows the created_at day.
    h.poll([_rec("a", h="h1", created_at="2026-09-01T00:00:00Z")])
    h.poll([_rec("a", h="h2", created_at="2026-09-02T00:00:00Z")])
    assert h.pg_row("a")["rawS3Key"] == "raw/fakesrc/2026-09-02/a.json"
    assert "raw/fakesrc/2026-09-01/a.json" in h.s3.objects  # orphaned, no cleanup


def test_update_not_found_is_logged_not_recreated_and_quiet_after(h):
    h.poll([_rec("a", h="h1")])
    del h.pg.rows[("source-fake-123", "fakesrc:a")]  # admin delete / DB reset

    result = h.poll([_rec("a", h="h2")])

    assert h.pg.ops() == ["update"]
    assert h.pg_row("a") is None, "a deleted signal stays deleted"
    assert h.metadata(result, "push")["not_found"] == 1
    assert h.gold["a"]["pushedState"] == "h2|0"

    h.poll([_rec("a", h="h2")])
    assert h.pg.calls == []


def test_other_update_error_leaves_the_row_for_retry(h):
    h.poll([_rec("a", h="h1")])
    h.pg.fail_next["update"] = RuntimeError("clear-api 500")

    result = h.poll([_rec("a", h="h2")])

    assert h.metadata(result, "push")["failed"] == 1
    assert h.gold["a"]["pushedState"] == "h1|0"
    assert h.pg_row("a")["contentHash"] == "h1"

    h.poll([_rec("a", h="h2")])
    assert h.pg.ops() == ["update"]
    assert h.pg_row("a")["contentHash"] == "h2"


def test_revert_after_a_failed_push_is_not_skipped_and_ends_quiet(h):
    # h1 pushed; h2 polled but its update fails; IDMC reverts to h1.
    h.poll([_rec("a", h="h1")])
    h.pg.fail_next["update"] = RuntimeError("clear-api 500")
    h.poll([_rec("a", h="h2")])

    result = h.poll([_rec("a", h="h1")])

    assert h.metadata(result, "bronze")["skipped_unchanged"] == 0
    assert h.pg.calls == [], "Postgres already holds h1"
    assert h.gold["a"]["pushedState"] == "h1|0"
    assert b'"hash": "h1"' in h.s3.objects["raw/fakesrc/2026-09-01/a.json"]


def test_rows_are_isolated(h):
    h.poll([_rec("a", h="h1"), _rec("b", h="h1")])
    h.pg.fail_next["update"] = RuntimeError("boom")  # whichever goes first

    result = h.poll([_rec("a", h="h2"), _rec("b", h="h2")])

    meta = h.metadata(result, "push")
    assert (meta["updated"], meta["failed"]) == (1, 1)


def test_push_metadata_keys(h):
    result = h.poll([_rec("a")])
    assert set(h.metadata(result, "push")) == {"created", "updated", "probed", "not_found", "failed"}


def test_legacy_pushed_row_without_state_gets_one_update_with_its_key(h):
    # A legacy pushed row has pushedState NULL and no rawS3Key.
    h.poll([_rec("a", h="h1")])
    w, c = h._iceberg()
    with w, c:
        table = iceberg_signals.get_signals_table("fakesrc")
        (stored,) = table.scan().to_pandas().to_dict("records")
        legacy = iceberg_signals._from_column_dict(stored)
        legacy["pushedState"] = None
        legacy["signalInput"].pop("rawS3Key")
        iceberg_signals.upsert_signals(table, [legacy])
    h.pg.rows[("source-fake-123", "fakesrc:a")]["rawS3Key"] = None

    h.poll([_rec("a", h="h1")])

    assert h.pg.ops() == ["update"]
    assert h.pg_row("a")["rawS3Key"] == "raw/fakesrc/2026-09-01/a.json"
    h.poll([_rec("a", h="h1")])
    assert h.pg.calls == []


# ── retraction ───────────────────────────────────────────────────────────────

T = "Triangulation"
R = "Recommended figure"


def test_retraction_after_push_then_reversal(h):
    h.poll([_rec("t", event_id="e1", role=T, created_at="2026-09-05T00:00:00Z")])
    h.poll([_rec("r", event_id="e1", role=R, created_at="2026-09-02T00:00:00Z")])
    assert h.pg_row("t")["retracted"] is True
    assert [(op, d["externalId"], d.get("retracted")) for op, d in h.pg.calls] == [
        ("create", "fakesrc:r", None), ("update", "fakesrc:t", True),
    ]

    # r revised to Triangulation: t (more recent) lives again, r is retracted.
    h.poll([_rec("r", h="h2", event_id="e1", role=T, created_at="2026-09-02T00:00:00Z")])
    assert h.pg_row("t")["retracted"] is False
    assert h.pg_row("r")["retracted"] is True
    assert sorted((d["externalId"], d["retracted"]) for _, d in h.pg.calls) == [
        ("fakesrc:r", True), ("fakesrc:t", False),
    ]


def test_retraction_sends_the_real_content_hash(h):
    h.poll([_rec("t", event_id="e1", role=T)])
    h.poll([_rec("r", event_id="e1", role=R)])
    update = next(d for op, d in h.pg.calls if op == "update")
    assert update["contentHash"] == "h1"


def test_content_and_retraction_in_one_poll_is_exactly_one_call(h):
    h.poll([_rec("t", event_id="e1", role=T)])
    h.poll([_rec("t", h="h2", event_id="e1", role=T), _rec("r", event_id="e1", role=R)])
    t_calls = [d for _, d in h.pg.calls if d["externalId"] == "fakesrc:t"]
    assert len(t_calls) == 1
    assert (t_calls[0]["contentHash"], t_calls[0]["retracted"]) == ("h2", True)


def test_one_revised_sibling_among_unchanged_ones(h):
    h.poll([_rec("r", event_id="e1", role=R), _rec("x", event_id="e2", role=R)])
    result = h.poll([_rec("r", h="h2", event_id="e1", role=R), _rec("x", event_id="e2", role=R)])
    assert h.metadata(result, "bronze")["skipped_unchanged"] == 1
    assert [d["externalId"] for _, d in h.pg.calls] == ["fakesrc:r"]


# ── probe ────────────────────────────────────────────────────────────────────


def test_probe_never_created_row_gets_not_found_and_no_create(h):
    h.pg.fail_next["create"] = RuntimeError("clear-api down")
    h.poll([_rec("t", event_id="e1", role=T)])  # create fails before Postgres
    assert h.gold["t"]["pushedAt"] is None

    result = h.poll([_rec("r", event_id="e1", role=R)])

    assert h.metadata(result, "push")["probed"] == 1
    t_ops = [(op, d.get("retracted")) for op, d in h.pg.calls if d["externalId"] == "fakesrc:t"]
    assert t_ops == [("update", True)]
    assert h.pg_row("t") is None
    assert h.gold["t"]["pushedAt"] is None
    assert h.gold["t"]["pushedState"] == "h1|1"

    h.poll([_rec("r", event_id="e1", role=R)])
    assert h.pg.calls == []


def test_probe_lands_when_an_earlier_create_reached_postgres(h):
    # Create reached Postgres, but gold never recorded it.
    original = h.pg.create

    def create_then_fail(data):
        original(data)
        raise RuntimeError("timeout after commit")

    with patch.object(h.pg, "create", side_effect=create_then_fail):
        h.poll([_rec("t", event_id="e1", role=T)])
    assert h.pg_row("t")["retracted"] is False

    h.poll([_rec("r", event_id="e1", role=R)])

    assert h.pg_row("t")["retracted"] is True


def test_probe_then_unretract_creates_it_once(h):
    h.pg.fail_next["create"] = RuntimeError("clear-api down")
    h.poll([_rec("t", event_id="e1", role=T, created_at="2026-09-05T00:00:00Z")])
    h.poll([_rec("r", event_id="e1", role=R, created_at="2026-09-02T00:00:00Z")])
    assert h.pg_row("t") is None

    h.poll([_rec("r", h="h2", event_id="e1", role=T, created_at="2026-09-02T00:00:00Z")])

    assert [op for op, d in h.pg.calls if d["externalId"] == "fakesrc:t"] == ["create"]
    assert h.pg_row("t")["retracted"] is False


# ── hookless sources stay create-only ────────────────────────────────────────


def test_hookless_source_never_updates_or_probes(tmp_path):
    hk = Harness(tmp_path, GroupingFakeSource([]))
    hk.pg.fail_next["create"] = RuntimeError("down")
    hk.poll([_rec("t", event_id="e1", role=T)])
    hk.poll([_rec("r", event_id="e1", role=R)])  # retracts t (never pushed)
    hk.poll([_rec("r", h="h2", event_id="e1", role=R)])  # revised
    assert all(op == "create" for op in hk.pg.ops())
    assert ("source-fake-123", "fakesrc:t") not in hk.pg.rows


def test_mark_seen_only_on_create(h):
    with patch.object(HookedSource, "mark_seen") as mark_seen:
        h.poll([_rec("a", h="h1")])
        h.poll([_rec("a", h="h2")])
    assert mark_seen.call_count == 1


# ── gold helpers ─────────────────────────────────────────────────────────────


def _table(tmp_path, name="units"):
    w = patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_warehouse", f"file://{tmp_path / 'warehouse'}")
    c = patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_catalog_uri", f"sqlite:///{tmp_path / 'catalog.db'}")
    return w, c


def _row(ext, *, pushed_at=None, pushed_state=None, retracted=False, h="h1"):
    return {"externalId": ext, "pushedAt": pushed_at, "pushedState": pushed_state,
            "retracted": retracted, "signalInput": {"contentHash": h}}


def test_sync_state():
    assert iceberg_signals.sync_state({"signalInput": {"contentHash": "h1"}, "retracted": False}) == "h1|0"
    assert iceberg_signals.sync_state({"signalInput": {"contentHash": "h1"}, "retracted": True}) == "h1|1"
    assert iceberg_signals.sync_state({"signalInput": {}, "retracted": None}) == "None|0"


def test_signals_to_sync_partition(tmp_path):
    w, c = _table(tmp_path)
    with w, c:
        table = iceberg_signals.get_signals_table("units")
        iceberg_signals.upsert_signals(table, [
            _row("A"),                                                     # create
            _row("B", retracted=True),                                     # probe
            _row("C", retracted=True, pushed_state="h1|1"),                # probed already
            _row("D", pushed_at="t", pushed_state="h1|0"),                 # in sync
            _row("E", pushed_at="t", pushed_state="h0|0"),                 # update (content)
            _row("F", pushed_at="t", pushed_state="h1|0", retracted=True),  # update (retraction)
            _row("G", pushed_at="t"),                                      # update (legacy)
        ])

        def ids(d):
            return {k: sorted(r["externalId"] for r in v) for k, v in d.items()}

        assert ids(iceberg_signals.signals_to_sync(table, can_update=True)) == {
            "create": ["A"], "probe": ["B"], "update": ["E", "F", "G"],
        }
        assert ids(iceberg_signals.signals_to_sync(table, can_update=False)) == {
            "create": ["A"], "probe": [], "update": [],
        }


def test_sync_hashes_and_existing_push_state(tmp_path):
    w, c = _table(tmp_path)
    with w, c:
        table = iceberg_signals.get_signals_table("units")
        iceberg_signals.upsert_signals(table, [_row("A", pushed_at="t", pushed_state="h1|0"), _row("B", h="h9")])
        assert iceberg_signals.sync_hashes(table, ["A", "B", "Z"]) == {"A": ("h1", "h1|0"), "B": ("h9", None)}
        assert iceberg_signals.existing_push_state(table, ["A", "B"]) == {
            "A": {"pushedAt": "t", "pushedState": "h1|0"},
            "B": {"pushedAt": None, "pushedState": None},
        }
        assert iceberg_signals.sync_hashes(table, []) == {}


def test_push_state_survives_a_rerun_through_gold(h):
    h.poll([_rec("a")])
    before = h.gold["a"]
    h.poll([_rec("a"), _rec("b")])  # a skipped at bronze, b new
    assert (h.gold["a"]["pushedAt"], h.gold["a"]["pushedState"]) == (before["pushedAt"], "h1|0")


# ── IDMC hooks ───────────────────────────────────────────────────────────────


def test_idmc_sync_hooks_delegate_to_provider():
    src = IDMCGXSource()
    assert src.content_hash({"content_hash": "abc"}) == "abc"
    with patch("clear_pipeline.defs.gx_pipeline.sources.idmc.build_signal_content_update") as build:
        src.content_update_input({"x": 1}, retracted=True)
    build.assert_called_once_with({"x": 1}, retracted=True)


def test_only_idmc_defines_sync_hooks():
    for src in (DataminrGXSource(), ACLEDGXSource(), Darfur24GXSource()):
        assert not hasattr(src, "content_hash")
        assert not hasattr(src, "content_update_input")
    assert hasattr(IDMCGXSource(), "content_hash")
    assert hasattr(IDMCGXSource(), "content_update_input")
