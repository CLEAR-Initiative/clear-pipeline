"""Smoke test for the generic bronze -> silver -> gold GX-gated factory
(defs/gx_pipeline/). Exercises the real Dagster wiring end to end — a fake
S3 + fake clear-api calls, one source registered via a throwaway
``GXSource``, so this catches asset-input wiring bugs (a mismatched
``ins=``/parameter name silently drops data — the exact class of bug this
guards against) that a pure-function unit test would miss.
"""

import json
from unittest.mock import patch

import dagster as dg

from clear_pipeline.defs.gx_pipeline.factory import build_gx_source_assets
from clear_pipeline.defs.gx_pipeline.sources import (
    GX_SOURCES,
    ACLEDGXSource,
    Darfur24GXSource,
    DataminrGXSource,
    GXSource,
    IDMCGXSource,
)
from clear_pipeline.providers import idmc


def test_registered_sources_conform_to_protocol():
    sources = {s.source: s for s in GX_SOURCES}
    assert sources.keys() == {"dataminr", "acled", "darfur24", "idmc"}
    assert all(isinstance(s, GXSource) for s in sources.values())


def test_idmc_source_to_silver_input_is_pure_transform():
    """`to_silver_input` must never promote a geoparser candidate to an L4
    location (a clear-api write, forbidden before `_push`), like ACLEDGXSource."""
    with patch(
        "clear_pipeline.defs.gx_pipeline.sources.idmc.build_idmc_signal_input"
    ) as mock_build:
        IDMCGXSource().to_silver_input({"idu_id": "1"}, "source-1")
    mock_build.assert_called_once_with({"idu_id": "1"}, "source-1", promote=False)


def test_idmc_source_group_hooks_delegate_to_provider():
    with (
        patch("clear_pipeline.defs.gx_pipeline.sources.idmc.group_member") as mock_member,
        patch("clear_pipeline.defs.gx_pipeline.sources.idmc.resolve_group") as mock_resolve,
    ):
        mock_member.return_value = {"externalId": "1"}
        mock_resolve.return_value = {"1": "keep"}
        source = IDMCGXSource()
        assert source.group_member("1", {"event_id": "ev-1"}) == {"externalId": "1"}
        assert source.resolve_group([{"externalId": "1"}]) == {"1": "keep"}
    mock_member.assert_called_once_with("1", {"event_id": "ev-1"})
    mock_resolve.assert_called_once_with([{"externalId": "1"}])


def test_only_idmc_source_defines_group_hooks():
    """Group hooks are optional, IDMC-only and outside the Protocol (`_reconcile`
    probes via getattr): other sources must not define them."""
    for source in (DataminrGXSource(), ACLEDGXSource(), Darfur24GXSource()):
        assert not hasattr(source, "group_member")
        assert not hasattr(source, "resolve_group")
    assert hasattr(IDMCGXSource(), "group_member")
    assert hasattr(IDMCGXSource(), "resolve_group")


class FakeS3:
    """In-memory S3 stand-in: just enough of the boto3 surface the factory
    calls (put_object/get_object/list via a paginator)."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, **_):
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode("utf-8")

    def get_object(self, Bucket, Key):
        import io

        from botocore.exceptions import ClientError
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def get_paginator(self, _op_name):
        objects = self.objects

        class _Paginator:
            def paginate(self, Bucket, Prefix):
                keys = [k for k in objects if k.startswith(Prefix)]
                yield {"Contents": [{"Key": k} for k in keys]}

        return _Paginator()


class FakeSource:
    """Two synthetic records; second poll returns none (simulates drained)."""

    source = "fakesrc"

    def __init__(self):
        self._records = [
            {"id": "r1", "ts": "2026-09-01T00:00:00Z", "title": "Clash in Testville", "lat": 12.0, "lng": 30.0},
            {"id": "r2", "ts": "2026-09-01T01:00:00Z", "title": "Clash near Testville", "lat": 12.01, "lng": 30.01},
        ]
        self._polled = False
        self._watermark = None

    def poll(self, since):
        if self._polled:
            return []
        self._polled = True
        return self._records

    def external_id(self, record):
        return record["id"]

    def published_at(self, record):
        return record["ts"]

    def raw_bytes(self, record):
        import json
        return json.dumps(record).encode("utf-8")

    def parse(self, raw):
        import json
        return json.loads(raw)

    def api_source_id(self):
        return "source-fake-123"

    def last_synced(self):
        return self._watermark

    def set_watermark(self, ts):
        self._watermark = ts

    def mark_seen(self, external_id):
        pass

    def to_silver_input(self, record, source_id):
        return {
            "sourceId": source_id,
            "externalId": f"fakesrc:{record['id']}",
            "title": record["title"],
            "description": f"{record['title']} — details",
            "severity": 3,
            "casualties": 2,
            "lat": record["lat"],
            "lng": record["lng"],
            "publishedAt": record["ts"],
            # Same district for both records -> exercises the merge path
            # (both should land in the SAME gold event, not two isolated ones).
            "geoparsedData": {"display_name": "Testville, Test State, Testland"},
        }


def test_gx_pipeline_end_to_end(tmp_path):
    fake_s3 = FakeS3()
    iceberg_warehouse = f"file://{tmp_path / 'warehouse'}"
    iceberg_catalog_uri = f"sqlite:///{tmp_path / 'catalog.db'}"
    created_signals = []

    def fake_create_signal(input_data):
        row = {**input_data, "id": f"sig-{len(created_signals)}", "generalLocation": {"id": "loc-1", "level": 2, "ancestorIds": []}}
        created_signals.append(row)
        return row

    defs_list = build_gx_source_assets(FakeSource())
    assets = [d for d in defs_list if isinstance(d, dg.AssetsDefinition)]
    checks = [d for d in defs_list if isinstance(d, dg.AssetChecksDefinition)]

    with (
        patch("clear_pipeline.defs.gx_pipeline.factory.lake.s3_client", return_value=fake_s3),
        patch("clear_pipeline.defs.gx_pipeline.factory.settings.s3_bucket", "test-bucket"),
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_warehouse", iceberg_warehouse),
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_catalog_uri", iceberg_catalog_uri),
        patch("clear_pipeline.defs.gx_pipeline.factory.create_signal_for_sync", side_effect=fake_create_signal),
        patch("clear_pipeline.defs.gx_pipeline.factory.classify_locally") as mock_classify,
    ):
        mock_classify.return_value.relevance = 0.9
        mock_classify.return_value.type_level_2 = "conflict"
        mock_classify.return_value.disaster_types = ["cv"]

        result = dg.materialize(assets + checks)

    assert result.success

    # Both records went through bronze -> silver -> ... -> gold and pushed.
    # Gold signals is Iceberg (§6, Type-1) — inspect the table directly.
    from clear_pipeline.defs.gx_pipeline import iceberg_signals
    with (
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_warehouse", iceberg_warehouse),
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_catalog_uri", iceberg_catalog_uri),
    ):
        signals_table = iceberg_signals.get_signals_table("fakesrc")
        all_signals = signals_table.scan().to_pandas()
        assert len(all_signals) == 2
        assert all_signals["pushedAt"].notna().all(), "every gold row should be stamped pushed"

    # One createSignal per record; event push is stubbed (iceberg_events.py) —
    # no createEvent/escalate_to_alert call exists on factory to even patch.
    assert len(created_signals) == 2

    # Gold events persistence is stubbed — nothing is ever written.
    from clear_pipeline.defs.gx_pipeline import iceberg_events
    assert iceberg_events.get_events_table("fakesrc") is None
    assert iceberg_events.current_events_df(None).empty

    # A second run against the SAME (now-pushed) rows should push nothing new.
    with (
        patch("clear_pipeline.defs.gx_pipeline.factory.lake.s3_client", return_value=fake_s3),
        patch("clear_pipeline.defs.gx_pipeline.factory.settings.s3_bucket", "test-bucket"),
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_warehouse", iceberg_warehouse),
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_catalog_uri", iceberg_catalog_uri),
        patch("clear_pipeline.defs.gx_pipeline.factory.create_signal_for_sync", side_effect=fake_create_signal),
    ):
        push_asset = next(a for a in assets if "fakesrc_push" in [k.to_user_string() for k in a.keys])
        second_result = dg.materialize([push_asset], selection=[push_asset])
        assert second_result.success
    assert len(created_signals) == 2, "second push run must not re-push already-pushed rows"


# ══════════════════════════════════════════════════════════════════════════
# Supersession groups (`<source>_reconcile`). The verdict is computed over
# batch ∪ gold, so a row superseded by a later poll is still caught; most of
# these tests exercise that round-trip through the gold table.
# ══════════════════════════════════════════════════════════════════════════


def _group_record(rec_id, event_id, role, created_at):
    """A record shaped like a parsed IDU row — the fields idmc.group_member
    reads (`event_id`/`role`/`created_at`) plus what FakeSource needs."""
    return {
        "id": rec_id, "ts": created_at, "title": f"Displacement {rec_id}",
        "lat": 12.0, "lng": 30.0,
        "event_id": event_id, "role": role, "created_at": created_at,
    }


class GroupingFakeSource(FakeSource):
    """A source whose rows compete in an `event_id` group, like IDMC. Uses the
    real providers/idmc.py rules: failures come from how rules, gold round-trip
    and wiring combine. Each `poll` returns the next batch."""

    def __init__(self, batches):
        self._batches = list(batches)
        self._poll_count = 0
        self._watermark = None

    def poll(self, since):
        if self._poll_count >= len(self._batches):
            return []
        batch = self._batches[self._poll_count]
        self._poll_count += 1
        return batch

    def to_silver_input(self, record, source_id):
        data = super().to_silver_input(record, source_id)
        # Where build_idmc_signal_input puts the verbatim row; read back from gold.
        data["rawData"] = record
        return data

    def group_member(self, external_id, raw_data):
        return idmc.group_member(external_id, raw_data)

    def resolve_group(self, members):
        return idmc.resolve_group(members)


def _run_gx(assets, checks, fake_s3, warehouse, catalog_uri, create_signal):
    with (
        patch("clear_pipeline.defs.gx_pipeline.factory.lake.s3_client", return_value=fake_s3),
        patch("clear_pipeline.defs.gx_pipeline.factory.settings.s3_bucket", "test-bucket"),
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_warehouse", warehouse),
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_catalog_uri", catalog_uri),
        patch("clear_pipeline.defs.gx_pipeline.factory.create_signal_for_sync", side_effect=create_signal),
        patch("clear_pipeline.defs.gx_pipeline.factory.classify_locally") as mock_classify,
    ):
        mock_classify.return_value.relevance = 0.9
        mock_classify.return_value.type_level_2 = "conflict"
        mock_classify.return_value.disaster_types = ["cv"]
        result = dg.materialize(assets + checks)
    assert result.success
    return result


def _gold_rows(warehouse, catalog_uri, source="fakesrc"):
    """Gold rows by externalId, read from Iceberg. pandas NaN is normalized to
    None, as iceberg_signals' reader does, so `is None` assertions hold."""
    import pandas as pd

    from clear_pipeline.defs.gx_pipeline import iceberg_signals
    with (
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_warehouse", warehouse),
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_catalog_uri", catalog_uri),
    ):
        table = iceberg_signals.get_signals_table(source)
        return {
            row["externalId"]: {k: (None if pd.isna(v) else v) for k, v in row.items()}
            for row in table.scan().to_pandas().to_dict("records")
        }


def _gold_role(gold_row: dict) -> str | None:
    """The role in a gold row's stored rawData: what a later poll's
    `group_member` reads for this row when it is not re-sent."""
    signal_input = json.loads(gold_row["signalInputJson"]) if gold_row.get("signalInputJson") else {}
    return (signal_input.get("rawData") or {}).get("role")


class _GroupHarness:
    """Builds the assets once; successive polls share one S3 + Iceberg
    warehouse, so gold persists between runs as in production."""

    def __init__(self, tmp_path, batches):
        self.warehouse = f"file://{tmp_path / 'warehouse'}"
        self.catalog_uri = f"sqlite:///{tmp_path / 'catalog.db'}"
        self.s3 = FakeS3()
        self.created = []
        defs_list = build_gx_source_assets(GroupingFakeSource(batches))
        self.assets = [d for d in defs_list if isinstance(d, dg.AssetsDefinition)]
        self.checks = [d for d in defs_list if isinstance(d, dg.AssetChecksDefinition)]

    def _create_signal(self, input_data):
        row = {**input_data, "id": f"sig-{len(self.created)}",
               "generalLocation": {"id": "loc-1", "level": 2, "ancestorIds": []}}
        self.created.append(row)
        return row

    def poll(self):
        _run_gx(self.assets, self.checks, self.s3, self.warehouse,
                self.catalog_uri, self._create_signal)

    @property
    def gold(self):
        return _gold_rows(self.warehouse, self.catalog_uri)

    @property
    def created_ids(self):
        return [row["externalId"] for row in self.created]


def test_reconcile_retracts_a_gold_row_superseded_by_a_later_poll(tmp_path):
    """A Triangulation row is pushed alone; the Recommended figure superseding
    it arrives in the next poll's batch of one. Reconcile must read t1 back
    from gold to see the group at all."""
    harness = _GroupHarness(tmp_path, batches=[
        [_group_record("t1", "ev-1", "Triangulation", "2026-09-01T00:00:00Z")],
        [_group_record("r1", "ev-1", "Recommended figure", "2026-09-02T00:00:00Z")],
    ])

    harness.poll()
    assert harness.created_ids == ["fakesrc:t1"]
    assert harness.gold["t1"]["retracted"] is False
    assert harness.gold["t1"]["groupKey"] == "idmc:eventId:ev-1", "gold must record the group to be findable later"

    harness.poll()

    gold = harness.gold
    assert gold["t1"]["retracted"] is True, "superseded by the Recommended figure from the second poll"
    assert gold["r1"]["retracted"] is False
    # The retraction is a gold-state change, not a re-push: t1 keeps the
    # pushedAt from poll 1 and is never sent to clear-api twice.
    assert harness.created_ids == ["fakesrc:t1", "fakesrc:r1"]


def test_reconcile_never_creates_a_signal_superseded_by_existing_gold(tmp_path):
    """A row superseded on arrival makes ZERO clear-api calls: create-then-retract
    would leave a live `status=NEW` row in between, which the drain picks up."""
    harness = _GroupHarness(tmp_path, batches=[
        [_group_record("r1", "ev-1", "Recommended figure", "2026-09-01T00:00:00Z")],
        [_group_record("t1", "ev-1", "Triangulation", "2026-09-02T00:00:00Z")],
    ])

    harness.poll()
    harness.poll()

    assert harness.created_ids == ["fakesrc:r1"], "t1 was superseded on arrival — never created"
    assert "t1" not in harness.gold, "a row that never reached clear-api needs no gold tombstone"
    assert harness.gold["r1"]["retracted"] is False


def test_reconcile_brings_a_retracted_row_back_when_the_verdict_reverses(tmp_path):
    """Retraction is reversible: revising r1 to Triangulation makes the group
    all-Triangulation, so t1 (more recent) lives again. Reconcile must read
    already-retracted rows and write both directions."""
    harness = _GroupHarness(tmp_path, batches=[
        [_group_record("t1", "ev-1", "Triangulation", "2026-09-05T00:00:00Z")],
        [_group_record("r1", "ev-1", "Recommended figure", "2026-09-02T00:00:00Z")],
        # Same row id as poll 2, revised role — the batch copy must win over
        # the stale gold copy, or the group is resolved on an outdated role.
        [_group_record("r1", "ev-1", "Triangulation", "2026-09-02T00:00:00Z")],
    ])

    harness.poll()
    harness.poll()
    assert harness.gold["t1"]["retracted"] is True

    harness.poll()

    gold = harness.gold
    assert gold["t1"]["retracted"] is False, "verdict reversed — t1 is the most recent Triangulation"
    assert gold["r1"]["retracted"] is True
    # r1 is RETRACT, so it skips `_gold`'s overwrite: reconcile must refresh its
    # stored content, or gold keeps poll 2's role under the new verdict.
    assert _gold_role(gold["r1"]) == "Triangulation", \
        "gold's stored content must match what resolve_group actually used, not freeze at the last poll r1 was kept"
    # Neither reversal re-pushes: both rows were already created once.
    assert harness.created_ids == ["fakesrc:t1", "fakesrc:r1"]


def test_reconcile_uses_refreshed_content_on_a_later_poll_that_omits_the_row(tmp_path):
    """r1 is revised in poll 3 and never resent. Poll 4 revisits the group via
    t1 and must use r1's refreshed stored role, not poll 2's "Recommended
    figure", or it would wrongly retract t1."""
    harness = _GroupHarness(tmp_path, batches=[
        [_group_record("t1", "ev-1", "Triangulation", "2026-09-05T00:00:00Z")],
        [_group_record("r1", "ev-1", "Recommended figure", "2026-09-02T00:00:00Z")],
        [_group_record("r1", "ev-1", "Triangulation", "2026-09-02T00:00:00Z")],
        # Poll 4: only t1; an unchanged r1 is not resent.
        [_group_record("t1", "ev-1", "Triangulation", "2026-09-05T00:00:00Z")],
    ])

    for _ in range(4):
        harness.poll()

    gold = harness.gold
    assert gold["t1"]["retracted"] is False, \
        "t1 is still the more recent of an all-Triangulation group — nothing should have changed"
    assert gold["r1"]["retracted"] is True
    assert harness.created_ids == ["fakesrc:t1", "fakesrc:r1"], "no re-push on either poll 3 or poll 4"


def test_reconcile_resolves_a_group_delivered_within_one_poll(tmp_path):
    """Both rows in one poll: both get a silver blob (reconcile runs after
    silver); only the survivor reaches gold and push."""
    harness = _GroupHarness(tmp_path, batches=[[
        _group_record("r1", "ev-1", "Recommended figure", "2026-09-01T00:00:00Z"),
        _group_record("t1", "ev-1", "Triangulation", "2026-09-01T01:00:00Z"),
    ]])

    harness.poll()

    silver_keys = [k for k in harness.s3.objects if "silver" in k]
    assert any("r1" in k for k in silver_keys)
    assert any("t1" in k for k in silver_keys), "silver keeps every role; reconcile decides supersession"
    assert harness.created_ids == ["fakesrc:r1"]
    assert "t1" not in harness.gold


def test_reconcile_leaves_ungrouped_rows_alone(tmp_path):
    """A row with no `event_id` is a group of one: nothing can supersede it,
    and it must not be swept up by another group's verdict."""
    harness = _GroupHarness(tmp_path, batches=[[
        _group_record("r1", "ev-1", "Recommended figure", "2026-09-01T00:00:00Z"),
        _group_record("t1", "ev-1", "Triangulation", "2026-09-01T01:00:00Z"),
        _group_record("solo", None, "Triangulation", "2026-09-01T02:00:00Z"),
    ]])

    harness.poll()

    assert sorted(harness.created_ids) == ["fakesrc:r1", "fakesrc:solo"]
    assert harness.gold["solo"]["groupKey"] is None
    assert harness.gold["solo"]["retracted"] is False


class DupSource(FakeSource):
    """Like FakeSource but `poll` keeps returning the same 2 records every
    call, unconditionally — simulates a feed with no dedup (G2), so a
    second full run re-processes already-pushed externalIds through gold."""

    def poll(self, since):
        return self._records


def test_gx_pipeline_rerun_does_not_repush_already_pushed_signal(tmp_path):
    """G1 regression: `_match` always emits `pushedAt: None` for a record
    it processes, so a signal that re-enters gold on a later run (e.g. a
    source without dedup re-delivering it) must keep the `pushedAt` its
    first run set — not have it reset to NULL by the Type-1 upsert and
    get pushed to clear-api again."""
    fake_s3 = FakeS3()
    iceberg_warehouse = f"file://{tmp_path / 'warehouse'}"
    iceberg_catalog_uri = f"sqlite:///{tmp_path / 'catalog.db'}"
    created_signals = []

    def fake_create_signal(input_data):
        row = {**input_data, "id": f"sig-{len(created_signals)}", "generalLocation": {"id": "loc-1", "level": 2, "ancestorIds": []}}
        created_signals.append(row)
        return row

    defs_list = build_gx_source_assets(DupSource())
    assets = [d for d in defs_list if isinstance(d, dg.AssetsDefinition)]
    checks = [d for d in defs_list if isinstance(d, dg.AssetChecksDefinition)]

    def _run():
        with (
            patch("clear_pipeline.defs.gx_pipeline.factory.lake.s3_client", return_value=fake_s3),
            patch("clear_pipeline.defs.gx_pipeline.factory.settings.s3_bucket", "test-bucket"),
            patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_warehouse", iceberg_warehouse),
            patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_catalog_uri", iceberg_catalog_uri),
            patch("clear_pipeline.defs.gx_pipeline.factory.create_signal_for_sync", side_effect=fake_create_signal),
            patch("clear_pipeline.defs.gx_pipeline.factory.classify_locally") as mock_classify,
        ):
            mock_classify.return_value.relevance = 0.9
            mock_classify.return_value.type_level_2 = "conflict"
            mock_classify.return_value.disaster_types = ["cv"]
            return dg.materialize(assets + checks)

    first = _run()
    assert first.success
    assert len(created_signals) == 2

    second = _run()
    assert second.success
    assert len(created_signals) == 2, "second full run must not re-push already-pushed signals"


def test_iceberg_events_stubbed():
    """Gold events (SCD2) persistence is paused pending the clear-api sync
    decision — see iceberg_events.py's module docstring. Every function is
    a no-op; nothing is ever created or read back."""
    from clear_pipeline.defs.gx_pipeline import iceberg_events

    table = iceberg_events.get_events_table("scdtest")
    assert table is None
    assert iceberg_events.current_event(table, "e1") is None
    assert iceberg_events.current_events_df(table).empty

    event = {"eventId": "e1", "signalIds": ["r1"], "severity": 3}
    merged = iceberg_events.merge_event(table, event)
    assert merged["version"] == 1 and merged["isCurrent"]
    assert merged["signalIds"] == ["r1"]  # passthrough, not persisted anywhere


def test_gold_table_gains_new_columns_on_load(tmp_path):
    """A gold table lacking the new columns is migrated additively on load, and
    its pre-migration rows (`retracted` NULL) count as live, not stranded."""
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType

    from clear_pipeline.defs.gx_pipeline import iceberg_catalog, iceberg_signals

    pre_migration_schema = Schema(
        NestedField(1, "externalId", StringType(), required=True),
        NestedField(2, "eventId", StringType(), required=False),
        NestedField(3, "relevanceScore", DoubleType(), required=False),
        NestedField(4, "eventType", StringType(), required=False),
        NestedField(5, "districtKey", StringType(), required=False),
        NestedField(6, "matchOutcome", StringType(), required=False),
        NestedField(7, "severity", LongType(), required=False),
        NestedField(8, "populationAffectedContribution", LongType(), required=False),
        NestedField(9, "casualtiesContribution", LongType(), required=False),
        NestedField(10, "createdAt", StringType(), required=False),
        NestedField(11, "pushedAt", StringType(), required=False),
        NestedField(12, "signalInputJson", StringType(), required=False),
        identifier_field_ids=[1],
    )

    with (
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_warehouse", f"file://{tmp_path / 'warehouse'}"),
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_catalog_uri", f"sqlite:///{tmp_path / 'catalog.db'}"),
    ):
        cat = iceberg_catalog.catalog()
        iceberg_catalog.ensure_namespace(cat)
        old_table = cat.create_table(
            f"{iceberg_catalog.NAMESPACE}.oldsrc_signals", schema=pre_migration_schema
        )
        old_table.append(pa.Table.from_pylist(
            [{"externalId": "pre-1", "eventId": "e1", "relevanceScore": 0.9,
              "eventType": "conflict", "districtKey": "North Darfur",
              "matchOutcome": "new_event", "severity": 3,
              "populationAffectedContribution": None, "casualtiesContribution": 2,
              "createdAt": "2026-09-01T00:00:00Z", "pushedAt": None,
              "signalInputJson": '{"title": "Clash"}'}],
            schema=pre_migration_schema.as_arrow(),
        ))

        table = iceberg_signals.get_signals_table("oldsrc")
        assert "groupKey" in table.schema().column_names
        assert "retracted" in table.schema().column_names
        assert "pushedState" in table.schema().column_names

        unpushed = iceberg_signals.signals_to_sync(table, can_update=False)["create"]
        assert [row["externalId"] for row in unpushed] == ["pre-1"], \
            "a pre-migration row reads retracted=NULL, which means live, not retracted"

        # The write path works against the migrated schema.
        iceberg_signals.upsert_signals(table, [{**unpushed[0], "retracted": True}])
        assert iceberg_signals.signals_to_sync(table, can_update=False)["create"] == []


def test_iceberg_signals_type1_upsert(tmp_path):
    """Gold signals is Type-1 (§6): a second upsert with the same
    externalId updates in place, no history row spawned — the opposite of
    the events table's SCD2 behavior above."""
    from clear_pipeline.defs.gx_pipeline import iceberg_signals

    with (
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_warehouse", f"file://{tmp_path / 'warehouse'}"),
        patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_catalog_uri", f"sqlite:///{tmp_path / 'catalog.db'}"),
    ):
        table = iceberg_signals.get_signals_table("scdtest")
        row = {
            "externalId": "r1", "eventId": "e1", "relevanceScore": 0.9,
            "eventType": "conflict", "districtKey": "North Darfur",
            "matchOutcome": "new_event", "severity": 3,
            "populationAffectedContribution": None, "casualtiesContribution": 2,
            "createdAt": "2026-09-01T00:00:00Z", "pushedAt": None,
            "signalInput": {"title": "Clash"},
        }

        iceberg_signals.upsert_signals(table, [row])
        assert len(iceberg_signals.signals_to_sync(table, can_update=False)["create"]) == 1

        # Mark pushed — same externalId, updates in place.
        pushed = {**row, "pushedAt": "2026-09-01T01:00:00Z"}
        iceberg_signals.upsert_signals(table, [pushed])

        all_rows = table.scan().to_pandas()
        assert len(all_rows) == 1, "Type-1: update in place, no history row"
        assert iceberg_signals.signals_to_sync(table, can_update=False)["create"] == []
