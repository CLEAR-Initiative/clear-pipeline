"""Smoke test for the generic bronze -> silver -> gold GX-gated factory
(defs/gx_pipeline/). Exercises the real Dagster wiring end to end — a fake
S3 + fake clear-api calls, one source registered via a throwaway
``GXSource``, so this catches asset-input wiring bugs (a mismatched
``ins=``/parameter name silently drops data — the exact class of bug this
guards against) that a pure-function unit test would miss.
"""

from unittest.mock import patch

import dagster as dg

from clear_pipeline.defs.gx_pipeline.factory import build_gx_source_assets
from clear_pipeline.defs.gx_pipeline.sources import GX_SOURCES, GXSource


def test_registered_sources_conform_to_protocol():
    sources = {s.source: s for s in GX_SOURCES}
    assert sources.keys() == {"dataminr", "acled", "darfur24"}
    assert all(isinstance(s, GXSource) for s in sources.values())
    # IDMC deliberately isn't registered — see sources.py's comment above
    # GX_SOURCES: this factory has no equivalent of production's
    # drained=False, and IDMC's event-grouping semantics aren't designed yet.
    assert "idmc" not in sources


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
        patch("clear_pipeline.defs.gx_pipeline.factory.create_signal", side_effect=fake_create_signal),
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
        patch("clear_pipeline.defs.gx_pipeline.factory.create_signal", side_effect=fake_create_signal),
    ):
        push_asset = next(a for a in assets if "fakesrc_push" in [k.to_user_string() for k in a.keys])
        second_result = dg.materialize([push_asset], selection=[push_asset])
        assert second_result.success
    assert len(created_signals) == 2, "second push run must not re-push already-pushed rows"


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
        assert len(iceberg_signals.unpushed_signals(table)) == 1

        # Mark pushed — same externalId, updates in place.
        pushed = {**row, "pushedAt": "2026-09-01T01:00:00Z"}
        iceberg_signals.upsert_signals(table, [pushed])

        all_rows = table.scan().to_pandas()
        assert len(all_rows) == 1, "Type-1: update in place, no history row"
        assert iceberg_signals.unpushed_signals(table) == []
