"""Smoke test for the generic bronze -> silver -> gold medallion factory
(defs/medallion/). Exercises the real Dagster wiring end to end — a fake
S3 + fake clear-api calls, one source registered via a throwaway
``MedallionSource``, so this catches asset-input wiring bugs (a mismatched
``ins=``/parameter name silently drops data — the exact class of bug this
guards against) that a pure-function unit test would miss.
"""

from unittest.mock import patch

import dagster as dg

from clear_pipeline.defs.medallion.factory import build_medallion_assets
from clear_pipeline.defs.medallion.sources import MEDALLION_SOURCES, MedallionSource


def test_dataminr_source_conforms_to_protocol():
    source = MEDALLION_SOURCES[0]
    assert source.source == "dataminr"
    assert isinstance(source, MedallionSource)


class FakeS3:
    """In-memory S3 stand-in: just enough of the boto3 surface the factory
    calls (put_object/get_object/list via a paginator)."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, **_):
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode("utf-8")

    def get_object(self, Bucket, Key):
        import io
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


def test_medallion_pipeline_end_to_end():
    fake_s3 = FakeS3()
    created_signals = []
    created_events = []
    alerted_events = []

    def fake_create_signal(input_data):
        row = {**input_data, "id": f"sig-{len(created_signals)}", "generalLocation": {"id": "loc-1", "level": 2, "ancestorIds": []}}
        created_signals.append(row)
        return row

    def fake_create_event(input_data):
        row = {**input_data, "id": f"evt-{len(created_events)}"}
        created_events.append(row)
        return row

    def fake_escalate(event, status="published"):
        alerted_events.append(event["id"])
        return {"id": f"alert-{event['id']}", "eventId": event["id"]}

    defs_list = build_medallion_assets(FakeSource())
    assets = [d for d in defs_list if isinstance(d, dg.AssetsDefinition)]
    checks = [d for d in defs_list if isinstance(d, dg.AssetChecksDefinition)]

    with (
        patch("clear_pipeline.defs.medallion.factory.lake.s3_client", return_value=fake_s3),
        patch("clear_pipeline.defs.medallion.factory.settings.s3_bucket", "test-bucket"),
        patch("clear_pipeline.defs.medallion.factory.create_signal", side_effect=fake_create_signal),
        patch("clear_pipeline.defs.medallion.factory.create_event", side_effect=fake_create_event),
        patch("clear_pipeline.defs.medallion.factory.escalate_to_alert", side_effect=fake_escalate),
        patch("clear_pipeline.defs.medallion.factory.resolve_signal_admin2", return_value="admin2-1"),
        patch("clear_pipeline.defs.medallion.factory.is_stale_signal", return_value=False),
        patch("clear_pipeline.defs.medallion.factory.classify_locally") as mock_classify,
    ):
        mock_classify.return_value.relevance = 0.9
        mock_classify.return_value.type_level_2 = "conflict"
        mock_classify.return_value.disaster_types = ["cv"]

        result = dg.materialize(assets + checks)

    assert result.success

    # Both records went through bronze -> silver -> ... -> gold and pushed.
    gold_signal_keys = [k for k in fake_s3.objects if k.startswith("gold/fakesrc/signals/")]
    assert len(gold_signal_keys) == 2

    import json
    pushed = [json.loads(fake_s3.objects[k]) for k in gold_signal_keys]
    assert all(row["pushedAt"] is not None for row in pushed), "every gold row should be stamped pushed"

    # Same district+type within the active window -> one merged event, one
    # createSignal per record, one createEvent (not two), one alert (severity 3 < 4 -> none).
    assert len(created_signals) == 2
    assert len(created_events) == 1
    assert alerted_events == []  # severity 3 stays below ALERT_MIN_SEVERITY (4)

    # A second run against the SAME (now-pushed) rows should push nothing new.
    with (
        patch("clear_pipeline.defs.medallion.factory.lake.s3_client", return_value=fake_s3),
        patch("clear_pipeline.defs.medallion.factory.settings.s3_bucket", "test-bucket"),
        patch("clear_pipeline.defs.medallion.factory.create_signal", side_effect=fake_create_signal),
    ):
        push_asset = next(a for a in assets if "fakesrc_push" in [k.to_user_string() for k in a.keys])
        second_result = dg.materialize([push_asset], selection=[push_asset])
        assert second_result.success
    assert len(created_signals) == 2, "second push run must not re-push already-pushed rows"
