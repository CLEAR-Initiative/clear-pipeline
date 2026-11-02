"""Country partitioning: S3 layout parity, the pipelineCountries iso3 list,
the add-only partition-sync sensor, and the fan-out weekly schedule."""

from datetime import datetime, timezone
from unittest.mock import patch

import dagster as dg
from dagster import DagsterInstance

from clear_pipeline.defs import reliefweb_partitions as rp
from clear_pipeline.defs.reliefweb_to_s3 import (
    kb_fetch_sensor,
    reliefweb_country_partition_sensor,
    reliefweb_fetch_schedule,
)

_PART = rp.country_partitions.name


# ── S3 layout parity: prefixes for `sdn` must equal the pre-partition strings ──

def test_s3_prefixes_match_legacy_sdn_paths():
    assert rp.reports_prefix("sdn") == "reliefweb/reports/sdn/situation-report/"
    assert rp.pdf_prefix("sdn") == "reliefweb/pdfs/sdn/situation-report/"
    assert rp.pdf_key("sdn", "r1", "a.pdf") == "reliefweb/pdfs/sdn/situation-report/r1/a.pdf"
    assert rp.text_prefix("sdn") == "reliefweb/kb/text/sdn/situation-report"
    assert rp.chunks_prefix("sdn") == "reliefweb/kb/chunks/sdn/situation-report"
    assert rp.enriched_prefix("sdn") == "reliefweb/kb/enriched/sdn/situation-report"
    assert rp.datapoints_prefix("sdn") == "reliefweb/kb/datapoints/sdn/situation-report"


def test_prefixes_are_per_country():
    assert rp.reports_prefix("eth") == "reliefweb/reports/eth/situation-report/"
    assert rp.text_prefix("eth") == "reliefweb/kb/text/eth/situation-report"


# ── list_pipeline_iso3s: lowercased iso3s from pipelineCountries ──────────────

def test_list_pipeline_iso3s_lowercases_and_skips_blank():
    with patch(
        "clear_pipeline.defs.reliefweb_partitions.clear_api.get_pipeline_countries",
        return_value=[{"iso3": "SDN"}, {"iso3": "Eth"}, {"iso3": None}, {}],
    ):
        assert rp.list_pipeline_iso3s() == ["sdn", "eth"]


# ── Sync sensor: add-only reconciliation against pipelineCountries ────────────

def test_sensor_registers_only_new_partitions():
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PART, ["sdn"])  # sdn already established
    ctx = dg.build_sensor_context(instance=instance)
    with patch(
        "clear_pipeline.defs.reliefweb_to_s3.list_pipeline_iso3s",
        return_value=["sdn", "eth"],
    ):
        result = reliefweb_country_partition_sensor(ctx)
    assert isinstance(result, dg.SensorResult)
    (req,) = result.dynamic_partitions_requests
    assert req.partition_keys == ["eth"]  # only the new one; sdn untouched


def test_sensor_skips_when_in_sync():
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PART, ["sdn"])
    ctx = dg.build_sensor_context(instance=instance)
    with patch(
        "clear_pipeline.defs.reliefweb_to_s3.list_pipeline_iso3s",
        return_value=["sdn"],
    ):
        result = reliefweb_country_partition_sensor(ctx)
    assert isinstance(result, dg.SkipReason)


def test_sensor_is_add_only_never_removes_delisted():
    # `sdn` is registered but no longer in pipelineCountries — it must be KEPT
    # (history-preserving), not dropped, and no add request is emitted.
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(_PART, ["sdn"])
    ctx = dg.build_sensor_context(instance=instance)
    with patch(
        "clear_pipeline.defs.reliefweb_to_s3.list_pipeline_iso3s",
        return_value=["eth"],
    ):
        result = reliefweb_country_partition_sensor(ctx)
    # eth is new → added; sdn is stale → kept (still in the instance).
    assert isinstance(result, dg.SensorResult)
    (req,) = result.dynamic_partitions_requests
    assert req.partition_keys == ["eth"]
    assert "sdn" in instance.get_dynamic_partitions(_PART)


# ── Per-source fetch schedule: one RunRequest per live partition ─────────────

_TICK = datetime(2026, 8, 10, 6, 0, tzinfo=timezone.utc)


def test_schedule_fans_out_one_run_per_partition():
    with DagsterInstance.local_temp() as instance:
        instance.add_dynamic_partitions(_PART, ["sdn", "eth"])
        ctx = dg.build_schedule_context(instance=instance, scheduled_execution_time=_TICK)
        run_requests = list(reliefweb_fetch_schedule(ctx))
    assert sorted(r.partition_key for r in run_requests) == ["eth", "sdn"]
    # run_key is per-country-per-week so a re-tick in the same week is deduped.
    assert all(r.run_key and r.partition_key in r.run_key for r in run_requests)


def test_schedule_skips_when_no_partitions_registered():
    with DagsterInstance.local_temp() as instance:
        ctx = dg.build_schedule_context(instance=instance, scheduled_execution_time=_TICK)
        result = reliefweb_fetch_schedule(ctx)
    assert isinstance(result, dg.SkipReason)


# ── kb_fetch_sensor: trigger the downstream on new fetch materializations ────


def _materialize_fetch(instance, partition):
    """Record a runless reliefweb_fetch materialization for a country partition,
    the way the fetch asset's run would."""
    instance.report_runless_asset_event(
        dg.AssetMaterialization(asset_key="reliefweb_fetch", partition=partition),
    )


def test_kb_fetch_sensor_primes_then_triggers_on_new_data():
    instance = DagsterInstance.ephemeral()
    _materialize_fetch(instance, "sdn")  # pre-existing before the sensor is enabled

    # First tick primes to the current high-water mark and triggers NOTHING —
    # so enabling the sensor never backfills history.
    ctx = dg.build_sensor_context(instance=instance)
    primed = kb_fetch_sensor(ctx)
    assert isinstance(primed, dg.SkipReason)
    assert ctx.cursor is not None

    # A fresh fetch after priming triggers the downstream for just that country.
    _materialize_fetch(instance, "eth")
    ctx2 = dg.build_sensor_context(instance=instance, cursor=ctx.cursor)
    requests = kb_fetch_sensor(ctx2)
    assert isinstance(requests, list)
    assert [r.partition_key for r in requests] == ["eth"]
    assert requests[0].run_key and "eth" in requests[0].run_key


def test_kb_fetch_sensor_skips_when_no_new_materializations():
    instance = DagsterInstance.ephemeral()
    _materialize_fetch(instance, "sdn")
    ctx = dg.build_sensor_context(instance=instance)
    kb_fetch_sensor(ctx)  # prime
    # No new materialization since priming.
    ctx2 = dg.build_sensor_context(instance=instance, cursor=ctx.cursor)
    result = kb_fetch_sensor(ctx2)
    assert isinstance(result, dg.SkipReason)


def test_kb_fetch_sensor_dedups_reprocessing_via_run_key():
    # The same materialization never yields two different run_keys: run_key is
    # keyed on the event storage_id, so Dagster dedups a re-tick.
    instance = DagsterInstance.ephemeral()
    ctx = dg.build_sensor_context(instance=instance)
    kb_fetch_sensor(ctx)  # prime (no events yet)
    _materialize_fetch(instance, "sdn")
    ctx2 = dg.build_sensor_context(instance=instance, cursor=ctx.cursor)
    first = kb_fetch_sensor(ctx2)
    assert [r.partition_key for r in first] == ["sdn"]
    # Re-tick with the advanced cursor: the already-seen event is not re-emitted.
    ctx3 = dg.build_sensor_context(instance=instance, cursor=ctx2.cursor)
    second = kb_fetch_sensor(ctx3)
    assert isinstance(second, dg.SkipReason)
