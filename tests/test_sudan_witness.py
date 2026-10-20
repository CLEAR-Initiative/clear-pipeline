"""Unit tests for the Sudan Witness provider (fetch, parse, signal mapping),
its gx adapter and its drain connector. Plain dict fixtures shaped like real
layer rows; ArcGIS is mocked at the module's `_get` boundary — no network/DB.
"""

import json
from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch

import httpx
import pytest

from clear_pipeline.defs.gx_pipeline.sources import SudanWitnessGXSource
from clear_pipeline.defs.signals.connectors import SudanWitnessConnector
from clear_pipeline.providers import sudan_witness
from clear_pipeline.providers.sudan_witness import (
    build_signal_content_update,
    build_sudan_witness_signal_input,
    fetch_sudan_witness_records,
    parse_feature,
)

LAYER = sudan_witness.settings.sudan_witness_layer_url
EDIT_DATE = 1791790020939


def _feature(**overrides) -> dict:
    attributes = {
        "Entry_Number": "SUW11503",
        "OBJECTID": 11503,
        "OBJECTID1": 42,
        "A_CIR_Status": "Reviewed - Verified",
        "B_Data_Type": "CIR VERIFIED",
        "Link": "https://t.me/azwyokgsbzzz/57209",
        "C_Brief_Description": "SAF inside Kurmuk",
        "D_Verified_Date": 1783555200000,  # 2026-07-09T00:00:00Z
        "D_Chronolocation_status": "Post date,Most likely",
        "D_Location_status": "Geolocated",
        "D_Country": "Sudan",
        "D_States": "Blue Nile (ولاية النيل الأزرق Wilāyat an-Nīl al-Azraq)",
        "D_City": "Al Kurmuk",
        "D_Latitude": 10.5525,
        "D_Longitude": 34.2861,
        "Map_category": "Conflict activity",
        "E_Activities_of_military_police_and_armed_groups": "Locations & Movements",
        "E_Visible_Verified_Perpetrators": "SAF",
        "E_Alleged_claimed_Perpetrators": None,
        "E_Harm_to_Persons": None,
        "E_Reported_CivCas": "false",
    }
    attributes.update(overrides)
    return {"attributes": attributes, "geometry": {"x": attributes["D_Longitude"], "y": attributes["D_Latitude"]}}


def _record(**overrides) -> dict:
    record = parse_feature(_feature(**overrides))
    assert record is not None
    return record


# ──────────────────────────────────────────────────────────────────────────────
# parse_feature / content hash
# ──────────────────────────────────────────────────────────────────────────────
def test_parse_feature_normalises_the_entry_number_and_date():
    record = _record()
    assert record["suw_id"] == "suw:11503"
    assert record["published_at"] == "2026-07-09T00:00:00+00:00"
    assert record["raw"] == _feature()


def test_parse_feature_drops_zero_padding_so_both_spellings_share_a_key():
    assert _record(Entry_Number="SUW0050")["suw_id"] == "suw:50"
    assert _record(Entry_Number=" SUW50 ")["suw_id"] == "suw:50"


def test_parse_feature_skips_rows_that_cannot_become_signals():
    assert parse_feature(_feature(D_Verified_Date=None)) is None
    assert parse_feature(_feature(Entry_Number="X-12")) is None
    assert parse_feature(_feature(Entry_Number=None)) is None


def test_content_hash_ignores_the_id_arcgis_regenerates_on_republish():
    assert _record(OBJECTID1=1)["content_hash"] == _record(OBJECTID1=999)["content_hash"]


def test_content_hash_changes_when_cir_revises_the_row():
    assert _record()["content_hash"] != _record(E_Harm_to_Persons="Death")["content_hash"]


# ──────────────────────────────────────────────────────────────────────────────
# Signal mapping
# ──────────────────────────────────────────────────────────────────────────────
def test_signal_input_core_fields():
    record = _record()
    out = build_sudan_witness_signal_input(record, "source-1")
    assert out["sourceId"] == "source-1"
    assert out["externalId"] == "suw:11503"
    assert out["publishedAt"] == "2026-07-09T00:00:00+00:00"
    assert out["title"] == "SAF inside Kurmuk"
    assert out["url"] == "https://t.me/azwyokgsbzzz/57209"
    assert out["contentHash"] == record["content_hash"]
    assert (out["lat"], out["lng"]) == (10.5525, 34.2861)
    assert out["rawData"] == record["raw"]["attributes"]
    # No count field exists, and the geolocation URL is not media of the event.
    assert "casualties" not in out
    assert "media" not in out


def test_title_strips_the_video_segment_prefix_but_description_keeps_it():
    brief = "[0:12 - 0:20] RSF commanders congratulate troops"
    out = build_sudan_witness_signal_input(_record(C_Brief_Description=brief), "s")
    assert out["title"] == "RSF commanders congratulate troops"
    assert out["description"].startswith(brief)
    for prefixed in ("(0:55 - 1:11) Drone strike", "Video (@1:03) Drone strike", "[Original] Drone strike"):
        assert build_sudan_witness_signal_input(_record(C_Brief_Description=prefixed), "s")["title"] == "Drone strike"


def test_description_labels_visible_and_alleged_perpetrators_separately():
    out = build_sudan_witness_signal_input(
        _record(E_Visible_Verified_Perpetrators="RSF", E_Alleged_claimed_Perpetrators="SAF,Unknown"), "s"
    )
    lines = out["description"].splitlines()
    assert "Visible perpetrator (verified): RSF" in lines
    assert "Alleged perpetrator (claimed): SAF, Unknown" in lines
    assert "Place: Al Kurmuk, Blue Nile, Sudan" in lines
    assert "Date basis: Post date, Most likely" in lines


def test_description_says_when_the_date_basis_is_missing():
    out = build_sudan_witness_signal_input(_record(D_Chronolocation_status=None), "s")
    assert "Date basis: not stated" in out["description"].splitlines()


def test_severity_comes_from_recorded_harm_only():
    sev = lambda **o: build_sudan_witness_signal_input(_record(**o), "s")["severity"]  # noqa: E731
    assert sev(E_Harm_to_Persons="Displacement,Death") == 4
    assert sev(E_Harm_to_Persons="Mass graves & Burial sites") == 4
    assert sev(E_Harm_to_Persons="Arrest Detention & Abduction") == 3
    assert sev(E_Reported_CivCas="true") == 3
    # Unknown stays null (ADR-0010) — Map_category alone never sets it.
    assert sev(Map_category="Harm to persons") is None


def test_url_drops_links_analysts_cannot_open_and_repairs_malformed_ones():
    url = lambda link: build_sudan_witness_signal_input(_record(Link=link), "s")["url"]  # noqa: E731
    assert url("https://sentinelshare.page.link/xE9a") is None
    assert url("https://drive.google.com/drive/folders/195BeArT6") is None
    assert url("https://scontent-lhr8-1.xx.fbcdn.net/o1/v/t2/f2/m69/AQ") is None
    assert url(None) is None
    assert url("link.dataspace.copernicus.eu/r94w\r") == "https://link.dataspace.copernicus.eu/r94w"
    assert url("\nhttps://x.com/a/status/1") == "https://x.com/a/status/1"


def test_coordinates_are_omitted_when_missing():
    out = build_sudan_witness_signal_input(_record(D_Latitude=None), "s")
    assert "lat" not in out and "lng" not in out


def test_content_update_reuses_the_create_payload():
    signal_input = build_sudan_witness_signal_input(_record(), "s")
    update = build_signal_content_update(signal_input, retracted=False)
    assert update["externalId"] == "suw:11503"
    assert update["contentHash"] == signal_input["contentHash"]
    assert update["retracted"] is False


# ──────────────────────────────────────────────────────────────────────────────
# Fetch
# ──────────────────────────────────────────────────────────────────────────────
class FakeLayer:
    """Answers the three request kinds the fetch makes: layer metadata, count,
    and paged feature queries. Ignores `where`; records every query."""

    def __init__(self, features, page_size=2, edit_dates=None, floored_count=None):
        self.features = features
        self.page_size = page_size
        self.edit_dates = list(edit_dates or [])
        self.floored_count = floored_count
        self.queries: list[dict] = []

    def __call__(self, url, **params):
        self.queries.append(params)
        if url == LAYER:
            edit = self.edit_dates.pop(0) if self.edit_dates else EDIT_DATE
            return {"editingInfo": {"dataLastEditDate": edit}}
        if params.get("returnCountOnly"):
            if self.floored_count is not None and params.get("where", "1=1") != "1=1":
                return {"count": self.floored_count}
            return {"count": len(self.features)}
        offset = int(params["resultOffset"])
        page = self.features[offset:offset + self.page_size]
        return {"features": page, "exceededTransferLimit": offset + self.page_size < len(self.features)}


def test_fetch_pages_through_the_whole_layer():
    features = [_feature(Entry_Number=f"SUW{i}") for i in range(1, 6)]
    with patch.object(sudan_witness, "_get", FakeLayer(features)):
        records = fetch_sudan_witness_records()
    assert [r["suw_id"] for r in records] == [f"suw:{i}" for i in range(1, 6)]


def test_fetch_keeps_sudan_and_countryless_rows_only():
    features = [
        _feature(Entry_Number="SUW1"),
        _feature(Entry_Number="SUW2", D_Country=None),
        _feature(Entry_Number="SUW3", D_Country="Chad"),
        _feature(Entry_Number="SUW4", D_Verified_Date=None),
    ]
    with patch.object(sudan_witness, "_get", FakeLayer(features, page_size=10)):
        records = fetch_sudan_witness_records()
    assert [r["suw_id"] for r in records] == ["suw:1", "suw:2"]


def test_fetch_keeps_one_row_per_normalised_id():
    features = [_feature(Entry_Number="SUW0050"), _feature(Entry_Number="SUW50")]
    with patch.object(sudan_witness, "_get", FakeLayer(features, page_size=10)):
        assert len(fetch_sudan_witness_records()) == 1


def test_fetch_raises_when_the_layer_is_republished_mid_pull():
    layer = FakeLayer([_feature()], edit_dates=[EDIT_DATE, EDIT_DATE + 1])
    with patch.object(sudan_witness, "_get", layer), pytest.raises(RuntimeError, match="republished"):
        fetch_sudan_witness_records()


def test_fetch_raises_when_the_pull_misses_rows_the_floored_count_promised():
    # Whole-layer count matches the pull; only the floored count catches the gap.
    layer = FakeLayer([_feature()], floored_count=2)
    with patch.object(sudan_witness, "_get", layer), pytest.raises(RuntimeError, match="floored count says 2"):
        fetch_sudan_witness_records()


def test_fetch_raises_on_http_failure():
    with patch.object(sudan_witness, "_get", side_effect=httpx.ConnectError("down")), \
         pytest.raises(httpx.ConnectError):
        fetch_sudan_witness_records()


def test_fetch_raises_on_arcgis_errors_reported_with_http_200():
    response = httpx.Response(200, json={"error": {"code": 400, "message": "Invalid query"}},
                              request=httpx.Request("GET", LAYER))
    with patch.object(sudan_witness.httpx, "get", return_value=response), pytest.raises(RuntimeError):
        fetch_sudan_witness_records()


def test_fetch_raises_on_malformed_metadata():
    with patch.object(sudan_witness, "_get", return_value={"name": "SUW_Incidents"}), pytest.raises(KeyError):
        fetch_sudan_witness_records()


def test_fetch_raises_on_an_empty_page_that_claims_more_results():
    def layer(url, **params):
        if url == LAYER:
            return {"editingInfo": {"dataLastEditDate": EDIT_DATE}}
        if params.get("returnCountOnly"):
            return {"count": 5}
        return {"features": [], "exceededTransferLimit": True}

    with patch.object(sudan_witness, "_get", layer), pytest.raises(RuntimeError, match="exceededTransferLimit"):
        fetch_sudan_witness_records()


@pytest.fixture
def floor_jul_1():
    """Non-default floor: proves the setting drives both filters. Fixture rows are 2026-07-09."""
    with patch.object(sudan_witness.settings, "sudan_witness_min_incident_date", date(2026, 7, 1)):
        yield


def test_fetch_drops_rows_dated_before_the_floor_even_if_the_server_ignores_it(floor_jul_1):
    jun_30 = int(datetime(2026, 6, 30, tzinfo=UTC).timestamp() * 1000)
    features = [_feature(Entry_Number="SUW1"), _feature(Entry_Number="SUW2", D_Verified_Date=jun_30)]
    with patch.object(sudan_witness, "_get", FakeLayer(features, page_size=10)):
        assert [r["suw_id"] for r in fetch_sudan_witness_records()] == ["suw:1"]


def test_the_pull_and_its_consistency_count_carry_the_floor(floor_jul_1):
    layer = FakeLayer([_feature()])
    with patch.object(sudan_witness, "_get", layer):
        fetch_sudan_witness_records()
    floor = "D_Verified_Date >= DATE '2026-07-01'"
    pages = [q for q in layer.queries if "resultOffset" in q]
    assert pages and all(q["where"] == floor for q in pages)
    # Fingerprint count stays whole-layer; the consistency count matches the pull.
    assert {q["where"] for q in layer.queries if q.get("returnCountOnly")} == {floor, "1=1"}


def test_every_poll_logs_the_layer_edit_date(caplog):
    with caplog.at_level("INFO", logger=sudan_witness.__name__), \
         patch.object(sudan_witness, "_get", FakeLayer([_feature()])):
        fetch_sudan_witness_records()
    assert "2026-10-12" in caplog.text


class _KV:
    """The two redis calls the provider makes."""

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value


@pytest.fixture
def fake_redis():
    return _KV()


def _synced_layer(fake_redis, layer, *, age=timedelta(hours=1)):
    """A clean poll `age` ago of `layer`; returns the `since` gx would pass next."""
    since = datetime.now(UTC) - age
    with patch.object(sudan_witness, "_redis", fake_redis), patch.object(sudan_witness, "_get", layer):
        fetch_sudan_witness_records()
        sudan_witness.set_last_synced(since)
    return since


def test_fetch_skips_the_pull_when_the_layer_is_unchanged(fake_redis, caplog):
    layer = FakeLayer([_feature()])
    since = _synced_layer(fake_redis, layer)
    with caplog.at_level("INFO", logger=sudan_witness.__name__), \
         patch.object(sudan_witness, "_redis", fake_redis), \
         patch.object(sudan_witness, "_get", layer), \
         patch.object(sudan_witness, "_pull_features") as pull:
        assert fetch_sudan_witness_records(since=since) == []
    pull.assert_not_called()
    (unchanged,) = [r for r in caplog.records if "unchanged" in r.getMessage()]
    assert unchanged.levelname == "INFO"
    assert "2026-10-12" in unchanged.getMessage()


_GROWN = [_feature(), _feature(Entry_Number="SUW7")]


def _down(url, **params):
    raise httpx.ConnectError("down")


@pytest.mark.parametrize("layer, error", [
    pytest.param(_down, httpx.ConnectError, id="http-failure"),
    # Edit-date reads: fingerprint check, before the pull, after it.
    pytest.param(FakeLayer(_GROWN, edit_dates=[EDIT_DATE + 1, EDIT_DATE + 1, EDIT_DATE + 2]),
                 RuntimeError, id="republished-mid-pull"),
    pytest.param(FakeLayer(_GROWN, floored_count=3), RuntimeError, id="short-pull"),
])
def test_a_failed_poll_leaves_the_watermark_and_fingerprint_untouched(fake_redis, layer, error):
    since = _synced_layer(fake_redis, FakeLayer([_feature()]))
    stored = dict(fake_redis.store)
    with patch.object(sudan_witness, "_redis", fake_redis), \
         patch.object(sudan_witness, "_get", layer), \
         pytest.raises(error):
        fetch_sudan_witness_records(since=since)
    assert fake_redis.store == stored
    with patch.object(sudan_witness, "_redis", fake_redis):
        assert sudan_witness.get_last_synced() == since


def test_a_failed_fingerprint_check_falls_back_to_a_full_pull(fake_redis):
    since = _synced_layer(fake_redis, FakeLayer([_feature()]))
    layer = FakeLayer([_feature()])
    flaky = iter([httpx.ConnectError("blip")])

    def get(url, **params):
        if url == LAYER and (err := next(flaky, None)):
            raise err
        return layer(url, **params)

    with patch.object(sudan_witness, "_redis", fake_redis), patch.object(sudan_witness, "_get", get):
        assert len(fetch_sudan_witness_records(since=since)) == 1


def test_fetch_pulls_when_the_edit_date_or_row_count_moved(fake_redis):
    since = _synced_layer(fake_redis, FakeLayer([_feature()]))
    with patch.object(sudan_witness, "_redis", fake_redis), \
         patch.object(sudan_witness, "_get", FakeLayer([_feature()], edit_dates=[EDIT_DATE + 1] * 3)):
        assert len(fetch_sudan_witness_records(since=since)) == 1
    two_rows = FakeLayer([_feature(), _feature(Entry_Number="SUW7")])
    with patch.object(sudan_witness, "_redis", fake_redis), patch.object(sudan_witness, "_get", two_rows):
        assert len(fetch_sudan_witness_records(since=since)) == 2


def test_fetch_pulls_when_the_floor_moved_on_an_unchanged_layer(fake_redis):
    layer = FakeLayer([_feature()])
    with patch.object(sudan_witness.settings, "sudan_witness_min_incident_date", date(2026, 8, 1)):
        since = _synced_layer(fake_redis, layer)
    # Floor lowered back to the default: the unchanged layer must be pulled again.
    with patch.object(sudan_witness, "_redis", fake_redis), patch.object(sudan_witness, "_get", layer):
        assert len(fetch_sudan_witness_records(since=since)) == 1


def test_fetch_forces_a_full_pull_once_the_last_one_is_too_old(fake_redis):
    layer = FakeLayer([_feature()])
    max_age = timedelta(hours=sudan_witness.settings.sudan_witness_full_pull_max_age_hours)
    since = _synced_layer(fake_redis, layer, age=max_age)
    with patch.object(sudan_witness, "_redis", fake_redis), patch.object(sudan_witness, "_get", layer):
        assert len(fetch_sudan_witness_records(since=since)) == 1


def test_fingerprint_is_only_committed_by_a_clean_bronze_batch(fake_redis):
    layer = FakeLayer([_feature()])
    with patch.object(sudan_witness, "_redis", fake_redis), patch.object(sudan_witness, "_get", layer):
        fetch_sudan_witness_records()  # bronze fails after this: no set_last_synced
        since = datetime.now(UTC) - timedelta(hours=1)
        assert len(fetch_sudan_witness_records(since=since)) == 1


# ──────────────────────────────────────────────────────────────────────────────
# gx adapter + drain connector
# ──────────────────────────────────────────────────────────────────────────────
def test_gx_source_round_trips_records_through_the_lake():
    source = SudanWitnessGXSource()
    record = _record()
    assert source.source == "sudan_witness"
    assert source.external_id(record) == "suw:11503"
    assert source.published_at(record) == "2026-07-09T00:00:00+00:00"
    assert source.parse(source.raw_bytes(record)) == record
    assert source.content_hash(record) == record["content_hash"]


def test_gx_source_silver_input_is_the_provider_mapping():
    record = _record()
    assert SudanWitnessGXSource().to_silver_input(record, "s") == build_sudan_witness_signal_input(record, "s")


def test_gx_source_update_hook_carries_the_retraction_flag():
    signal_input = build_sudan_witness_signal_input(_record(), "s")
    assert SudanWitnessGXSource().content_update_input(signal_input, retracted=True)["retracted"] is True


def test_connector_is_drained():
    connector = SudanWitnessConnector()
    assert (connector.polled, connector.drained) == (False, True)


def test_connector_projects_the_gx_blob():
    record = _record()
    raw = json.dumps(record).encode("utf-8")
    created = {"id": "sig-1", "title": "SAF inside Kurmuk", "publishedAt": record["published_at"],
               "description": "desc", "url": "https://t.me/azwyokgsbzzz/57209"}
    view = SudanWitnessConnector().project(SudanWitnessConnector().parse(raw), created)
    assert view.external_id == "suw:11503"
    assert view.location_name == "Al Kurmuk"
    assert (view.lat, view.lng) == (10.5525, 34.2861)
