"""INSO through the real GX job (seam 1): fixture emails in at the mailbox,
assertions on gold rows, clear-api creates, quarantine entries, check results
and the cursor. Fake S3, a SQLite Iceberg catalog in a temp dir, the shared
fake Redis. Replaced boundaries: the mailbox fetch, the LLM (extraction and
classify answers) and the geoparser's network lookup; clear-api reads/creates
are faked as in the other GX tests. No LLM call is made.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import dagster as dg
import pytest

from clear_pipeline.defs.gx_pipeline import sources
from clear_pipeline.defs.gx_pipeline.factory import build_gx_source_assets
from clear_pipeline.defs.gx_pipeline.sources import InsoGXSource
from clear_pipeline.providers import inso, newsletter_mailbox
from clear_pipeline.providers.newsletter_mailbox import FolderBatch
from tests.test_gx_pipeline import FakeS3, _gold_rows

GOLDEN = Path(__file__).parent / "golden" / "inso"
SENDER = "reports@security-reports.example"
L0 = [
    {"id": "loc-sd", "name": "Sudan", "level": 0, "pCode": "SD"},
    {"id": "loc-af", "name": "Afghanistan", "level": 0, "pCode": "AF"},
]
# The geoparser resolves these places; anything else stays unresolved.
GAZETTEER = {"El Fasher city market": (13.63, 25.35), "Omdurman": (15.65, 32.48), "Kassala town": (15.45, 36.4)}


def _eml(name: str) -> bytes:
    return (GOLDEN / f"{name}.eml").read_bytes()


def _incidents(name: str) -> dict:
    return json.loads((GOLDEN / f"{name}.incidents.json").read_text())


@dataclass
class FakeFolder:
    """The mailbox folder: honours fetch_after's contract (UIDs above the
    cursor, or everything when UIDVALIDITY differs)."""

    uidvalidity: int = 7
    messages: list | None = None

    def add(self, *names: str) -> None:
        self.messages = self.messages or []
        start = max((uid for uid, _ in self.messages), default=0)
        self.messages += [(start + i, _eml(n)) for i, n in enumerate(names, start=1)]

    def fetch_after(self, folder, uidvalidity, last_uid):
        after = last_uid if uidvalidity == self.uidvalidity else 0
        return FolderBatch(self.uidvalidity, [(u, raw) for u, raw in (self.messages or []) if u > after])


class FakeLLM:
    """Answers by schema: extraction from the `*.incidents.json` matching the
    INFORMATION it's given, classify from `classifications` (by title
    prefix) or a default RELEVANT attack."""

    def __init__(self):
        self.extractions = {inso.parse(_eml(n)).information: _incidents(n) for n in
                            ("01_multi_incident", "02_single_incident", "03_new_year", "04_html_only")}
        self.classifications: dict[str, dict] = {}
        self.calls: list[str] = []

    def complete_structured(self, *, system, user, schema, **_):
        if schema is inso.IncidentList:
            self.calls.append("extract")
            return schema.model_validate(self.extractions[user.removeprefix("INFORMATION:\n")])
        self.calls.append("classify")
        for prefix, answer in self.classifications.items():
            if user.startswith(prefix):
                return schema.model_validate(answer)
        return schema.model_validate({"events": [{"event_type": "attack", "relevance": "RELEVANT", "severity": 3}]})


def _fake_geoparser(input_data, *, title, **_):
    place = title.split(",")[0]
    if place not in GAZETTEER:
        return None
    lat, lng = GAZETTEER[place]
    input_data["geoparsedData"] = {"candidate": place, "display_name": title, "lat": lat, "lng": lng}
    return SimpleNamespace(lat=lat, lng=lng, candidate=place)


class Harness:
    def __init__(self, tmp_path, fake_redis):
        self.warehouse = f"file://{tmp_path / 'warehouse'}"
        self.catalog_uri = f"sqlite:///{tmp_path / 'catalog.db'}"
        self.s3 = FakeS3()
        self.redis = fake_redis
        self.folder = FakeFolder()
        self.llm = FakeLLM()
        self.created: list[dict] = []
        self.fail_s3_writes = False
        self.fail_raw_for: bytes | None = None  # fail only this email's bronze write
        self.source = InsoGXSource()
        defs_list = build_gx_source_assets(self.source)
        self.assets = [d for d in defs_list if isinstance(d, dg.AssetsDefinition)]
        self.checks = [d for d in defs_list if isinstance(d, dg.AssetChecksDefinition)]

    def _create(self, data):
        self.created.append(data)
        return {"id": f"sig-{len(self.created)}", **data}

    def _put(self, Bucket, Key, Body, **kw):
        if self.fail_s3_writes and Key.startswith("raw/"):
            raise OSError("S3 down")
        if self.fail_raw_for is not None and Body == self.fail_raw_for:
            raise OSError("S3 down")
        FakeS3.put_object(self.s3, Bucket, Key, Body, **kw)

    def run(self, *, require_dkim=True) -> dg.ExecuteInProcessResult:
        self.created.clear()
        with (
            patch.object(self.s3, "put_object", side_effect=self._put),
            patch("clear_pipeline.defs.gx_pipeline.factory.lake.s3_client", return_value=self.s3),
            patch("clear_pipeline.defs.gx_pipeline.factory.settings.s3_bucket", "test-bucket"),
            patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_warehouse", self.warehouse),
            patch("clear_pipeline.defs.gx_pipeline.iceberg_catalog.settings.iceberg_catalog_uri", self.catalog_uri),
            patch("clear_pipeline.defs.gx_pipeline.factory.create_signal_for_sync", side_effect=self._create),
            patch.object(sources, "get_source_id_by_name", return_value="src-inso"),
            patch.object(sources, "get_locations_by_level", return_value=L0),
            patch.object(sources.settings, "inso_allowed_senders", f"{SENDER}, other@security-reports.example"),
            patch.object(sources.settings, "inso_require_dkim", require_dkim),
            patch.object(newsletter_mailbox, "fetch_after", side_effect=self.folder.fetch_after),
            patch.object(newsletter_mailbox, "_redis", self.redis),
            patch.object(inso, "make_llm_provider", return_value=self.llm),
            patch.object(inso, "enrich_with_geoparser", side_effect=_fake_geoparser),
        ):
            return dg.materialize(self.assets + self.checks, raise_on_error=False)

    @property
    def gold(self) -> dict:
        rows = _gold_rows(self.warehouse, self.catalog_uri, source="inso")
        for row in rows.values():
            row["signalInput"] = json.loads(row.pop("signalInputJson"))
        return rows

    @property
    def cursor(self):
        return self.redis.get("inso:last_synced")

    def quarantine(self) -> dict[str, dict]:
        """Quarantine sidecars by key."""
        return {k: json.loads(v) for k, v in self.s3.objects.items()
                if k.startswith("quarantine/inso/") and k.endswith(".meta.json")}

    def check(self, result, name) -> dg.AssetCheckEvaluation:
        (evaluation,) = [e for e in result.get_asset_check_evaluations() if e.check_name == name]
        return evaluation


@pytest.fixture
def h(tmp_path, fake_redis):
    return Harness(tmp_path, fake_redis)


def _signal(h, external_id) -> dict:
    return h.gold[external_id]["signalInput"]


# ── one signal per incident ──────────────────────────────────────────────────


def test_multi_and_single_incident_emails_become_one_signal_per_incident(h):
    h.folder.add("01_multi_incident", "02_single_incident")
    result = h.run()

    assert result.success
    assert set(h.gold) == {
        "inso:SDN/Darfur/IR/20262010/R1:1",
        "inso:SDN/Darfur/IR/20262010/R1:2",
        "inso:SDN/Darfur/IR/20262010/R1:3",
        "inso:SDN/Khartoum/IR/20261410/R1:1",
    }
    assert sorted(c["externalId"] for c in h.created) == sorted(h.gold)
    assert h.check(result, "quarantine_empty").passed


def test_an_incident_signal_carries_its_own_place_date_and_casualties(h):
    h.folder.add("01_multi_incident")
    h.run()

    market = _signal(h, "inso:SDN/Darfur/IR/20262010/R1:1")
    assert market["publishedAt"] == "2026-10-20T09:30:00+00:00"
    assert market["title"] == (
        "On 18 October, in North Darfur, the RSF conducted artillery shelling on a market in El Fasher city, "
        "El Fasher locality."
    )
    assert market["rawData"]["incident"]["date"] == "2026-10-18"
    assert market["rawData"]["incident"]["source_text"].endswith("nine injuries among civilians.")
    assert market["casualties"] == 4
    assert (market["lat"], market["lng"]) == (13.63, 25.35)
    assert "locationId" not in market
    assert "Place: El Fasher city market, El Fasher, North Darfur, Sudan" in market["description"].splitlines()
    # Report-level text stays on the report.
    assert market["rawData"]["report"]["analysis"].startswith("Mid-October confirmed")
    assert market["rawData"]["report"]["serial"] == "SDN/Darfur/IR/20262010/R1"
    # The ANALYSIS-only Kutum strike never becomes a signal.
    assert not any("Kutum" in _signal(h, x)["description"] for x in h.gold)


def test_an_unresolved_place_is_pinned_to_the_report_country(h):
    h.folder.add("01_multi_incident")
    h.run()
    tawila = _signal(h, "inso:SDN/Darfur/IR/20262010/R1:2")
    assert tawila["locationId"] == "loc-sd"
    assert "lat" not in tawila and "pointName" not in tawila


def test_none_reported_is_zero_and_unspecified_is_null(h):
    h.folder.add("01_multi_incident")
    h.run()
    tawila = _signal(h, "inso:SDN/Darfur/IR/20262010/R1:2")
    nyala = _signal(h, "inso:SDN/Darfur/IR/20262010/R1:3")
    assert (tawila["rawData"]["incident"]["killed"], tawila["rawData"]["incident"]["injured"]) == (None, None)
    assert "casualties" not in tawila
    assert {"Killed: not specified", "Injured: not specified"} <= set(tawila["description"].splitlines())
    assert (nyala["rawData"]["incident"]["killed"], nyala["rawData"]["incident"]["injured"]) == (0, 0)
    assert nyala["casualties"] == 0
    assert h.gold["inso:SDN/Darfur/IR/20262010/R1:3"]["casualtiesContribution"] == 0
    assert h.gold["inso:SDN/Darfur/IR/20262010/R1:2"]["casualtiesContribution"] is None


def test_the_year_comes_from_the_email_and_rolls_back_across_new_year(h):
    h.folder.add("03_new_year")
    h.run()
    assert _signal(h, "inso:SDN/Kassala/IR/20270201/R1:1")["rawData"]["incident"]["date"] == "2026-12-30"
    assert _signal(h, "inso:SDN/Kassala/IR/20270201/R1:2")["rawData"]["incident"]["date"] == "2027-01-01"


def test_an_incident_with_no_stated_date_has_a_null_date_not_the_report_date(h):
    information = inso.parse(_eml("02_single_incident")).information
    h.llm.extractions[information]["incidents"][0].update(day=None, month=None)
    h.folder.add("02_single_incident")
    h.run()
    signal = _signal(h, "inso:SDN/Khartoum/IR/20261410/R1:1")
    assert signal["rawData"]["incident"]["date"] is None
    assert "Date: not stated" in signal["description"].splitlines()


def test_html_only_email_goes_through(h):
    h.folder.add("04_html_only")
    assert h.run().success
    assert set(h.gold) == {"inso:SDN/Khartoum/IR/20261410/R2:1"}


def test_bronze_keeps_the_email_verbatim_with_its_provenance(h):
    h.folder.add("02_single_incident")
    h.run()
    import hashlib

    digest = hashlib.sha256(_eml("02_single_incident")).hexdigest()
    key = f"raw/inso/2026-10-14/{digest}.eml"
    assert h.s3.objects[key] == _eml("02_single_incident")
    sidecar = json.loads(h.s3.objects[f"raw/inso/2026-10-14/{digest}.meta.json"])
    assert sidecar["imapUid"] == 1 and sidecar["uidValidity"] == 7 and sidecar["folder"] == "INSO-TEST"
    assert sidecar["authenticationResults"].startswith("mx.mailbox.example; dkim=pass")
    assert _signal(h, "inso:SDN/Khartoum/IR/20261410/R1:1")["rawS3Key"] == key


# ── classification ───────────────────────────────────────────────────────────


def test_classification_maps_level3_to_level2_glide_relevance_and_severity(h):
    h.llm.classifications = {
        "On 14 October": {"events": [{"event_type": "Looting / property destruction", "relevance": "RELEVANT", "severity": 2}]},
    }
    h.folder.add("02_single_incident")
    assert h.run().success
    row = h.gold["inso:SDN/Khartoum/IR/20261410/R1:1"]
    assert row["eventType"] == "strategic developments / political violence"
    assert row["relevanceScore"] == 1.0
    assert row["severity"] == 2
    assert row["signalInput"]["severity"] == 2


def test_other_maps_to_the_catch_all_not_political_violence(h):
    h.llm.classifications = {"On 14 October": {"events": [{"event_type": "other", "relevance": "RELEVANT", "severity": 1}]}}
    h.folder.add("02_single_incident")
    assert h.run().success
    assert h.gold["inso:SDN/Khartoum/IR/20261410/R1:1"]["eventType"] == "other"


def test_not_relevant_scores_zero_and_unknown_severity_stays_null(h):
    h.llm.classifications = {
        "On 14 October": {"events": [{"event_type": "attack", "relevance": "NOT_RELEVANT", "severity": None}]},
    }
    h.folder.add("02_single_incident")
    assert h.run().success
    row = h.gold["inso:SDN/Khartoum/IR/20261410/R1:1"]
    assert row["relevanceScore"] == 0.0
    assert row["severity"] is None
    assert row["signalInput"]["severity"] is None


@pytest.mark.parametrize("answer", [
    pytest.param({"events": [{"event_type": "spaceship landing", "relevance": "RELEVANT", "severity": 3}]}, id="unmapped-level-3"),
    pytest.param({"events": []}, id="zero-events"),
    pytest.param({"events": [{"event_type": "attack", "relevance": "RELEVANT", "severity": 3}] * 2}, id="two-events"),
])
def test_an_unusable_classification_fails_the_run_and_pushes_nothing(h, answer):
    h.llm.classifications = {"On 14 October": answer}
    h.folder.add("02_single_incident")
    result = h.run()
    assert not result.success
    failed = [e.step_key for e in result.all_events if e.event_type_value == "STEP_FAILURE"]
    assert failed == ["inso_classify"]
    assert h.created == []


# ── quarantine ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize("name, stage, reason", [
    pytest.param("07_forged_sender", "bronze", "no Authentication-Results header", id="forged-sender"),
    pytest.param("05_missing_serial", "silver", "missing label(s): SERIAL #", id="missing-serial"),
    pytest.param("06_unknown_country", "silver", "'Republic of Chad' matches no country location", id="unknown-country"),
])
def test_a_quarantined_email_turns_the_run_red_but_the_good_ones_go_through(h, name, stage, reason):
    h.folder.add("02_single_incident", name)
    result = h.run()

    assert not result.success
    check = h.check(result, "quarantine_empty")
    assert not check.passed
    assert check.metadata["quarantined"].value == 1
    (sidecar,) = h.quarantine().values()
    assert sidecar["stage"] == stage
    assert reason in sidecar["reason"]
    assert sidecar["imapUid"] == 2
    # The good email still reached clear-api, and the cursor moved past both.
    assert [c["externalId"] for c in h.created] == ["inso:SDN/Khartoum/IR/20261410/R1:1"]
    assert h.cursor == "7:2"


def test_a_quarantined_email_keeps_its_bytes_for_replay(h):
    h.folder.add("05_missing_serial")
    h.run()
    (key,) = [k for k in h.s3.objects if k.startswith("quarantine/inso/") and k.endswith(".eml")]
    assert h.s3.objects[key] == _eml("05_missing_serial")


def test_a_sender_off_the_allowlist_is_quarantined(h):
    raw = _eml("02_single_incident").replace(SENDER.encode(), b"spoof@security-reports.example")
    h.folder.messages = [(1, raw)]
    result = h.run()
    assert not h.check(result, "quarantine_empty").passed
    (sidecar,) = h.quarantine().values()
    assert "not allowlisted" in sidecar["reason"]
    assert h.created == []


def test_dkim_signed_by_another_domain_is_not_a_pass(h):
    raw = _eml("02_single_incident").replace(
        b"dkim=pass\n header.d=security-reports.example", b"dkim=pass\n header.d=attacker.example",
    ).replace(b"dmarc=pass", b"dmarc=fail")
    h.folder.messages = [(1, raw)]
    h.run()
    (sidecar,) = h.quarantine().values()
    assert "no aligned DKIM or DMARC pass" in sidecar["reason"]


def test_dkim_can_be_relaxed_by_configuration(h):
    h.folder.add("07_forged_sender")
    assert h.run(require_dkim=False).success
    assert set(h.gold) == {"inso:SDN/Khartoum/IR/20261410/R9:1"}


# ── cursor ───────────────────────────────────────────────────────────────────


def test_each_email_is_processed_once(h):
    h.folder.add("02_single_incident")
    h.run()
    h.folder.add("01_multi_incident")
    h.llm.calls.clear()
    h.run()
    assert h.cursor == "7:2"
    assert h.llm.calls.count("extract") == 1
    assert len(h.created) == 3


def test_a_uidvalidity_change_rescans_the_folder_without_duplicates(h):
    h.folder.add("01_multi_incident", "02_single_incident")
    h.run()
    h.folder.uidvalidity = 8  # folder recreated: same mail, new UIDs space
    result = h.run()
    assert result.success
    assert h.cursor == "8:2"
    assert len(h.gold) == 4
    assert h.created == []  # already pushed rows aren't created again


def test_the_cursor_does_not_advance_when_bronze_fails(h):
    h.folder.add("02_single_incident")
    h.fail_s3_writes = True
    h.run()
    assert h.cursor is None
    h.fail_s3_writes = False
    assert h.run().success
    assert h.cursor == "7:1"
    assert set(h.gold) == {"inso:SDN/Khartoum/IR/20261410/R1:1"}


def test_an_email_that_fails_after_bronze_is_fetched_again(h):
    h.llm.classifications = {"On 14 October": {"events": []}}
    h.folder.add("02_single_incident")
    assert not h.run().success
    assert h.cursor is None
    h.llm.classifications = {}
    assert h.run().success
    assert h.cursor == "7:1"
    assert set(h.gold) == {"inso:SDN/Khartoum/IR/20261410/R1:1"}


def test_a_cursor_staged_by_a_failed_run_is_not_committed_by_a_later_one(h):
    h.folder.add("01_multi_incident", "02_single_incident")
    h.llm.classifications = {"": {"events": []}}  # every classify fails: 7:2 staged, never committed
    assert not h.run().success
    h.llm.classifications = {}
    h.fail_raw_for = _eml("02_single_incident")  # UID 2 misses bronze; UID 1 reaches _push
    h.run()
    assert h.cursor is None
    h.fail_raw_for = None
    assert h.run().success
    assert h.cursor == "7:2"
    assert "inso:SDN/Khartoum/IR/20261410/R1:1" in h.gold


def test_empty_credentials_refuse_to_poll(h):
    with (
        patch.object(newsletter_mailbox.settings, "clear_mailbox_imap_host", "imap.example"),
        patch.object(newsletter_mailbox.settings, "clear_mailbox_imap_user", ""),
        patch.object(newsletter_mailbox.settings, "clear_mailbox_imap_password", ""),
        pytest.raises(RuntimeError, match="credentials are not configured"),
    ):
        newsletter_mailbox.fetch_after("INSO-TEST", None, 0)


# ── drain connector ──────────────────────────────────────────────────────────


def test_the_drain_connector_projects_an_incident_from_its_clear_api_row(h):
    from clear_pipeline.defs.signals.connectors import CONNECTORS_BY_SOURCE

    connector = CONNECTORS_BY_SOURCE["inso"]
    created = {
        "id": "sig-1", "externalId": "inso:SDN/Darfur/IR/20262010/R1:2", "title": "Similarly, on 19 October…",
        "publishedAt": "2026-10-20T09:30:00+00:00", "description": "d",
        "generalLocation": {"id": "loc-sd", "name": "Sudan"},
    }
    view = connector.project(connector.parse(_eml("01_multi_incident")), created)
    assert (view.external_id, view.title, view.location_name) == (created["externalId"], created["title"], "Sudan")
