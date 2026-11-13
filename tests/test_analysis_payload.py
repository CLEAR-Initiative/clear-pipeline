"""Published payload: v6 build + v5 projection (PR-8a)."""

from clear_pipeline.defs.analysis.payload import (
    PAYLOAD_SCHEMA_VERSION,
    assemble_published_data,
    build_v6_payload,
    project_v5,
)


def _art(name, task, finding, rids, completeness=0.7):
    return {
        "node": {"name": name, "task": task, "level": 1},
        "descriptive": {
            "findings": [{"description": finding, "source_report_ids": rids}],
            "completeness": completeness,
            "questions_not_answered": [],
            "source_report_ids": rids,
        },
    }


def _artifacts():
    return {
        "sectors/health": _art("Health", "Sectors", "Clinics closed", ["r1"]),
        "pillars_1d/displacement": _art("Displacement", "Pillars 1D", "100k displaced", ["r2"], 0.8),
        "pillars_1d/shock_event": _art("Shock-Event", "Pillars 1D", "Flooding", ["r3"]),
        "pillars_1d/casualties": _art("Casualties", "Pillars 1D", "200 killed", ["r4"]),  # new-only
        "pillars_1d/context": _art("Context", "Pillars 1D", "Fragile state", ["r5"]),
    }


_NUMBERS = {"data": {"displacement": {"value": 100000}}, "report_count": 5}
_OVERVIEW = "Overall situation worsening."


def test_build_v6_payload_keys_nodes():
    v6 = build_v6_payload(_artifacts(), _NUMBERS, _OVERVIEW)
    assert v6["schema_version"] == PAYLOAD_SCHEMA_VERSION == "v6"
    assert v6["taxonomy_depth"] == "L1"
    assert v6["overview"] == _OVERVIEW
    assert set(v6["nodes"]) == set(_artifacts())
    assert v6["nodes"]["sectors/health"]["findings"][0]["description"] == "Clinics closed"
    assert v6["numbers"] == _NUMBERS


def test_project_v5_maps_sectors_and_pillars():
    v6 = build_v6_payload(_artifacts(), _NUMBERS, _OVERVIEW)
    v5 = project_v5(v6)
    assert v5["schema_version"] == "v5"
    # sector by key
    assert v5["sectors"]["health"]["findings"] == ["Clinics closed"]
    assert v5["sectors"]["health"]["completeness"] == 0.7
    # situation pillars mapped
    assert v5["displacement"]["findings"] == ["100k displaced"]
    assert v5["hazards_and_vulnerabilities"]["findings"] == ["Flooding"]
    # overview → ai_summary; numbers → datapoints
    assert v5["ai_summary"]["summary"] == _OVERVIEW
    assert v5["datapoints"] == {"displacement": {"value": 100000}}
    # sources union
    assert set(v5["sources"]) == {"r1", "r2", "r3", "r4", "r5"}


def test_project_v5_omits_new_only_and_coarse_context():
    v5 = project_v5(build_v6_payload(_artifacts(), _NUMBERS, _OVERVIEW))
    # "Casualties" is a new-only L1 — it has no v5 field anywhere
    flat = str(v5)
    assert "200 killed" not in flat
    # Context stays coarse (not expanded into the 9 v5 domains)
    assert v5["context_risks"] == {}


def test_assemble_embeds_v5_projection():
    v6 = build_v6_payload(_artifacts(), _NUMBERS, _OVERVIEW)
    data = assemble_published_data(v6)
    assert data["schema_version"] == "v6"       # top-level is v6
    assert "nodes" in data                       # v6 taxonomy view
    assert data["v5"]["schema_version"] == "v5"  # embedded legacy view
    assert "health" in data["v5"]["sectors"]
