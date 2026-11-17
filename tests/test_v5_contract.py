"""v5 contract (ADR-0015): the assembled payload keeps the structured v5 fields
clear-mvp (`mappers/situation-analysis.ts`) reads — no regression. Pure."""

from clear_pipeline.defs.analysis import reconciliation as rec
from clear_pipeline.defs.analysis.assemble_v5 import assemble_v5
from clear_pipeline.defs.analysis.output_schemas import NodeEnvelope
from clear_pipeline.defs.analysis.taxonomy import node_by_key, nodes_at_depth


def _env(node_key, archetype, data, name="", task="", level=1):
    n = node_by_key(node_key)
    return NodeEnvelope(
        node_key=node_key, name=name or (n.name if n else ""),
        task=task or (n.task if n else ""), level=level, archetype=archetype, data=data,
    )


def _displacement_l2(name):
    return next(
        n for n in nodes_at_depth(2)
        if n.task == "Pillars 1D" and n.l1 == "Displacement" and n.name == name
    )


def _sample_l1_envelopes():
    envs: dict[str, NodeEnvelope] = {}

    # Sector — SeverityNeeds → data.sectors.health
    envs["sectors/health"] = _env(
        "sectors/health", "SeverityNeeds",
        {"severity": "high", "top_needs": ["trauma care", "medicines"], "findings": []},
    )

    # Context Section — 9 L2 domains → data.context_risks.<domain>
    ctx_subs = {
        n.key: _env(n.key, "Described",
                    {"findings": [{"description": f"{n.name} finding", "source_ids": ["r1"]}]},
                    name=n.name, level=2).model_dump()
        for n in rec.context_domain_nodes()
    }
    envs["pillars_1d/context"] = _env(
        "pillars_1d/context", "Section", {"overview": "ctx", "subsections": ctx_subs},
    )

    # Displacement Section — Push factors + Intentions → data.displacement
    push = _displacement_l2("Push factors")
    intent = _displacement_l2("Intentions")
    disp_subs = {
        push.key: _env(push.key, "Drivers",
                       {"factors": [{"factor": "armed violence", "description": "clashes", "source_ids": ["r2"]}]},
                       name="Push factors", level=2).model_dump(),
        intent.key: _env(intent.key, "Distribution",
                         {"options": [{"option": "return", "description": "intend to return", "source_ids": ["r3"]}]},
                         name="Intentions", level=2).model_dump(),
    }
    envs["pillars_1d/displacement"] = _env(
        "pillars_1d/displacement", "Section", {"overview": "disp", "subsections": disp_subs},
    )

    # Shock-Event Drivers → data.hazards_and_vulnerabilities
    envs["pillars_1d/shock_event"] = _env(
        "pillars_1d/shock_event", "Drivers",
        {"factors": [{"factor": "flood", "description": "seasonal flooding", "source_ids": ["r4"]}]},
    )

    # Humanitarian Access StatusGaps → data.humanitarian_access
    envs["pillars_1d/humanitarian_access"] = _env(
        "pillars_1d/humanitarian_access", "StatusGaps",
        {"status": "constrained", "gaps": [{"description": "checkpoints", "source_ids": ["r5"]}], "enablers": []},
    )

    # Casualties (new-only) Figures → data.taxonomy.pillars_1d
    envs["pillars_1d/casualties"] = _env(
        "pillars_1d/casualties", "Figures",
        {"figures": [{"metric": "killed", "value": {"point": 12, "unit": "people"}}]},
    )
    return envs


def test_sectors_keep_severity_and_top_needs():
    data = assemble_v5(_sample_l1_envelopes())
    health = data["sectors"]["health"]
    assert health["severity"] == "high"
    assert health["top_needs"] == ["trauma care", "medicines"]


def test_context_risks_uses_bullets_the_frontend_reads():
    data = assemble_v5(_sample_l1_envelopes())
    cr = data["context_risks"]
    assert set(cr) == set(rec.CONTEXT_DOMAIN_V5_KEY.values())  # all 9 v5 keys
    assert len(cr) == 9
    # the frontend reads `.bullets` (string list), NOT `.findings`
    assert cr["security"]["bullets"] and isinstance(cr["security"]["bullets"][0], str)
    assert "findings" not in cr["security"]


def test_displacement_keeps_push_factors_and_return_intention():
    data = assemble_v5(_sample_l1_envelopes())
    disp = data["displacement"]
    # push factor = Drivers "factor — description"; sources under source_report_ids
    assert disp["push_factors"][0]["description"] == "armed violence — clashes"
    assert disp["push_factors"][0]["source_report_ids"] == ["r2"]
    assert disp["return_intention"][0]["description"] == "intend to return"
    assert disp["return_intention"][0]["source_report_ids"] == ["r3"]


def test_hazards_and_access_use_frontend_shapes():
    data = assemble_v5(_sample_l1_envelopes())
    # hazards is an OBJECT with a `.hazards` list, not a bare list
    hazards = data["hazards_and_vulnerabilities"]["hazards"]
    assert hazards[0]["description"] == "flood — seasonal flooding"
    assert hazards[0]["source_report_ids"] == ["r4"]
    # access exposes `.barriers` (not status/gaps) with resolved report ids
    barriers = data["humanitarian_access"]["barriers"]
    assert barriers[0]["description"] == "checkpoints"
    assert barriers[0]["source_report_ids"] == ["r5"]


def test_new_only_nodes_are_additive_under_taxonomy():
    data = assemble_v5(_sample_l1_envelopes())
    # new-only Casualties lands under the taxonomy section, NOT a v5 field
    assert "pillars_1d/casualties" in data["taxonomy"]["pillars_1d"]
    assert "casualties" not in data  # no regression: it doesn't shadow a v5 field


def test_full_v5_skeleton_is_always_present_for_backward_compat():
    # Even with NO nodes, every overlapping v5 field is present (empty) — a failed
    # or skipped node leaves an empty field, never a missing key.
    data = assemble_v5({})
    for key in ("sectors", "context_risks", "displacement",
                "hazards_and_vulnerabilities", "humanitarian_access",
                "scenarios", "changes"):
        assert key in data
    assert set(data["sectors"]) == set(rec.SECTOR_V5_KEY.values())  # all 10 sectors
    assert data["displacement"] == {"push_factors": [], "return_intention": [], "summary": None}
    assert data["hazards_and_vulnerabilities"]["hazards"] == []
    # scenarios/changes present-but-empty (overview overlays real content)
    assert data["scenarios"]["most_likely"] == "" and data["changes"]["notes"] == {}


def test_partial_run_keeps_skeleton_and_fills_present_nodes():
    # a deep-dive / partial run with just one sector → that field is filled, the
    # rest of the v5 skeleton stays present-but-empty (no missing keys).
    one = {"sectors/health": _sample_l1_envelopes()["sectors/health"]}
    data = assemble_v5(one)
    assert data["sectors"]["health"]["severity"] == "high"
    assert data["sectors"]["agriculture"] == {
        "severity": None, "top_needs": [], "source_report_ids": []
    }
    assert "displacement" in data and data["displacement"]["push_factors"] == []
