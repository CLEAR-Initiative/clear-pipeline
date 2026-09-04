"""Tests for the USGS FDSN provider (Expo #465 backend).

Deterministic — http_retry is monkeypatched, no network. Covers the slim
transform, has_shakemap gating, bbox padding, and build_seismic_collection
(reduction stats + ShakeMap attach + serve-side age/stale omission)."""

from clear_pipeline.providers import usgs


def _fat(event_id="us1", *, mag=6.1, types=",origin,shakemap,", coords=(10.0, 20.0, 35.5)):
    return {
        "type": "Feature",
        "id": event_id,
        "geometry": {"type": "Point", "coordinates": list(coords)},
        "properties": {
            "mag": mag, "magType": "mww", "place": "X", "title": "M 6.1", "time": 1, "updated": 2,
            "alert": "yellow", "mmi": 5.2, "url": "u", "types": types, "status": "reviewed",
            "detail": f"https://usgs/detail/{event_id}.geojson",
            # Heavy fields the slim transform drops (make the fat payload dominate,
            # mirroring real FDSN — reduction is a genuine ~60-70% shrink).
            "nst": 120, "dmin": 0.123, "rms": 0.98, "gap": 34, "sig": 700, "net": "us",
            "code": "6000tjl2", "ids": ",us6000tjl2,pt26123000," * 4, "sources": ",us,pt," * 4,
            "products": {"shakemap": [{"contents": {"x": "y"}}], "losspager": [{}]},
        },
    }


def test_slim_feature_shape():
    s = usgs.slim_feature(_fat("us6000tjl2"))
    assert s["id"] == "us6000tjl2"
    assert s["geometry"] == {"type": "Point", "coordinates": [10.0, 20.0, 35.5]}
    assert s["properties"]["depth_km"] == 35.5
    assert s["properties"]["has_shakemap"] is True
    # age_days / stale are serve-side — never stored.
    assert "age_days" not in s["properties"] and "stale" not in s["properties"]


def test_slim_uses_top_level_id_not_properties_id():
    fat = _fat("top")
    fat["properties"]["id"] = "should-be-ignored"
    assert usgs.slim_feature(fat)["id"] == "top"


def test_slim_rejects_non_point_and_missing_id():
    assert usgs.slim_feature({"id": "x", "geometry": {"type": "LineString", "coordinates": []}}) is None
    assert usgs.slim_feature({"geometry": {"type": "Point", "coordinates": [1, 2]}}) is None
    assert usgs.slim_feature({"id": "x", "geometry": None}) is None


def test_has_shakemap():
    assert usgs.has_shakemap(_fat(types=",origin,shakemap,losspager,")) is True
    assert usgs.has_shakemap(_fat(types=",origin,")) is False


def test_pad_bbox_expands_and_clamps():
    assert usgs.pad_bbox((21.8, 8.5, 38.6, 22.0), 2.5) == (19.3, 6.0, 41.1, 24.5)
    # clamps at valid lat/lng bounds
    assert usgs.pad_bbox((-179.0, -89.0, 179.0, 89.0), 5.0) == (-180.0, -90.0, 180.0, 90.0)


def test_build_seismic_collection(monkeypatch):
    # Two events; one has a ShakeMap.
    raw = [_fat("with-sm", types=",origin,shakemap,"), _fat("no-sm", types=",origin,")]
    monkeypatch.setattr(usgs, "fetch_earthquakes", lambda **k: raw)
    monkeypatch.setattr(
        usgs, "fetch_shakemap_contours",
        lambda detail_url, **k: [{"type": "Feature", "geometry": {"type": "MultiLineString", "coordinates": []},
                                  "properties": {"value": 5.0, "units": "intensity"}}],
    )

    blob = usgs.build_seismic_collection(bbox=(20.0, 8.0, 40.0, 23.0), min_magnitude=5.5, window_days=30)

    assert blob["source"] == "usgs-ingest"
    assert blob["min_magnitude"] == 5.5 and blob["window_days"] == 30
    assert len(blob["features"]) == 2
    # only the has_shakemap event got contours
    assert [s["eventId"] for s in blob["shakemaps"]] == ["with-sm"]
    assert blob["bytes_in"] > 0 and blob["bytes_out"] > 0
    assert 0.0 <= blob["reduction_ratio"] <= 1.0


def test_build_skips_contours_when_disabled(monkeypatch):
    monkeypatch.setattr(usgs, "fetch_earthquakes", lambda **k: [_fat("with-sm")])
    called = {"n": 0}
    def _boom(*a, **k):
        called["n"] += 1
        return None
    monkeypatch.setattr(usgs, "fetch_shakemap_contours", _boom)
    blob = usgs.build_seismic_collection(bbox=(0, 0, 1, 1), fetch_contours=False)
    assert blob["shakemaps"] == []
    assert called["n"] == 0
