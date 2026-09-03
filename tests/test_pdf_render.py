"""Tests for figure render/dedup helpers (infographic capture §6 C).

Deterministic — no PDF rendering (that needs a raster backend + a real page).
Covers the content-hash dedup key and the S3 key scheme, which are pure.
"""

from clear_pipeline.providers.pdf_render import figure_s3_key, image_hash


def test_image_hash_is_stable_and_content_addressed():
    a = image_hash(b"same-bytes")
    b = image_hash(b"same-bytes")
    c = image_hash(b"other-bytes")
    assert a == b
    assert a != c
    assert len(a) == 16  # 16-hex-char prefix


def test_figure_s3_key_layout():
    key = figure_s3_key("reliefweb/kb/figures/sdn/situation-report", "12345", 7, b"png")
    # <prefix>/<report_id>/<page zero-padded>-<hash>.png
    assert key.startswith("reliefweb/kb/figures/sdn/situation-report/12345/0007-")
    assert key.endswith(".png")
    assert key.split("/")[-1] == f"0007-{image_hash(b'png')}.png"


def test_figure_s3_key_is_deterministic_for_same_crop():
    args = ("reliefweb/kb/figures/sdn/situation-report", "r1", 3, b"identical")
    assert figure_s3_key(*args) == figure_s3_key(*args)
