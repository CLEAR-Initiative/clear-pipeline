"""Render a pdfplumber page (or a figure region within it) to a PNG for the
vision pass + the S3 image asset (infographic capture §6 C/D).

Runtime dependency: pdfplumber's ``to_image()`` needs a raster backend available
in the image. If the deployed image lacks one, swap the two ``to_image`` calls
here for pymupdf (``fitz``) — ``page.get_pixmap(clip=...)`` — which is a single
wheel with no external binary. Pillow ships with pdfplumber, so the crop/resize
path below is already available.
"""

from __future__ import annotations

import hashlib
import io

# Longest-edge cap: Claude bills image tokens ≈ W·H/750, and there's no accuracy
# gain past ~1568px, so downscale to keep crops cheap + fast.
_MAX_EDGE_PX = 1568
_RENDER_DPI = 150

BBox = tuple[float, float, float, float]


def render_region_png(page, bbox: BBox, *, is_full_page: bool = False) -> bytes:
    """Render `bbox` on `page` (or the whole page when `is_full_page`) to PNG
    bytes, downscaled so the longest edge is ≤ _MAX_EDGE_PX."""
    from PIL import Image  # bundled with pdfplumber

    target = page if is_full_page else page.within_bbox(bbox)
    pageimage = target.to_image(resolution=_RENDER_DPI)
    pil: Image.Image = pageimage.original.convert("RGB")

    w, h = pil.size
    scale = min(1.0, _MAX_EDGE_PX / max(w, h)) if max(w, h) else 1.0
    if scale < 1.0:
        pil = pil.resize((max(1, int(w * scale)), max(1, int(h * scale))))

    buf = io.BytesIO()
    pil.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def image_hash(png_bytes: bytes) -> str:
    """Content hash for dedup + a stable S3 key suffix (collapses repeated
    banners/logos across pages of the same report)."""
    return hashlib.sha256(png_bytes).hexdigest()[:16]


def figure_s3_key(prefix: str, report_id: str, page_num: int, png_bytes: bytes) -> str:
    """`<prefix>/<report_id>/<page>-<hash>.png` — page-attributed, content-
    addressed so a re-render of the same crop is idempotent. `prefix` is the
    partition's `figures_prefix(iso3)` so figure keys sit parallel to text/
    chunks/enriched under `reliefweb/kb/figures/<iso3>/<format>`."""
    return f"{prefix}/{report_id}/{page_num:04d}-{image_hash(png_bytes)}.png"
