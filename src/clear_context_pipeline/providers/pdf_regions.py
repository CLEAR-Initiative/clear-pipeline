"""Figure-region detection over a pdfplumber page — the cheap, deterministic
first step of infographic capture (docs/infographic-capture-spec.md §6 A).

Decides WHERE the figures are so the renderer can crop them, using only the
page's already-parsed object model — no rendering, no LLM. Two modes:

  - **Composite full-page** (takes precedence): a page that is *itself* one
    infographic (high graphic coverage / little prose — DTM snapshots) is a single
    whole-page region, so a panel isn't fragmented into its sub-figures.
  - **Discrete figures amid prose**: per-region bounding boxes from embedded
    images (`page.images`) and ruled tables (`page.find_tables()`).

The ~5% of pages that are vector-only charts with neither an image object nor a
ruled table fall back to full-page (a layout model is deferred — §6/§10).
"""

from __future__ import annotations

from dataclasses import dataclass

# Require a real ruled grid for tables — matches _pdf_extract's Phase-1 settings
# so region-detection and text-extraction agree on what a table is.
_TABLE_SETTINGS = {
    "vertical_strategy": "lines",
    "horizontal_strategy": "lines",
    "snap_tolerance": 4,
    "join_tolerance": 4,
}

# An embedded image must cover at least this fraction of the page to be a figure
# (drops logos, icons, header banners).
_MIN_IMAGE_AREA_FRAC = 0.04
# Whole-page rule: graphic area ≥ this fraction of the page, OR the page carries
# less than _MIN_TEXT_CHARS of prose while still holding graphic content.
_COMPOSITE_COVERAGE = 0.55
_MIN_TEXT_CHARS = 250
# Vector-primitive count above which a page counts as "carries graphic content"
# even without an embedded image or ruled table (a drawn chart/map/panel).
_VECTOR_DENSE = 250

# pdfplumber bboxes are (x0, top, x1, bottom) in PDF points, top-origin.
BBox = tuple[float, float, float, float]


@dataclass(frozen=True)
class FigureRegion:
    """One croppable region on a page.

    kind_hint: a cheap structural guess — "image" | "table" | "page" — NOT the
    final classification (the vision pass decides chart/map/table/photo). bbox is
    in PDF points; is_full_page marks the composite / fallback whole-page crop."""
    bbox: BBox
    kind_hint: str
    is_full_page: bool


def _area(b: BBox) -> float:
    return max(0.0, b[2] - b[0]) * max(0.0, b[3] - b[1])


def _image_bbox(im: dict) -> BBox:
    return (float(im.get("x0", 0)), float(im.get("top", 0)),
            float(im.get("x1", 0)), float(im.get("bottom", 0)))


def detect_figure_regions(page) -> list[FigureRegion]:
    """Return the figure regions to crop on `page`. Empty when the page is plain
    prose. Best-effort: any pdfplumber failure yields no regions rather than
    raising (a page we can't analyse simply contributes no figures)."""
    try:
        page_bbox: BBox = (0.0, 0.0, float(page.width), float(page.height))
        parea = _area(page_bbox) or 1.0
        text_len = len((page.extract_text() or "").strip())

        images = [im for im in (page.images or []) if _area(_image_bbox(im)) / parea >= _MIN_IMAGE_AREA_FRAC]
        try:
            tables = list(page.find_tables(_TABLE_SETTINGS) or [])
        except Exception:
            tables = []
        vcount = len(page.rects or []) + len(page.lines or []) + len(page.curves or [])

        has_graphic = bool(images) or bool(tables) or vcount >= _VECTOR_DENSE
        if not has_graphic:
            return []

        # Composite / whole-page: the page IS one infographic. Overlapping bboxes
        # make this an over-estimate of coverage, which is the safe direction —
        # it errs toward keeping a dense panel whole rather than slicing it.
        graphic_area = sum(_area(_image_bbox(im)) for im in images)
        graphic_area += sum(_area(tuple(t.bbox)) for t in tables)
        coverage = graphic_area / parea
        if coverage >= _COMPOSITE_COVERAGE or text_len < _MIN_TEXT_CHARS:
            return [FigureRegion(bbox=page_bbox, kind_hint="page", is_full_page=True)]

        regions: list[FigureRegion] = []
        for im in images:
            regions.append(FigureRegion(bbox=_image_bbox(im), kind_hint="image", is_full_page=False))
        for t in tables:
            regions.append(FigureRegion(bbox=tuple(float(v) for v in t.bbox), kind_hint="table", is_full_page=False))
        return regions
    except Exception:
        return []


def pad_bbox(bbox: BBox, page_width: float, page_height: float, *, frac: float = 0.02) -> BBox:
    """Expand a crop bbox by `frac` of the page on each side (clamped to the page)
    so a figure's title / legend / axis labels aren't clipped off the edge."""
    dx = page_width * frac
    dy = page_height * frac
    return (
        max(0.0, bbox[0] - dx), max(0.0, bbox[1] - dy),
        min(page_width, bbox[2] + dx), min(page_height, bbox[3] + dy),
    )
