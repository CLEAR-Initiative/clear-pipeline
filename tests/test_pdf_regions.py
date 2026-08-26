"""Tests for figure-region detection (infographic capture §6 A).

Deterministic — synthetic page objects, no real PDF. Covers the whole-page
(composite) rule taking precedence, discrete image/table regions amid prose, the
logo area filter, and the vector-only fallback.
"""

from clear_context_pipeline.providers.pdf_regions import (
    FigureRegion,
    detect_figure_regions,
    pad_bbox,
)


class _Table:
    def __init__(self, bbox):
        self.bbox = bbox


class _Page:
    def __init__(self, *, width=600, height=800, text="", images=None,
                 tables=None, rects=0, lines=0, curves=0):
        self.width = width
        self.height = height
        self._text = text
        self.images = images or []
        self._tables = tables or []
        self.rects = [object()] * rects
        self.lines = [object()] * lines
        self.curves = [object()] * curves

    def extract_text(self):
        return self._text

    def find_tables(self, settings=None):
        return self._tables


def _img(x0, top, x1, bottom):
    return {"x0": x0, "top": top, "x1": x1, "bottom": bottom}


PROSE = "word " * 200  # ~1000 chars of prose


def test_plain_prose_page_has_no_regions():
    assert detect_figure_regions(_Page(text=PROSE)) == []


def test_composite_full_page_when_graphic_covers_most_of_page():
    # One big image over >55% of the page → a single whole-page region, not sliced.
    page = _Page(text=PROSE, images=[_img(0, 0, 600, 500)])  # 300k/480k = 62%
    regions = detect_figure_regions(page)
    assert regions == [FigureRegion(bbox=(0.0, 0.0, 600.0, 800.0), kind_hint="page", is_full_page=True)]


def test_low_text_graphic_page_is_full_page():
    # Little prose but a graphic present → treat the whole page as one figure.
    page = _Page(text="Figure 1", images=[_img(0, 0, 200, 200)])
    regions = detect_figure_regions(page)
    assert len(regions) == 1 and regions[0].is_full_page


def test_discrete_image_and_table_amid_prose():
    page = _Page(
        text=PROSE,
        images=[_img(0, 0, 200, 200)],          # 40k/480k = 8% → a figure, not full-page
        tables=[_Table((300, 300, 500, 400))],  # a ruled table region
    )
    regions = detect_figure_regions(page)
    kinds = sorted(r.kind_hint for r in regions)
    assert kinds == ["image", "table"]
    assert all(not r.is_full_page for r in regions)


def test_logo_sized_image_is_dropped():
    # A tiny image (<4% of page) with no other graphic → nothing to capture.
    assert detect_figure_regions(_Page(text=PROSE, images=[_img(0, 0, 50, 50)])) == []


def test_vector_only_page_falls_back_to_full_page():
    # A drawn chart: dense vectors, no image object, no ruled table → full-page.
    page = _Page(text="Chart", rects=150, lines=150)
    regions = detect_figure_regions(page)
    assert len(regions) == 1 and regions[0].is_full_page


def test_pad_bbox_expands_and_clamps():
    padded = pad_bbox((100, 100, 300, 300), 600, 800, frac=0.05)
    assert padded == (70.0, 60.0, 330.0, 340.0)
    # clamps at the page edges
    assert pad_bbox((0, 0, 600, 800), 600, 800, frac=0.05) == (0.0, 0.0, 600.0, 800.0)
