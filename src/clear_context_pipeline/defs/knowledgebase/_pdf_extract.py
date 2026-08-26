"""Isolated PDF text extraction — deliberately dependency-light.

This runs in a short-lived worker process spawned by `reliefweb_weekly_pdf_text`,
so it imports only `io` + `os` + `pdfplumber` and nothing from Dagster. Under the
`spawn` start method the worker re-imports the target function's module; keeping
this module lean means the child starts with a small memory baseline and leaves
maximum headroom for pdfplumber's peak on a single dense PDF — which is what was
OOM-killing the worker on the heavier weekly sitreps.

Tables (infographic-capture spec, Phase 1): `extract_text()` flattens a table's
cells into reading-order text, so multi-column rows interleave and row↔column
meaning is lost. We additionally pull the page's tables as STRUCTURED grids and
append them as markdown to the page text, so both the datapoint extractor and RAG
see a clean table instead of a jumble. Best-effort and env-gated
(`KB_EXTRACT_TABLES=0` disables) — table detection can be heavy on graphics-dense
pages, so any failure falls back to text-only rather than breaking extraction.
"""

import io
import os

import pdfplumber

# Env-gated so a bad table run can be turned off without a redeploy. Inherited
# from the container env by the spawned worker.
_EXTRACT_TABLES = os.environ.get("KB_EXTRACT_TABLES", "1").strip().lower() not in ("0", "false", "no", "")

# Require a REAL ruled grid — drawn lines on both axes — rather than pdfplumber's
# default text-alignment heuristic, which mistakes prose blocks, title boxes, and
# two-column layouts for tables (the §4 "find_tables precision" caveat). This
# favours precision over recall: genuine data tables (commodity prices, PIN-by-
# state, funding) are ruled and survive; borderless tables are missed here and
# left to the Phase-3 vision pass. Values in report pixels.
_TABLE_SETTINGS = {
    "vertical_strategy": "lines",
    "horizontal_strategy": "lines",
    "snap_tolerance": 4,
    "join_tolerance": 4,
}

# A "cell" longer than this is a paragraph, not tabular data → the region is
# prose that happened to sit inside rules, so reject the whole table.
_MAX_CELL_CHARS = 200


def _table_to_markdown(rows: list[list]) -> str:
    """Serialise one extracted table (list of rows of cells) to a markdown grid,
    with quality gates that reject layout/prose false-positives. Returns "" for an
    empty or non-tabular region."""
    cleaned: list[list[str]] = []
    for row in rows or []:
        if row is None:
            continue
        cleaned.append([("" if c is None else str(c)).replace("\n", " ").strip() for c in row])
    cleaned = [r for r in cleaned if any(cell for cell in r)]
    if len(cleaned) < 2 or not cleaned[0]:
        return ""  # need a header + ≥1 body row
    width = max(len(r) for r in cleaned)
    if width < 2:
        return ""  # a single column is a list, not a table
    cleaned = [r + [""] * (width - len(r)) for r in cleaned]

    # Reject prose-in-a-box: any paragraph-length cell.
    if any(len(cell) > _MAX_CELL_CHARS for r in cleaned for cell in r):
        return ""
    # Reject mostly-empty grids (layout artifacts): >55% blank cells.
    total = len(cleaned) * width
    blanks = sum(1 for r in cleaned for cell in r if not cell)
    if total and blanks / total > 0.55:
        return ""
    # Need ≥2 columns that actually carry content (else it's a single-column list
    # padded with empty layout columns).
    nonempty_cols = sum(1 for c in range(width) if any(cleaned[r][c] for r in range(len(cleaned))))
    if nonempty_cols < 2:
        return ""

    def fmt(r: list[str]) -> str:
        return "| " + " | ".join(cell.replace("|", "\\|") for cell in r) + " |"

    out = [fmt(cleaned[0]), "| " + " | ".join(["---"] * width) + " |"]
    out += [fmt(r) for r in cleaned[1:]]
    return "\n".join(out)


def _tables_markdown(page) -> str:
    """All of a page's tables as markdown blocks, or "" if none / on any error.
    Best-effort: table detection must never break text extraction."""
    if not _EXTRACT_TABLES:
        return ""
    try:
        blocks = [md for t in (page.extract_tables(_TABLE_SETTINGS) or []) if (md := _table_to_markdown(t))]
    except Exception:
        return ""
    if not blocks:
        return ""
    return "\n\n[structured tables]\n\n" + "\n\n".join(blocks)


def extract_pages(pdf_bytes: bytes) -> list[dict]:
    """Return one dict per page with non-empty text. Page-level granularity is
    what the chunker + citation UI both need — chunk boundaries then respect
    page breaks so a search hit can cite an exact page span.

    Each page's `text` is the extracted prose plus any tables re-serialised as
    markdown grids (see module docstring); `tables_markdown` carries just the
    tables for downstream use. Pages that yield only whitespace (scanned images
    with no OCR, blank separators) and no tables are silently dropped — otherwise
    they'd bloat the chunk set without contributing retrievable content.
    """
    pages: list[dict] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = (page.extract_text() or "").strip()
            tables_md = _tables_markdown(page)
            combined = (text + tables_md).strip()
            if combined:
                pages.append({
                    "page_num": i,
                    "text": combined,
                    "tables_markdown": tables_md.strip() or None,
                })
            # pdfplumber caches every parsed object for each Page's lifetime;
            # on a many-page PDF that grows unbounded within this single
            # extraction. Flush per page to cap peak memory.
            page.flush_cache()
    return pages


def extract_pages_pypdf(pdf_bytes: bytes) -> list[dict]:
    """Lighter fallback for when pdfplumber OOMs on a graphics-dense PDF.

    pypdf reads the content stream's text operators without building
    pdfplumber's per-object model (a Python object per char/line/rect/curve),
    so it survives pages whose vector-graphics density exhausts pdfplumber —
    at some cost to layout fidelity, an acceptable trade when the alternative
    is no text for the report at all. pypdf is imported lazily so the
    pdfplumber worker doesn't pay for it.
    """
    from pypdf import PdfReader

    pages: list[dict] = []
    reader = PdfReader(io.BytesIO(pdf_bytes))
    for i, page in enumerate(reader.pages, start=1):
        text = (page.extract_text() or "").strip()
        if text:
            pages.append({"page_num": i, "text": text})
    return pages
