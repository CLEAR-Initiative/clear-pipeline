"""Isolated PDF text extraction — deliberately dependency-light.

This runs in a short-lived worker process spawned by `reliefweb_weekly_pdf_text`,
so it imports only `io` + `pdfplumber` and nothing from Dagster. Under the
`spawn` start method the worker re-imports the target function's module; keeping
this module lean means the child starts with a small memory baseline and leaves
maximum headroom for pdfplumber's peak on a single dense PDF — which is what was
OOM-killing the worker on the heavier weekly sitreps.
"""

import io

import pdfplumber


def extract_pages(pdf_bytes: bytes) -> list[dict]:
    """Return one dict per page with non-empty text. Page-level granularity is
    what the chunker + citation UI both need — chunk boundaries then respect
    page breaks so a search hit can cite an exact page span.

    Pages that yield only whitespace (scanned images with no OCR, blank
    separators) are silently dropped — otherwise they'd bloat the chunk set
    without contributing retrievable text.
    """
    pages: list[dict] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = (page.extract_text() or "").strip()
            if text:
                pages.append({"page_num": i, "text": text})
            # pdfplumber caches every parsed object for each Page's lifetime;
            # on a many-page PDF that grows unbounded within this single
            # extraction. Flush per page to cap peak memory.
            page.flush_cache()
    return pages
