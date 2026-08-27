"""Tests for the Phase-1 structured-table extraction in `_pdf_extract`.

The serializer's quality gates are the precision lever — pdfplumber's table
detection over-fires on prose blocks and layout boxes, so `_table_to_markdown`
must reject those while keeping genuine ruled data tables.
"""

from clear_context_pipeline.defs.knowledgebase._pdf_extract import (
    _table_to_markdown,
    _tables_markdown,
)


def test_real_table_becomes_markdown_grid():
    md = _table_to_markdown([
        ["Commodity", "Unit", "Price"],
        ["Sorghum", "1 Kg", "1,275"],
        ["Wheat Flour", "1 Kg", "3,388"],
    ])
    lines = md.splitlines()
    assert lines[0] == "| Commodity | Unit | Price |"
    assert lines[1] == "| --- | --- | --- |"
    assert lines[2] == "| Sorghum | 1 Kg | 1,275 |"
    assert lines[3] == "| Wheat Flour | 1 Kg | 3,388 |"


def test_rejects_prose_in_a_box():
    # A paragraph-length cell → the region is prose that sat inside rules.
    long = "The Protection Cluster is issuing this alert " * 8
    assert _table_to_markdown([["", long, ""], ["", "more prose here", ""]]) == ""


def test_rejects_single_column():
    assert _table_to_markdown([["Item"], ["A"], ["B"]]) == ""


def test_rejects_mostly_empty_grid():
    # Layout artifact: a 3x3 grid with only one populated cell (>55% blank).
    assert _table_to_markdown([["", "", ""], ["", "x", ""], ["", "", ""]]) == ""


def test_rejects_single_nonempty_column_padded_by_layout():
    # 2 columns wide but only one carries content.
    assert _table_to_markdown([["Title", ""], ["Row 1", ""], ["Row 2", ""]]) == ""


def test_requires_header_plus_body_row():
    assert _table_to_markdown([["A", "B"]]) == ""  # header only
    assert _table_to_markdown([]) == ""


def test_pads_ragged_rows_and_escapes_pipes():
    md = _table_to_markdown([
        ["A", "B", "C"],
        ["x", "y|z"],          # ragged (2 cells) + a pipe to escape
    ])
    rows = md.splitlines()
    assert rows[0] == "| A | B | C |"
    # ragged row padded to width 3; the pipe in y|z escaped
    assert rows[2] == "| x | y\\|z |  |"


def test_flattens_newlines_in_cells():
    md = _table_to_markdown([["Sector", "PIN"], ["Food\nSecurity", "12M"]])
    assert "| Food Security | 12M |" in md


# ── _tables_markdown wrapper (best-effort, header-wrapped) ────────────────────

class _FakePage:
    def __init__(self, tables):
        self._tables = tables

    def extract_tables(self, settings=None):
        return self._tables


class _BoomPage:
    def extract_tables(self, settings=None):
        raise RuntimeError("dense page blew up table detection")


def test_wrapper_prefixes_header_and_joins_blocks():
    out = _tables_markdown(_FakePage([
        [["A", "B"], ["1", "2"]],
        [["C", "D"], ["3", "4"]],
    ]))
    assert out.startswith("\n\n[structured tables]\n\n")
    assert "| A | B |" in out and "| C | D |" in out


def test_wrapper_returns_empty_when_no_valid_tables():
    # A detected-but-degenerate table (single column) → nothing appended.
    assert _tables_markdown(_FakePage([[["only"], ["one"]]])) == ""
    assert _tables_markdown(_FakePage([])) == ""


def test_wrapper_swallows_extraction_errors():
    # Table detection must never break text extraction.
    assert _tables_markdown(_BoomPage()) == ""
