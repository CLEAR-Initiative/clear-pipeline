"""Vision transcription of a cropped figure — the LLM step of infographic
capture (docs/infographic-capture-spec.md §6 C/D + §Phase 5).

Takes the PNG bytes of one cropped region and asks a vision-capable model
(Haiku by default, via the ``vision`` LLM role) to transcribe it into a
structured, embeddable form: a final kind classification (chart/map/table/
infographic/photo), the figure's title / as-of date / unit / cited source /
headline, its data as flat ``rows`` or nested ``groups`` (composite panels like
the South Darfur DTM snapshot), standalone ``callouts``, and a factual prose
``description`` for semantic retrieval.

The model NEVER invents data: every field is null / empty when the figure does
not show it. Transcription is best-effort — any model or parse failure yields
``None`` so the asset can store the crop with a null transcription rather than
dropping the figure.
"""

from __future__ import annotations

import logging
from typing import Literal

from pydantic import BaseModel, Field

from clear_pipeline.providers.llm import make_llm_provider

logger = logging.getLogger(__name__)

# Final figure kinds. MUST stay in lockstep with clear-api's VALID_KINDS on
# upsertReportFigures (reportFigure.resolver.ts) — a kind outside that set 400s
# the figures write. Declaring it a Literal (not a bare str) puts the enum into
# the vision tool's input schema, so the model is grammar-constrained to emit a
# valid kind rather than inventing "logo"/"diagram"/"other". "logo"/"decorative"
# are deliberately absent: region detection drops those upstream.
FigureKind = Literal["chart", "map", "table", "infographic", "photo"]


class FigureRow(BaseModel):
    """One row of a transcribed table / chart series — a label and its value(s).

    ``value`` is kept as a string so "1,240", "12%", "~3.5M" and "n/a" all round-
    trip verbatim (the figure's own formatting is the ground truth; downstream
    parsing owns any numeric coercion). ``columns`` holds extra cells when a row
    has more than the label+value pair (multi-column tables)."""

    label: str
    value: str | None = None
    columns: dict[str, str] = Field(default_factory=dict)
    note: str | None = None


class FigureGroup(BaseModel):
    """A named sub-panel of a composite infographic (e.g. one locality's block in
    a multi-locality DTM snapshot), holding its own rows. Used when a single
    captured image stacks several mini-tables under headings — §6 D."""

    name: str
    rows: list[FigureRow] = Field(default_factory=list)


class FigureTranscription(BaseModel):
    """Structured, embeddable transcription of one cropped figure.

    Every field beyond ``kind`` / ``description`` is optional: a photo carries
    almost none, a data table fills ``rows``, a composite panel fills ``groups``.
    """

    kind: FigureKind
    title: str | None = None
    # The date/period the figure's data describes ("as of 15 Mar 2024",
    # "Q1 2024"), verbatim — distinct from the report's publication date.
    as_of: str | None = None
    # Unit of the values shown, when the figure states one ("individuals",
    # "households", "%", "USD", "mm").
    unit: str | None = None
    # Attribution printed ON the figure (source line / logo caption) — feeds
    # source-id resolution for the figure. Phase 5.
    source: str | None = None
    # The figure's own headline / key-message text, if it carries one. Phase 5.
    headline: str | None = None
    rows: list[FigureRow] = Field(default_factory=list)
    groups: list[FigureGroup] = Field(default_factory=list)
    # Standalone highlighted figures ("2.5M people in need", "43% increase")
    # that aren't part of a table.
    callouts: list[str] = Field(default_factory=list)
    # 2–4 sentence factual summary of what the figure shows, written for
    # semantic search. Always populated.
    description: str = ""


_SYSTEM = """You transcribe a single figure cropped from a humanitarian PDF report \
(ReliefWeb: OCHA, IOM DTM, WFP, UNHCR, etc.) into structured JSON for a search index.

Rules:
- Transcribe ONLY what is visible. Never infer, extrapolate, or invent numbers, \
labels, dates, or sources. If the figure does not show something, leave that field \
null or its list empty.
- Copy values verbatim, including their formatting and symbols ("1,240", "12%", \
"~3.5M", "n/a"). Do not convert or round.
- Classify `kind` as exactly one of: chart, map, table, infographic, photo.
  - map: a geographic map (admin boundaries, choropleth, point/flow overlays), even \
if it also carries numbers or a legend.
  - chart: bar/line/pie/scatter and other plotted data that is not a map.
  - table: a ruled or aligned grid of rows and columns.
  - infographic: a composite panel mixing several mini-charts/tables/icons/callouts \
into one designed graphic (e.g. a DTM situation snapshot).
  - photo: a photograph with no data content.
- For a table or single chart, put the data in `rows` (label + value, plus `columns` \
for extra cells). For a composite `infographic` that stacks several mini-tables under \
sub-headings, put each sub-panel in `groups` (name + its rows) instead.
- `callouts`: standalone highlighted statistics not part of any table \
("2.5M people in need").
- `title`, `as_of` (the date/period the data describes), `unit`, `source` (attribution \
printed on the figure), `headline` (the figure's own key-message text): fill each only \
if visibly present, else null.
- `description`: 2–4 factual sentences stating what the figure shows and its key \
numbers, written for semantic search. Always fill this."""


def flatten_transcription(t: FigureTranscription) -> str:
    """Flatten a transcription into a single text blob — title + headline +
    description + callouts + row/group labels. Used two ways: as the enrichment
    input for figure tagging, and as the ``embedded_text`` when a figure is
    merged into the knowledge base (so a figure is retrievable by its own
    numbers/labels alongside body text). Empty when the figure carries no text."""
    parts: list[str] = []
    for v in (t.title, t.headline, t.description):
        if v:
            parts.append(v)
    parts.extend(t.callouts)
    for row in t.rows:
        cells = [row.label, row.value or ""] + list(row.columns.values())
        parts.append(" ".join(c for c in cells if c))
    for group in t.groups:
        parts.append(group.name)
        for row in group.rows:
            parts.append(f"{row.label} {row.value or ''}".strip())
    return "\n".join(p for p in parts if p).strip()


def _user_prompt(kind_hint: str | None, page_context: str | None) -> str:
    parts = [
        "Transcribe the attached figure into the structured schema.",
    ]
    if kind_hint:
        parts.append(
            f"A cheap structural detector guessed this region is a '{kind_hint}', "
            "but decide the final `kind` yourself from the image.",
        )
    if page_context:
        # A little surrounding page text helps resolve an untitled figure or an
        # ambiguous unit — but the IMAGE is authoritative, not this text.
        snippet = page_context.strip()[:800]
        parts.append(
            "Surrounding page text (context only — transcribe the image, not this):\n"
            f"\"\"\"{snippet}\"\"\"",
        )
    return "\n\n".join(parts)


def transcribe_figure(
    *,
    png_bytes: bytes,
    kind_hint: str | None = None,
    page_context: str | None = None,
    media_type: str = "image/png",
    max_tokens: int = 2048,
) -> FigureTranscription | None:
    """Transcribe one cropped figure to a ``FigureTranscription``.

    ``kind_hint`` is the region detector's structural guess (image/table/page) —
    passed to the model as a hint only; the model sets the final ``kind``.
    ``page_context`` is optional surrounding page text to disambiguate an
    untitled figure. Returns ``None`` on any model/parse failure (the caller
    stores the crop with a null transcription rather than dropping it)."""
    try:
        return make_llm_provider("vision").complete_structured(
            system=_SYSTEM,
            user=_user_prompt(kind_hint, page_context),
            schema=FigureTranscription,
            max_tokens=max_tokens,
            images=[(media_type, png_bytes)],
        )
    except NotImplementedError:
        # The resolved `vision` provider can't take image input (an
        # openai_compat/Ollama backend). That's a CONFIG error, not a transient
        # one: EVERY figure will null out. Log loudly so an all-null capture run
        # is diagnosable instead of looking successful. Point at the fix.
        logger.error(
            "[VISION] the resolved 'vision' provider does not support image input "
            "(openai_compat/Ollama). All figure transcriptions will be null. Set "
            "LLM_VISION_* (or LLM_NARRATIVE_*, the fallback) to an Anthropic model.",
        )
        return None
    except Exception as exc:  # noqa: BLE001 — best-effort; never fail the asset
        logger.warning(
            "[VISION] figure transcription failed (kind_hint=%s): %s",
            kind_hint, exc,
        )
        return None
