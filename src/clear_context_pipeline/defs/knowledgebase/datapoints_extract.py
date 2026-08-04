"""Extract structured humanitarian datapoints from each week's reports.

Phase 1 of the datapoint pipeline (docs/humanitarian-datapoint-extraction.md).
For each report in ``reliefweb_weekly_pdf_text``:

  1. Read the concatenated page text back from S3.
  2. Run the 6 domain-partitioned LLM calls in sequence, sharing a
     prompt-cached document-level prefix so calls 2..6 read at cache
     rate (5% of full input cost).
  3. Post-process: resolve location refs via clear-api, compute the
     denormalised hot totals (total_affected / total_displaced /
     total_killed), collect union sets.
  4. Debug-snapshot the merged JSON to S3 for replay.
  5. Hand off to clear-api's ``upsertReportDatapoints`` mutation.

Failure isolation:
  - A parse failure in one domain writes ``null`` for that key in the
    merged blob and continues with the others. The operator can
    re-run a targeted extraction on that report + domain later.
  - A report with ALL six domains failed is skipped entirely — nothing
    to write.
"""

import json
import logging
import os
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dotenv import load_dotenv
from pydantic import BaseModel

from clear_context_pipeline.defs.knowledgebase.datapoints_schemas import (
    DOMAINS,
    SCHEMA_VERSION,
    LocationRef,
)
from clear_context_pipeline.providers import (
    clear_api,
    load_guardrails,
    make_llm_provider,
)

load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

logger = logging.getLogger(__name__)

COUNTRY_ISO3 = "sdn"
FORMAT_SLUG = "situation-report"

# S3 debug snapshot prefix — mirrors the vector pipeline's convention
# (`reliefweb/kb/…`) so both layers' artefacts live in one namespace.
S3_DATAPOINTS_PREFIX = f"reliefweb/kb/datapoints/{COUNTRY_ISO3}/{FORMAT_SLUG}"


# ── Plausibility crisis briefs (ADR-0004 §4) ──────────────────────────
# A compact, stable per-country baseline the extractor weighs a report's claims
# against when rating the `plausibility_in_context` credibility criterion — so an
# order-of-magnitude-off figure ("12 million displaced in a 2-million state") is
# caught, instead of the model judging plausibility from the single document in
# isolation. Keyed by ISO3 (the report's `primary_country`); a country without a
# curated brief falls back to `_GENERIC_CRISIS_BRIEF`, so this works for every
# supported country and new briefs can be added incrementally.
#
# NOTE: the magnitudes below are best-effort baselines and should be reviewed by
# a domain expert (and refreshed periodically). Keep each to a few sentences of
# well-established figures; it rides in the cached system prefix (paid once per
# report). A future option is to derive these from the location_metadata layer
# (WorldPop population + latest DTM/IPC/HNO totals) instead of hardcoding.
_CRISIS_BRIEFS: dict[str, str] = {
    "sdn": (
        "Sudan (2023–2026 conflict): population ~48 million. The SAF–RSF war since "
        "April 2023 is the world's largest displacement crisis — order of 10–11 "
        "million IDPs and 2–3 million refugees to neighbouring countries (Chad, "
        "Egypt, South Sudan, Ethiopia). ~24–25 million people in need; roughly half "
        "the population acutely food insecure (IPC phase 3+), with famine (IPC 5) "
        "confirmed in parts of Darfur. Worst-affected: Khartoum, the Darfur states, "
        "the Kordofans, Gezira. National totals run to the millions; a single state "
        "or locality is typically tens of thousands to low millions. Treat a "
        "single-source figure far outside these magnitudes, with no explanation, as "
        "implausible."
    ),
    "afg": (
        "Afghanistan (post-2021): population ~41 million. Since the August 2021 "
        "Taliban takeover and economic collapse, ~23 million people are in need and "
        "roughly half the population is acutely food insecure (IPC phase 3+). "
        "Protracted internal displacement of ~3–4 million, plus large-scale returnee "
        "flows — millions returning/deported from Pakistan and Iran (2023–2025). "
        "Compounded by drought and recurring earthquakes (e.g. Herat 2023). National "
        "totals run to the millions; a single province is typically tens of thousands "
        "to low millions. Treat figures far outside these magnitudes, unexplained, as "
        "implausible."
    ),
    "ven": (
        "Venezuela (protracted crisis): population ~28 million. Prolonged political / "
        "economic collapse has driven the region's largest displacement — ~7.7 million "
        "refugees and migrants OUTWARD (mainly Colombia, Peru, Ecuador, Chile, Brazil); "
        "this crisis is defined by cross-border outflow more than internal displacement. "
        "~7 million people in need inside the country amid health-system and service "
        "collapse. National totals run to the millions; a state/municipality is "
        "typically tens of thousands to low millions. Treat figures far outside these "
        "magnitudes, unexplained, as implausible."
    ),
}

# Fallback when no country baseline is on file — keeps the criterion meaningful
# (internal-consistency-based) rather than defaulting everything to plausible.
_GENERIC_CRISIS_BRIEF = (
    "No country baseline is on file. Judge plausibility from internal consistency "
    "and general humanitarian magnitudes; flag figures that contradict other "
    "figures in the report or are orders of magnitude apart from related ones."
)


def _crisis_brief(country_iso3: str) -> str:
    """The plausibility baseline for a country (ISO3), or a generic fallback."""
    return _CRISIS_BRIEFS.get(country_iso3.lower(), _GENERIC_CRISIS_BRIEF)


# The system prompt is stable across all six domain calls — that's what
# Anthropic's prompt cache keys off. Keep everything domain-specific in
# the user message so the cache actually hits.
SYSTEM_PROMPT_TEMPLATE = (
    "You are a humanitarian information extractor working on Norwegian "
    "Refugee Council (NRC) situation reports. You will be given the "
    "FULL text of one report plus a specific extraction task per call.\n"
    "\n"
    "Rules that apply to every call:\n"
    "- Extract ONLY what the report explicitly states. Do not infer "
    "  numbers, dates, or locations that aren't in the text. When "
    "  something is missing, leave the field null.\n"
    "- Every numeric value must be wrapped in the provenance envelope: "
    "  {{ value, unit, confidence, source_quote, chunk_index, "
    "  page_number, scope_location_name, source_name, credibility }}. "
    "  `source_quote` must "
    "  be a verbatim sentence from the report — a substring, not a paraphrase. "
    "  Always set `chunk_index` to null: it is filled automatically after "
    "  extraction by matching your `source_quote` to the report's chunks — do "
    "  not guess it. `page_number` (1-indexed) comes from the nearest "
    "  `[page N]` marker.\n"
    "- SOURCE ATTRIBUTION: set `source_name` to the organisation the number is "
    "  attributed to IN THE TEXT — 'according to IOM DTM', 'WHO reports', 'per "
    "  OCHA figures' -> the org name ('IOM DTM', 'WHO', 'OCHA'). Emit the NAME "
    "  only. Set it null when the figure names no distinct source; do NOT "
    "  default to the report's own publisher. Never emit `source_id` — like "
    "  the resolved location id, it is filled in after extraction.\n"
    "- PER-FIGURE CREDIBILITY (`credibility`): leave null for a typical figure "
    "  — it inherits the report-wide credibility you rate in "
    "  `narrative_and_confidence.information_credibility`. Set a criterion "
    "  (met/partial/unmet) ONLY where THIS figure differs from the document: a "
    "  precisely-sourced, well-specified figure in an otherwise vague report, "
    "  or a suspiciously round/unattributed number in a credible one.\n"
    "- FIGURE SCOPE: for every numeric value, set `scope_location_name` to "
    "  the ONE place that number is a total FOR — the area it counts, NOT "
    "  every place the report mentions. A report framed nationally may state "
    "  a figure for a single state or town; the scope is that state or town. "
    "  If a figure is explicitly a combined total across several named areas, "
    "  use their common parent (e.g. three Darfur states -> \"Darfur\"). If "
    "  the figure cannot be tied to one place, set it null — do NOT default "
    "  to the country or the first place named. Emit the place NAME only; "
    "  never an admin level, and never the resolved id.\n"
    "- `confidence` is a tier: verified > reported > estimated > media > "
    "  unverified. Use `verified` only when the report explicitly attributes "
    "  the figure to a UN or government mission verification. `reported` "
    "  covers DTM, cluster leads, and named humanitarian partners.\n"
    "- ISO dates only (YYYY-MM-DD). If the report gives a month/year, "
    "  set the first-of-month date and drop the confidence one tier.\n"
    "- Locations: prefer OCHA pcode when the report cites it; else give "
    "  the plain place name. Set `admin_level` (0..3) when you can tell.\n"
    "- Ignore boilerplate: cover pages, contact blocks, footers, ToCs.\n"
    "- PLAUSIBILITY: when rating the `plausibility_in_context` credibility "
    "  criterion (in `narrative_and_confidence`, or a per-figure `credibility` "
    "  override), weigh the report's figures against the COUNTRY BASELINE below. "
    "  A claim far outside those magnitudes with no explanation is `unmet` "
    "  (or `partial`), not `met`; figures consistent with the baseline are `met`.\n"
    "\n"
    "---\n"
    "COUNTRY BASELINE (for plausibility only; cached):\n"
    "{crisis_brief}\n"
    "---\n"
    "FULL REPORT (cached; do not repeat back to me):\n"
    "{doc_text}\n"
    "---"
)


def _s3_client():
    from clear_context_pipeline.providers.s3 import s3_client

    return s3_client()


def _debug_key(report_id: str) -> str:
    return f"{S3_DATAPOINTS_PREFIX}/{report_id}.json"


def _read_doc_text(s3, bucket: str, key: str) -> str:
    """Concatenate the report's pages into one prompt-friendly string.
    Same shape the vector pipeline's contextualization step uses, so
    the prompt cache can hit across both pipelines when the same doc
    is being processed."""
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    pages = [json.loads(line) for line in body.splitlines() if line]
    return "\n\n".join(f"[page {p['page_num']}]\n{p['text']}" for p in pages)


def _domain_user_prompt(domain_name: str, schema: type[BaseModel]) -> str:
    """Per-domain user message. Keep it short — the doc text lives in
    the cached system block, so we don't repeat it here."""
    return (
        f"Extract the `{domain_name}` datapoints from the report above. "
        f"Return a JSON object matching the {schema.__name__} schema. "
        "Every numeric leaf uses the NumericField provenance envelope. "
        "Leave any field null if the report does not state it."
    )


def _run_domain(
    llm,
    doc_text: str,
    domain_name: str,
    schema: type[BaseModel],
    *,
    cache_key: str,
    country_iso3: str | None = None,
) -> BaseModel:
    """One domain call. cache_key is the same across all six domains
    for a given report so Anthropic's prompt cache is reused. `country_iso3`
    selects the plausibility crisis brief; None falls back to COUNTRY_ISO3."""
    return llm.complete_structured(
        system=SYSTEM_PROMPT_TEMPLATE.format(
            doc_text=doc_text, crisis_brief=_crisis_brief(country_iso3 or COUNTRY_ISO3),
        ),
        user=_domain_user_prompt(domain_name, schema),
        schema=schema,
        max_tokens=4096,
        cache_key=cache_key,
    )


def _collect_location_refs(obj: Any, out: list[LocationRef]) -> None:
    """Walk the merged domain blob looking for LocationRef-shaped
    dicts. Refs live inside nested structures (per-sector, per-flow,
    per-access-entry) so a recursive walk is cleaner than domain-
    specific traversal.

    A dict is treated as a LocationRef when it has any of the ref's
    fields and no fields foreign to the schema — cheap heuristic.
    """
    if isinstance(obj, dict):
        keys = set(obj.keys())
        ref_fields = {"pcode", "name", "admin_level"}
        if keys and keys.issubset(ref_fields):
            try:
                out.append(LocationRef.model_validate(obj))
            except Exception:  # noqa: BLE001
                pass
            return
        for v in obj.values():
            _collect_location_refs(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _collect_location_refs(v, out)


def _resolve_all_locations(
    refs: list[LocationRef],
) -> tuple[list[str], list[str]]:
    """De-dupe and resolve. Returns (resolved_ids, unresolved_pcodes)."""
    # De-dupe by (pcode, name, level) tuple — a report may cite the same
    # place across many fields; hitting clear-api once per unique tuple
    # keeps latency bounded even for locations-heavy reports.
    seen: dict[tuple[str | None, str | None, int | None], str | None] = {}
    for ref in refs:
        key = (ref.pcode, ref.name, ref.admin_level)
        if key in seen:
            continue
        try:
            seen[key] = clear_api.resolve_location(
                pcode=ref.pcode, name=ref.name, admin_level=ref.admin_level,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[DATAPOINTS] resolve_location hiccup pcode=%s name=%s: %s",
                ref.pcode, ref.name, exc,
            )
            seen[key] = None

    resolved_ids: list[str] = []
    unresolved_pcodes: list[str] = []
    for (pcode, _name, _level), resolved in seen.items():
        if resolved:
            resolved_ids.append(resolved)
        elif pcode:
            unresolved_pcodes.append(pcode)
    return (sorted(set(resolved_ids)), sorted(set(unresolved_pcodes)))


def _collect_numeric_fields(obj: Any, out: list[dict]) -> None:
    """Walk the merged blob collecting NumericField dicts — identified by
    the `scope_location_name` key, which only NumericField carries. A
    NumericField is a leaf (no nested NumericFields), so we don't recurse
    into one once found."""
    if isinstance(obj, dict):
        if "scope_location_name" in obj and "value" in obj:
            out.append(obj)
            return
        for v in obj.values():
            _collect_numeric_fields(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _collect_numeric_fields(v, out)


def _resolve_figure_scopes(merged: Any) -> tuple[int, int, int]:
    """Resolve each numeric figure's `scope_location_name` to a
    `locations` id, writing it into `scope_location_id` in place.

    Figure Scope (ADR-0002): the LLM emits the place name per
    figure; here we map name -> id via the same resolver the report-level
    locations use — name-only, since the LLM does not emit an admin level
    (level/ancestors are intrinsic to the id and looked up by the
    aggregator, #273). `scope_location_id` is overwritten unconditionally;
    the LLM must not supply it.

    A null id — because the LLM abstained (no name) or the name didn't
    resolve — marks the figure unscoped, so the aggregator excludes it
    from cross-report roll-up (matching the rule for unresolved locations).

    Returns (figures, figures_with_name, figures_resolved) so the caller
    can log the resolver-match rate.
    """
    fields: list[dict] = []
    _collect_numeric_fields(merged, fields)

    # Resolve each distinct name once — a report re-states the same scope
    # across many figures; one clear-api hit per unique name.
    cache: dict[str, str | None] = {}
    for f in fields:
        name = f.get("scope_location_name")
        if not isinstance(name, str) or not name.strip():
            continue
        key = name.strip()
        if key not in cache:
            try:
                cache[key] = clear_api.resolve_location(name=key)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[DATAPOINTS] scope resolve hiccup name=%s: %s", key, exc,
                )
                cache[key] = None

    figures = with_name = resolved = 0
    for f in fields:
        figures += 1
        name = f.get("scope_location_name")
        rid: str | None = None
        if isinstance(name, str) and name.strip():
            with_name += 1
            rid = cache.get(name.strip())
        f["scope_location_id"] = rid  # unconditional — never trust an LLM id
        if rid:
            resolved += 1
    return figures, with_name, resolved


def _resolve_figure_sources(merged: Any) -> tuple[int, int]:
    """Resolve each numeric figure's `source_name` (the org the number is
    attributed to in the text) to a `data_sources` id, writing it into
    `source_id` in place. Mirrors `_resolve_figure_scopes` (ADR-0004).

    Uncited figures (no `source_name`) keep `source_id = None`; the aggregator
    then attributes them to the report's publisher. `source_id` is overwritten
    unconditionally — the LLM must not supply it.

    Returns (figures_with_source_name, figures_resolved) for logging.
    """
    fields: list[dict] = []
    _collect_numeric_fields(merged, fields)

    # One resolveDataSource call per distinct cited name — a report attributes
    # many figures to the same org (e.g. "IOM DTM" across a displacement table).
    cache: dict[str, str | None] = {}
    for f in fields:
        name = f.get("source_name")
        if not isinstance(name, str) or not name.strip():
            continue
        key = name.strip()
        if key not in cache:
            try:
                cache[key] = clear_api.resolve_data_source(name=key)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[DATAPOINTS] source resolve hiccup name=%s: %s", key, exc,
                )
                cache[key] = None

    with_name = resolved = 0
    for f in fields:
        name = f.get("source_name")
        sid: str | None = None
        if isinstance(name, str) and name.strip():
            with_name += 1
            sid = cache.get(name.strip())
        f["source_id"] = sid  # unconditional — never trust an LLM id
        if sid:
            resolved += 1
    return with_name, resolved


def _dig(obj: Any, *path: str) -> Any:
    """Walk nested dicts safely — returns None the moment any step
    is missing / null. Used to pull hot totals out of the merged blob
    without a stack of if-checks."""
    for step in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(step)
        if obj is None:
            return None
    return obj


def _num_or_none(obj: Any) -> int | None:
    """Extract .value from a NumericField dict, coerced to int for the
    hot columns. Money and rates that shouldn't be int-truncated stay
    inside the JSON blob and are read from there when needed."""
    if not isinstance(obj, dict):
        return None
    v = obj.get("value")
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# ── chunk_index backfill ──────────────────────────────────────────────
# The LLM only sees whole-document text with `[page N]` markers — never the
# chunk boundaries the vector store uses — so it cannot emit a reliable
# `chunk_index` (it's prompted to leave it null). We fill it deterministically
# after extraction by matching each figure's `source_quote` back to the
# report's authoritative chunk artifact. This keeps chunk_index aligned with
# the vector store (its whole purpose: Layer-3 drill-down), where an LLM guess
# would not. page_number stays the durable citation that survives re-chunking.
_CHUNK_MATCH_MIN_RATIO = 0.6  # min longest-common-block / quote-length for a fuzzy hit


def _norm_text(s: Any) -> str:
    """Whitespace-collapsed, lowercased text for tolerant substring/fuzzy match."""
    return " ".join(str(s or "").split()).lower()


def _read_report_chunks(s3, bucket: str, report_id: str) -> list[dict] | None:
    """Fetch the report's authoritative chunks (the vector-store artifact,
    `{chunk_index, page_start, page_end, text}` per line) from S3. Returns
    None when absent — the chunks asset is a sibling of this one (both fan
    out from pdf_text), and a manual doc may skip chunking — so the caller
    degrades to a null chunk_index rather than failing extraction."""
    from clear_context_pipeline.defs.knowledgebase.chunks import _chunks_key

    try:
        body = s3.get_object(Bucket=bucket, Key=_chunks_key(report_id))["Body"].read()
    except Exception as exc:  # noqa: BLE001 — missing/unreadable → degrade gracefully
        logger.info(
            "[DATAPOINTS] no chunk artifact for %s (%s) — chunk_index stays null",
            report_id, exc,
        )
        return None
    chunks: list[dict] = []
    for line in body.decode("utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            chunks.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return chunks or None


def _match_in(quote: str, candidates: list[dict]) -> int | None:
    """Match `quote` against `candidates`: exact substring first (lowest
    chunk_index wins for determinism), then a longest-common-block fuzzy match
    above `_CHUNK_MATCH_MIN_RATIO`."""
    substring_hits = [
        int(c["chunk_index"])
        for c in candidates
        if c.get("chunk_index") is not None and quote in _norm_text(c.get("text"))
    ]
    if substring_hits:
        return min(substring_hits)

    best_idx: int | None = None
    best_ratio = 0.0
    for c in candidates:
        if c.get("chunk_index") is None:
            continue
        text = _norm_text(c.get("text"))
        if not text:
            continue
        match = SequenceMatcher(None, quote, text).find_longest_match(
            0, len(quote), 0, len(text),
        )
        ratio = match.size / len(quote)
        if ratio > best_ratio:
            best_ratio, best_idx = ratio, int(c["chunk_index"])
    return best_idx if best_ratio >= _CHUNK_MATCH_MIN_RATIO else None


def _match_chunk_index(
    source_quote: Any, page_number: Any, chunks: list[dict],
) -> int | None:
    """Find the chunk_index of the chunk that contains `source_quote`.

    The figure's `page_number` (same 1-indexed page space as the `[page N]`
    markers) is a **preference, not a hard filter**: chunks whose
    `[page_start, page_end]` range covers it are searched first — which
    disambiguates the token-overlap case where one sentence spans two adjacent
    chunks — but if that finds nothing we widen to ALL chunks. Otherwise an
    off-by-one `page_number`, or a quote on a page boundary, would null the
    result even when an exact substring hit exists elsewhere."""
    quote = _norm_text(source_quote)
    if not quote:
        return None

    if isinstance(page_number, int):
        page_scoped = [
            c for c in chunks
            if isinstance(c.get("page_start"), int)
            and isinstance(c.get("page_end"), int)
            and c["page_start"] <= page_number <= c["page_end"]
        ]
        if page_scoped:
            idx = _match_in(quote, page_scoped)
            if idx is not None:
                return idx

    return _match_in(quote, chunks)


def _backfill_chunk_indices(merged: Any, chunks: list[dict]) -> tuple[int, int]:
    """Set `chunk_index` on every extracted NumericField by matching its
    `source_quote` to `chunks`. Overwrites any value the LLM emitted (it is
    never authoritative). Returns (figures_with_quote, figures_matched)."""
    fields: list[dict] = []
    _collect_numeric_fields(merged, fields)
    with_quote = 0
    matched = 0
    for f in fields:
        quote = f.get("source_quote")
        if not isinstance(quote, str) or not quote.strip():
            f["chunk_index"] = None
            continue
        with_quote += 1
        idx = _match_chunk_index(quote, f.get("page_number"), chunks)
        f["chunk_index"] = idx
        if idx is not None:
            matched += 1
    return with_quote, matched


class _NothingExtracted(RuntimeError):
    """Raised by :func:`extract_datapoints_for_one_report` when every
    domain call failed — the caller decides whether to skip or bubble
    the failure (weekly asset skips the row, manual op raises `dg.Failure`)."""


def extract_datapoints_for_one_report(
    *,
    report_id: str,
    report_title: str,
    source_url: str,
    published_at: str,
    doc_text: str,
    llm,
    s3=None,
    s3_bucket: str | None = None,
    log_context=None,
    publisher_name: str | None = None,
    publisher_homepage: str | None = None,
    country_iso3: str | None = None,
) -> dict:
    """Run the six domain LLM extractions for one report, resolve
    locations, hoist hot totals, snapshot a debug artefact to S3, and
    upsert into clear-api.

    Extracted from the weekly asset so the manual-document job can
    reuse the same code path — a report that arrives via
    `uploadKnowledgebaseDocument` should get exactly the same
    structured datapoints treatment as a ReliefWeb one.

    Args:
      report_id / report_title / source_url / published_at: report
        identity + provenance passed straight to
        `upsertReportDatapoints`.
      doc_text: concatenated page text with `[page N]` markers.
        Caller is responsible for extraction (from S3 in the weekly
        path, from the manual op's earlier text-extraction in the
        manual path).
      llm: shared LLM provider — the caller pins the model.
      s3, s3_bucket: optional; when both are supplied the function
        writes a debug snapshot to
        `reliefweb/kb/datapoints/<iso3>/<format>/<report_id>.json`.
      log_context: optional Dagster / Python logger. Any object
        exposing `.info` / `.warning` / `.error`. Falls back to the
        module logger when None.
      publisher_name / publisher_homepage: the report's publisher
        (ReliefWeb `report.source`, first entry). Resolved to the
        report-level `sourceId` — the source fallback for any figure
        that cites no distinct origin. None for manual documents.

    Returns:
      Summary dict identical in shape to the weekly asset's per-report
      summary — the caller uses it to log or aggregate.

    Raises:
      _NothingExtracted: all six domains failed. Caller decides UX.
      clear_api.ClearApiError: upsert rejected as non-retryable
        (bad payload, missing FK). Caller must not retry.
    """
    log = log_context or logger

    # ── Domain-partitioned extraction ─────────────────────────────
    merged: dict[str, dict | None] = {}
    domains_ok: list[str] = []
    domains_failed: list[str] = []
    for domain_name, schema in DOMAINS:
        try:
            model_out = _run_domain(
                llm, doc_text, domain_name, schema, cache_key=report_id,
                country_iso3=country_iso3,
            )
            merged[domain_name] = model_out.model_dump(mode="json")
            domains_ok.append(domain_name)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "[%s] domain=%s extraction failed: %s",
                report_id, domain_name, exc,
            )
            merged[domain_name] = None
            domains_failed.append(domain_name)

    if not domains_ok:
        raise _NothingExtracted(f"every domain failed extraction for {report_id}")

    # ── Post-process: locations, figure scopes, event types, totals ─
    refs: list[LocationRef] = []
    _collect_location_refs(merged, refs)
    resolved_ids, unresolved_pcodes = _resolve_all_locations(refs)

    # Figure Scope: resolve each numeric figure's
    # scope_location_name to a locations id, in place on `merged`. Done
    # before both the debug snapshot and the upsert so the stored blob
    # carries the ids.
    scope_figures, scope_named, scope_resolved = _resolve_figure_scopes(merged)
    log.info(
        "[%s] figure scope: %d figures, %d named, %d resolved "
        "(name rate %.0f%%, resolve rate %.0f%%)",
        report_id, scope_figures, scope_named, scope_resolved,
        (100 * scope_named / scope_figures) if scope_figures else 0,
        (100 * scope_resolved / scope_named) if scope_named else 0,
    )

    # chunk_index backfill: match each figure's source_quote to the report's
    # authoritative chunks (see helpers above). Runs before the debug snapshot
    # and the upsert so both carry the filled indices. Skipped (chunk_index
    # left null) when the chunk artifact isn't in S3 yet.
    if s3 is not None and s3_bucket:
        chunks = _read_report_chunks(s3, s3_bucket, report_id)
        if chunks:
            cq_total, cq_matched = _backfill_chunk_indices(merged, chunks)
            log.info(
                "[%s] chunk_index backfill: %d/%d figures matched (%d chunks)",
                report_id, cq_matched, cq_total, len(chunks),
            )

    # Source attribution: resolve each figure's cited source_name -> source_id
    # in place, and the report's publisher -> the report-level sourceId (the
    # fallback for any figure that cites no distinct source). See ADR-0004.
    src_named, src_resolved = _resolve_figure_sources(merged)
    publisher_source_id: str | None = None
    if publisher_name:
        try:
            publisher_source_id = clear_api.resolve_data_source(
                name=publisher_name, homepage=publisher_homepage,
            )
        except Exception as exc:  # noqa: BLE001 — publisher stays null on hiccup
            log.warning(
                "[%s] publisher resolve hiccup name=%s: %s",
                report_id, publisher_name, exc,
            )
    log.info(
        "[%s] source attribution: %d/%d figures cite a source resolved; "
        "publisher=%s -> %s",
        report_id, src_resolved, src_named, publisher_name, publisher_source_id,
    )

    timing = merged.get("timing_and_scope") or {}
    event_types = list(dict.fromkeys(timing.get("event_types") or []))

    total_killed = _num_or_none(_dig(merged, "casualties", "killed", "total"))
    total_displaced = _num_or_none(_dig(merged, "displacement", "idp_stock"))
    # Population Affected (widest circle), not PIN — the two are different
    # populations (clear-context-pipeline ADR-0001). Was overall_pin, which
    # conflated the hot total with People in Need; overall_affected is the
    # real affected figure the extractor now records. Null for most reports.
    total_affected = _num_or_none(_dig(merged, "needs_and_funding", "overall_affected"))

    # ── Debug snapshot — replay-friendly ──────────────────────────
    debug_key: str | None = None
    if s3 is not None and s3_bucket:
        debug_key = _debug_key(report_id)
        debug_payload = {
            "report_id": report_id,
            "schema_version": SCHEMA_VERSION,
            "model": llm.model,
            "domains_ok": domains_ok,
            "domains_failed": domains_failed,
            "data": merged,
            "location_ids": resolved_ids,
            "location_pcodes": unresolved_pcodes,
        }
        try:
            s3.put_object(
                Bucket=s3_bucket, Key=debug_key,
                Body=json.dumps(debug_payload, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as exc:  # noqa: BLE001 — snapshot is non-fatal
            log.warning(
                "[%s] debug snapshot upload failed (continuing): %s",
                report_id, exc,
            )
            debug_key = None

    # ── Upsert into clear-api ─────────────────────────────────────
    result = clear_api.upsert_report_datapoints(
        report_id=report_id,
        report_title=report_title,
        source_url=source_url,
        published_at=published_at,
        reporting_period_start=timing.get("reporting_period_start"),
        reporting_period_end=timing.get("reporting_period_end"),
        location_ids=resolved_ids,
        location_pcodes=unresolved_pcodes,
        event_types=event_types,
        total_affected=total_affected,
        total_displaced=total_displaced,
        total_killed=total_killed,
        data=merged,
        schema_version=SCHEMA_VERSION,
        extracted_by_model=llm.model,
        source_id=publisher_source_id,
    )

    return {
        "report_id": report_id,
        "schema_version": SCHEMA_VERSION,
        "domains_ok": domains_ok,
        "domains_failed": domains_failed,
        "resolved_locations": len(resolved_ids),
        "unresolved_pcodes": len(unresolved_pcodes),
        "s3_debug_key": debug_key,
        "upsert_result": result,
        "reporting_period_start": timing.get("reporting_period_start"),
        "reporting_period_end": timing.get("reporting_period_end"),
    }


@dg.asset(
    group_name="reliefweb_kb",
    # Ordering dep on chunks (not a parameter): extraction backfills each
    # figure's `chunk_index` by reading the report's chunk artifact from S3
    # (see `_backfill_chunk_indices`). Both assets fan out from
    # `reliefweb_weekly_pdf_text`; without this edge they'd race and the
    # chunk file could be absent when extraction runs, leaving chunk_index
    # null. We read the artifact by report_id, so we need the ordering, not
    # the chunks value passed in.
    deps=["reliefweb_weekly_chunks"],
)
def reliefweb_weekly_datapoints(
    context: AssetExecutionContext,
    reliefweb_weekly_pdf_text: list[dict],
) -> list[dict]:
    """One extraction pass per report in this week's ingest.

    Returns a summary list ``[{report_id, domains_ok, domains_failed,
    resolved_locations, s3_debug_key}]`` — the aggregation asset
    (Phase 2) will consume clear-api directly rather than piping
    through Dagster IO, so the summary is for observability only.

    Runs after ``reliefweb_weekly_chunks`` so the chunk artifact exists
    when we backfill ``chunk_index`` (see the decorator's `deps`).
    """
    bucket = os.environ["S3_BUCKET"]
    s3 = _s3_client()
    guardrails = load_guardrails()

    if guardrails.skip_contextualization:
        # We reuse the KB kill-switch to skip datapoint extraction too
        # — same rationale (LLM provider down, budget exhausted).
        context.log.warning(
            "KB_SKIP_CONTEXTUALIZATION is set — skipping datapoint extraction",
        )
        return []

    llm = make_llm_provider("datapoints")

    summaries: list[dict] = []
    reused = 0
    for report in reliefweb_weekly_pdf_text:
        report_id = report["report_id"]

        # Idempotency: skip reports whose datapoints a prior run already
        # extracted + upserted. Each report otherwise costs 6 LLM extraction
        # calls, so a backfill re-run or a resume after a later-stage failure
        # shouldn't re-pay for them. clear-api's report_datapoints row is the
        # source of truth — the S3 debug snapshot is written BEFORE the upsert
        # and so can't confirm the write landed.
        try:
            already_done = clear_api.report_datapoints_exist(report_id)
        except Exception as exc:  # noqa: BLE001 — treat as not-done and extract
            context.log.warning(
                "[%s] datapoint existence check failed (%s) — extracting anyway",
                report_id, exc,
            )
            already_done = False
        if already_done:
            reused += 1
            summaries.append({"report_id": report_id, "reused": True})
            context.log.info("[%s] datapoints already extracted — skipping", report_id)
            continue

        try:
            doc_text = _read_doc_text(s3, bucket, report["s3_text_key"])
        except Exception as exc:
            context.log.warning(
                "s3 fetch failed for %s (%s) — skipping: %s",
                report["s3_text_key"], report_id, exc,
            )
            continue

        try:
            summary = extract_datapoints_for_one_report(
                report_id=report_id,
                report_title=report["report_title"],
                source_url=report["source_url"],
                published_at=report["published_at"],
                doc_text=doc_text,
                llm=llm,
                s3=s3,
                s3_bucket=bucket,
                log_context=context.log,
                publisher_name=report.get("publisher_name"),
                publisher_homepage=report.get("publisher_homepage"),
                country_iso3=report.get("country_iso3"),
            )
        except _NothingExtracted:
            context.log.error(
                "[%s] every domain failed extraction — skipping upsert",
                report_id,
            )
            continue
        except clear_api.ClearApiError as exc:
            context.log.error(
                "[%s] clear-api rejected datapoint upsert (non-retryable): %s",
                report_id, exc,
            )
            continue
        except Exception as exc:  # noqa: BLE001
            context.log.error(
                "[%s] clear-api datapoint upsert failed after retries: %s",
                report_id, exc,
            )
            continue

        summaries.append({
            # Rebuild the flat summary shape the weekly asset used to
            # emit — some fields moved names in the helper (e.g. the
            # helper returns list counts, we surface them as-is).
            "report_id": summary["report_id"],
            "schema_version": summary["schema_version"],
            "domains_ok": summary["domains_ok"],
            "domains_failed": summary["domains_failed"],
            "resolved_locations": summary["resolved_locations"],
            "unresolved_pcodes": summary["unresolved_pcodes"],
            "s3_debug_key": summary["s3_debug_key"],
            "upsert_result": summary["upsert_result"],
            # Carried so the aggregation asset can widen its refresh to cover a
            # retrospective report's OLD bucket, not just the rolling recent
            # window (ADR-0005 §5).
            "reporting_period_end": summary["reporting_period_end"],
        })
        context.log.info(
            "[%s] extracted %d/%d domains, %d locations resolved (%s)",
            report_id, len(summary["domains_ok"]), len(DOMAINS),
            summary["resolved_locations"], llm.model,
        )

    context.add_output_metadata({
        "reports_processed": dg.MetadataValue.int(len(summaries)),
        "reports_reused": dg.MetadataValue.int(reused),
        "schema_version": dg.MetadataValue.text(SCHEMA_VERSION),
        "extraction_model": dg.MetadataValue.text(llm.model),
        "s3_prefix": dg.MetadataValue.text(f"s3://{bucket}/{S3_DATAPOINTS_PREFIX}/"),
    })
    return summaries
