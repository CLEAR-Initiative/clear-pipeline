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
from pathlib import Path
from typing import Any

import boto3
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
    "  {{ value, unit, confidence, source_quote, chunk_index (may be null), "
    "  page_number }}. `source_quote` must be a verbatim sentence from the "
    "  report — a substring, not a paraphrase.\n"
    "- `confidence` is a tier: verified > reported > estimated > media > "
    "  unverified. Use `verified` only when the report explicitly attributes "
    "  the figure to a UN or government mission verification. `reported` "
    "  covers DTM, cluster leads, and named humanitarian partners.\n"
    "- ISO dates only (YYYY-MM-DD). If the report gives a month/year, "
    "  set the first-of-month date and drop the confidence one tier.\n"
    "- Locations: prefer OCHA pcode when the report cites it; else give "
    "  the plain place name. Set `admin_level` (0..3) when you can tell.\n"
    "- Ignore boilerplate: cover pages, contact blocks, footers, ToCs.\n"
    "\n"
    "---\n"
    "FULL REPORT (cached; do not repeat back to me):\n"
    "{doc_text}\n"
    "---"
)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        region_name=os.environ["S3_REGION"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    )


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
) -> BaseModel:
    """One domain call. cache_key is the same across all six domains
    for a given report so Anthropic's prompt cache is reused."""
    return llm.complete_structured(
        system=SYSTEM_PROMPT_TEMPLATE.format(doc_text=doc_text),
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


@dg.asset(group_name="reliefweb_kb")
def reliefweb_weekly_datapoints(
    context: AssetExecutionContext,
    reliefweb_weekly_pdf_text: list[dict],
) -> list[dict]:
    """One extraction pass per report in this week's ingest.

    Returns a summary list ``[{report_id, domains_ok, domains_failed,
    resolved_locations, s3_debug_key}]`` — the aggregation asset
    (Phase 2) will consume clear-api directly rather than piping
    through Dagster IO, so the summary is for observability only.
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

    llm = make_llm_provider("extraction")

    summaries: list[dict] = []
    for report in reliefweb_weekly_pdf_text:
        report_id = report["report_id"]

        try:
            doc_text = _read_doc_text(s3, bucket, report["s3_text_key"])
        except Exception as exc:
            context.log.warning(
                "s3 fetch failed for %s (%s) — skipping: %s",
                report["s3_text_key"], report_id, exc,
            )
            continue

        # ── Domain-partitioned extraction ─────────────────────────
        merged: dict[str, dict | None] = {}
        domains_ok: list[str] = []
        domains_failed: list[str] = []
        for domain_name, schema in DOMAINS:
            try:
                model_out = _run_domain(
                    llm, doc_text, domain_name, schema, cache_key=report_id,
                )
                merged[domain_name] = model_out.model_dump(mode="json")
                domains_ok.append(domain_name)
            except Exception as exc:  # noqa: BLE001
                context.log.warning(
                    "[%s] domain=%s extraction failed: %s",
                    report_id, domain_name, exc,
                )
                merged[domain_name] = None
                domains_failed.append(domain_name)

        if not domains_ok:
            context.log.error(
                "[%s] every domain failed extraction — skipping upsert",
                report_id,
            )
            continue

        # ── Post-process: locations, event types, hot totals ──────
        refs: list[LocationRef] = []
        _collect_location_refs(merged, refs)
        resolved_ids, unresolved_pcodes = _resolve_all_locations(refs)

        timing = merged.get("timing_and_scope") or {}
        event_types = list(dict.fromkeys(timing.get("event_types") or []))

        # Hot totals — hoisted from the merged blob for cheap dashboard
        # filter/sort. Left as None when the report doesn't headline
        # them; do NOT paper over that with zeroes.
        total_killed = _num_or_none(_dig(merged, "casualties", "killed", "total"))
        total_displaced = _num_or_none(_dig(merged, "displacement", "idp_stock"))
        total_affected = _num_or_none(
            _dig(merged, "needs_and_funding", "overall_pin"),
        )

        # ── Debug snapshot — replay-friendly ──────────────────────
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
                Bucket=bucket, Key=debug_key,
                Body=json.dumps(debug_payload, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as exc:  # noqa: BLE001 — non-fatal for the upsert
            context.log.warning(
                "[%s] debug snapshot upload failed (continuing): %s",
                report_id, exc,
            )

        # ── Upsert into clear-api ─────────────────────────────────
        try:
            result = clear_api.upsert_report_datapoints(
                report_id=report_id,
                report_title=report["report_title"],
                source_url=report["source_url"],
                published_at=report["published_at"],
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
            )
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
            "report_id": report_id,
            "schema_version": SCHEMA_VERSION,
            "domains_ok": domains_ok,
            "domains_failed": domains_failed,
            "resolved_locations": len(resolved_ids),
            "unresolved_pcodes": len(unresolved_pcodes),
            "s3_debug_key": debug_key,
            "upsert_result": result,
        })
        context.log.info(
            "[%s] extracted %d/%d domains, %d locations resolved (%s)",
            report_id, len(domains_ok), len(DOMAINS),
            len(resolved_ids), llm.model,
        )

    context.add_output_metadata({
        "reports_processed": dg.MetadataValue.int(len(summaries)),
        "schema_version": dg.MetadataValue.text(SCHEMA_VERSION),
        "extraction_model": dg.MetadataValue.text(llm.model),
        "s3_prefix": dg.MetadataValue.text(f"s3://{bucket}/{S3_DATAPOINTS_PREFIX}/"),
    })
    return summaries
