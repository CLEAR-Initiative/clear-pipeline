"""Corpus building + provider construction + per-step runners for the eval.

Everything here reuses the **production** step functions so the eval measures
the real prompts and schemas, not a re-implementation:

  - context     → ``enrich._run_context``
  - extraction  → ``enrich._run_extraction``
  - datapoints  → ``datapoints_extract._run_domain`` over ``DOMAINS``
                  (the domain loop only — NOT ``extract_datapoints_for_one_report``,
                  which would upsert into production clear-api)

The only thing that varies per run is the LLM provider:

  - reference  → ``make_llm_provider(role)`` — exactly what Claude does in
                 production for that role (reads LLM_<ROLE>_* env).
  - candidate  → ``OpenAICompatibleProvider`` pointed at OpenRouter with a
                 free model slug.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from clear_context_pipeline.defs.evals.candidates import Candidate
from clear_context_pipeline.defs.knowledgebase import chunks as _chunks
from clear_context_pipeline.defs.knowledgebase import datapoints_extract as _dp
from clear_context_pipeline.defs.knowledgebase import enrich as _enrich
from clear_context_pipeline.defs.knowledgebase._pdf_extract import (
    extract_pages,
    extract_pages_pypdf,
)
from clear_context_pipeline.providers import make_llm_provider
from clear_context_pipeline.providers.llm import LLMProvider, OpenAICompatibleProvider

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# The eval PDFs and outputs live under <repo root>/evals. In a source checkout
# the repo root is five parents up (evals/ → defs/ → clear_context_pipeline/ →
# src/ → <root>), but that assumption breaks when the package is installed as a
# wheel (no evals/ dir beside site-packages). Resolve robustly: an explicit
# EVAL_DATA_DIR override wins; else walk up looking for an evals/ dir; else fall
# back to the parents[4] guess. This harness is a dev/offline tool, so a missing
# corpus surfaces as build_corpus() → dg.Failure, never a silent wrong path.
def _eval_data_root() -> Path:
    override = os.environ.get("EVAL_DATA_DIR")
    if override:
        return Path(override)
    here = Path(__file__).resolve()
    # Look for the DATA dir (repo-root `evals/`, marked by its tracked README or a
    # populated reports/), NOT any `evals/` — `defs/evals/` is this code package
    # and would otherwise match one level up and win.
    for parent in here.parents:
        evals = parent / "evals"
        if (evals / "README.md").is_file() or (evals / "reports").is_dir():
            return parent
    return here.parents[4]


_REPO_ROOT = _eval_data_root()
REPORTS_DIR = _REPO_ROOT / "evals" / "reports"
CACHE_DIR = _REPO_ROOT / "evals" / "cache"
RESULTS_DIR = _REPO_ROOT / "evals" / "results"


# ────────────────────────────────────────────────────────────────────
# Corpus
# ────────────────────────────────────────────────────────────────────

@dataclass
class EvalReport:
    """One eval PDF, extracted + chunked once (model-independent)."""

    report_id: str
    doc_text: str          # "[page N]\n<text>" joined, as the prod steps expect
    chunks: list[dict]     # {chunk_index, page_start, page_end, text}
    num_pages: int


def _doc_text(pages: list[dict]) -> str:
    """Same "[page N]" concatenation the prod context/datapoints steps use."""
    return "\n\n".join(f"[page {p['page_num']}]\n{p['text']}" for p in pages)


def build_corpus() -> list[EvalReport]:
    """Extract + chunk every PDF in ``evals/reports/``. Deterministic and
    model-independent, so it's done once and shared by reference + all
    candidates. ``report_id`` is the PDF filename stem."""
    reports: list[EvalReport] = []
    for pdf_path in sorted(REPORTS_DIR.glob("*.pdf")):
        pdf_bytes = pdf_path.read_bytes()
        try:
            pages = extract_pages(pdf_bytes)
        except Exception:  # noqa: BLE001 — mirror the prod pypdf fallback
            pages = []
        if not pages:
            pages = extract_pages_pypdf(pdf_bytes)
        if not pages:
            continue
        chunks = _chunks._slice_into_chunks(
            pages,
            chunk_tokens=_chunks.CHUNK_TOKENS,
            overlap_tokens=_chunks.CHUNK_OVERLAP_TOKENS,
        )
        reports.append(EvalReport(
            report_id=pdf_path.stem,
            doc_text=_doc_text(pages),
            chunks=chunks,
            num_pages=len(pages),
        ))
    return reports


# ────────────────────────────────────────────────────────────────────
# Providers
# ────────────────────────────────────────────────────────────────────

def reference_provider(role: str) -> LLMProvider:
    """The production Claude provider for ``role`` — the eval's oracle."""
    return make_llm_provider(role)  # type: ignore[arg-type]


# Free OpenRouter models share a hard 20/min cap across ALL free models (and,
# without account credit, a 50/day cap). 20/min == 1 call per 3s exactly, so we
# pace slightly under that (default 4s ≈ 15/min) through one global minimum-
# interval throttle; this keeps 429s reflecting the model, not the free tier.
# Tune with OPENROUTER_MIN_INTERVAL_SECONDS (0 disables). Caveats: the daily cap
# needs credit (not pacing); and a specific free model can still be rate-limited
# UPSTREAM by its provider (e.g. Google AI Studio) regardless of our pace.
_throttle_lock = threading.Lock()
_last_call_at = 0.0


def _throttle(min_interval: float) -> None:
    global _last_call_at
    if min_interval <= 0:
        return
    with _throttle_lock:
        wait = _last_call_at + min_interval - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_call_at = time.monotonic()


class _ThrottledProvider:
    """Wraps a provider to enforce the global inter-call pace + duck-types the
    ``LLMProvider`` surface the eval calls read."""

    def __init__(self, inner: LLMProvider, min_interval: float) -> None:
        self._inner = inner
        self._min_interval = min_interval
        self.role = inner.role
        self.model = inner.model
        self.provider_name = inner.provider_name

    def complete_structured(self, **kwargs: Any):
        _throttle(self._min_interval)
        try:
            return self._inner.complete_structured(**kwargs)
        except TypeError as exc:
            # OpenRouter/providers occasionally return choices: null (empty or
            # moderated response); the OpenAI client then does choices[0] →
            # cryptic "'NoneType' object is not subscriptable". Re-raise clearly.
            if "subscriptable" in str(exc):
                raise RuntimeError(
                    f"empty response (no choices) from provider for model {self.model}",
                ) from exc
            raise

    def complete_text(self, **kwargs: Any):
        _throttle(self._min_interval)
        try:
            return self._inner.complete_text(**kwargs)
        except TypeError as exc:
            if "subscriptable" in str(exc):
                raise RuntimeError(
                    f"empty response (no choices) from provider for model {self.model}",
                ) from exc
            raise


def candidate_provider(role: str, candidate: Candidate) -> LLMProvider:
    """A free OpenRouter model wearing ``role`` (so prompts/schemas match),
    paced through the shared throttle."""
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENROUTER_API_KEY (set it in .env for evals).")
    inner = OpenAICompatibleProvider(
        role=role,  # type: ignore[arg-type]
        model=candidate.slug,
        base_url=OPENROUTER_BASE_URL,
        api_key=api_key,
        json_schema_mode=candidate.json_schema_mode,
    )
    # Only free models hit the shared 20/min free-tier cap; paid models have
    # far higher limits, so don't slow them down.
    min_interval = (
        float(os.environ.get("OPENROUTER_MIN_INTERVAL_SECONDS", "4.0"))
        if candidate.free else 0.0
    )
    return _ThrottledProvider(inner, min_interval)  # type: ignore[return-value]


# ────────────────────────────────────────────────────────────────────
# Per-step runners — each returns (output, call-stats)
# ────────────────────────────────────────────────────────────────────

@dataclass
class CallStats:
    """Reliability/latency signal per report, aggregated by the scorer."""

    ok: int = 0            # calls that returned a schema-valid object
    failed: int = 0        # calls that raised after retries (timeouts, bad JSON)
    seconds: float = 0.0   # wall-clock across the report's calls
    errors: list[str] = field(default_factory=list)


def run_context(provider: LLMProvider, report: EvalReport) -> tuple[dict, CallStats]:
    """Contextual prefix per chunk. Returns {chunk_index: prefix}."""
    out: dict[str, str] = {}
    stats = CallStats()
    for chunk in report.chunks:
        t0 = time.monotonic()
        try:
            prefix = _enrich._run_context(
                provider, report.doc_text, chunk["text"], cache_key=report.report_id,
            )
            out[str(chunk["chunk_index"])] = prefix
            stats.ok += 1
        except Exception as exc:  # noqa: BLE001 — capture; a failure IS a datapoint
            stats.failed += 1
            stats.errors.append(f"ctx chunk {chunk['chunk_index']}: {exc}")
        finally:
            stats.seconds += time.monotonic() - t0
    return out, stats


def run_extraction(provider: LLMProvider, report: EvalReport) -> tuple[dict, CallStats]:
    """Structured per-chunk tags. Returns {chunk_index: params-dict}.

    Runs on the RAW chunk text (no context prefix) so extraction is scored
    independently of the context step's output — otherwise a bad context
    model would contaminate the extraction score.
    """
    out: dict[str, Any] = {}
    stats = CallStats()
    for chunk in report.chunks:
        t0 = time.monotonic()
        try:
            params = _enrich._run_extraction(provider, chunk["text"])
            out[str(chunk["chunk_index"])] = params.model_dump(mode="json")
            stats.ok += 1
        except Exception as exc:  # noqa: BLE001
            stats.failed += 1
            stats.errors.append(f"extract chunk {chunk['chunk_index']}: {exc}")
        finally:
            stats.seconds += time.monotonic() - t0
    return out, stats


def run_datapoints(provider: LLMProvider, report: EvalReport) -> tuple[dict, CallStats]:
    """The six-domain numeric extraction — the domain loop only, no upsert.

    Returns the merged {domain: blob|None} dict, i.e. exactly the ``data``
    payload production would compute (minus location resolution, which is a
    deterministic clear-api step, model-independent, and so out of scope).
    """
    merged: dict[str, Any] = {}
    stats = CallStats()
    for domain_name, schema in _dp.DOMAINS:
        t0 = time.monotonic()
        try:
            model_out = _dp._run_domain(
                provider, report.doc_text, domain_name, schema,
                cache_key=report.report_id,
            )
            merged[domain_name] = model_out.model_dump(mode="json")
            stats.ok += 1
        except Exception as exc:  # noqa: BLE001
            merged[domain_name] = None
            stats.failed += 1
            stats.errors.append(f"domain {domain_name}: {exc}")
        finally:
            stats.seconds += time.monotonic() - t0
    return merged, stats


# Maps the eval step name → (role for provider construction, runner fn).
STEPS: dict[str, tuple[str, Any]] = {
    "context": ("context", run_context),
    "extraction": ("extraction", run_extraction),
    "datapoints": ("datapoints", run_datapoints),
}
