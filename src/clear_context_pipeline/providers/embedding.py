"""Embedding provider abstraction — Voyage (v1) + OpenAI-compatible (v2 OSS).

Every embedding call in the knowledge-base pipeline goes through
``make_embedding_provider()`` so switching from Voyage to a
self-hosted / OSS-hosted BGE-M3 endpoint is a `.env` change.

The `knowledgebase.embedding_provider` and `embedding_model` columns
mirror what this module produces, so future re-embed backfills can
filter to the rows still on the old model without touching current
data.

Dimensions:
  Both v1 and v2 default to 1024 dims — Voyage's matryoshka lets us
  truncate `voyage-3-large`'s native 2048 down to 1024 with negligible
  quality loss, and BGE-M3's native size is already 1024. Matching
  dims means the pgvector column stays fixed at ``vector(1024)`` across
  the provider swap; no schema migration needed.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Literal, Protocol

import openai
import voyageai
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


# Voyage exposes `input_type={"document","query"}` to give asymmetric
# retrieval a boost. The knowledge-base ingest always writes documents;
# search-side callers explicitly request `"query"`. OpenAI-compatible
# endpoints (Together, TEI) ignore this — that's fine, we tolerate it.
InputType = Literal["document", "query"]


@dataclass(frozen=True, slots=True)
class EmbeddingResult:
    """One embedding + provenance for a single input string.

    Provenance travels with the vector so the caller can write the same
    provider/model into the ``knowledgebase`` row without a second env
    lookup — keeps the write and the vector generation tied together.
    """
    provider: str
    model: str
    dimensions: int
    embedding: list[float]


class EmbeddingProvider(Protocol):
    """Batch embedding client. Implementations must accept up to the
    provider's per-batch limit; callers are expected to slice larger
    inputs before calling."""

    provider_name: str
    model: str
    dimensions: int

    def embed(
        self,
        texts: list[str],
        *,
        input_type: InputType = "document",
    ) -> list[EmbeddingResult]:
        ...


# ────────────────────────────────────────────────────────────────────
# Voyage — v1 default. Anthropic's recommended embedding model.
# ────────────────────────────────────────────────────────────────────


class VoyageProvider:
    """Voyage AI embeddings via the native SDK.

    ``voyage-3-large`` is the default; ``output_dimension=1024`` invokes
    the model's matryoshka head so we get a 1024-dim vector directly
    (matching the DB column) without a client-side truncate. Batch
    limit is 128 inputs per call.

    Rate-limit throttling:
      Voyage's free tier caps at 3 requests per minute (RPM). At 128
      texts per request that's 384 embeddings/min — enough for a
      typical week's Sudan sitreps but tight enough that a burst of
      unpaced calls provokes 429s. Each 429 also counts against the
      same window, so relying on SDK retry loops actively makes it
      worse. We enforce the ceiling client-side with a monotonic
      inter-request delay: no request goes out until at least
      ``60/RPM`` seconds have elapsed since the previous one.
      ``rpm_limit=0`` disables throttling (paid-tier + unmetered
      endpoints).
    """

    provider_name = "voyage"
    _BATCH_LIMIT = 128

    def __init__(
        self,
        *,
        model: str,
        api_key: str,
        dimensions: int = 1024,
        rpm_limit: int = 3,
    ) -> None:
        self.model = model
        self.dimensions = dimensions
        self._client = voyageai.Client(api_key=api_key)
        # Inter-request floor in seconds. Instance-level state assumes
        # a single provider instance per Dagster asset run (the upsert
        # asset creates one and reuses). If we ever fan out to
        # concurrent workers each with their own instance, this needs
        # to move to a shared coordinator (Redis token bucket, etc.).
        self._min_interval_seconds = (60.0 / rpm_limit) if rpm_limit > 0 else 0.0
        self._last_request_time: float | None = None
        self._throttle_lock = threading.Lock()

    def _throttle(self) -> None:
        """Sleep until we're clear of the client-side RPM ceiling.

        Uses a monotonic clock so a wall-clock jump (NTP adjustment,
        DST, container clock skew) can't wedge us into a permanent
        wait or accidentally waive the throttle.
        """
        if self._min_interval_seconds <= 0:
            return
        with self._throttle_lock:
            now = time.monotonic()
            if self._last_request_time is not None:
                elapsed = now - self._last_request_time
                wait_s = self._min_interval_seconds - elapsed
                if wait_s > 0:
                    logger.info(
                        "[voyage throttle] sleeping %.1fs to stay under %.1f RPM",
                        wait_s, 60.0 / self._min_interval_seconds,
                    )
                    time.sleep(wait_s)
                    now = time.monotonic()
            self._last_request_time = now

    @retry(
        # voyageai raises requests-style exceptions; we filter on the
        # SDK's own error type + a small allowlist of transient ones.
        # Anything else propagates immediately so the caller can log
        # the failed chunk and move on.
        #
        # Even with client-side throttling above, a 429 can still fire
        # if another process shares the same API key. Retry with big
        # backoff so we drop below the shared ceiling.
        retry=retry_if_exception_type(voyageai.error.RateLimitError),
        wait=wait_exponential(multiplier=2, min=20, max=120),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def embed(
        self,
        texts: list[str],
        *,
        input_type: InputType = "document",
    ) -> list[EmbeddingResult]:
        if not texts:
            return []
        if len(texts) > self._BATCH_LIMIT:
            raise ValueError(
                f"Voyage batch size {len(texts)} exceeds SDK limit "
                f"of {self._BATCH_LIMIT}. Caller must slice.",
            )
        self._throttle()
        # Voyage's `input_type` maps to their asymmetric embedding heads;
        # writing docs and querying with different heads is what boosts
        # retrieval quality vs a single symmetric model.
        response = self._client.embed(
            texts=texts,
            model=self.model,
            input_type=input_type,
            output_dimension=self.dimensions,
        )
        return [
            EmbeddingResult(
                provider=self.provider_name,
                model=self.model,
                dimensions=self.dimensions,
                embedding=vec,
            )
            for vec in response.embeddings
        ]


# ────────────────────────────────────────────────────────────────────
# OpenAI-compatible — v2 OSS default via Together AI / TEI / vLLM.
# ────────────────────────────────────────────────────────────────────


class OpenAICompatibleEmbeddingProvider:
    """OpenAI-wire-format embeddings.

    Works with:
      - Together AI (``BAAI/bge-m3`` and friends)
      - Fireworks AI embeddings
      - HuggingFace Text Embeddings Inference (TEI) self-hosted
      - self-hosted vLLM built with an embedding model

    ``dimensions`` is set explicitly because most OSS models don't
    honour the OpenAI ``dimensions`` parameter — we assume the model's
    native dim matches what the caller configured. Passing a mismatched
    value will silently insert wrong-length vectors and blow up on the
    pgvector side; the config module cross-checks against
    ``EMBEDDING_DIMENSIONS``.
    """

    provider_name = "openai_compat"

    def __init__(
        self,
        *,
        model: str,
        base_url: str,
        api_key: str,
        dimensions: int = 1024,
    ) -> None:
        self.model = model
        self.dimensions = dimensions
        self._client = openai.OpenAI(base_url=base_url, api_key=api_key, timeout=600.0)

    @retry(
        retry=retry_if_exception_type(
            (openai.RateLimitError, openai.APIConnectionError, openai.InternalServerError),
        ),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def embed(
        self,
        texts: list[str],
        *,
        input_type: InputType = "document",
    ) -> list[EmbeddingResult]:
        if not texts:
            return []
        # OpenAI-compatible endpoints don't have Voyage's asymmetric
        # input_type. We swallow it silently — retrieval quality is
        # slightly worse than Voyage's but not catastrophic.
        del input_type
        response = self._client.embeddings.create(model=self.model, input=texts)
        return [
            EmbeddingResult(
                provider=self.provider_name,
                model=self.model,
                dimensions=self.dimensions,
                embedding=item.embedding,
            )
            for item in response.data
        ]


# ────────────────────────────────────────────────────────────────────
# Factory
# ────────────────────────────────────────────────────────────────────


def make_embedding_provider() -> EmbeddingProvider:
    """Instantiate the embedding provider configured via env.

    Env vars:
      EMBEDDING_PROVIDER   = "voyage" | "openai_compat"
      EMBEDDING_MODEL      = e.g. "voyage-3-large", "BAAI/bge-m3"
      EMBEDDING_API_KEY    = provider API key
      EMBEDDING_BASE_URL   = required for "openai_compat" only
      EMBEDDING_DIMENSIONS = int (default 1024). MUST match the
                             pgvector column dimension (currently 1024).
      EMBEDDING_RPM_LIMIT  = int (default 3, Voyage's free-tier ceiling).
                             Set to 0 to disable client-side throttling
                             (paid tiers, self-hosted TEI, etc.). Only
                             honoured by VoyageProvider today; other
                             providers ignore it.
    """
    provider = os.environ.get("EMBEDDING_PROVIDER", "voyage").strip().lower()
    model = _require_env("EMBEDDING_MODEL")
    api_key = _require_env("EMBEDDING_API_KEY")
    dimensions = int(os.environ.get("EMBEDDING_DIMENSIONS", "1024"))
    rpm_limit = int(os.environ.get("EMBEDDING_RPM_LIMIT", "3"))

    if dimensions != 1024:
        # Prevent the "silent wrong-length insert" failure mode by
        # refusing at config time. Bumping this requires a coordinated
        # pgvector column type change.
        raise ValueError(
            f"EMBEDDING_DIMENSIONS={dimensions} does not match the current "
            "knowledgebase.embedding column dimension (1024). Update the "
            "column type before changing this.",
        )

    if provider == "voyage":
        return VoyageProvider(
            model=model, api_key=api_key, dimensions=dimensions,
            rpm_limit=rpm_limit,
        )
    if provider == "openai_compat":
        base_url = _require_env("EMBEDDING_BASE_URL")
        return OpenAICompatibleEmbeddingProvider(
            model=model, base_url=base_url, api_key=api_key, dimensions=dimensions,
        )
    raise ValueError(
        f"Unsupported EMBEDDING_PROVIDER={provider!r}. "
        "Expected 'voyage' or 'openai_compat'.",
    )


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing env var {name}. Set it in .env or export it.")
    return value
