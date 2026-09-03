"""Provider abstractions for the knowledge-base pipeline.

The knowledge-base ingest uses two families of external calls:
  - LLM chat completions (contextualization + parameter extraction)
  - Embeddings (the vector we store in `knowledgebase.embedding`)

We front both behind role-scoped factories so switching between the v1
stack (Claude + Voyage) and the v2 stack (open-source models via
Together / Fireworks / self-hosted vLLM) is a `.env` change, not code.

Public surface:
  - LLMProvider, make_llm_provider
  - EmbeddingProvider, make_embedding_provider
  - guardrails module for env-driven cost / volume ceilings
"""

from clear_pipeline.providers import clear_api
from clear_pipeline.providers.embedding import (
    EmbeddingProvider,
    EmbeddingResult,
    make_embedding_provider,
)
from clear_pipeline.providers.guardrails import (
    Guardrails,
    RunBudget,
    load_guardrails,
)
from clear_pipeline.providers.llm import (
    LLMProvider,
    LLMRole,
    make_llm_provider,
)

__all__ = [
    "EmbeddingProvider",
    "EmbeddingResult",
    "Guardrails",
    "LLMProvider",
    "LLMRole",
    "RunBudget",
    "clear_api",
    "load_guardrails",
    "make_embedding_provider",
    "make_llm_provider",
]
