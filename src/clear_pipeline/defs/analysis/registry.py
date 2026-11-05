"""Prompt / model / schema versioning for the analysis module.

Every analysis artifact is stamped ``(prompt_version, model, schema_version)``
so results stay auditable and comparable when wording or providers shift
(doc §1.5, §12). The *model* is not pinned here — it's resolved at run time from
``LLM_ANALYSIS_*`` env via ``make_llm_provider(ANALYSIS_LLM_ROLE)`` — but the
model string that actually served is recorded on each artifact.

Bumping ``SCHEMA_VERSION`` or ``PROMPT_VERSION`` re-keys artifacts (versions
coexist; trend reads never mix them — doc §12, §13).

PR-1 establishes the versions + the stamp helper; prompt text and output
schemas are wired in later PRs.
"""

from __future__ import annotations

from typing import TypedDict

# The LLM role this module runs under (see providers/llm.py LLMRole +
# _ROLE_ENV_FALLBACK). Driven by LLM_ANALYSIS_* env; falls back to the
# extraction role's creds when unset.
ANALYSIS_LLM_ROLE = "analysis"

# Output-schema version for descriptive artifacts. Bump on any breaking change
# to a node's output shape; coexists with prior versions in storage.
SCHEMA_VERSION = "v1"

# Prompt-template version. Bump when prompt wording changes enough that outputs
# are no longer comparable to earlier runs.
PROMPT_VERSION = "v1"


class ProvenanceStamp(TypedDict):
    """Stamped on every analysis artifact for auditability (doc §11)."""

    prompt_version: str
    model: str
    schema_version: str


def provenance_stamp(model: str) -> ProvenanceStamp:
    """Build the provenance stamp for an artifact produced by ``model``.

    ``model`` is the model string that actually served the call (read from the
    provider after the call), so a fallback to a different model is recorded
    faithfully.
    """
    return ProvenanceStamp(
        prompt_version=PROMPT_VERSION,
        model=model,
        schema_version=SCHEMA_VERSION,
    )
