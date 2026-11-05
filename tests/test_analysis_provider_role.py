"""The ``analysis`` LLM role resolves from ``LLM_ANALYSIS_*`` env, and falls
back to the ``extraction`` role's creds when its own block is unset (PR-1)."""

from clear_pipeline.providers.llm import make_llm_provider

_ANALYSIS_VARS = [
    "LLM_ANALYSIS_PROVIDER",
    "LLM_ANALYSIS_MODEL",
    "LLM_ANALYSIS_API_KEY",
    "LLM_ANALYSIS_BASE_URL",
    "LLM_ANALYSIS_NO_THINK",
    "LLM_ANALYSIS_REQUIRE_PARAMETERS",
]
_EXTRACTION_VARS = [
    "LLM_EXTRACTION_PROVIDER",
    "LLM_EXTRACTION_MODEL",
    "LLM_EXTRACTION_API_KEY",
    "LLM_EXTRACTION_BASE_URL",
]


def _clear(monkeypatch, names):
    for n in names:
        monkeypatch.delenv(n, raising=False)


def test_analysis_role_resolves_deepseek_via_openrouter(monkeypatch):
    _clear(monkeypatch, _ANALYSIS_VARS)
    for k, v in {
        "LLM_ANALYSIS_PROVIDER": "openai_compat",
        "LLM_ANALYSIS_MODEL": "deepseek/deepseek-v3.2",
        "LLM_ANALYSIS_API_KEY": "sk-openrouter",
        "LLM_ANALYSIS_BASE_URL": "https://openrouter.ai/api/v1",
        "LLM_ANALYSIS_NO_THINK": "true",
        "LLM_ANALYSIS_REQUIRE_PARAMETERS": "true",
    }.items():
        monkeypatch.setenv(k, v)

    provider = make_llm_provider("analysis")
    assert provider.provider_name == "openai_compat"
    assert provider.model == "deepseek/deepseek-v3.2"
    assert provider.role == "analysis"


def test_analysis_role_falls_back_to_extraction_creds_when_unset(monkeypatch):
    # No LLM_ANALYSIS_* block → the factory reads the extraction role's env
    # (safe pre-infra-apply; defaults to the OSS backend, not Anthropic).
    _clear(monkeypatch, _ANALYSIS_VARS)
    _clear(monkeypatch, _EXTRACTION_VARS)
    for k, v in {
        "LLM_EXTRACTION_PROVIDER": "anthropic",
        "LLM_EXTRACTION_MODEL": "claude-haiku-4-5",
        "LLM_EXTRACTION_API_KEY": "sk-anthropic",
    }.items():
        monkeypatch.setenv(k, v)

    provider = make_llm_provider("analysis")
    # Served by the extraction role's creds.
    assert provider.model == "claude-haiku-4-5"
    assert provider.provider_name == "anthropic"
    assert provider.role == "analysis"
