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


_JUDGE_VARS = [
    "LLM_ANALYSIS_JUDGE_PROVIDER",
    "LLM_ANALYSIS_JUDGE_MODEL",
    "LLM_ANALYSIS_JUDGE_API_KEY",
    "LLM_ANALYSIS_JUDGE_BASE_URL",
    "LLM_ANALYSIS_JUDGE_THINK",
    "LLM_ANALYSIS_JUDGE_NO_THINK",
    "LLM_ANALYSIS_JUDGE_REQUIRE_PARAMETERS",
]


def test_judge_role_reasons_even_while_inheriting_generation_no_think(monkeypatch):
    """The judge inherits LLM_ANALYSIS_NO_THINK=true via the role fallback, but an
    explicit LLM_ANALYSIS_JUDGE_THINK=true must still turn reasoning ON (reasoning
    enabled in the request body), not be beaten by the inherited NO_THINK."""
    _clear(monkeypatch, _ANALYSIS_VARS)
    _clear(monkeypatch, _JUDGE_VARS)
    for k, v in {
        # generation role: NO_THINK on (the judge would inherit this)
        "LLM_ANALYSIS_PROVIDER": "openai_compat",
        "LLM_ANALYSIS_MODEL": "deepseek/deepseek-v3.2",
        "LLM_ANALYSIS_API_KEY": "sk-openrouter",
        "LLM_ANALYSIS_BASE_URL": "https://openrouter.ai/api/v1",
        "LLM_ANALYSIS_NO_THINK": "true",
        # judge role: creds inherited, but reasoning explicitly ON
        "LLM_ANALYSIS_JUDGE_THINK": "true",
        "LLM_ANALYSIS_JUDGE_REQUIRE_PARAMETERS": "true",
    }.items():
        monkeypatch.setenv(k, v)

    provider = make_llm_provider("analysis_judge")
    assert provider.role == "analysis_judge"
    assert provider.model == "deepseek/deepseek-v3.2"  # inherited from analysis
    # Reasoning ON, NOT disabled by the inherited NO_THINK.
    assert provider._base_extra_body.get("reasoning") == {"enabled": True}
    assert provider._base_extra_body.get("think") is True


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
