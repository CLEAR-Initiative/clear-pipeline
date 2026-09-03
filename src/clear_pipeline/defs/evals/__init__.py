"""Model-replacement eval: free OpenRouter models vs Claude.

Scores how closely each free candidate model reproduces Claude's output on the
three cheap LLM steps — context, extraction, datapoints — over a fixed set of
non-sensitive ReliefWeb PDFs in ``evals/reports/``. Narrative is out of scope
(it needs cross-report aggregation that standalone eval PDFs don't have).

Reference = the production Claude provider per role (``make_llm_provider``).
Candidates = free ``:free`` OpenRouter slugs (see ``candidates.py``).
See ``assets.py`` for the Dagster asset graph and ``evals/README.md`` to run it.
"""
