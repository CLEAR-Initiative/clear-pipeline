"""Signal pipeline ported from clear-pipeline (Celery\u2192Dagster consolidation).

Sources \u2192 events \u2192 alerts. Reuses clear-context-pipeline shared providers
(clear_api._execute, providers.s3, make_llm_provider). Assets live in defs/signals/.
"""
