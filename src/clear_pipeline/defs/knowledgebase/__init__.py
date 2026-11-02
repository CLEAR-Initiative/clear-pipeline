"""Source-neutral knowledge-base build over staged KB PDFs.

Downstream of ``reliefweb_fetch`` (and future per-source fetch assets),
triggered by ``kb_fetch_sensor``:

  pdf_text  → chunks  → enriched_chunks  → knowledgebase_upsert

Splitting into four assets rather than one big job means each stage is
independently retriable / replayable via Dagster's UI — the LLM-heavy
enrichment step can be reprocessed against a fixed chunk snapshot
without re-extracting PDFs.
"""
