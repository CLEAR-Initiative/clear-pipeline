"""Weekly knowledge-base build over ReliefWeb Sudan PDFs.

Downstream of ``reliefweb_weekly_pdfs_in_s3``:

  pdf_text  → chunks  → enriched_chunks  → knowledgebase_upsert

Splitting into four assets rather than one big job means each stage is
independently retriable / replayable via Dagster's UI — the LLM-heavy
enrichment step can be reprocessed against a fixed chunk snapshot
without re-extracting PDFs.
"""
