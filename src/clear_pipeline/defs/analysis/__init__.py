"""On-demand analysis Dagster drain (ingest-free, queue-driven) — ADR-0007 §4.

clear-api enqueues a ``analysisRequest`` (a frame) when a user asks for an
on-demand analysis and exposes a ``pendingAnalyses`` queue +
``markAnalysisRequestGenerated`` / ``markAnalysisRequestFailed`` completion
mutations. The ``drain_analysis_requests`` asset here is the consumer.

Per PENDING request it:

  1. builds the ``Frame`` (canonicalised location/event/sector + window),
  2. resolves the retrieval scope (``build_rag_filters`` — literal locations +
     a time filter for the frame's window, decision #1) and the structured
     datapoints when available (clear-api returns a precomputed bucket or an
     on-demand roll-up for a single-location frame, decision #2), and
  3. runs the unified generator (``generate_and_upsert_for_frame``) and marks
     the request GENERATED / FAILED.

Uses the same queue-drain pattern as the signal drains: a single-flight Redis
drain lock and a sensor that ticks it on an interval (requests materialise no
ingest asset, so eager automation never fires). Auto-discovered by
``load_from_defs_folder``.
"""
