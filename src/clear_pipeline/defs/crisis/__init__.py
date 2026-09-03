"""Crisis-enrichment Dagster drain (ingest-free, queue-driven).

Ported from clear-pipeline's Celery ``enrich_crisis`` task as part of the
Celery→Dagster consolidation. clear-api sets every crisis to
``enrichmentStatus = PENDING`` on write and exposes a ``pendingCrises`` queue +
``markCrisisEnriched`` completion mutation; the ``enrich_crises`` asset here is
the consumer that drains that queue.

Per PENDING crisis it:

  1. gathers the linked events (full detail — title / types / severity /
     location metadata) via ``get_event_for_crisis``,
  2. runs a knowledgebase RAG search scoped to the crisis's country + event
     types (the grounding the legacy Celery task never had), and
  3. generates, RAG-grounded, the narrative (title + summary), forward-looking
     scenarios, and NRC-SAF needs analysis; computes ``populationInArea``;
     writes them back; enqueues translation; and flips the crisis to ENRICHED.

The drain mirrors ``defs/signals/stages.py``: a single-flight Redis lock,
per-crisis lock + failure isolation, and a sensor that ticks it on an interval
(crises materialise no ingest asset, so eager automation never fires for them).
Auto-discovered by ``load_from_defs_folder``.
"""
