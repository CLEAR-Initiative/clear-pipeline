"""Hotline enrichment Dagster drain (ingest-free, queue-driven).

clear-api stages WhatsApp hotline messages in `ground_messages` (one
placeholder `ground_threads` row per message, V1) with `classification =
NULL` until labelled. The `ground_hotline_enrich` asset here is the
consumer: per active hotline `groundSource`, it drains messages awaiting
classification, asks an LLM for a classification/headline/severity/
disaster-type guess and runs the text geoparser for a location candidate,
then writes the classification back (`upsertGroundMessageClassifications`)
and the rest as a draft on the thread (`upsertGroundThreadDrafts`) for a
human reviewer to accept/edit before promotion.

Mirrors `defs/signals/stages.py`: a single-flight Redis lock, per-item
failure isolation, and a poll sensor (hotline messages arrive via a webhook,
not a polled ingest asset, so there's nothing to be eager on — same reason
`defs/signals/stages.py` needs `signals_drain_sensor` for `manual` signals).
Auto-discovered by `load_from_defs_folder`.
"""
