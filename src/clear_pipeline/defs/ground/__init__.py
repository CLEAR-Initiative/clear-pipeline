"""Hotline enrichment + transcription Dagster drains (ingest-free,
queue-driven).

clear-api stages WhatsApp hotline messages in `ground_messages` (one
placeholder `ground_threads` row per message, V1) with `classification =
NULL` until labelled. Two independent drains consume that queue:

  - `ground_hotline_enrich` (stages.py): per active hotline `groundSource`,
    drains messages awaiting classification, asks an LLM for a
    classification/headline/severity/disaster-type guess and runs the text
    geoparser for a location candidate, then writes the classification back
    (`upsertGroundMessageClassifications`) and the rest as a draft on the
    thread (`upsertGroundThreadDrafts`) for a human reviewer to accept/edit
    before promotion. A message with an untranscribed voice note
    (`voiceMediaKeys` non-empty, `transcript` null) is held out until
    `ground_transcribe` has run — enrichment on a voice message's usually-
    empty `text` field would waste an LLM call on no content.
  - `ground_transcribe` (transcribe.py): per active hotline `groundSource`,
    drains messages with an untranscribed voice note, fetches each
    attachment from S3 and transcribes it via Whisper
    (`providers/stt.py`), then writes the transcript back
    (`upsertGroundMessageTranscripts`). Separate asset from enrichment so a
    slow/expensive transcription doesn't block classification throughput
    for text-only messages on the same source.

Both mirror `defs/signals/stages.py`: a single-flight Redis lock, per-item
failure isolation, and a poll sensor (hotline messages arrive via a webhook,
not a polled ingest asset, so there's nothing to be eager on — same reason
`defs/signals/stages.py` needs `signals_drain_sensor` for `manual` signals).
Auto-discovered by `load_from_defs_folder`.
"""
