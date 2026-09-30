"""Hotline enrichment + transcription Dagster drains (ingest-free,
queue-driven).

clear-api stages WhatsApp hotline messages in `ground_messages` (one
placeholder `ground_threads` row per message, V1) with `classification =
NULL` until labelled. Two independent drains consume that queue:

  - `ground_hotline_enrich` (stages.py): per active hotline `groundSource`,
    drains messages awaiting classification, asks an LLM for a
    classification/headline/severity/disaster-type guess and runs the text
    geoparser (country-scoped) to suggest an existing location, then writes
    the classification back (`upsertGroundMessageClassifications`) and the
    rest as a draft on the thread (`upsertGroundThreadDrafts`) for a human
    reviewer to accept/edit before promotion. A voice note with no
    transcript yet (`hasVoice` true, `transcript` null) is held out until
    `ground_transcribe` has run — enrichment on a voice message's usually-
    empty `text` field would waste an LLM call on no content.
  - `ground_transcribe` (transcribe.py): per active hotline `groundSource`,
    drains messages with an untranscribed voice note, fetches each
    attachment from S3 and transcribes it via Whisper
    (`providers/stt.py`), then writes the transcript back
    (`upsertGroundMessageTranscripts`). Separate asset from enrichment so a
    slow/expensive transcription doesn't block classification throughput
    for text-only messages on the same source.

`ground_hotline_backfill_drafts` (backfill.py) is a manual-only, one-off
asset: it writes drafts for threads classified before the enrichment drain
existed, which the drain (unclassified messages only) never revisits.

Both drains mirror `defs/signals/stages.py`: a single-flight Redis lock, per-item
failure isolation (a message that keeps failing is marked failed in
clear-api — out of the queue, visible in the inbox, retryable — instead of
re-billed every tick; see `attempts.py`), and a poll sensor (hotline messages arrive via a webhook,
not a polled ingest asset, so there's nothing to be eager on — same reason
`defs/signals/stages.py` needs `signals_drain_sensor` for `manual` signals).
Auto-discovered by `load_from_defs_folder`.
"""
