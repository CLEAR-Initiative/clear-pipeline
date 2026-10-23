# INSO — TODO after written approval

Companion to `docs/inso-spec.md`. Exponential isn't set up locally, so this file stands in for the tracker tickets.

## Before sending the approval request

- [ ] Request lists each use explicitly: (1) raw `.eml` stored in S3 and for how long, (2) INFORMATION sent to a third-party LLM, (3) signals pushed to clear-api / dashboard and to which audience, (4) verbatim text displayed vs only derived fields.
- [ ] Decide beforehand what a partial "yes, except…" means (e.g. no third-party processors → self-hosted model, or drop the source).

## Go-live (INSO)

- [ ] Add the publisher's sender address to the Infomaniak whitelist, and add a filter rule that moves its mail to the `INSO` folder.
- [ ] Check the real emails' `Authentication-Results` header. If the publisher doesn't sign with DKIM, revisit the sender-trust check.
- [ ] Confirm whether other countries use other sender addresses (e.g. `afg.` / `ven.` subdomains) and add them to `INSO_ALLOWED_SENDERS`.
- [ ] Rebuild the fixtures from the first real email (anonymised) and re-check the set of field labels against it.
- [ ] Confirm whether serials carry revisions. If they do, implement group supersession (`group_member` / `resolve_group`) and reconsider ordinal-based `externalId`s.
- [ ] Check whether INCIDENT TYPE ever holds several values.
- [ ] Collect the publisher's incident type list. Compare the prompt's `event_type` with each email's `incident_type` on real samples.
- [ ] Collect the severity banner values and decide whether to map them to a severity.
- [ ] Re-judge the prompt's severity and relevance on the first ~20 real emails before trusting `_push`.
- [ ] Decide whether INSO can go live while the drain still classifies with Jev (spec, "Shadow classification"), or wait for the drain rework. If it goes live first, say so to the analysts: INSO incidents Jev reads as `none` get no event.
- [ ] Measure the disagreement: incidents with gold `relevanceScore = 1` that have no event in clear-api, plus the `eventType` mismatches between gold and the event. Use the number to set the drain rework's priority.
- [ ] Check `geo_resolution_rate` on real place names; if it's poor, fall back to `locate_in_country`.
- [ ] Write the manual LLM eval (`evals/inso_extraction/`, following the figure-scope pattern) and run it first, before `_push` runs on anything real.
- [ ] Set the S3 lifecycle and retention rule for `raw/inso/` and `quarantine/inso/`, according to the approval.
- [ ] Create the INSO `DataSource` row in clear-api (staging, then prod).
- [ ] Put the `CLEAR_MAILBOX_IMAP_*` and `INSO_*` variables into the staging env, then the prod env. `INSO_REQUIRE_DKIM` must stay `true` there.
- [ ] Switch `INSO_IMAP_FOLDER` from `INSO-TEST` to `INSO`.
- [ ] Tune `INSO_POLL_INTERVAL_MINUTES` from the observed sending times. Daily means up to 24 h of lag.
- [ ] Add a freshness warning (no INSO email in N days) once the frequency is known.
- [ ] Check that the `inso_gx` max runtime (3600 s) is enough.
- [ ] Rebase onto `dev` before merging (`factory.py` and the registry assertions will have moved on).

## Cross-cutting (don't wait for approval)

- [ ] Review the `CONTEXT.md` change to Incident with the glossary's author.
- [ ] Give incident extraction its own LLM role (`incident`) with no fallback to `extraction`, so a cheap-model swap for PDF backfills can't change INSO extraction. Re-run the eval on that model.
- [ ] Drain rework: let a connector supply the signal's classification (opt-in hook, found with `getattr`; sources without it keep Jev). When it's present, `_group` uses it and never calls Jev; a missing one requeues the signal. Implement it for INSO by reading the gold row by `externalId`, or from the signal itself if clear-api can return `rawData`. Add a drain test where the INSO answer is relevant and Jev's is `none`, asserting an event is created. This answers Greptile's P1 "The drain discards INSO classification".
- [ ] Replace Jev with the shared event-classification prompt for all sources. This includes per-source context blocks, binary relevance vs the `relevance_threshold` score, the factory change, and the drain hook above.
- [ ] Agree the source context block mechanism with the prompt's author.
- [ ] Reconcile ADR-0010 (unknown stays null) with the prompt's always-filled 1-5 severity. Write an ADR if needed.
- [ ] Context extraction from ANALYSIS (future).
- [ ] Learn how `providers/llm.py:complete_structured` works before reviewing the extraction code (example: `defs/ground/stages.py`).
