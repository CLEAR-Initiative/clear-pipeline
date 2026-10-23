# Spec — INSO newsletter datasource (skeleton, pre-approval)

Status: skeleton. The source is covered by terms of use; no real INSO content may be processed until the written approval is obtained. Until then, only synthetic emails are processed, locally. Post-approval work lives in `docs/inso-TODO.md`.

## Problem Statement

NGO security newsletters (INSO) carry incident-level information on attacks, strikes and crime affecting civilians and humanitarian operations in the countries CLEAR covers. This information reaches us only by email, as compound reports: one email describes several incidents across different places, dates and actors, wrapped in report-level fields (location, incident type, date & time, serial, analysis, advice). The pipeline has no way to ingest email, nor to split a compound report into incidents, so this information never reaches the Events side of CLEAR.

Using the source is blocked by its terms of use until a written approval arrives. The team still needs the ingestion built and tested, so it can be switched on quickly once approval lands, without having processed any real content beforehand.

## Solution

A new GX-path source, `inso`, that:

- reads a dedicated folder of a shared newsletter mailbox over IMAP, read-only, accepting only authenticated mail from allowlisted senders;
- lands every email verbatim in bronze, so parsing and prompts can be replayed without the mailbox;
- parses the report-level fields with code and splits INFORMATION into incidents with an LLM at silver, giving one signal per incident;
- classifies each incident with the shared event-classification prompt (the pilot for replacing Jev), with an INSO-specific context block;
- goes through the existing GX stages (geo, temporal, match, gold, push) unchanged, and is drained by a signals connector like Sudan Witness;
- puts anything it cannot trust or parse into quarantine and turns the run red;
- runs daily from a poll sensor. Before approval it runs only on a local instance, against a test folder holding synthetic emails.

## User Stories

1. As a data engineer, I want INSO emails fetched from the newsletter mailbox automatically, so that no one has to forward or upload them by hand.
2. As a data engineer, I want the mailbox connection settings to be shared and named after their role rather than the vendor, so that other newsletters can reuse the mailbox and a later move from Infomaniak to Outlook doesn't rename variables.
3. As a data engineer, I want each newsletter source to read only its own mailbox folder, so that other newsletters in the same mailbox never enter the INSO pipeline.
4. As a data engineer, I want only mail from allowlisted senders with a passing DKIM/DMARC result accepted, so that a forged email can't put fabricated incidents on a humanitarian security dashboard.
5. As a data engineer, I want to relax the DKIM requirement through local configuration only, so that I can test with synthetic emails I send myself.
6. As a data engineer, I want the pipeline never to change mailbox state (no read flags, no moves), so that a person opening an email in webmail can't make the pipeline skip it.
7. As a data engineer, I want a per-source cursor (folder UIDVALIDITY plus last UID) in Redis, advanced only after a clean batch, so that each email is processed once and a failed run is retried.
8. As a data engineer, I want a UIDVALIDITY change to trigger a full rescan of the folder, so that a recreated or migrated folder doesn't silently drop emails; dedup on `externalId` prevents duplicates.
9. As a data engineer, I want every fetched email stored verbatim as `.eml` in bronze, keyed by its content hash, so that fixing the parser or a prompt can be replayed from S3 without the mailbox.
10. As a data engineer, I want a small sidecar next to each raw email (IMAP UID, folder, fetch time, authentication result), so that the provenance of every signal can be traced back to the exact message.
11. As a data engineer, I want the report-level fields (location, incident type, date & time, serial, issued by, severity banner, information, analysis, advice) parsed by code from the labelled fields, so that a template change fails loudly instead of being guessed.
12. As a data engineer, I want the parser to read the HTML part first and fall back to plain text, so that it survives emails sent without one of the two.
13. As a data engineer, I want the serial number treated as an opaque identifier, so that we don't build on a guessed meaning of its parts from a single sample.
14. As a data engineer, I want an email with a missing serial sent to quarantine rather than given a generated ID, so that IDs stay stable across re-runs.
15. As a data engineer, I want the email's `Date` header used as `publishedAt`, so that every signal has an unambiguous, timezone-aware publication time.
16. As a data engineer, I want the report's DATE & TIME parsed strictly as day/month/year, with a failure going to quarantine, so that a wrong day/month order is never silently accepted.
17. As a data engineer, I want the INCIDENT TYPE kept as one verbatim string, so that we don't split it on commas without knowing the publisher's list of types.
18. As an analyst, I want each incident described in INFORMATION to become its own signal, so that place, date, actor and casualties are attributed to the right incident and not merged into one report-wide record.
19. As an analyst, I want incidents mentioned only in ANALYSIS not to become signals, so that past incidents already reported aren't counted twice.
20. As an analyst, I want ANALYSIS and ADVICE kept as report-level text, so that they can feed context extraction later.
21. As a data engineer, I want the LLM to return only day and month for an incident and code to add the year from `publishedAt`, so that incidents reported around New Year get the right year.
22. As an analyst, I want an incident with no stated date to have a null date, not the report date, so that temporal matching isn't skewed by invented dates.
23. As an analyst, I want killed and injured counts to tell "none reported" (0) apart from "unspecified" (null), so that casualty totals stay honest.
24. As an analyst, I want each incident to keep the sentence(s) it was extracted from, so that I can check any extracted field against the original text.
25. As a data engineer, I want each incident's `externalId` built from the source prefix, the verbatim serial and the incident's ordinal, so that re-runs are idempotent in clear-api.
26. As an analyst, I want each incident classified with the shared event-classification prompt (type level 3, relevance, severity), so that INSO is the pilot for replacing Jev.
27. As an analyst, I want incidents that affect humanitarian staff, assets, premises or access counted as relevant, including crime, so that INSO's core content isn't discarded as "ordinary crime".
28. As an analyst, I want severity left null when the text gives no basis for it, so that ADR-0010 (unknown stays null) holds and no invented severity drives the bundle maximum.
29. As a data engineer, I want a level-3 type that doesn't map to the taxonomy to fail loudly, so that unmapped labels never fall back to "other" unnoticed.
30. As a data engineer, I want classification to answer exactly one object per incident, failing otherwise, so that the prompt's splitting rules never fan out or swallow an incident at the classify stage.
31. As a data engineer, I want other GX sources to keep using Jev, so that the INSO pilot doesn't change their behaviour.
32. As an analyst, I want each incident's place, locality, admin1 and country resolved through the existing geoparser, limited to the country from the report header, so that INSO signals are located like other GX sources.
33. As a data engineer, I want an email whose country doesn't match a known country location sent to quarantine, so that incidents are never located in the wrong country.
34. As a data engineer, I want INSO signals drained by the shared signals drain through a connector that reads the GX output, so that they aren't marked failed as an unknown source.
35. As a data engineer, I want every quarantined email kept with its reason, without stopping the rest of the batch, so that one malformed newsletter doesn't block the ones after it.
36. As a data engineer, I want any quarantined email to turn the run red through a blocking GX check, so that a parser break is noticed even though the newsletter frequency is unknown.
37. As a data engineer, I want the source polled daily by a sensor whose interval can be changed live, so that the cadence can be tuned once the real frequency is known.
38. As a data engineer, I want the source to refuse to run when the mailbox credentials are empty, so that an environment without credentials can't use it by accident.
39. As a data engineer, I want to run the whole pipeline locally against a test folder of synthetic emails, with local S3, Redis and clear-api, so that I can build and test before approval without touching real content or shared infrastructure.
40. As a data engineer, I want CI to cover the parser and the full job without any LLM call, so that tests are deterministic, free and can't send content to a third party.
41. As a developer, I want the parser's output compared field by field against a golden file per fixture, so that a parsing regression points straight at the field that broke.
42. As a reviewer, I want INSO registered in the existing source, sensor and connector lists, so that the tests that pin those lists keep protecting every source.

## Implementation Decisions

- **Path.** Ingestion goes only through the GX path. The signals path is touched only to add a `polled=False, drained=True` connector, following Sudan Witness. It's needed because the push creates NEW signals and the shared drain fails any source it doesn't know.
- **Source name** `inso` (`INSO_SOURCE_NAME`). It is used in asset names, S3 prefixes, the job name, the Redis cursor key and the clear-api `DataSource` lookup. Like every other source, it's a settings field with a default, set per environment like the other source names; it must match the clear-api `DataSource` row and must not change once an environment holds data. The `externalId` prefix is fixed (`inso:`), not taken from the setting, as for the other sources.
- **Shared newsletter mailbox module.** A new provider module, not specific to INSO. It connects to the mailbox (implicit TLS on the IMAPS port), lists messages in a given folder after a given cursor, and fetches their raw bytes without setting the seen flag. It returns `(uid, raw_bytes)` pairs and the folder's UIDVALIDITY. It is deliberately not an abstract interface: IMAP is the only transport, and a later Graph/Outlook move would replace this module. Parsing never imports it.
- **Cursor.** Stored in Redis under the source's `last_synced` key as `UIDVALIDITY:UID`, and advanced only after a clean batch. Quarantined emails still advance it; they're replayed from bronze, not by rewinding.
- **Sender trust.** A sender allowlist (comma-separated setting), plus a check of the `Authentication-Results` header for DKIM or DMARC pass. The check can be turned off by a setting that defaults to on and is turned off only in a local `.env`.
- **Bronze.** Raw `.eml` bytes plus a JSON sidecar, keyed by publication date and the sha256 of the bytes. Quarantine uses the same layout under its own prefix, with a reason. Bronze holds no parsed content.
- **INSO provider module.**
  - A pure `parse(raw_bytes)` that returns the parsed report. It finds fields by label text, reads HTML first and falls back to plain text, and raises a typed parse error on a missing label, a malformed date or a missing serial.
  - Report fields: message id, sender address, subject, `published_at`, `severity_raw`, `location_raw`, `country_raw`, `incident_type`, report datetime, serial, issued by, information, analysis, advice list.
- **Silver extraction (LLM).**
  - INFORMATION goes to the extraction role through the existing structured-output helper, with a Pydantic schema whose root is an object holding a list of incidents.
  - Incident fields: day, month, admin1, locality, place, actors, killed, injured, source text. Field names follow the shared classification prompt where they overlap.
  - Code assigns the ordinals and adds the year: the previous year when the date would fall more than about 7 days after `published_at`.
  - Silver has one row per incident.
- **Incident identity.** `inso:<serial verbatim>:<ordinal>`. Revisions aren't handled.
- **Classify hook.**
  - The GX source protocol gets an optional `classify(row)` hook, found with `getattr` in the same way as the existing optional hooks. When it's present, the classify stage calls it instead of the Jev classifier; other sources are unchanged.
  - INSO's implementation runs the colleague's shared event-classification prompt through the `signal` LLM role, one incident per call, with a single-object schema.
  - Before the shared prompt, it adds an INSO source context block: incidents affecting humanitarian staff, assets, premises or access are relevant, crime included; severity is null when the text gives no basis, and at least 2 for crime against humanitarian staff or assets.
  - Mapping: level-3 type → level 2 and GLIDE code through the existing taxonomy mirror (an unmapped label raises); RELEVANT/NOT_RELEVANT → relevance 1.0/0.0; severity passed through, nullable.
- **Geo.** The geo input is a composed string of place, locality, admin1 and country. Resolution uses the existing geoparser, limited to that country, without promotion. The country comes from the first token of the header location, matched to an L0 location; no match means quarantine.
- **Quarantine visibility.** A blocking GX check fails the run when any email in the batch was quarantined; the good emails in the batch still go through. A freshness check is postponed until the frequency is known.
- **Trigger.** The poll sensor runs at a default interval of 1440 minutes, editable live from the sensor cursor. GX-only sensors start running by default, so before approval the only protection is the folder setting pointing at the synthetic test folder on a local instance.
- **Configuration.** Shared settings: `NEWSLETTER_MAILBOX_IMAP_HOST`, `_PORT` (default 993), `_USER`, `_PASSWORD`. Per-source settings: `INSO_IMAP_FOLDER`, `INSO_ALLOWED_SENDERS`, `INSO_REQUIRE_DKIM`, `INSO_POLL_INTERVAL_MINUTES`. There are no new LLM variables. There are no SMTP settings; SMTP is only used by hand to send synthetic fixtures to the test folder.
- **Glossary.** Here, an Incident is the unit extracted from an event-driven compound report, one signal each. `CONTEXT.md` currently says incidents aren't extractable (that was written for Reports). The update is drafted separately and reviewed by the glossary's author.

## Testing Decisions

- **A good test** feeds fixture `.eml` bytes in at the edge and asserts only on outputs: parsed report fields, silver and gold rows, `externalId`s, quarantine entries and their reasons, check results, and how the cursor moved. No assertions on private helpers or on intermediate call order.
- **Seam 1, the main one: materializing the GX job.**
  - The INSO assets are materialized end to end, using the existing GX test pattern: a fake S3, a SQLite Iceberg catalog with a file warehouse in a temp dir, and the shared fake Redis.
  - Only three outside boundaries are replaced: the mailbox fetch (returns fixture bytes), the silver extraction call (returns the fixture incidents), and the per-incident classify call (returns canned classifications).
  - Cases to cover:
    - multi-incident and single-incident emails;
    - year filled in across New Year;
    - null date and 0-vs-null casualties;
    - a forged sender (no DKIM) quarantined, with the run red;
    - a missing serial quarantined;
    - an unknown country quarantined;
    - an unmapped level-3 type failing;
    - a classify answer with zero or several objects failing;
    - a UIDVALIDITY change triggering a rescan without duplicates;
    - the cursor not advancing when the batch fails.
  - Existing GX sources without the hook still go through Jev (covered by the existing tests).
- **Seam 2: the parser on its own.** `parse(raw_bytes)` is compared field by field against a golden `*.report.json` per fixture, so a parsing regression is reported by field. Prior art: Sudan Witness tests its feature parser directly.
- **Fixtures** live in the existing, empty golden directory, under an `inso` subfolder: `*.eml` (input), `*.report.json` (exact parser output), `*.incidents.json` (mocked extraction output). The current synthetic files are moved there, adjusted to the decisions above (no contextual incidents, single `incident_type` string, day/month only), and the root `synthetic/` folder is removed.
- **CI makes no LLM calls.** A manual LLM eval following the existing figure-scope eval pattern is postponed (see TODO).
- **Required updates to existing tests:** add `inso` to the exact registered-GX-source set, the polled-sensor set, and the connector flags/map in the signals drain test.

## Out of Scope

- Processing any real INSO email, or reading the real INSO folder, before written approval.
- Microsoft Graph or OAuth2 IMAP access to an Outlook mailbox.
- Handling revisions (supersession across serials).
- Forward-unwrapping (inline forwards or attached messages).
- Turning ANALYSIS into incidents, or extracting context from it.
- Mapping the severity banner to a 1-5 scale.
- A dedicated LLM role for incident extraction (it uses the extraction role for now).
- Replacing Jev for any source other than INSO, and changing the shared prompt itself.
- The manual LLM eval harness.
- A freshness or staleness alert.
- Deploying to the dev Dagster instance and merging into `dev`.

## Further Notes

- Every format decision rests on one sample, rebuilt after going through Outlook and Gmail. Production mail will come directly from the publisher, so the fixtures are approximate. The first real email is expected to change the parser and fixtures.
- Conclusions about the prompt drawn from synthetic fixtures are provisional; re-judge on the first ~20 real emails before trusting the push output.
- The sender's `sdn.` subdomain suggests one sender per country. The allowlist is a list for that reason.
- Daily polling means up to 24 hours of lag. That's acceptable for analysis, not for operational warnings.
- Related ADRs: ADR-0010 (unknown stays null), ADR-0004 (source attribution), ADR-0003 (LLM model selection).
