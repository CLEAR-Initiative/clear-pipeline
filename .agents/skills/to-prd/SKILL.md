---
name: to-prd
description: Turn the current conversation into a human-readable PRD - a Knowledge page linked to a Feature in Exponential, with EARS requirements as native rows. No interview, just synthesis of what you've already discussed.
disable-model-invocation: true
---

This skill takes the current conversation context and codebase understanding and produces the **human PRD**: the top zone of a feature's PRD page, written concisely for a human reader. Do NOT interview the user - just synthesize what you already know.

A PRD page has two zones for two readers. This skill writes the top (human) zone. `/to-robo-prd` appends the bottom `## Agent PRD` zone with full implementation detail, and `/to-tickets` turns that into backlog work. A PRD is an optional origin - a feature may also carry only direct requirement rows - so run this skill when the feature needs an argued case, not for every change.

The issue tracker configuration should have been provided to you - run `/setup-matt-pocock-skills` if not. You need a target **product** (slug or CUID); ask if the user hasn't said which.

## Process

1. **Explore the repo** to understand the current state of the codebase, if you haven't already. Use the project's domain glossary vocabulary throughout, and respect ADRs in the area you're touching.

2. **Check the registry first.** The feature may already exist:

   - `exponential features list --product <product> --json` and match by name/meaning, not just exact string.
   - **No match** → you will create the feature in step 6.
   - **Match** → you are adding to it, not creating a duplicate. Fetch it: `exponential features get <id> --json`. Look at its scopes:
     - If it has scopes - especially SHIPPED (Live) ones - this work is an **extension**: a new scope on the existing feature. Frame the PRD's Rollout section against what is already live ("V1 is live; this adds V2: ...").
     - If it already has a linked PRD page, extend that page rather than creating a second one (one PRD per feature is the default).

   Confirm the match (or the decision to create new) with the user before writing anything.

3. **Sketch the seams** at which you're going to test the feature. Existing seams should be preferred to new ones. Use the highest seam possible. If new seams are needed, propose them at the highest point you can. The fewer seams across the codebase, the better - the ideal number is one. Check with the user that these seams match their expectations.

4. **Author the requirements as EARS statements** - one testable "shall" sentence each (ubiquitous / When / While / If-then patterns), e.g. "When a form is submitted, the system shall create exactly one Insight." Optionally tag each with a kind: `FUNCTIONAL`, `NON_FUNCTIONAL`, or `CONSTRAINT` (a constraint restricts allowed solutions, not behavior). These are published as **native, checkable rows** on the feature in step 6 - the rows are canonical; the page copy is the draft that ages into history. Do NOT write user stories - they are retired as a write path.

5. **Write the human PRD** using the template below. Concise, present-tense, for a human deciding whether and what to build - implementation detail belongs in the Agent PRD (`/to-robo-prd`), not here.

6. **Publish to Exponential:**

   - **Feature** (if new): `exponential features create --product <product> -n "<name>" -d "<one-paragraph living description>" --vision "<target outcome>" --status DEFINED --json` - capture `id`. Offer to file it under an Area: `exponential features areas list --product <product> --json`, then `--area <id>` (create one with `features areas create` only if the user confirms). The `-d` description is the feature's living description of what the capability is - NOT the PRD.
   - **Scopes** (from the Rollout section): for each planned increment, `exponential features scopes add --feature <id> --version "<V1>" -d "<what it delivers>" --json` - capture ids. Skip if the user wants no scopes yet; scopes are optional.
   - **PRD page**: write the PRD body to a temp file, then `exponential pages create -t "PRD: <feature name>" --body-file <path> --json` - capture the page id.
   - **Link it**: `exponential features link-page --feature <feature-id> --page <page-id>`.
   - **Requirement rows**: pipe a JSON array of `{ statement, kind?, scopeId? }` to `exponential features requirements add --feature <feature-id>` - pin each requirement to its scope where one applies.

7. **Report back**: feature CUID, page CUID, scope CUIDs, and requirement count. Suggest `/to-robo-prd` as the next step when agents will build this.

<prd-template>

## Problem

The problem being solved, from the user's perspective. Evidence (linked insights, quotes) belongs here.

## Goals

What success looks like. Measurable where possible.

## Non-goals

What this deliberately does not do.

## Solution

What we're building, briefly, from the user's perspective. User flows go here if they earn their space.

## Requirements

The EARS statements from step 4, as a checklist draft. Note under the heading: "Canonical, checkable copies of these live as requirement rows on the feature."

## Rollout

The scope cut: which increments ship in what order, one line each, matching the registry scopes created in step 6. For an extension of a live feature, state what is already live first.

## Open questions

Unresolved decisions, each with an owner if known.

</prd-template>
