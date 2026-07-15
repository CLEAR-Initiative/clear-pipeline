---
name: to-expo
description: Break a plan, spec, or PRD into independently-grabbable tickets in Exponential using tracer-bullet vertical slices. Use when user wants to convert a plan into Exponential tickets, push slices to Exponential, or break work down into tickets / features / epics in Exponential.
---

# To Expo

Break a plan into independently-grabbable **tickets** in Exponential using vertical slices (tracer bullets). Tickets are the unit of backlog work; they live under a **product** and can be grouped by a **feature** (product-scoped) or an **epic** (workspace-scoped, cross-product).

Hierarchy reminder: `workspace -> product -> feature -> ticket -> action`. Epics are a workspace-scoped cross-cut.

## Prerequisites

`exponential auth status` must succeed. If not, instruct the user to run `exponential auth login --token <jwt> --api-url <url>`.

You will need a target **product** (slug or CUID). If the user hasn't said which, ask — it's required to file tickets. Use `exponential products list --workspace <slug|id> --json` to enumerate options.

## Process

### 0. Work in a fresh git worktree

Always run this skill from a dedicated git worktree so the user's main checkout — open IDE, in-progress edits, current branch — stays untouched while you explore the codebase and draft tickets. Create it before doing anything else and do all subsequent work from inside it:

```bash
git worktree add /tmp/<repo>-to-expo HEAD
```

Remember the path — you'll offer to remove it once the tickets are filed (step 9). If the repo isn't a git repository, skip the worktree and work in place.

### 1. Gather context

Work from whatever is already in the conversation. If the user passes a ticket reference (CUID, shortId, or URL) as an argument, fetch it with `exponential tickets get <id> --json` and read its body, dependencies, and comments.

### 2. Explore the codebase (optional)

If you have not already explored the codebase, do so to understand the current state of the code. Ticket titles and descriptions should use the project's domain glossary vocabulary, and respect ADRs in the area you're touching.

### 3. Decide on a grouping

Ask the user (or infer):

- **Existing feature?** If yes, capture its CUID — every ticket will get `--feature <id>`.
- **Existing epic?** If yes, capture its CUID — every ticket will get `--epic <id>`.
- **New feature?** Create one with `exponential features create --product <slug> -n "<name>" -d "<description>" --json` and capture the `id` from the response. Prefer this when the plan covers a single coherent capability inside one product.
- **New epic?** Create one with `exponential epics create -n "<name>" -d "<description>" --workspace <slug|id> --json` and capture the `id`. Prefer this when the work cuts across products or represents a strategic initiative.
- **Standalone tickets?** Skip grouping. Tickets are still tied to a product.

### 4. Draft vertical slices

Break the plan into **tracer bullet** tickets. Each ticket is a thin vertical slice that cuts through ALL integration layers end-to-end, NOT a horizontal slice of one layer.

Slices may be **HITL** or **AFK**. HITL slices require human interaction (architectural decisions, design reviews). AFK slices can be implemented and merged without human interaction. Prefer AFK over HITL where possible.

<vertical-slice-rules>
- Each slice delivers a narrow but COMPLETE path through every layer (schema, API, UI, tests)
- A completed slice is demoable or verifiable on its own
- Prefer many thin slices over few thick ones
</vertical-slice-rules>

For each slice also decide:

- **Ticket type**: `FEATURE` (default for new behaviour), `BUG`, `CHORE`, `IMPROVEMENT`, `SPIKE` (timeboxed investigation), `RESEARCH` (open-ended exploration).
- **Initial status**: `READY_TO_PLAN` for AFK slices that are ready for an agent to grab, `NEEDS_REFINEMENT` for HITL slices that still need a human conversation.

### 5. Quiz the user

Present the proposed breakdown as a numbered list. For each slice, show:

- **Title**: short descriptive name
- **Type**: HITL / AFK
- **Ticket type**: FEATURE / BUG / CHORE / IMPROVEMENT / SPIKE / RESEARCH
- **Blocked by**: which other slices (if any) must complete first
- **User stories covered**: which user stories this addresses (if the source material has them)

Ask the user:

- Does the granularity feel right? (too coarse / too fine)
- Are the dependency relationships correct?
- Should any slices be merged or split further?
- Are the correct slices marked as HITL and AFK?
- Is the chosen feature/epic grouping right?

Iterate until the user approves the breakdown.

### 6. Publish the tickets

Publish in **dependency order** (blockers first) so you can wire `--by <blocker-id>` to real CUIDs. Use `--json` on every `create` and capture `.id` from the response — that is the CUID you'll reference for dependencies and links.

For each approved slice:

```bash
exponential tickets create \
  --product <slug-or-cuid> \
  --type <FEATURE|BUG|CHORE|IMPROVEMENT|SPIKE|RESEARCH> \
  --status <READY_TO_PLAN|NEEDS_REFINEMENT> \
  --feature <feature-cuid>      # if grouping under a feature
  --epic <epic-cuid>            # if grouping under an epic
  -t "<title>" \
  -b "$(cat <<'EOF'
<body, per template below>
EOF
)" \
  --json
```

After each create, parse the response's `id` and `shortId` from the JSON response, and stash them under the slice's number. Then, for each declared blocker:

```bash
exponential tickets block <slice-id> --by <blocker-id>
```

Immediately after a Ticket is created, assign it an auto-generated `branchName` so `/start-ticket` and `/ship-ticket` can rely on the field being set:

```bash
exponential tickets update --id <cuid> --branch "<shortId-lowercased>-<slug(title)>"
```

The slug rules: lowercase, replace non-alphanumeric runs with `-`, trim leading/trailing `-`, cap at 50 chars. Example: shortId `EXPO-42`, title "Wire PR-URL filter into list endpoint" → `expo-42-wire-pr-url-filter-into-list-endpoint`.

If a slice already has implementation actions queued, link them with `exponential tickets link-action --id <ticket-id> --action <action-id>` — but only if those actions already exist.

Do NOT modify the parent feature, epic, or any pre-existing ticket beyond the dependency edges you add (and the `branchName` write-back just described).

### 6a. Leave decision comments where pertinent

If something about how a ticket was framed isn't obvious from its body alone, drop a short comment on the ticket capturing the **why**. The body describes *what to build*; comments are where rationale lives. A future agent (or human) fetching the ticket cold should be able to reconstruct the decisions you made.

Worth commenting:

- **Why this slice was split** off from a larger one ("originally combined with #4 but the auth surface needed its own demo").
- **Why a non-default type/status** was chosen (e.g. `SPIKE` over `FEATURE` because the API shape is unknown; `NEEDS_REFINEMENT` because the user flagged an open question).
- **Why this dependency edge exists** if it isn't structurally obvious from the bodies.
- **Constraints surfaced in the conversation** that didn't fit cleanly in the body (deadlines, stakeholder preferences, prototype findings, ADR pointers).
- **Explicitly-rejected alternatives** the user considered and discarded — so a later agent doesn't re-litigate them.

Not worth commenting: anything already plainly stated in the ticket body or acceptance criteria. Don't narrate the obvious.

```bash
exponential tickets comment --id <ticket-cuid> -b "<short rationale, 1–3 sentences>"
```

Keep comments short and decision-focused. One comment per distinct point, not a wall of text.

### 7. Report back

Print a summary to the user listing each created ticket with its CUID, shortId (if present), and any dependency edges added. If you created a feature or epic in step 3, include its CUID too. Future commands (`exponential tickets show <id>`, `exponential tickets update --id <id> ...`) operate on CUIDs.

### 8. Emit a clean-context execution prompt

End your response with a fenced code block containing a **self-contained prompt** the user can paste into a fresh Claude conversation to start executing the tickets you just filed. The prompt must not rely on any of the current conversation's context — a cold agent reading only this prompt should know exactly what to do.

Include in the prompt:

- One sentence stating the goal: implement the tickets listed below in dependency order.
- The parent feature/epic CUID (if one was created or chosen) so the agent can fetch it for shared context: `exponential features get <cuid>` or `exponential epics get <cuid>`.
- A table or bullet list of every created ticket, in dependency order, with: CUID, shortId, title, type (FEATURE/BUG/...), status (READY_TO_PLAN / NEEDS_REFINEMENT), and HITL/AFK marker.
- Explicit instructions to:
  - before the first Ticket, check out the **featureBase** branch (from `docs/agents/git-flow.md`; default `main`) and pull — never start ticket work from an unmerged feature branch or a dirty working tree; `/start-ticket` cuts each Ticket's branch from that base,
  - fetch each ticket with `exponential tickets get <cuid> --json` before starting it,
  - work tickets in dependency order, starting with `READY_TO_PLAN` AFK tickets that have no open blockers,
  - merge a Ticket's PR to `main` before starting any Ticket that depends on it (reaching `QA` is not enough — the blocker's code isn't in `main` until its PR merges), so each new Ticket branches off an up-to-date `main` and no stack of PRs forms,
  - stop and surface to the human on `NEEDS_REFINEMENT` (HITL) tickets rather than guessing.
- The product slug/CUID so the agent can list siblings if needed.

Do not include conversation-specific narrative (e.g. "we just decided X"). The prompt must read as a standalone work order.

Template:

<execution-prompt-template>
````
You are picking up a batch of tickets just filed in Exponential. Implement them in dependency order.

**Product**: `<product-slug>` (CUID `<product-cuid>`)
**Parent**: <feature|epic> `<cuid>` — fetch with `exponential <features|epics> get <cuid> --json` for shared context.

**Tickets** (dependency order):

| # | CUID | shortId | Title | Type | Status | HITL/AFK | Blocked by |
|---|------|---------|-------|------|--------|----------|------------|
| 1 | <cuid> | <shortId> | <title> | FEATURE | READY_TO_PLAN | AFK | — |
| 2 | <cuid> | <shortId> | <title> | FEATURE | READY_TO_PLAN | AFK | #1 |
| ... |

**How to work** (one Ticket at a time, in dependency order):

0. **Get on a clean base first**: `git checkout <featureBase>` (from `docs/agents/git-flow.md`; default `main`) `&& git pull`. Commit or stash anything dirty. Never start from an unmerged feature branch — `/start-ticket` cuts each Ticket's branch from the base you're standing on being up to date.
1. **Start** the Ticket: `/start-ticket <cuid>` — fetches the body, transitions to `IN_PROGRESS`, checks out (or creates) its branch, and writes `.exponential/current-ticket`.
2. Read the Ticket body and acceptance criteria.
3. Implement the slice end-to-end. Verify the acceptance criteria locally.
4. **Ship** the Ticket: `/ship-ticket` (no arg — it auto-detects from the marker file). Pre-ship checks run; a PR opens (or extends, if you stacked on the same branch); the Ticket transitions to `QA`; `ticket.prUrl` is set. The `QA`→`DONE` promotion happens only once the PR actually merges — so reaching `QA` does **not** make the Ticket's code available to later Tickets.
5. **Merge a Ticket's PR to `main` before starting any Ticket that depends on it.** `QA` is only a status; the blocker's *code* lives on its branch, not in `main`, until its PR merges. `/start-ticket` branches each new Ticket off `origin/<featureBase>`, so a freshly-merged `main` is what both keeps dependent Tickets compiling and prevents stacked PRs. Independent Tickets (no shared blocker) may proceed in parallel off `main`.
6. On `NEEDS_REFINEMENT` / HITL tickets, stop and surface the open questions to the human — do not guess.
7. If a Ticket's body is ambiguous or contradicts the code, leave a comment with `exponential tickets comment add --id <cuid> -m "<question>"` and pause that Ticket.

> **Do NOT create separate PRs stacked on each other.** Two acceptable shapes:
> (a) **Merge-between (default):** ship a Ticket, merge its PR to `main`, then `/start-ticket` the next — it branches off the updated `main`, so nothing stacks.
> (b) **One PR for a tightly-coupled chain:** keep the whole chain on ONE branch and let `/ship-ticket`'s stack mode append each Ticket to a SINGLE PR, merged once.
> Never leave a tower of N branches each based on the previous one — a change to the base then forces a full restack of everything above it.
````
</execution-prompt-template>

### 9. Offer to clean up the worktree

Once the tickets are filed and the execution prompt is emitted, offer to remove the worktree you created in step 0:

```bash
git worktree remove /tmp/<repo>-to-expo
```

Don't remove it without asking — the user may want to inspect it first. Nothing is lost by removing it: the tickets live in Exponential, not in the worktree.

## Ticket body template

<ticket-template>
## Parent

A reference to the parent ticket, feature, or epic in Exponential (CUID or shortId). Omit if there is no parent.

## What to build

A concise description of this vertical slice. Describe the end-to-end behavior, not layer-by-layer implementation.

Avoid specific file paths or code snippets — they go stale fast. Exception: if a prototype produced a snippet that encodes a decision more precisely than prose can (state machine, reducer, schema, type shape), inline it here and note briefly that it came from a prototype. Trim to the decision-rich parts — not a working demo, just the important bits.

## Acceptance criteria

- [ ] Criterion 1
- [ ] Criterion 2
- [ ] Criterion 3

## Blocked by

- `<blocker-ticket-cuid-or-shortId>` — short description

Or "None — can start immediately" if no blockers.

</ticket-template>

## Useful commands

```bash
# Discover where to file
exponential workspaces list --json
exponential products list --workspace <slug|id> --json
exponential features list --product <slug|id> --json
exponential epics list --workspace <slug|id> --json

# Create groupings (optional)
exponential features create --product <slug|id> -n "<name>" -d "<desc>" --json
exponential epics create -n "<name>" -d "<desc>" --workspace <slug|id> --json

# Create tickets + wire dependencies
exponential tickets create --product <slug|id> -t "<title>" -b "<body>" \
  --type FEATURE --status READY_TO_PLAN --feature <feature-cuid> --json
exponential tickets block <ticket-cuid> --by <blocker-cuid>

# Inspect what you just made
exponential tickets list --product <slug|id> --feature <feature-cuid> --json
exponential tickets show <ticket-cuid>
```
