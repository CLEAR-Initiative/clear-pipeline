---
name: start-ticket
description: Start work on an Exponential **Ticket** — fetch it, transition it to `IN_PROGRESS`, check out (or create) its branch, and write the `.exponential/current-ticket` marker. The front-end bookend to `/ship-ticket`. Use when picking up a `READY_TO_PLAN` Ticket to begin work, or when an agent needs to load context for a specific Ticket before coding.
---

# Start Ticket

Begin **Started** work on one Ticket. Sets up the working tree (branch checked out, marker written) so `/ship-ticket` can later auto-detect what to ship.

## Usage

```
/start-ticket [<cuid-or-shortId>]
```

If no argument is given, prompt the user with a list of `READY_TO_PLAN` Tickets they own (or in the current product, if no assignee context exists) and let them pick one.

## Prerequisites

- `exponential auth status` succeeds.
- Inside a git repo (`git rev-parse --show-toplevel` succeeds).
- The user is OK with switching branches — bail with a clear error if the working tree has uncommitted changes (the user must stash/commit first).

## Process

### 1. Resolve the Ticket

- If an arg was given (`<cuid-or-shortId>`), pass it directly to `exponential tickets get <id> --json`.
- Otherwise, ask the user which Ticket. Show the candidates from `exponential tickets list --status READY_TO_PLAN --product <product> --json` (the product comes from `docs/agents/issue-tracker.md` if present, else ask).

Display the resolved Ticket's title, shortId, type, status, body summary (first paragraph), and any open blockers (`isBlocked` / `openBlockerCount`). If the Ticket is **blocked**, warn the user before continuing — they can override or pick a different Ticket.

### 2. Determine the branch

- If `ticket.branchName` is set, use it verbatim.
- Otherwise, auto-generate `<shortId-lowercased>-<slug(title)>` where slug:
  - lowercases
  - replaces non-alphanumeric runs with `-`
  - trims leading/trailing `-`
  - caps at 50 chars
- Persist the auto-generated name back to Exponential immediately:

  ```bash
  exponential tickets update --id <cuid> --branch <generated-name>
  ```

  This guarantees `/ship-ticket`'s branch-lookup auto-detect will work later.

### 3. Check out the branch

- If the branch already exists locally: `git checkout <branchName>`.
- If it exists on the remote only: `git fetch origin <branchName> && git checkout <branchName>`.
- If it doesn't exist anywhere: read `docs/agents/git-flow.md` for `featureBase` (default `main` if the file is missing) and run:

  ```bash
  git checkout -b <branchName> origin/<featureBase>
  ```

  Fetch first if needed. If `featureBase` is missing or unreadable, ask the user before defaulting.

If the working tree has uncommitted changes, refuse to switch branches and tell the user to commit or stash. Do not use `git stash` silently — destructive moves need an explicit user decision.

### 4. Transition the Ticket to `IN_PROGRESS`

```bash
exponential tickets update --id <cuid> --status IN_PROGRESS
```

Only run this after the branch is successfully checked out — if the checkout fails, don't move the Ticket status.

### 5. Write the marker

Append `.exponential/` to `.gitignore` if not already present, then write the CUID:

```bash
mkdir -p .exponential
printf '%s' '<cuid>' > .exponential/current-ticket
```

The marker is intentionally tiny: just the CUID, no newline, no JSON. `/ship-ticket` reads this one line.

### 6. Report

Print a concise summary:

- Ticket shortId + title
- Branch checked out (and whether it was newly created)
- Status transition (`READY_TO_PLAN → IN_PROGRESS`)
- Path to the body (suggest the user / agent read `exponential tickets get <cuid> --json` for full context)
- Next step: implement, then run `/ship-ticket` (no arg needed — it'll auto-detect from the marker).

## Failure modes

- **Dirty working tree** — refuse; tell the user to commit or stash.
- **Ticket already `IN_PROGRESS` by someone else** — surface the existing assignee and ask the user to confirm before continuing.
- **Branch exists but points elsewhere than featureBase** — don't reset it; just check it out. The user may have stacked work intentionally.
- **`exponential` CLI not authed** — surface `exponential auth login --token <jwt> --api-url <url>` and bail.
