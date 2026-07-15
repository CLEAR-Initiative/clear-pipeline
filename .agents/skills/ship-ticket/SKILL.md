---
name: ship-ticket
description: Ship one Exponential **Ticket** — commit any leftover work, run pre-ship checks, open (or extend) a PR, link the PR back to the Ticket, and transition the Ticket to `QA`. The back-end bookend to `/start-ticket`. Runs in **new PR mode** for the first Ticket on a branch and **stack mode** for subsequent Tickets on the same branch. Use when work for a Ticket is ready for review.
---

# Ship Ticket

Ship exactly one Ticket per invocation. To put multiple Tickets on one PR, stay on the branch and run `/ship-ticket` again after `/start-ticket`-ing the next Ticket — same-branch reuse triggers **stack mode** automatically. See [ADR-0003](../../../docs/adr/0003-one-ticket-per-ship-stacking-via-branch.md).

## Usage

```
/ship-ticket [<cuid-or-shortId>] [--skip-checks]
```

- `<cuid-or-shortId>` (optional): explicit Ticket. Overrides auto-detection.
- `--skip-checks`: bypass pre-ship checks. PR is opened as **draft** to signal incomplete verification.

## Prerequisites

- Inside a git repo (`git rev-parse --show-toplevel`).
- `gh` authenticated (`gh auth status`).
- `exponential auth status` ok.
- `docs/agents/git-flow.md` exists (run `/setup-git-flow` first if not). Parse its front-matter to read `featureBase`.

## Process

### 1. Resolve the Ticket

Auto-detect order — first hit wins:

1. **Explicit arg** — `<cuid-or-shortId>` passed in.
2. **Marker file** — `.exponential/current-ticket` (one CUID, no newline). Skip if the file is missing or empty.
3. **Branch lookup** — `exponential tickets list --branch <current-branch> --json`. **Filter out** any ticket already in `QA` (those are already shipped and stacked onto this branch — we want the still-`IN_PROGRESS` one).

If multiple Tickets match after filtering, surface them and ask the user to disambiguate. If none match, error: tell the user to pass an explicit arg or run `/start-ticket` first.

Fetch the full Ticket via `exponential tickets get <cuid> --json` and capture: `id`, `shortId`, `title`, `body`, `type`, `status`, `prUrl`, `branchName`.

### 2. Pre-ship checks (Node-only first cut)

If `--skip-checks` is set, **skip this section entirely** and remember to open the PR as draft.

Discover from `package.json` scripts (top-level only, no monorepo traversal for now):

```bash
node -e "const s=require('./package.json').scripts||{}; \
  for (const k of ['typecheck','test','lint']) if (s[k]) console.log(k)"
```

For each script that exists, run it in this order: `typecheck`, then `test`, then `lint`. Stop on the first failure and refuse to push. Tell the user:

- which check failed
- the failing output (last ~30 lines)
- that they can either fix and re-run, or re-invoke with `--skip-checks` to open as draft

If `package.json` has none of those scripts, skip silently — don't invent checks.

> Note: other ecosystems (Cargo, pyproject, gradle) are out of scope for this first cut. Extend later as needed.

### 3. Commit handling (three states)

Run `git status --porcelain` and `git rev-list --count <featureBase>..HEAD` to classify:

**State A — working tree changes + zero commits ahead of `featureBase`:**

Stage everything and commit:

```bash
git add -A
git commit -m "$(cat <<'EOF'
<type>(<scope>): <title>

Refs: <shortId>
Exponential-Ticket: <cuid>
EOF
)"
```

- `<type>` derives from Ticket type: `FEATURE` → `feat`, `BUG` → `fix`, `CHORE` → `chore`, `IMPROVEMENT` → `refactor`, `SPIKE` → `chore`, `RESEARCH` → `docs`.
- `<scope>` is optional. Omit it if you don't know — `feat: <title>` is fine.
- Trailers: `Refs` is human-readable, `Exponential-Ticket` is machine-readable. The trailers are decoration only — see [ADR-0002](../../../docs/adr/0002-pr-url-primary-trailer-fallback.md). PR-URL linkage is the source of truth.

**State B — clean tree + N>0 commits ahead:**

Don't create a new commit. The work is already committed; just push.

**State C — mixed (some commits ahead AND working tree changes):**

Commit only the leftovers. Use the same trailers. Existing commits ride along unchanged — don't rewrite history.

### 4. Pre-ship checks, take two

If commits were created in step 3, **re-run** the pre-ship checks (step 2) after the commit, before the push. Reason: a failing typecheck in the new commit shouldn't ship.

### 5. Push

```bash
git push -u origin HEAD
```

If push fails (e.g. remote has commits we don't), surface the failure and stop. Don't force-push.

### 6. PR handling

Check for an existing open PR on this branch:

```bash
gh pr view --json url,body,isDraft,baseRefName 2>/dev/null
```

**Stack mode** (PR exists):

- Append a line to the PR body: `Ships: <shortId>` (only if not already present).
- Do not change the PR title (the first Ticket named it).
- Do not change draft state.
- Update the PR body via `gh pr edit --body "<new-body>"`.

**New PR mode** (no existing PR):

- Read `featureBase` from `docs/agents/git-flow.md` front-matter. Default to `main` if the file is missing (and warn the user that `/setup-git-flow` should be run).
- Title: `<type>: <title>` (from the same `<type>` derivation as step 3).
- Body template:

  ````markdown
  ## What

  <ticket.body up through the first acceptance-criteria block; if there are no headings, use the full body>

  ## Acceptance criteria

  - [ ] <criterion 1>
  - [ ] <criterion 2>

  ---

  Ships: <shortId>

  [View in Exponential](<exponential-ticket-url-if-known>)
  ````

  - The acceptance-criteria list comes from parsing `- [ ]` lines under the Ticket's "Acceptance criteria" heading. If the Ticket body doesn't have one, drop the section.
  - The Exponential URL: if `exponential` config exposes a base URL (e.g. `https://app.exponential.im`), build `<base>/tickets/<cuid>`. If unknown, omit the link.

- Open ready-for-review unless `--skip-checks` was used:

  ```bash
  gh pr create --base "<featureBase>" --title "<title>" --body "<body>" [--draft]
  ```

  Capture the URL from stdout.

### 7. Link the Ticket back

Always run, both modes:

```bash
exponential tickets update --id <cuid> \
  --pr "<pr-url>" \
  --branch "<current-branch>" \
  --status QA
```

`--branch` is a no-op if it's already set to the current branch, but harmless. `--pr` is what powers the GitHub Action's `findByPrUrl` lookup at merge time.

### 8. Clear the marker

On success only:

```bash
rm -f .exponential/current-ticket
```

If anything in steps 5–7 failed, **leave the marker** so the user can re-invoke `/ship-ticket` cleanly.

### 9. Report

Print:

- PR URL (clickable)
- Mode (new PR vs stack)
- Ticket status transition (`IN_PROGRESS → QA`)
- Reminder: once the PR merges into the **deploy trigger**, the Action scaffolded by `/setup-merge-hook` will auto-promote to `DONE`. If `/setup-merge-hook` hasn't been run, prompt the user.

## Failure modes

- **Pre-ship check failure** — refuse to push. The user can fix and re-invoke or pass `--skip-checks`.
- **No Ticket detected** — explicit arg, marker, and branch lookup all empty. Refuse with guidance.
- **Branch-lookup ambiguity** — multiple non-`QA` Tickets share the branch. Ask the user.
- **PR exists in draft + this invocation has `--skip-checks`** — leave it draft.
- **PR exists ready + this invocation has `--skip-checks`** — leave it ready; don't downgrade.
- **`docs/agents/git-flow.md` missing** — warn loudly, default to `main`, recommend `/setup-git-flow`.

## What this skill does NOT do

- It does not unship a Ticket (no automatic `QA → IN_PROGRESS` rollback). Deferred.
- It does not clean up abandoned PRs. Deferred.
- It does not promote `QA → DONE` itself — that's the GitHub Action's job, scaffolded by `/setup-merge-hook`.
