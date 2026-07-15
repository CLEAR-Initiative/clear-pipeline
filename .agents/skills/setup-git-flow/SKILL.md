---
name: setup-git-flow
description: Detect this repo's branching model (GitFlow, simple develop+trunk, or trunk-based) and persist the resulting **Promotion chain** to `docs/agents/git-flow.md`. Other skills (`/ship-ticket`, `/setup-merge-hook`) read that file to know which branch to target as the **featureBase** and which branch is the **deploy trigger**. Use when starting a new repo, or when the existing `docs/agents/git-flow.md` is missing or stale.
---

# Setup Git Flow

Detect the repo's branching model and record it so the rest of the **Ticket** lifecycle skills know:

- **Trunk** — the repo's default branch (almost always `main`)
- **Promotion chain** — the ordered list of branches work flows through before reaching the Trunk
- **featureBase** — the branch new feature PRs target (the first node in the Promotion chain)
- **Deploy trigger** — the branch whose merge transitions a Ticket from `QA` to `DONE` (almost always equals the Trunk)

The output is a single markdown file at `docs/agents/git-flow.md` with deterministic front-matter that downstream skills can parse.

## Prerequisites

- Inside a git repo (`git rev-parse --show-toplevel` succeeds).
- `gh` authenticated (`gh auth status` ok) — needed to read the default branch reliably.

If `gh` isn't installed, fall back to `git symbolic-ref refs/remotes/origin/HEAD` and tell the user the result is best-effort.

## Process

### 1. Detect

Run these in parallel:

```bash
gh repo view --json defaultBranchRef --jq .defaultBranchRef.name
git ls-remote --heads origin | awk '{print $2}' | sed 's@refs/heads/@@'
```

The first gives the **Trunk**. The second is the full list of remote-tracking branches — scan it for these names: `develop`, `staging`, `release`, `qa`, `uat`, `preprod`.

### 2. Apply heuristics

Pick the model in this order — first match wins:

1. **Full GitFlow** — `develop` AND (`staging` OR `release`) AND trunk all exist.
   - Promotion chain: `develop → staging → main` (or `develop → release → main`)
   - featureBase: `develop`
   - Deploy trigger: trunk (the last node)
2. **Simple develop+trunk** — `develop` exists AND trunk exists, no intermediate.
   - Promotion chain: `develop → main`
   - featureBase: `develop`
   - Deploy trigger: trunk
3. **Trunk-based** — no `develop`/`staging`/`release`/`qa`/`uat`/`preprod` branches found.
   - Promotion chain: just `main` (single node)
   - featureBase: trunk
   - Deploy trigger: trunk

If the detection finds something unusual (e.g. `staging` but no `develop`, or multiple intermediate branches in an unexpected order), don't guess — surface what you found and let the user dictate the chain.

### 3. Confirm with the user

Show:

- The detected model with a one-line explanation
- The proposed Promotion chain as an arrow-separated list
- The featureBase and Deploy trigger

Ask the user to confirm or correct. Common corrections:

- The Deploy trigger isn't the trunk (e.g. `main` is a release-tag branch, `production` is where deploys actually fire from). Re-prompt.
- featureBase should be a specific long-lived branch other than what was detected.
- The user prefers a different ordering than the heuristic produced.

### 4. Write `docs/agents/git-flow.md`

Create the file (or overwrite if the user agreed) using this template. Downstream skills parse the YAML-style front-matter — preserve key names exactly.

````markdown
---
trunk: <branch>
featureBase: <branch>
deployTrigger: <branch>
promotionChain:
  - <branch>
  - <branch>
  - <branch>
---

# Git flow for this repo

**Model**: <full-gitflow | simple-develop-trunk | trunk-based | custom>

**Promotion chain**: `<a>` → `<b>` → `<c>`

- **featureBase** (`<branch>`) — new feature PRs (the output of `/ship-ticket`) target this branch.
- **deployTrigger** (`<branch>`) — when a PR merges into this branch, the GitHub Action scaffolded by `/setup-merge-hook` transitions any linked Tickets from `QA` to `DONE`.

## How skills use this file

- `/ship-ticket` reads `featureBase` to set the base branch of new PRs.
- `/setup-merge-hook` reads `deployTrigger` to set the `on.pull_request.branches` filter for the GitHub Action.
- The Action scans **Rollup PRs** (PRs that promote work between chain nodes) for child PR references in commit messages so Tickets linked to feature PRs are still promoted when their work reaches the deployTrigger through the chain.
````

If `docs/agents/` does not exist yet, create it. Don't touch any other files in that directory.

### 5. Tell the user what's next

Once written, point out:

- `/ship-ticket` will now know what base branch to target.
- `/setup-merge-hook` is the next step to wire up auto-promotion to `DONE`.
- The file is plain markdown — they can edit it directly later. Re-running this skill only needed if the branching model itself changes.
