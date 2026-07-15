---
name: setup-merge-hook
description: Scaffold the GitHub Action that auto-promotes Exponential **Tickets** from `QA` to `DONE` when their PRs merge into the **deploy trigger**. Sets the `EXPONENTIAL_TOKEN` repo secret, captures the API URL, and writes `.github/workflows/exponential-promote.yml`. Use once per repo, after `/setup-git-flow` has identified the deploy trigger.
---

# Setup Merge Hook

Wire up the GitHub Action that closes the Ticket lifecycle: when a feature PR (or a **Rollup PR** that promotes work through the chain) merges into the **deploy trigger** branch, the Action looks up every Ticket linked to the merged PR (and to any child PRs referenced from a Rollup PR's commit messages) and transitions them to `DONE`.

PR-URL is the primary linkage and commit trailers are decoration — see [ADR-0002](../../../docs/adr/0002-pr-url-primary-trailer-fallback.md).

## Prerequisites

- `gh auth status` succeeds.
- `docs/agents/git-flow.md` exists. If not, error out with: *"Run `/setup-git-flow` first — this skill needs to know the deploy trigger."*
- Working inside a GitHub-hosted repo (the file format is GitHub Actions, not GitLab CI).

## Process

### 1. Read the deploy trigger

Parse `docs/agents/git-flow.md` front-matter for `deployTrigger`. If missing, fail with the same error as above.

### 2. Ensure the `EXPONENTIAL_TOKEN` secret exists

```bash
gh secret list --json name --jq '.[].name' | grep -q '^EXPONENTIAL_TOKEN$'
```

If absent, prompt the user:

> The Action needs a JWT to call the Exponential API. Get yours by running:
>
> ```bash
> exponential auth show --token
> ```
>
> Paste it here (input will be hidden):

Then:

```bash
echo "$token" | gh secret set EXPONENTIAL_TOKEN
```

Long-term, a scoped service-account token is preferable over a personal JWT — note that to the user.

### 3. Capture `EXPONENTIAL_API_URL`

Read it from the local exponential config (`exponential auth status` shows the API URL line) or prompt the user. Validate it's a URL (starts with `http://` or `https://`).

### 4. Scaffold the workflow file

Create `.github/workflows/exponential-promote.yml` from the template below. Substitute:

- `<deployTrigger>` — from step 1
- `<EXPO_URL>` — from step 3

Don't overwrite if the file already exists — show the existing content and ask the user before clobbering.

````yaml
name: Promote Exponential Tickets
on:
  pull_request:
    types: [closed]
    branches: [<deployTrigger>]
jobs:
  promote:
    if: github.event.pull_request.merged == true
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }
      - uses: actions/setup-node@v4
        with: { node-version: '20' }
      - run: npm i -g exponential-cli
      - name: Auth
        env:
          EXPO_TOKEN: ${{ secrets.EXPONENTIAL_TOKEN }}
        run: exponential auth login --token "$EXPO_TOKEN" --api-url <EXPO_URL>
      - name: Collect Ticket CUIDs to promote
        id: collect
        env:
          PR_URL: ${{ github.event.pull_request.html_url }}
          BASE_SHA: ${{ github.event.pull_request.base.sha }}
          HEAD_SHA: ${{ github.event.pull_request.head.sha }}
          REPO_URL: ${{ github.event.pull_request.base.repo.html_url }}
        run: |
          > /tmp/ids
          # Direct lookup: any Ticket whose prUrl equals this merged PR
          exponential tickets list --pr "$PR_URL" --json | jq -r '.[].id' >> /tmp/ids
          # Rollup lookup: a Rollup PR's commit log carries references to the
          # child feature PRs. Scan those, then look up each child's linked Tickets.
          git log "$BASE_SHA..$HEAD_SHA" --format=%B \
            | grep -oE '(\(#|Merge pull request #|^\* #)[0-9]+' \
            | grep -oE '[0-9]+' \
            | sort -u \
            | while read -r num; do
                exponential tickets list --pr "${REPO_URL}/pull/${num}" --json \
                  | jq -r '.[].id' >> /tmp/ids
              done
          sort -u /tmp/ids > /tmp/ids.uniq
      - name: Promote
        env:
          PR_URL: ${{ github.event.pull_request.html_url }}
        run: |
          while IFS= read -r cuid; do
            [ -z "$cuid" ] && continue
            exponential tickets update --id "$cuid" --status DONE
            exponential tickets comment add --id "$cuid" -m "Shipped: $PR_URL"
          done < /tmp/ids.uniq
````

### 5. Do not commit

After writing the file, print:

> Workflow written to `.github/workflows/exponential-promote.yml`. Review it, then commit + push to activate:
>
> ```bash
> git add .github/workflows/exponential-promote.yml
> git commit -m "ci: add Exponential Ticket auto-promotion on merge"
> git push
> ```

This is intentional: a CI workflow is shared infrastructure, and the user should eyeball it before pushing.

## Failure modes

- **No `gh` auth** — bail with `gh auth login` instructions.
- **No `docs/agents/git-flow.md`** — bail; recommend `/setup-git-flow`.
- **Workflow file already exists** — diff against the new template, ask before overwriting.
- **API URL not in config** — prompt the user explicitly; don't assume `https://app.exponential.im`.

## What this skill does NOT do

- Run the workflow. The user commits + pushes to activate.
- Set up rotating service-account tokens. Personal JWT is the current state; rotate later.
- Handle GitLab CI / Bitbucket pipelines. GitHub Actions only.
