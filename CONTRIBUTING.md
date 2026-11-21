# Contributing to clear-pipeline

Thanks for helping. This guide covers how to get started, how to make a change, and how we handle configuration and secrets. For what the pipeline is and how it fits into CLEAR, read the [README](README.md) first.

## Two ways in

### Explore on your own (no account needed)

You can clone the repo, run the tests and browse the whole pipeline in the Dagster UI without any credentials.

You need [uv](https://docs.astral.sh/uv/getting-started/installation/) and Python 3.10 to 3.14 (CI uses 3.12).

```bash
git clone https://github.com/CLEAR-Initiative/clear-pipeline.git
cd clear-pipeline
uv sync                            # dependencies plus the dev group (dagster-dg-cli, pytest, ty)
uv run --frozen pytest -q          # the test suite, as CI runs it
uvx ruff@0.16.9 check src tests    # lint, at the version CI pins
uv run dg check defs               # check that every Dagster definition loads
uv run dg dev                      # the Dagster UI at http://localhost:3000
```

None of these need a `.env` file. The tests need no credentials, and the definitions load with every credential unset: a missing credential only fails when an asset that needs it runs.

In the Dagster UI you can browse every asset, job, sensor and schedule, see how they connect, and read their descriptions. The [README](README.md#what-the-pipeline-does) maps each area to its code. Most sensors ship stopped, but a few ship running, so expect some runs to fail for missing credentials. That is normal. Stop those sensors in the UI if the noise gets in the way.

Running the pipeline for real needs credentials the team shares. [`.env.example`](.env.example) documents every variable. The main groups:

| Variables | What it is | What won't run without it |
|---|---|---|
| `CLEAR_API_URL`, `CLEAR_API_KEY` | A clear-api endpoint and a key for a user with the `admin` or `pipeline` role | Almost everything. Every read and write goes through clear-api. |
| `CLEAR_WORKER_API_KEY` | A key for clear-api's `worker` service user | The Task Worker (`drain_tasks`) |
| `S3_ENDPOINT`, `S3_REGION`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` | An S3-compatible bucket | The knowledge base, raw source blobs and the data-quality layers |
| `LLM_<ROLE>_API_KEY` (with `LLM_<ROLE>_PROVIDER`, `LLM_<ROLE>_MODEL`) | Anthropic, or another provider, per role | Classification, grouping, extraction, translation and generation |
| `EMBEDDING_API_KEY` | Voyage embeddings | Knowledge-base embeddings |
| `STT_API_KEY` | OpenAI Whisper | Hotline voice-note transcription |
| `OPENROUTER_API_KEY` | OpenRouter | The Jev disaster-type classifier and the model-replacement eval |
| `DATAMINR_CLIENT_ID`, `DATAMINR_CLIENT_SECRET` | Dataminr | The Dataminr signal source |
| `ACLED_USERNAME`, `ACLED_PASSWORD` | ACLED | The ACLED signal source |
| `IDMC_CLIENT_ID` | IDMC | The IDMC signal source |
| `IOM_DTM_SUBSCRIPTION_KEY`, `ACAPS_API_KEY` | IOM DTM and ACAPS | The monthly location-metadata job |
| `HAPI_APP_IDENTIFIER` | HDX HAPI (you can generate your own at https://hapi.humdata.org/docs) | The daily location-metadata job |
| `REDIS_URL` | Redis, defaulting to `redis://localhost:6379/0` | The signal and queue drains (not a credential: run Redis locally) |

GDACS and darfur24 need no source credentials, but like every source they still write to clear-api.

### Join the team

1. Register at https://api.clearinitiative.io/portal/login.
2. The team will get in touch with next steps, including access to the tracker and any dev credentials you need. Credentials are shared privately.

Never ask for credentials in an issue or a pull request.

## Making a change

### Where work is tracked

Work is tracked in [Exponential](https://www.exponential.im/w/clear/products/clear), not GitHub Issues. Team members get access after registering. See [docs/agents/issue-tracker.md](docs/agents/issue-tracker.md) for the conventions.

Outside contributors: small fixes are welcome as pull requests. For anything bigger, register first so it can be discussed before you build it.

### Branch and pull request

- Branch from `dev` and open a PR back into `dev`. A merge into `dev` ships to dev; "Sync prod with dev" PRs ship `dev` to `prod` (production). `master` is legacy; do not target it. See [docs/agents/git-flow.md](docs/agents/git-flow.md).
- Name branches `feat/<short-slug>` (or `fix/<short-slug>` for a fix).
- Merge through a PR, never a direct push to `dev`. Each push to `dev` ships a new dev image.

### Checks before you push

Run the same checks CI runs ([`.github/workflows/check.yml`](.github/workflows/check.yml)):

```bash
uv run --frozen pytest -q
uvx ruff@0.16.9 check src tests
```

Ruff is pinned to 0.16.9. Use that version so your result matches CI.

### Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/), usually with the area as the scope:

```
feat(signals): unified LLM event extraction + severity
fix(dataminr): keep the source link from legacy alerts (publicPost.link)
docs: rewrite README for first-time readers
```

PRs are squash-merged, so the PR title becomes the commit on `dev`. Give it the same form.

### Review and merge

- Every PR into `dev` runs pytest and ruff (`Check`). `Check` also fails while the PR description's **Manual deploy steps** section is blank: write "None", or list each step the code needs but no code performs (for example a clear-api `data_sources` row) with its exact command, a `- [ ] dev` and a `- [ ] prod` checkbox, and a link to the ticket that tracks the prod step. You own each step until prod is ticked.
- [Greptile](https://www.greptile.com/) reviews PRs automatically. A workflow ([`bot-greptile-automerge.yml`](.github/workflows/bot-greptile-automerge.yml)) squash-merges the PR once Greptile has reviewed its latest commit with no unresolved P0/P1 findings and the required checks pass. P2 findings don't block.
- Add the `no-automerge` label to keep a PR for a human.
- PRs from forks, drafts and bot authors are never auto-merged. A maintainer merges them.

### Shipping to prod

A "Sync prod with dev" PR (head `dev`, base `prod`) is the release. Open it and
leave the description alone: [`sync-pr-body.yml`](.github/workflows/sync-pr-body.yml)
fills it in with every PR the sync ships and the **Manual deploy steps** those
PRs declared, as a checklist. Anything you write outside the generated block is
kept.

Work down the checklist against prod, ticking each box as you go. While any
`prod` box is unticked the PR carries a **`prod-steps-outstanding`** label and a
*changes requested* review listing what is left; both clear themselves on the
next run once every box is ticked, and re-running keeps the ticks you have
already earned. PRs merged before the Manual deploy steps section was required
have their **Merge Danger** notes reproduced instead; read those by hand.

**The review really does block the merge.** `prod` is a protected branch
requiring one approving review, so a sync PR sits at `BLOCKED` while the bot's
*changes requested* review stands — verified on clear-api #358, which went from
`CLEAN` to `BLOCKED` the moment protection was applied (clear #1556). Ticking
the last box dismisses the review, and the PR then needs a human approval like
any other.

Administrators can still merge without an approval: `enforce_admins` is
deliberately off so an emergency prod hotfix is never wedged behind an absent
reviewer. The gate is loud, not absolute, and an override is visible in the PR
timeline.

The signal is a label and a review rather than a failing check on purpose. A
sync PR's head branch is `dev`, so its head SHA *is* dev's tip, and GitHub
attaches check runs to commits — a failing check showed up against whatever had
just been merged into `dev` and read as that PR having broken something
(clear #1562). So nothing here will ever make `dev` look red.

To see what the next sync would say, or what is still outstanding on an open
one:

```bash
GITHUB_REPOSITORY=CLEAR-Initiative/clear-pipeline \
  node .github/scripts/build-sync-pr-body.mjs render

gh pr view <sync-pr> --json body --jq .body \
  | PR_BODY="$(cat)" node .github/scripts/build-sync-pr-body.mjs check
```

### Rules that matter here

- **Never write to a database directly.** Every read and write goes through clear-api's GraphQL API ([`providers/clear_api.py`](src/clear_pipeline/providers/clear_api.py)). clear-api owns the domain schema.
- **All LLM calls go through [`providers/llm.py`](src/clear_pipeline/providers/llm.py)** (`make_llm_provider(role)`). That is where provider and model choice, timeouts, fallbacks and token usage are handled, so don't call a provider SDK directly.
- **Extraction schemas are versioned.** Every extracted row carries its `SCHEMA_VERSION`, and aggregation only combines rows of the same version. A bump applies to reports as they are next extracted, and older reports need an explicit backfill to move to the new version. Re-extraction costs real money, so bundle schema changes into one bump. See [Schema versioning](docs/humanitarian-datapoint-extraction.md#7-schema-versioning).
- **Use the domain vocabulary.** [CONTEXT.md](CONTEXT.md) defines the terms (Population Affected vs People in Need, Report vs Incident, why unknown severity stays null). Design decisions live in [docs/adr/](docs/adr/).
- **Adding a signal source** means adding a connector to `CONNECTORS` in [`defs/signals/connectors.py`](src/clear_pipeline/defs/signals/connectors.py).

## Environment variables

To add a variable:

1. Add it to [`.env.example`](.env.example) with a comment: what it is, whether it is required, and its default.
2. Read it where the code needs it. Signal-pipeline settings live in [`signals/config.py`](src/clear_pipeline/signals/config.py); other areas read it in the provider or asset that uses it. Follow the existing pattern: a missing credential must not stop the definitions loading. Check for it when the asset runs and fail with a clear message.
3. Values for the deployed environments are set by the team in a private infrastructure repo. List it under **Manual deploy steps** in your PR so it gets added for dev and prod.

## Secrets

- Never commit `.env` or any secret.
- Never paste secret values into issues, PRs, chat or AI tools.
- If a secret leaks, tell the team immediately so it can be rotated.
- Credentials only ever come from the team, privately.

## Reporting a security issue

Don't open a public issue. Use GitHub's private vulnerability reporting: open the [Security tab](https://github.com/CLEAR-Initiative/clear-pipeline/security) and choose "Report a vulnerability", or go straight to https://github.com/CLEAR-Initiative/clear-pipeline/security/advisories/new.

## Licence

clear-pipeline is licensed under the [GNU Affero General Public License v3.0 or later](./LICENSE).
By contributing, you agree your contribution is licensed the same way.
