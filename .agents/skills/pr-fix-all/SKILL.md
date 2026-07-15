---
name: pr-fix-all
description: Address review comments on a PR end-to-end — fix code, commit per-finding, push, prepend "✅ Addressed in <sha>" to each comment, then optionally resolve the threads. Use when you've got open review comments on a PR (manual, CodeRabbit, /pr-review output, etc.) and want to clear them without losing the historical record. Complements /pr-review.
---

# PR Fix All

End-to-end loop for clearing PR review comments without losing the historical context.

For each open review thread, this skill:

1. Reads the comment to understand the ask.
2. Edits the code in a **worktree** so the user's current branch stays untouched.
3. Commits one fix per finding, co-authored with the PR author.
4. Pushes (fast-forward — no force-push since fixes go on top of the PR head).
5. **Annotates the original comment** with a `✅ Addressed in <sha>` header and a divider; the original analysis stays below.
6. Optionally **resolves the thread** via `resolveReviewThread` GraphQL.

The annotate-don't-replace pattern preserves the audit trail: reviewers can still see what was flagged and what was fixed.

## Process

### 1. Pin the PR

If the user passed a number or URL, use it. Otherwise ask once: "Which PR — number, URL, or `current branch`?" Capture:

```bash
gh pr view <num> --json headRefOid,headRefName,baseRefName,url,author,number
```

Note the PR branch name (the head ref) and whether it's from a fork.

### 2. Fetch unresolved review comments

REST gives you comments; GraphQL gives you threads (with `isResolved` and the thread ID needed for step 7). Use GraphQL:

```bash
gh api graphql -f query='
query {
  repository(owner: "<owner>", name: "<repo>") {
    pullRequest(number: <num>) {
      reviewThreads(first: 100) {
        nodes {
          id
          isResolved
          comments(first: 50) {
            nodes { databaseId path line body author { login } }
          }
        }
      }
    }
  }
}'
```

Filter to `isResolved: false`. The first comment in each thread is the original review comment — that's what you'll annotate in step 6. Skim replies for any clarification or pushback.

If there are many unresolved threads, summarise back to the user — file → severity-or-title → confirm scope before proceeding. Don't silently address dozens of comments.

### 3. Set up the worktree

The PR branch is usually not what the user has checked out. Don't switch branches in the main checkout — make a worktree:

```bash
git fetch origin <pr-branch>
git worktree add -B <pr-branch> /tmp/<repo>-<pr-num> origin/<pr-branch>
```

All subsequent edits happen in `/tmp/<repo>-<pr-num>`. The user's main checkout is left alone, so an open IDE, an in-progress edit, and the rest of their work survives.

### 4. Fix one finding at a time

For each thread:

1. Read the file at the comment's line, plus enough surrounding context to understand the fix.
2. Apply the smallest fix that addresses the comment. If the reviewer proposed a fix, follow it unless you've spotted a problem with it — in which case surface the disagreement to the user rather than silently diverging.
3. **One commit per finding** with a descriptive subject and a body that summarises the original analysis (so the fix commit stands on its own, without the reviewer's prose). Include both co-authors:

   ```
   Co-Authored-By: <pr-author-login> <pr-author-email>
   Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
   ```

4. Capture the resulting short SHA — you'll need it for the annotation step.

If a finding is **already fixed** (the reviewer was prescient, or another author landed a fix), skip the code change and find the existing SHA via `git log --all --grep="<keyword>"`. You'll still annotate that comment in step 6 using that earlier SHA.

### 5. Push

```bash
cd /tmp/<repo>-<pr-num>
git push origin <pr-branch>
```

This is a regular fast-forward — **no `--force`**, because fixes go on top of the current PR head. If you hit a non-fast-forward error, **stop and surface to the user**: someone else pushed in the meantime, and the right move is theirs (rebase, merge, or coordinate).

### 6. Annotate each comment

For each fixed thread, prepend a header to the original comment body using the REST API (the body field on review comments accepts the full markdown, replacing what was there — so read, prefix, write back):

```
> ✅ Addressed in [`<sha>`](https://github.com/<owner>/<repo>/pull/<num>/commits/<sha>) — <commit subject>

---

<original body>
```

```bash
existing=$(gh api "repos/<owner>/<repo>/pulls/comments/<comment_id>" --jq .body)
new_body="> ✅ Addressed in [\`<sha>\`](https://github.com/<owner>/<repo>/pull/<num>/commits/<sha>) — <subject>

---

${existing}"
gh api -X PATCH "repos/<owner>/<repo>/pulls/comments/<comment_id>" -f "body=$new_body"
```

The leading `> ` quote prefix visually offsets the annotation; the `---` divider separates it from the original analysis. The original is preserved verbatim so anyone reading the thread later can still see what was caught and reason about whether the fix matches.

**Idempotency:** before patching, check whether the body already starts with `> ✅ Addressed`. If so, skip — re-running the skill shouldn't pile up headers.

### 7. Resolve the threads (gated)

Ask once before running:

> "Resolve all <N> threads now? They'll collapse under 'Resolved conversations' on the PR. (Y/n)"

Default is yes. Resolve uses GraphQL — REST does not expose this action:

```bash
gh api graphql -f query="
mutation { resolveReviewThread(input: {threadId: \"<thread_id>\"}) { thread { isResolved } } }
"
```

If the user wants to reopen one later, the mirror mutation is `unresolveReviewThread` with the same input shape.

## Edge cases & guardrails

- **Multiple comments from different reviewers in one thread.** Resolve once when the fix lands; don't try to address each comment separately.
- **Comment proposes the wrong fix.** If following the suggestion would introduce a regression, push back to the user before committing. The skill is not a rubber stamp.
- **Comment is no longer applicable.** (Code deleted, requirement changed.) Annotate with `> ℹ️ No longer applicable — <reason>` instead of `✅ Addressed`, then resolve in step 7.
- **PR is from a fork without push access.** Switch the workflow: open a follow-up PR targeting the fork's branch. Surface this to the user before proceeding — don't try to invent permissions.
- **Server-side schema changes.** If a fix touches a tRPC router input, a DB schema, or anything else with backend reach, flag it explicitly — that's a higher-risk class than UI tweaks and may warrant a separate review.
- **CI is your safety net.** A fresh worktree has no `node_modules`, so you typically can't run `tsc --noEmit` locally without an install step. Push and watch CI. Fix-forward if a check fails.
- **Force-push hazard.** This skill must never `--force` push. If you find yourself wanting to, you've drifted from the on-top-of-head model — stop and re-examine.

## Why this pattern

- **One commit per finding** keeps the trail traceable. Each `✅ Addressed in <sha>` is a focused diff a reviewer can verify in seconds.
- **Annotation, not replacement** preserves the historical record. The reviewer's analysis is still readable in 6 months; the fix is linked from it.
- **Worktree** isolates the work — the user's checked-out branch, open files, and in-progress edits survive untouched.
- **Gated resolve** keeps the destructive-feeling action out of autopilot. The user opts in once at the end.
- **Fast-forward only** is what you want on a PR branch with collaborators — no overwriting anyone else's commits, no SHA churn for reviewers.
