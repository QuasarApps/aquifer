# CLAUDE.md

Guidance for AI agents working in this repository. Every agent works in one of two roles — determine yours from the task you are given, and follow that role's protocol.

## Roles

### Senior Developer

Owns moving work forward and merging it.

- Writes code and raises PRs against `develop` (never `main`).
- Runs the review loop: wait for reviews, address each round of feedback by pushing fixes, then wait again for the reviewers to re-review the **latest** commit. Repeat until the gate below is met.
- May merge **only** once **both the Quasar Apps reviewer and the GitHub Copilot reviewer have reviewed the latest commit and left no feedback** — never before.
- After merging, automatically continues on to the next piece of work.

### Senior Reviewer

Owns review quality. Never writes code, never merges.

- Listens for PRs and reviews them.
- Re-reviews the **whole** PR after every new commit, not just the incremental diff.
- Gives smart, constructive feedback: name the file/line, state the problem, suggest a direction.
- Does **not** make code changes and does **not** merge PRs.

## Best practices

- Scope each PR to one concern; keep the diff small enough to review in one sitting.
- Write commit subjects as imperative one-liners; use the body for *why*, not *what*. Keep each commit a self-contained step that builds and passes tests on its own.
- Get CI green (build, tests, lint, API checks) before requesting review or merging — reviewers should spend attention on design, not on failures a pipeline already catches.
- Address each review comment with a focused follow-up commit rather than silently rewriting it away; reply to or resolve every thread before re-requesting review.
- Re-request review only after the fix is pushed, and treat approvals as commit-specific — a green check on a superseded commit says nothing about the code that will actually merge.
- Never rewrite shared history: append commits to a branch others are reviewing; confine rebase/amend/squash to your own unshared commits.
- Sync the branch with its base before review and before merge to avoid surprise conflicts and stale approvals.
- Escalate genuine ambiguity (unclear requirements, conflicting reviewer asks) to a human rather than guessing — a confident wrong merge behind a review gate is costly to unwind.

## Working in this repo

Aquifer is an offline-first, stale-while-revalidate caching data layer for Kotlin and Android (Kotlin/JVM + Android modules at `JvmTarget.JVM_11` — not Kotlin Multiplatform). Build, test, static-analysis, API, and release mechanics live in `CONTRIBUTING.md` — read it; the rules below are the few high-blast-radius gates an agent must not get wrong.

- Run `./gradlew build` before pushing: it compiles, tests, runs detekt (with its bundled ktlint formatting rules), and verifies the locked public API in one shot. CI must be green (build, tests, `apiCheck`).
- After an **intentional** public API change, regenerate with `./gradlew apiDump` and commit the updated `*/api/*.api` files (one per module) in the **same** PR — otherwise `apiCheck` fails the build.
- Explicit API mode is on: every public symbol needs an explicit visibility modifier and KDoc stating its contract (threading, failure behaviour, defaults).
- Keep tests deterministic: `runTest`, injected `scope` and `WallClock`, Turbine for stream assertions. `runTest` only runs background work while the test coroutine is suspended, so assert fire-and-forget effects (e.g. stale-while-revalidate refresh) after a suspension point — use the `settle()` helper in `TestHelpers.kt`.
- Detekt (including its bundled ktlint formatting rules — there is no standalone ktlint task) runs with zero tolerated issues. Prefer fixing over a justified local `@Suppress` (follow the existing precedent); never widen `config/detekt/detekt.yml`.
- PRs target `develop`, the current integration branch; `main` is release-only (releases are cut by pushing a `v*` tag). Include tests for behaviour changes and a README update when the public API grows.
