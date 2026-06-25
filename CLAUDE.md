# CLAUDE.md

Guidance for AI agents working in this repository.

## Pull request workflow

- **Base branch:** Raise all PRs against `develop`, not `main`. (`develop` is the
  integration branch; `main` is reserved for releases.)
- **Self-review loop:** After opening a PR, listen to its activity (CI runs, review
  comments, and reviews) and address the feedback automatically — push fixes for
  clear items; ask only when a comment is genuinely ambiguous or architecturally
  significant.
- **Merge criteria:** Once reviews from **both Quasar Apps and Copilot** come back
  green with no outstanding feedback, you may merge the PR without asking. Until
  both are green and clear, do not merge.
