# Contributing

Thanks for your interest in Aquifer!

## Building

```bash
./gradlew build          # compile, test, and verify the public API surface
./gradlew :sample:run    # runnable tour of the library
./gradlew dokkaGenerate  # aggregated API docs in build/dokka/
```

Requirements: JDK 17+ (CI uses 21). The Gradle wrapper handles everything else.

## Project conventions

- **Explicit API mode** is on: every public symbol needs an explicit visibility modifier and
  should carry KDoc that states its contract (threading, failure behaviour, defaults).
- **Public API surface is locked** by the binary-compatibility validator. `./gradlew build`
  fails on any signature change; if the change is intentional, regenerate the dumps with
  `./gradlew apiDump` and commit the updated `api/*.api` files in the same PR.
- **Tests must be deterministic.** Use `kotlinx-coroutines-test` (`runTest`), inject the
  store's `scope` and `WallClock`, and assert stream emissions with Turbine. Note that
  `runTest` only drives `backgroundScope` work while the test coroutine is suspended — see
  `TestHelpers.kt`.
- **Generated files** (Gradle wrapper scripts) are upstream-owned; don't hand-edit them.

## Pull requests

- Keep PRs focused on one concern; include tests for behaviour changes and a README update
  when the public API grows.
- CI must be green: build, tests, and `apiCheck` all run on every PR.

## Releasing (maintainers)

Releases are cut by tagging: pushing a `v*` tag runs the `release` workflow, which publishes
to Maven Central via the Central Portal. It requires these repository secrets:
`MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`, `SIGNING_KEY` (ASCII-armored PGP),
and `SIGNING_KEY_PASSWORD`. Bump `version` in the module build files and update
`CHANGELOG.md` before tagging — the workflow refuses to publish when the tag doesn't match
the module versions or when the version is a `-SNAPSHOT`.
