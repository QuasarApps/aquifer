# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) once 1.0 ships; until then minor
versions may contain breaking changes.

## [Unreleased]

### Added — TTL jitter

- `ttlJitter` (in `[0, 1]`) on `freshness { }`: each entry's effective time-to-live is
  deterministically shortened by a factor derived from its key and write timestamp, spreading
  the expiries of entries fetched together so they don't all revalidate at once — the
  request-stampede mirror of retry jitter. Shorten-only (`timeToLive` stays the hard upper
  bound), stable per entry (no fresh/stale flickering; the verdict also survives restarts
  for keys with value-based `hashCode`s — the norm), and per-call `maxAge` overrides are
  never jittered. 0 (default) disables it.

### Added — negative caching

- `negativeCache { }` on the builder: terminal fetch failures are remembered per key for
  `timeToLive`, during which strategy-driven refetches (`CacheFirst`, `StaleWhileRevalidate`,
  `NetworkFirst`, `revalidateActive`, new stream subscriptions) are suppressed — reads serve
  the cached value when one exists (stale-if-error without re-asking the network) and
  otherwise fail fast with the remembered error. `NetworkOnly`/`fresh()` deliberately bypass
  the memory; success, `put`, and `invalidate` clear it. Disabled unless configured.
- Backoff memory: consecutive failures (no intervening success or mutation) stretch the
  window by `backoffMultiplier`, capped at `maxTimeToLive` — window expiry re-allows
  fetching but never resets the streak.
- New `AquiferEvents.onFetchSuppressed(key, error, remaining)` reports each suppressed read.

### Added — conditional fetching (ETag / Last-Modified)

- `conditionalFetcher { key, validator -> FetchResult }` on the builder: the fetcher
  receives the cached entry's opaque validator (an `ETag`, `Last-Modified` value, or any
  token from a previous `FetchResult.Fresh`) and may answer `FetchResult.NotModified` — the
  store keeps the cached value, refreshes its age so TTL decisions start over, and the
  payload never crosses the network. Plain `fetcher { }` stores are untouched, including
  their exact fetch path (no pre-fetch entry read). Configure exactly one of the two.
- Validators live in memory and in `PersistedEntry` (new defaulted `validator` field —
  binary-breaking pre-1.0, source-compatible), and the JSON file store persists them:
  revalidation stays cheap across process restarts, and pre-validator cache files decode
  as `validator = null`.
- **`aquifer-okhttp`** (new module): `okHttpConditionalFetcher(callFactory, request, parse)`
  wires the headers automatically — captures `ETag`/`Last-Modified` from responses, replays
  `If-None-Match`/`If-Modified-Since`, maps 304 to `NotModified`, and fails non-2xx through
  Aquifer's normal retry/failure path.
- Local `put`s store no validator, so the first fetch after a local edit is unconditional
  by construction; `NotModified` without a cached entry fails the fetch (fetcher contract
  violation).
- Release workflow: the tag-vs-version gate now covers `aquifer-compose` (previously
  missed) and the new `aquifer-okhttp`.

### Added — Compose integration & DataState ergonomics

**`aquifer-compose`** (new module)

- `Aquifer.collectAsState(key, freshness, …)`: lifecycle-aware Compose collection of a key's
  stream, remembered across recompositions, starting from `Loading(null)`.
- `Aquifer.rememberStream(key, freshness)`: the remembered raw stream for custom operators.
- `previewAquifer(vararg entries)`: a fetch-free store for `@Preview`s and UI tests, with
  live `put`/`invalidate` behavior for interactive previews.

**`aquifer-core`**

- `DataState` extensions: `isLoading`, `valueOrThrow()`, `map`, `onContent`, `onFailure`.

### Added — `DataState.Empty` (observable deletion, RFC #23)

- New sealed member `DataState.Empty`: the store affirmatively has no value and the
  stream's strategy will not fetch one. Emitted only to `CacheOnly` streams — on initial
  collection of a missing key, and when the key is dropped by `invalidate`/`invalidateAll`
  while the stream is active. Cache-only screens now observe logout-style resets instead of
  rendering deleted data forever. Fetch-capable streams are unchanged (their refetch's
  `Loading(null)` already communicates the deletion). `previewAquifer` streams missing keys
  as `Empty` too.
- **Breaking (pre-1.0), source**: every exhaustive `when` over `DataState` needs an
  `is DataState.Empty ->` branch.
- **Breaking (pre-1.0), behavioral**: a `CacheOnly` stream of a missing key emits `Empty`
  where it previously emitted `Failure(CacheMissException)` — nothing failed, so it is no
  longer reported as a failure. One-shot `get(key, CacheOnly)` still throws
  `CacheMissException`.
- `map` passes `Empty` through; `valueOrThrow()` throws `NoSuchElementException` on it;
  `onContent`/`onFailure` ignore it.

### Added — bounded disk store

- `JsonFileSourceOfTruth` accepts optional `maxEntries`/`maxBytes` caps (constructor and
  `jsonFileSourceOfTruth` factory), enforced by least-recently-used eviction after every
  write. Recency is exact within a process (reads count as use) and seeds from file
  modification times across restarts; a store found over budget on first use — say, after
  caps were lowered in an update — is trimmed immediately. The byte cap is absolute: an
  entry larger than `maxBytes` on its own is not retained. Unbounded remains the default
  and adds no per-operation locking or accounting — just the one-time temp GC below and a
  per-call flag check.
- Temp files orphaned by a crash mid-write are now garbage-collected the first time a store
  touches the filesystem (previously only `deleteAll` removed them), bounded or not.

### Added — per-call freshness

- `maxAge` parameter on `Aquifer.get`, `Aquifer.stream`, and Compose's
  `collectAsState`/`rememberStream`: a per-call override of the store's time-to-live.
  Fetch decisions change only for the staleness-aware strategies (`CacheFirst`,
  `StaleWhileRevalidate`) — `CacheOnly`, `NetworkFirst`, and `NetworkOnly` fetch exactly as
  before — but a stream's `isStale` flags follow the caller's `maxAge` under every strategy.
  `Duration.INFINITE` is allowed and means "serve anything cached, fetch only on miss".
  Implementor note: the `Aquifer` interface methods gained a parameter (source-breaking for
  custom implementations, pre-1.0).

### Tooling

- Toolchain refresh (supersedes the Dependabot group bump): Gradle 9.5.1, Kotlin 2.4.0,
  coroutines 1.11.0, serialization 1.11.0, Dokka 2.2.0, vanniktech maven-publish 0.36.0
  (with the required AGP 8.13.0 companion), Robolectric 4.16.1, molecule 2.2.0. JUnit stays
  on the 5.x line: JUnit 6 ships JVM-17+ variants only, incompatible with the library's
  deliberate JVM 11 target — documented in the version catalog, and Dependabot now ignores
  junit-bom majors.

- Static analysis: detekt 1.23.8 (including the ktlint formatting ruleset) runs on every
  module as part of `check`/`build` with `maxIssues: 0`. The codebase is finding-free; the
  handful of deliberate engine patterns (catch-everything fences, the cohesive engine class)
  carry justified local suppressions.

### Hardening, round two (post-review fixes, pre-0.1.0)

- **Persistence hydration is now epoch-fenced like fetch commits**: a `SourceOfTruth.read`
  suspended across `invalidate`/`invalidateAll` can no longer put the deleted entry back
  into the memory cache, and memory is re-checked under the commit lock so a fetch commit
  that races the storage read is never overwritten by the older disk snapshot. The hot
  memory path still never takes the commit lock.
- **Stream ordering is clock-independent**: events carry a store-global commit sequence
  (assigned under the commit guard) instead of relying on `writtenAtMillis`, so same-
  millisecond ties and backwards wall-clock steps can neither reorder nor silence updates.
- A hydrated disk snapshot evicted by LRU pressure before its stream subscribes is no longer
  dropped: an unchanged epoch proves the gap was eviction, not invalidation.
- A failing revalidation *sweep* (e.g. a throwing storage read) no longer ends the
  `revalidateOn` subscription; only a failure of the trigger flow itself does. Both are
  reported via `AquiferEvents.onRevalidationTriggerFailed`, whose docs now say so.
- Docs: single-flight dedup is per-epoch (a mutation during a fetch can briefly overlap two
  requests for one key); the file store's new-entry durability after a crash is best-effort
  (no directory fsync) while the previous-entry guarantee stands.

### Hardening (post-review fixes, pre-0.1.0)

- **Mutation fencing**: `put`/`invalidate`/`invalidateAll` now fence off fetches that were
  already in flight — their responses can no longer resurrect invalidated data (memory *and*
  persistence) or clobber newer local writes, and post-invalidation stream refetches start
  genuinely new requests instead of joining the doomed one.
- A throwing `revalidateOn` trigger no longer escapes as an uncaught exception (an app crash
  on Android); it ends only its own subscription and is reported via the new
  `AquiferEvents.onRevalidationTriggerFailed`.
- A store whose parent scope is cancelled now reports itself closed: in-flight `get`s fail
  with `AquiferException` instead of silently cancelling the caller's coroutine, and
  subsequent calls fail fast instead of hanging.
- Stream startup no longer performs storage I/O while subscribed to the update bus, so a slow
  `SourceOfTruth.read` can no longer stall writers and fetch completions store-wide; bus
  events buffered across the snapshot can no longer regress a collector to an older value.
- True API 21 compatibility: replaced `ConcurrentHashMap.merge`/`computeIfPresent`
  (Android API 24+) with CAS loops; documented that `aquifer-persistence-file` requires
  API 26+ or NIO desugaring.
- `JsonFileSourceOfTruth` fsyncs before its atomic rename, making the crash-safety claim hold
  on journaling filesystems.
- `aquifer-android` now exposes `androidx.lifecycle` as an `api` dependency (it appears in
  public signatures); release builds skip duplicate unit-test runs.
- The release workflow's version gate now covers `aquifer-android` too.

### Added — 0.1.0 scope

**`aquifer-core`**

- `Aquifer<K, V>`: keyed, offline-first single source of truth with `stream`, `get`, `fresh`,
  `put`, `invalidate`, `invalidateAll`, `revalidateActive`, `revalidateOn`, and `close`.
- `DataState` stream snapshots (`Loading` / `Content` / `Failure`) that always carry the last
  known value, plus `Origin` (`MEMORY` / `PERSISTENCE` / `FETCHER` / `LOCAL`).
- Five `Freshness` strategies: `CacheOnly`, `CacheFirst`, `StaleWhileRevalidate`,
  `NetworkFirst`, `NetworkOnly`.
- `aquifer { }` builder DSL: `fetcher`, `freshness`, `memoryCache`, `persistence`, `retry`,
  `events`, `clock`, `scope`.
- Per-key single-flight deduplication; fetches run in the store's scope and survive caller
  cancellation.
- Update bus keeping every active stream of a key coherent, with per-collector isolation so a
  stalled collector can never block the engine.
- Bounded LRU memory cache with TTL-aware staleness via an injectable `WallClock`.
- `SourceOfTruth` persistence abstraction: hydration on memory misses, persisted timestamps,
  best-effort write-through after fetches, all-or-nothing direct mutations.
- Opt-in retries: exponential backoff, hard `maxDelay` cap, delay-shortening jitter,
  `retryOn` predicate; cancellation is never retried.
- `AquiferEvents` observability hooks: fetch started/succeeded/retried/failed and persistence
  write failures.

**`aquifer-android`**

- `Context.connectivityRestoredFlow()` and `Aquifer.revalidateOnReconnect(context)`:
  ConnectivityManager-backed offline→online trigger that ignores already-present connectivity
  and Wi-Fi↔cellular handovers; `ACCESS_NETWORK_STATE` is declared in the library manifest.
- `appForegroundedFlow()` and `Aquifer.revalidateOnAppForeground()`: ProcessLifecycleOwner-
  backed background→foreground trigger that ignores the app launch itself.

**`aquifer-persistence-file`**

- `JsonFileSourceOfTruth`: one JSON file per key via kotlinx.serialization, SHA-256 file
  naming, atomic writes, self-healing reads for corrupt files, forward-compatible JSON
  defaults.
- `jsonFileSourceOfTruth()` factory with reified serializer lookup.
