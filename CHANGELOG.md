# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) once 1.0 ships; until then minor
versions may contain breaking changes.

## [Unreleased]

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
