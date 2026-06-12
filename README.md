# Aquifer

[![CI](https://github.com/Quasar-Apps/api-library-example/actions/workflows/ci.yml/badge.svg)](https://github.com/Quasar-Apps/api-library-example/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Kotlin](https://img.shields.io/badge/kotlin-2.2-7F52FF.svg?logo=kotlin)](https://kotlinlang.org)

**An offline-first, stale-while-revalidate data layer for Kotlin and Android.**

Aquifer sits between your remote API and your UI as the single source of truth for keyed data.
You declare *how to fetch* and *how fresh data must be*; Aquifer decides when to serve the
cache, when to hit the network, and keeps every observer of a key in sync — through
process-wide caching, request deduplication, and reactive `Flow` streams.

```kotlin
val users: Aquifer<UserId, User> = aquifer {
    fetcher { id -> api.fetchUser(id) }
    freshness { timeToLive = 5.minutes }
    memoryCache { maxEntries = 256 }
}

// In a ViewModel: render cached data instantly, refresh in the background.
val state: StateFlow<DataState<User>> =
    users.stream(userId)
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), DataState.Loading())
```

```kotlin
// In Compose (aquifer-compose): one line, lifecycle-aware, stream shared across recompositions.
val state by users.collectAsState(userId)

state.value?.let { user -> UserProfile(user) }   // keep content visible while refreshing
if (state.isLoading) RefreshIndicator()
(state as? DataState.Failure)?.let { RefreshFailedSnackbar(it.error) }
```

> ⚠️ **Status: pre-1.0.** The API surface is locked by binary-compatibility validation and the
> Maven Central publishing pipeline is configured, but no release has been tagged yet. Once
> `v0.1.0` ships:
>
> ```kotlin
> dependencies {
>     implementation("io.github.quasar-apps:aquifer-core:0.1.0")
>     implementation("io.github.quasar-apps:aquifer-compose:0.1.0")          // Compose state collection
>     implementation("io.github.quasar-apps:aquifer-android:0.1.0")          // reconnect/foreground triggers
>     implementation("io.github.quasar-apps:aquifer-persistence-file:0.1.0") // disk persistence
> }
> ```

## Why Aquifer?

Every non-trivial Android app ends up re-implementing the same data plumbing:

- *"Show cached data immediately, then refresh"* — *stale-while-revalidate*, hand-rolled per screen.
- *"Two screens load the same user and fire two identical requests"* — no request deduplication.
- *"The detail screen edited the user but the list still shows stale data"* — no shared source of truth.
- *"The network failed but we have perfectly usable data from an hour ago"* — no stale-if-error fallback.

Aquifer packages those patterns into one small, focused library:

| Capability | What it means |
|---|---|
| **Freshness policies** | `CacheOnly`, `CacheFirst`, `StaleWhileRevalidate`, `NetworkFirst`, `NetworkOnly` — pick per call, not per architecture. |
| **Request deduplication** | Concurrent requests for a key share one in-flight fetch, whether they come from `get` or `stream`. |
| **Reactive streams** | `stream(key)` emits `Loading` / `Content` / `Failure` snapshots and keeps emitting as the key is fetched, written, or invalidated by anyone. |
| **Stale-if-error** | Fetch failed, cache has data? Serve the stale value and surface the error alongside it — never a blank screen. |
| **Caller-proof fetches** | Fetches run in the store's scope: navigating away mid-request doesn't waste the response — it still lands in the cache. |
| **LRU memory cache** | Bounded, thread-safe, TTL-aware. |
| **Deterministic tests** | Inject a `WallClock` and a `CoroutineScope`; staleness and concurrency are fully testable with virtual time. |

Pure Kotlin/JVM with a single dependency (`kotlinx-coroutines`): usable from any Android app
(API 21+; deliberately free of `java.util` methods added in API 24) and from JVM services —
persistence and Android-specific integrations layer on top as separate modules. The optional
`aquifer-persistence-file` module is built on `java.nio.file` and needs API 26+ or NIO
core-library desugaring.

## Core concepts

### One Aquifer per data family

An `Aquifer<K, V>` manages one kind of data, addressed by key — `Aquifer<UserId, User>`,
`Aquifer<Query, SearchResults>`. Make it a singleton (Hilt/Koin) so every screen shares the
same cache and update bus.

### Freshness decides the cache/network dance

A cached entry is **fresh** until it outlives `timeToLive`, then it's **stale** — still
servable, but due for revalidation:

| Strategy | Fresh entry | Stale entry | Missing entry |
|---|---|---|---|
| `CacheOnly` | cache | cache (stale) | error |
| `CacheFirst` *(default for `get`)* | cache | fetch → stale on failure | fetch |
| `StaleWhileRevalidate` *(default for `stream`)* | cache | cache, then revalidate | fetch |
| `NetworkFirst` | fetch → cache on failure | fetch → stale on failure | fetch |
| `NetworkOnly` | fetch | fetch | fetch |

### Streams keep every observer coherent

```kotlin
users.stream(id).collect { state ->
    when (state) {
        is DataState.Loading -> render(state.value, refreshing = true)   // value = previous data, if any
        is DataState.Content -> render(state.value, refreshing = false)  // state.origin: MEMORY / FETCHER / LOCAL
        is DataState.Failure -> renderError(state.error, fallback = state.value)
    }
}
```

Updates from *anywhere* — another screen's fetch, a local `put`, an `invalidate` — reach every
active stream of that key. Pull-to-refresh on one screen updates all of them:

```kotlin
suspend fun onPullToRefresh(id: UserId) {
    users.fresh(id)        // forces a fetch; all active streams emit Loading → Content
}

suspend fun onUserEdited(id: UserId, edited: User) {
    users.put(id, edited)  // optimistic local write; observers update instantly
}
```

### One-shot reads

```kotlin
val user = users.get(id)                            // fresh cache or fetch (CacheFirst)
val current = users.get(id, Freshness.NetworkFirst) // prefer network, tolerate offline
val pinned = users.get(id, Freshness.CacheOnly)     // cache or CacheMissException, never fetches
```

### Surviving process death

Add a `SourceOfTruth` and cached data outlives the process: cold starts render from disk
instantly, and LRU-evicted entries fall back to storage instead of the network. Timestamps are
persisted too, so time-to-live decisions stay correct across restarts.

```kotlin
val articles = aquifer<ArticleId, Article> {
    fetcher { id -> api.fetchArticle(id) }
    freshness { timeToLive = 10.minutes }
    persistence(
        jsonFileSourceOfTruth(context.filesDir.resolve("aquifer/articles").toPath()),
    )
}
```

`aquifer-persistence-file` stores one JSON file per key (kotlinx.serialization) with atomic
writes, SHA-256 file naming for arbitrary keys, and self-healing reads that treat corrupt
files as absent. Or implement `SourceOfTruth` yourself to back Aquifer with Room, SQLDelight,
or DataStore — it's four suspend functions.

### Retries with backoff and jitter

Fetches are not retried by default. Opt in per store:

```kotlin
retry {
    maxAttempts = 3                      // total, including the first attempt
    initialDelay = 250.milliseconds      // then ×2 per attempt, capped at maxDelay
    retryOn = { it is IOException }      // never retries cancellation
}
```

Retries happen *inside* the shared single-flight fetch: observers see one `Loading` and one
terminal state per cycle, and jitter only ever shortens delays so `maxDelay` is a hard cap.

### Refresh on reconnect (or foreground)

`revalidateActive()` refreshes exactly the keys someone is currently looking at — active
streams — and only if their entries are stale or missing. `aquifer-android` ships the two
triggers every app wants:

```kotlin
users.revalidateOnReconnect(context)   // internet restored (ConnectivityManager-backed)
users.revalidateOnAppForeground()      // app returned to foreground (ProcessLifecycleOwner)
```

Both fire only on genuine offline→online / background→foreground transitions: an
already-online device or already-visible app at subscription never emits, subscribing while
offline or backgrounded emits on the next recovery, and Wi-Fi↔cellular handovers are ignored.
The required `ACCESS_NETWORK_STATE` permission is merged in from the library manifest. For anything custom, `revalidateOn(trigger)` accepts
any `Flow` — a push message, a settings change, a timer.

### Observability

```kotlin
events(object : AquiferEvents<UserId> {
    override fun onFetchFailed(key: UserId, error: Throwable, attempts: Int) =
        Timber.w(error, "fetch failed after $attempts attempts")
    override fun onPersistenceWriteFailed(key: UserId, error: Throwable) =
        analytics.count("cache_write_failed")
})
```

Hooks cover fetch start/success/failure, every retry, and best-effort persistence write
failures. Listeners that throw never disturb the engine.

## Testing your repositories

Aquifer takes time and concurrency as injectable dependencies, so tests are deterministic:

```kotlin
@Test
fun `stale profile is served then revalidated`() = runTest {
    val clock = FakeClock()
    val users = aquifer<String, User> {
        scope(backgroundScope)          // background work runs on the test scheduler
        clock(clock)                    // staleness under your control
        fetcher { fakeApi.user(it) }
        freshness { timeToLive = 5.minutes }
    }

    users.put("u1", cachedUser)
    clock.advanceBy(10.minutes)

    users.stream("u1").test {
        assertEquals(DataState.Content(cachedUser, Origin.MEMORY, isStale = true), awaitItem())
        assertEquals(DataState.Loading(cachedUser), awaitItem())
        assertEquals(DataState.Content(freshUser, Origin.FETCHER, isStale = false), awaitItem())
    }
}
```

## Design notes

- **Single-flight fetches.** A per-key registry of in-flight `Deferred`s collapses concurrent
  fetches; the registry cleans up on completion, so memory use is bounded by concurrency, not
  key cardinality.
- **No lost updates.** A stream subscribes to the update bus *before* reading its cache
  snapshot (via `SharedFlow.onSubscription`), so an update can't slip between snapshot and
  subscription.
- **Fetches outlive callers.** Fetches run in the Aquifer's scope, not the caller's: a
  response that arrives after the requesting screen closed still warms the cache for the next
  screen. Cancelling the store's scope (or `close()`) cancels everything.
- **Errors are data.** Streams never terminate on fetch failure; failures are emitted as
  `DataState.Failure` carrying the last known value, and broadcast to all observers of the key.
- **Slow collectors are isolated.** Every stream drains the store's update bus through an
  unbounded per-collector buffer on the store's dispatcher, so one stalled screen can never
  block fetch completion, writes, or other streams.

## Try it

```bash
./gradlew :sample:run    # runnable tour: SWR, dedup, local edits, process death, reconnect
./gradlew dokkaGenerate  # aggregated API docs in build/dokka/
./gradlew build          # tests + binary-compatibility check (api/*.api dumps)
```

## Roadmap

Everything from the original plan has shipped: the core engine, persistence, retries,
reconnect/foreground revalidation, the Android module, release engineering, and two
review-driven hardening rounds. What's next, in order:

1. **v0.1.0 on Maven Central** — pipeline is ready; needs secrets + a tag.
2. **Rest of 0.2** — static analysis in CI, per-call freshness parameters, the bounded disk
   store, and the `DataState.Empty` RFC (`aquifer-compose` and the `DataState` helpers have
   shipped).
3. **Network efficiency** — conditional fetching (ETag/304), negative caching, prefetch.

The full plan through 1.0 and beyond — persistence adapters, Lincheck-verified concurrency,
KMP, offline mutations — lives in [ROADMAP.md](ROADMAP.md).

## Project layout

| Module | Description |
|---|---|
| `aquifer-core` | The store: public API + engine. Pure Kotlin/JVM, depends only on `kotlinx-coroutines-core`. |
| `aquifer-compose` | Jetpack Compose integration: `collectAsState(key)`, `rememberStream`, `previewAquifer` (molecule-tested). |
| `aquifer-android` | Android library: `revalidateOnReconnect` / `revalidateOnAppForeground` triggers (Robolectric-tested). |
| `aquifer-persistence-file` | JSON-files `SourceOfTruth` backed by kotlinx.serialization: atomic writes, self-healing reads. |
| `sample` | Runnable CLI walkthrough of every feature (`./gradlew :sample:run`). |

## License

```
Copyright 2026 Quasar Apps

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
