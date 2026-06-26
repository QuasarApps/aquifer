package io.github.quasarapps.aquifer

import kotlinx.coroutines.flow.Flow
import kotlin.time.Duration

/**
 * An offline-first, keyed data store that mediates between a remote fetcher and local caches.
 *
 * An Aquifer is the single source of truth for one kind of data (users, articles, search
 * results, …), addressed by key. Callers never talk to the network directly; they ask the
 * Aquifer for data with a desired [Freshness], and the Aquifer decides whether to serve the
 * cache, fetch, or both. Concurrent requests for the same key share a single fetch, and every
 * active [stream] of a key observes writes, fetches, and invalidations made through any other
 * call on the same instance.
 *
 * Create instances with the [aquifer] builder:
 *
 * ```
 * val users: Aquifer<UserId, User> = aquifer {
 *     fetcher { id -> api.fetchUser(id) }
 *     freshness { timeToLive = 5.minutes }
 *     memoryCache { maxEntries = 256 }
 * }
 * ```
 *
 * ### Threading & lifecycle
 *
 * All members are safe to call from any thread or coroutine. Fetches run on the Aquifer's own
 * coroutine scope (configurable via [AquiferBuilder.scope]), so a fetch survives the caller
 * that started it: the result still lands in the cache and reaches other observers even when
 * the originating screen is closed. Call [close] when the store as a whole is no longer
 * needed; for app-wide singletons that is typically never.
 */
// The public contract: each function is a distinct, cohesive cache operation — not a class
// to decompose. (read/observe: stream, get, fresh, prefetch, getAll, snapshot, stats; mutate:
// put, putAll, invalidate, invalidateWhere, invalidateAll; revalidate: revalidateActive,
// revalidateOn; lifecycle: close.)
@Suppress("TooManyFunctions")
public interface Aquifer<K : Any, V : Any> : AutoCloseable {

    /**
     * Observes the data for [key] as a cold [Flow] of [DataState]s.
     *
     * On collection the stream first emits the cached value when one exists (except with
     * [Freshness.NetworkOnly]) and triggers a fetch when the strategy calls for one; a
     * [Freshness.CacheOnly] collection of a missing key emits [DataState.Empty] instead of
     * fetching. The stream then stays active indefinitely, re-emitting whenever the key is
     * fetched, written, or invalidated by any caller — collect it in a scope that matches
     * your UI lifecycle.
     *
     * Multiple concurrent collectors are cheap: they share fetches and all observe the same
     * updates. Consecutive equal states are de-duplicated.
     *
     * @param freshness strategy for the initial emission; defaults to
     *   [Freshness.StaleWhileRevalidate].
     * @param maxAge per-call override of the store's time-to-live, replacing it everywhere
     *   this stream consults staleness: the initial fetch decision (for the staleness-aware
     *   strategies, [Freshness.CacheFirst] and [Freshness.StaleWhileRevalidate]) and the
     *   `isStale` flag of every [DataState.Content] this stream emits — the coloring applies
     *   to **every** strategy, so even a [Freshness.CacheOnly] stream renders staleness
     *   hints against its own bar while its fetch behavior stays untouched. `null` (the
     *   default) uses the store-wide [FreshnessConfig.timeToLive]; [Duration.INFINITE] makes
     *   this stream treat any cached entry as fresh. Must be positive.
     */
    public fun stream(
        key: K,
        freshness: Freshness = Freshness.StaleWhileRevalidate,
        maxAge: Duration? = null,
    ): Flow<DataState<V>>

    /**
     * Observes many [keys] at once as a combined [Flow] of `Map<K, DataState<V>>` — the
     * reactive twin of [getAll]. Each key is observed by its own [stream]; the combined map
     * re-emits whenever **any** key's state changes, so a list screen renders per-item
     * loading/content/failure coherently from a single collection.
     *
     * The initial fetches of the member keys are **batched**: the keys that need loading
     * (decided per [freshness], exactly as [getAll] decides) are collapsed into one
     * [batch fetcher][AquiferBuilder.batchFetcher] call, dispatched immediately — so collecting
     * `streamMany` of 50 missing keys is one backend round-trip, not 50, even without a
     * coalescing window. Without a batch fetcher the keys are streamed individually (still
     * single-flight-deduped). Every per-key guarantee (fencing, negative caching, persistence,
     * events) is unchanged; batching is purely a fetch-transport optimization.
     *
     * The emitted map carries an entry for every key in [keys] once each has produced its
     * first state — whatever the corresponding [stream] would emit: cached `Content`, `Loading`
     * on a miss that will fetch, `Empty` for a `CacheOnly` miss, or `Failure`. An empty [keys]
     * yields a single empty map. Like [stream], collect it in a scope matching your UI lifecycle.
     *
     * @param freshness strategy for each member key; defaults to
     *   [Freshness.StaleWhileRevalidate].
     */
    public fun streamMany(
        keys: Set<K>,
        freshness: Freshness = Freshness.StaleWhileRevalidate,
    ): Flow<Map<K, DataState<V>>>

    /**
     * Returns the value for [key] as a one-shot call, honouring [freshness]
     * (default [Freshness.CacheFirst]).
     *
     * @param maxAge per-call override of the store's time-to-live for this read's
     *   fresh/stale decision — a screen that needs minute-fresh data can demand it without
     *   changing the store-wide policy, and one happy with old data can avoid a fetch.
     *   Only the staleness-aware strategies ([Freshness.CacheFirst],
     *   [Freshness.StaleWhileRevalidate]) consult it for a one-shot read; the others ignore
     *   it beyond validation (unlike [stream], `get` emits no `isStale` flags to color).
     *   [Duration.INFINITE] turns a [Freshness.CacheFirst] read into "serve anything
     *   cached, fetch only on miss". Must be positive.
     * @throws CacheMissException with [Freshness.CacheOnly] when nothing is cached.
     * @throws Throwable the fetcher's exception, when the strategy required a fetch and no
     *   cached fallback was permitted.
     */
    public suspend fun get(
        key: K,
        freshness: Freshness = Freshness.CacheFirst,
        maxAge: Duration? = null,
    ): V

    /**
     * Fetches a guaranteed-fresh value for [key], bypassing cached reads.
     * Shorthand for `get(key, Freshness.NetworkOnly)`.
     *
     * @throws Throwable the fetcher's exception when the fetch fails.
     */
    public suspend fun fresh(key: K): V

    /**
     * Warms the cache for [key] without blocking the caller — fire-and-forget. Returns
     * immediately; the fetch (if any) runs in the store's scope and its result lands in the
     * cache for the next [get] or [stream], so a screen can prefetch what the user is likely
     * to open next.
     *
     * Honours [freshness] exactly as [get] would for the *decision to fetch* — by default
     * [Freshness.CacheFirst], so a still-fresh entry triggers no fetch at all — and shares a
     * single in-flight fetch with any concurrent [get], [stream], or [prefetch] of the same
     * key (no duplicate request). A fetch suppressed by negative caching is not started.
     * [Freshness.CacheOnly] is a no-op (it never fetches). Failures are not thrown to the
     * caller; they surface through [AquiferEvents] like any other fetch, and a later real
     * read still sees them. Calling on a closed store throws [IllegalStateException].
     */
    public fun prefetch(key: K, freshness: Freshness = Freshness.CacheFirst)

    /**
     * Warms the cache for many [keys] at once without blocking — the batched, fire-and-forget
     * mirror of [prefetch] (and the write-free twin of [getAll]). Returns immediately; the keys
     * that need loading (decided per [freshness], exactly as [prefetch] decides) are collapsed
     * into a single [batch fetcher][AquiferBuilder.batchFetcher] call in the store's scope, and
     * the results land in the cache for the next [get]/[getAll]/[stream].
     *
     * Honours [freshness] for the *decision to fetch* — by default [Freshness.CacheFirst], so
     * already-fresh keys trigger nothing — shares each in-flight fetch with any concurrent
     * [get]/[getAll]/[stream]/[prefetch] of the same key, and stands down per key under negative
     * caching (except [Freshness.NetworkOnly], the explicit-demand strategy).
     * [Freshness.CacheOnly] is a no-op. Failures are never thrown to the caller; they surface
     * through [AquiferEvents] like any other fetch. Calling on a closed store throws
     * [IllegalStateException].
     */
    public fun prefetchAll(keys: Set<K>, freshness: Freshness = Freshness.CacheFirst)

    /**
     * Resolves many [keys] at once, collapsing their network fetches into a single backend
     * call when the store has a [batch fetcher][AquiferBuilder.batchFetcher] — the cure for
     * the N+1 fetch on list screens. Each key is resolved per [freshness] exactly as [get]
     * decides whether to fetch; the keys that need fetching are gathered into one call (joining
     * any already in-flight single fetch for a key).
     *
     * Returns the **resolved subset**: a `Map` of the keys that produced a value, in iteration
     * order of [keys]. Unlike [get], a per-key failure does not throw — a key whose fetch fails
     * with no usable cached fallback (the batch omitted it, or the call errored) is simply
     * absent from the result, so one bad key never sinks the screen. Per-key failures still
     * reach [AquiferEvents]; for per-key error *states*, use [stream] on the individual keys.
     * Being one-shot, `getAll` awaits every fetch it triggers — there is no
     * [Freshness.StaleWhileRevalidate] background revalidation (it behaves like
     * [Freshness.CacheFirst] for that case). Without a batch fetcher the keys are fetched
     * individually (still single-flight-deduped). Defaults to [Freshness.CacheFirst].
     *
     * Retry: the store's `retry` policy wraps both each *single-key* fetch (including the
     * batch-of-one a `get` makes over a batch fetcher) and the *multi-key* batch call `getAll`
     * issues — a retryable transport failure re-runs the whole batch with backoff, firing
     * [AquiferEvents.onFetchRetried] per key. Keys the fetcher *omits* from an otherwise
     * successful map are definitive misses, not transient failures, so they are never retried
     * (each falls back to its cached value, or is omitted from the result).
     *
     * @throws IllegalStateException if the store is closed.
     */
    public suspend fun getAll(keys: Set<K>, freshness: Freshness = Freshness.CacheFirst): Map<K, V>

    /**
     * Writes [value] for [key] into the cache as a fresh entry and notifies active streams.
     *
     * Use this to apply local edits or server push payloads. The write is local only; pushing
     * the change to your backend remains the caller's responsibility. A fetch already in
     * flight for [key] is fenced off: its response cannot overwrite this newer local value
     * (callers awaiting that fetch still receive its result).
     */
    public suspend fun put(key: K, value: V)

    /**
     * Writes many [entries] into the cache as fresh entries in one fenced commit — the bulk,
     * write-side mirror of [getAll]: seed the store from a batch you fetched yourself, without N
     * separate [put] calls. Each key is committed and fenced exactly as [put] would (a fetch
     * already in flight for it cannot overwrite the new value, and its negative-cache record is
     * cleared), and every active stream of a written key observes a single update. An empty map
     * is a no-op.
     *
     * The write is local only; pushing the changes to your backend remains the caller's
     * responsibility.
     */
    public suspend fun putAll(entries: Map<K, V>)

    /**
     * Drops the cached entry for [key], in memory and in persistence. Fetch-capable active
     * streams re-fetch automatically with a *new* request; a fetch already in flight is
     * fenced off so its response cannot resurrect the invalidated data. [Freshness.CacheOnly]
     * observers see the deletion as [DataState.Empty] — they cannot fetch, so the empty
     * state is how a cache-only screen learns the data is gone.
     */
    public suspend fun invalidate(key: K)

    /**
     * Drops every cached entry whose key matches [predicate] — the bulk middle ground between
     * the surgical [invalidate] and the nuclear [invalidateAll], for "drop everything for this
     * tenant/scope" resets. Each matched key is dropped and fenced exactly as [invalidate]
     * would (memory and persistence cleared, any in-flight fetch fenced off, negative-cache
     * record cleared), and its observers see the deletion the same way, in one fenced commit.
     *
     * [predicate] runs outside the commit lock, so it must not call back into the store, and must
     * be a pure, side-effect-free key test: on an enumerable store it may be evaluated more than
     * once for the same key (once over in-process keys, once during store enumeration). It is
     * tested against the keys this store currently tracks in the process — resident memory
     * entries, active fetch-capable streams, in-flight fetches, and negative-cache and write-epoch
     * records — and, when the configured [SourceOfTruth] supports key enumeration
     * ([SourceOfTruth.keys]), against every persisted key too, so the reach is disk-wide. A store
     * that cannot enumerate (the default, including `aquifer-persistence-file`) limits the reach to
     * those in-process keys: a persisted entry evicted from memory and never re-touched, or one
     * never loaded this run, is then out of reach — use [invalidateAll] for a full wipe regardless
     * of what is currently loaded or what the store can enumerate.
     */
    public suspend fun invalidateWhere(predicate: (K) -> Boolean)

    /**
     * Drops all cached entries, in memory and in persistence, with the same fencing and
     * stream semantics as [invalidate]. This is the right call for logout-style resets:
     * responses already in flight for the previous state cannot land back in the caches.
     */
    public suspend fun invalidateAll()

    /**
     * A non-suspending snapshot of the keys currently resident in the in-memory cache;
     * `snapshot().size` is the live entry count. Together they're for debug overlays and eviction
     * tuning — how full the cache is and what survived eviction.
     *
     * This is a peek at memory only: it never suspends, never touches persistence, and is safe to
     * call from anywhere — including a closed store. Persisted-only keys (evicted from memory, or
     * not yet hydrated) are not listed, and the returned set is a stable copy, not a live view.
     */
    public fun snapshot(): Set<K>

    /**
     * A non-suspending snapshot of this store's cache counters — hit/miss totals, LRU evictions,
     * and the current in-flight fetch-registry size — the aggregate numbers [AquiferEvents] can't
     * give you, for hit-rate dashboards and cache tuning. Like [snapshot] it never suspends, never
     * touches persistence, and is safe to call anytime, including on a closed store.
     *
     * A **hit** is a caller read ([get], [getAll] per key, or a [stream]'s initial emission)
     * satisfied from cache under its requested [Freshness] without awaiting a fetch — a *fresh*
     * entry for [Freshness.CacheFirst], a present one for [Freshness.CacheOnly]/
     * [Freshness.StaleWhileRevalidate] (which serve stale and revalidate in the background). A
     * **miss** is any other read: the policy needed a fetch (no usable cached value, or a
     * network-first strategy — [Freshness.NetworkFirst]/[Freshness.NetworkOnly] always miss), or
     * [Freshness.CacheOnly] found nothing. Background revalidation and [prefetch]/[prefetchAll]
     * warmups are not counted, nor are a stream's later re-fetches; see [CacheStats].
     */
    public fun stats(): CacheStats

    /**
     * Triggers a refresh for every key that currently has an active [stream] collector and
     * whose entry is stale or missing. Fresh entries and keys observed only by
     * [Freshness.CacheOnly] streams are skipped, and concurrent refreshes share fetches as
     * usual. Returns once the refreshes are *triggered*; results arrive through the streams.
     *
     * This is the building block for "refresh when the app comes back online / to the
     * foreground" behaviour — see [revalidateOn].
     */
    public suspend fun revalidateActive()

    /**
     * Calls [revalidateActive] every time [trigger] emits, for the lifetime of this store.
     * Typical Android wiring passes a connectivity-restored or app-foregrounded event flow:
     *
     * ```
     * users.revalidateOn(connectivity.onlineAgain)   // Flow<Unit> from ConnectivityManager
     * ```
     *
     * May be called multiple times with different triggers. Collection runs in the store's
     * scope and stops when the store is closed (or the trigger flow completes); a trigger
     * that throws stops only its own subscription.
     */
    public fun revalidateOn(trigger: Flow<*>)

    /**
     * Closes the store: cancels in-flight fetches and stops update delivery. Streams stop
     * receiving emissions, subsequent calls to other members throw [IllegalStateException]
     * (except [snapshot], a read-only memory peek that stays callable), and callers already
     * awaiting a fetch get an [AquiferException] (never a bare cancellation of their own
     * coroutine). Cancelling the scope passed to [AquiferBuilder.scope] has the same effect.
     * Closing an already-closed store is a no-op.
     */
    override fun close()
}
