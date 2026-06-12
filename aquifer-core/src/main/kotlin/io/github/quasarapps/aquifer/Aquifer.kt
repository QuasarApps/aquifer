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
     * Writes [value] for [key] into the cache as a fresh entry and notifies active streams.
     *
     * Use this to apply local edits or server push payloads. The write is local only; pushing
     * the change to your backend remains the caller's responsibility. A fetch already in
     * flight for [key] is fenced off: its response cannot overwrite this newer local value
     * (callers awaiting that fetch still receive its result).
     */
    public suspend fun put(key: K, value: V)

    /**
     * Drops the cached entry for [key], in memory and in persistence. Fetch-capable active
     * streams re-fetch automatically with a *new* request; a fetch already in flight is
     * fenced off so its response cannot resurrect the invalidated data. [Freshness.CacheOnly]
     * observers see the deletion as [DataState.Empty] — they cannot fetch, so the empty
     * state is how a cache-only screen learns the data is gone.
     */
    public suspend fun invalidate(key: K)

    /**
     * Drops all cached entries, in memory and in persistence, with the same fencing and
     * stream semantics as [invalidate]. This is the right call for logout-style resets:
     * responses already in flight for the previous state cannot land back in the caches.
     */
    public suspend fun invalidateAll()

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
     * receiving emissions, subsequent calls to other members throw [IllegalStateException],
     * and callers already awaiting a fetch get an [AquiferException] (never a bare
     * cancellation of their own coroutine). Cancelling the scope passed to
     * [AquiferBuilder.scope] has the same effect. Closing an already-closed store is a no-op.
     */
    override fun close()
}
