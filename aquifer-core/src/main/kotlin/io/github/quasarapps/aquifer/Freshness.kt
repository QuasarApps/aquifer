package io.github.quasarapps.aquifer

/**
 * Controls how [Aquifer.get] and [Aquifer.stream] balance cached data against fetching.
 *
 * A cached entry is considered *fresh* while it is younger than the configured time-to-live
 * (see [FreshnessConfig.timeToLive]) and *stale* once it is older. The strategies below decide
 * what to do for fresh, stale, and missing entries:
 *
 * | Strategy               | Fresh entry | Stale entry                    | Missing entry |
 * |------------------------|-------------|--------------------------------|---------------|
 * | [CacheOnly]            | cache       | cache (stale)                  | error         |
 * | [CacheFirst]           | cache       | fetch, stale on fetch failure  | fetch         |
 * | [StaleWhileRevalidate] | cache       | cache, then revalidate         | fetch         |
 * | [NetworkFirst]         | fetch       | fetch, stale on fetch failure  | fetch         |
 * | [NetworkOnly]          | fetch       | fetch                          | fetch         |
 */
public sealed interface Freshness {

    /**
     * Never fetch. Serves whatever the cache holds, fresh or stale.
     *
     * [Aquifer.get] throws [CacheMissException] when nothing is cached. A [Aquifer.stream]
     * emits [DataState.Failure] with a [CacheMissException] instead, then stays subscribed —
     * it still observes values that other callers fetch or write later, which makes this a
     * cheap "passive observer" mode.
     */
    public data object CacheOnly : Freshness

    /**
     * Serve fresh cached values without fetching; fetch when the entry is stale or missing.
     *
     * This is the classic TTL cache strategy and the default for [Aquifer.get]. If the fetch
     * fails and a stale value exists, the stale value is served instead of the error
     * (stale-if-error).
     */
    public data object CacheFirst : Freshness

    /**
     * Always serve the cached value immediately when one exists — even stale — and trigger a
     * background revalidation when it is stale or missing.
     *
     * This is the default for [Aquifer.stream]: collectors render cached content instantly and
     * receive the revalidated value as a subsequent emission. With [Aquifer.get], a stale value
     * is returned immediately while the refresh continues in the background.
     */
    public data object StaleWhileRevalidate : Freshness

    /**
     * Always fetch, but fall back to the cached value — even stale — when the fetch fails.
     *
     * Useful for data that should be as current as possible while remaining usable offline.
     */
    public data object NetworkFirst : Freshness

    /**
     * Always fetch and never fall back to the cache. Fetch failures propagate to the caller.
     *
     * Successful results are still written to the cache for other readers.
     * [Aquifer.fresh] is shorthand for `get(key, NetworkOnly)`.
     */
    public data object NetworkOnly : Freshness
}
