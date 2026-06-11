package io.github.quasarapps.aquifer

import io.github.quasarapps.aquifer.internal.RealAquifer
import io.github.quasarapps.aquifer.internal.RetryPolicy
import kotlinx.coroutines.CoroutineScope
import kotlin.time.Duration

/** Restricts implicit receiver access inside the [aquifer] builder DSL. */
@DslMarker
public annotation class AquiferDsl

/**
 * Creates an [Aquifer] configured by [configure]. A [AquiferBuilder.fetcher] is mandatory;
 * everything else has sensible defaults.
 *
 * ```
 * val articles = aquifer<ArticleId, Article> {
 *     fetcher { id -> api.fetchArticle(id) }
 *     freshness { timeToLive = 10.minutes }
 *     memoryCache { maxEntries = 128 }
 * }
 * ```
 */
public fun <K : Any, V : Any> aquifer(configure: AquiferBuilder<K, V>.() -> Unit): Aquifer<K, V> =
    AquiferBuilder<K, V>().apply(configure).build()

/** Configuration collected by the [aquifer] builder. */
@AquiferDsl
public class AquiferBuilder<K : Any, V : Any> internal constructor() {

    private var fetcher: (suspend (key: K) -> V)? = null
    private val memoryCache = MemoryCacheConfig()
    private val freshness = FreshnessConfig()
    private val retry = RetryConfig()
    private var clock: WallClock = WallClock.SYSTEM
    private var scope: CoroutineScope? = null
    private var persistence: SourceOfTruth<K, V>? = null
    private var events: AquiferEvents<K>? = null

    /**
     * The authoritative source of values, typically a network call. Required.
     *
     * The fetcher runs on the Aquifer's coroutine scope and may be invoked concurrently for
     * different keys, but never concurrently for the same key — concurrent requests share one
     * in-flight fetch. Thrown exceptions become [DataState.Failure] emissions and, depending
     * on the requested [Freshness], propagate from [Aquifer.get].
     */
    public fun fetcher(fetch: suspend (key: K) -> V) {
        fetcher = fetch
    }

    /** Configures the in-memory cache; see [MemoryCacheConfig]. */
    public fun memoryCache(configure: MemoryCacheConfig.() -> Unit) {
        memoryCache.configure()
    }

    /** Configures freshness behaviour; see [FreshnessConfig]. */
    public fun freshness(configure: FreshnessConfig.() -> Unit) {
        freshness.configure()
    }

    /** Configures retries for failed fetches; see [RetryConfig]. Fetches are not retried by default. */
    public fun retry(configure: RetryConfig.() -> Unit) {
        retry.configure()
    }

    /** Registers an observer of store activity for logging and metrics; see [AquiferEvents]. */
    public fun events(listener: AquiferEvents<K>) {
        events = listener
    }

    /**
     * Adds persistent storage behind the memory cache; see [SourceOfTruth] for the contract.
     *
     * With persistence configured, memory misses (including entries evicted by the LRU cache
     * and cold starts after process death) are served from storage without fetching, and
     * successful fetches and [Aquifer.put]s are written through to storage.
     */
    public fun persistence(sourceOfTruth: SourceOfTruth<K, V>) {
        persistence = sourceOfTruth
    }

    /** Replaces the [WallClock] used for cache timestamps. Defaults to [WallClock.SYSTEM]. */
    public fun clock(clock: WallClock) {
        this.clock = clock
    }

    /**
     * Ties the Aquifer's work — fetches and update broadcasting — to [scope] instead of an
     * internally created scope. The Aquifer still isolates its children with a [SupervisorJob]
     * parented to [scope]'s job: cancelling [scope] closes the store, while [Aquifer.close]
     * leaves [scope] untouched.
     *
     * The default internal scope uses `Dispatchers.Default`. In tests, pass the test
     * framework's scope to make background work deterministic.
     */
    public fun scope(scope: CoroutineScope) {
        this.scope = scope
    }

    internal fun build(): Aquifer<K, V> {
        val fetch = requireNotNull(fetcher) { "aquifer { } requires a fetcher { }" }
        return RealAquifer(
            fetcher = fetch,
            timeToLive = freshness.timeToLive,
            maxEntries = memoryCache.maxEntries,
            clock = clock,
            parentScope = scope,
            persistence = persistence,
            retry = RetryPolicy(retry),
            listener = events,
        )
    }
}

/** Configuration for the in-memory LRU cache. */
@AquiferDsl
public class MemoryCacheConfig internal constructor() {

    /**
     * Maximum number of entries held in memory; least-recently-used entries are evicted
     * beyond this. Must be positive. Defaults to 512.
     */
    public var maxEntries: Int = 512
        set(value) {
            require(value > 0) { "maxEntries must be positive, was $value" }
            field = value
        }
}

/** Configuration for entry freshness. */
@AquiferDsl
public class FreshnessConfig internal constructor() {

    /**
     * How long a cached entry is considered fresh, measured from the moment it was fetched or
     * written. Once older, the entry is *stale*: still servable, but [Freshness] strategies
     * treat it as needing revalidation. Must be positive. Defaults to [Duration.INFINITE]
     * (entries never go stale).
     */
    public var timeToLive: Duration = Duration.INFINITE
        set(value) {
            require(value.isPositive()) { "timeToLive must be positive, was $value" }
            field = value
        }
}
