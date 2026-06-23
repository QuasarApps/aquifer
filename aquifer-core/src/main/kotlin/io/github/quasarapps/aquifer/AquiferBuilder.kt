package io.github.quasarapps.aquifer

import io.github.quasarapps.aquifer.internal.NegativeCachePolicy
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
@Suppress("TooManyFunctions") // A DSL surface: exactly one function per builder knob.
public class AquiferBuilder<K : Any, V : Any> internal constructor() {

    private var fetcher: (suspend (key: K) -> V)? = null
    private var conditionalFetcher: (suspend (key: K, validator: String?) -> FetchResult<V>)? = null
    private var batchFetcher: (suspend (keys: Set<K>) -> Map<K, V>)? = null
    private var conditionalBatchFetcher: (suspend (validators: Map<K, String?>) -> Map<K, FetchResult<V>>)? = null
    private var coalesceWindow: Duration = Duration.ZERO
    private var maxBatchSize: Int = Int.MAX_VALUE
    private val memoryCache = MemoryCacheConfig()
    private val freshness = FreshnessConfig()
    private val retry = RetryConfig()
    private val negativeCache = NegativeCacheConfig()
    private var negativeCacheEnabled = false
    private var clock: WallClock = WallClock.SYSTEM
    private var scope: CoroutineScope? = null
    private var persistence: SourceOfTruth<K, V>? = null
    private var events: AquiferEvents<K>? = null

    /**
     * The authoritative source of values, typically a network call. Required.
     *
     * The fetcher runs on the Aquifer's coroutine scope and may be invoked concurrently for
     * different keys; concurrent requests for one key share a single in-flight fetch. One
     * exception to that single-flight rule: a mutation ([Aquifer.put]/[Aquifer.invalidate])
     * during a fetch fences the running request off and lets a new one start, so two calls
     * for the same key can briefly overlap. Thrown exceptions become [DataState.Failure]
     * emissions and, depending on the requested [Freshness], propagate from [Aquifer.get].
     */
    public fun fetcher(fetch: suspend (key: K) -> V) {
        fetcher = fetch
    }

    /**
     * Like [fetcher], but conditional: receives the cached entry's
     * [validator][PersistedEntry.validator] (an `ETag`, `Last-Modified` value, or any opaque
     * revalidation token from a previous [FetchResult.Fresh]; `null` when nothing usable is
     * cached) and may answer [FetchResult.NotModified] — the store then keeps the cached
     * value, refreshes its age so time-to-live decisions start over, and the payload never
     * crosses the network.
     *
     * Configure exactly one of [fetcher] or [conditionalFetcher]. Everything else about
     * fetching — single-flight sharing, retries, fencing, [DataState.Failure] on thrown
     * exceptions — behaves identically for both.
     */
    public fun conditionalFetcher(fetch: suspend (key: K, validator: String?) -> FetchResult<V>) {
        conditionalFetcher = fetch
    }

    /**
     * A fetcher that resolves *many keys in one backend call* — the cure for the N+1 fetch on
     * list screens. Given the set of keys that need loading, it returns a map of results; a key
     * absent from the map fails only that key (with [BatchKeyMissingException]), never the
     * batch. [Aquifer.getAll] dispatches one call through it; individual `get`/`stream`/
     * `prefetch` use it as a batch of one.
     *
     * ```
     * batchFetcher { ids -> api.fetchUsers(ids) }   // POST /users?ids=…  ->  Map<UserId, User>
     * ```
     *
     * Configure exactly one of [fetcher], [conditionalFetcher], or [batchFetcher]. Every other
     * guarantee (single-flight dedup, fencing, negative caching, persistence, events) applies
     * per key, unchanged — batching is purely a fetch-transport optimization. To also
     * auto-coalesce individual fetches, use the [batchFetcher] overload that takes a
     * `coalesceWindow`. The reactive and warm-up batch reads are [Aquifer.streamMany] and
     * [Aquifer.prefetchAll].
     *
     * The store's [retry] policy wraps both single-key fetches (including the batch of one a
     * `get` makes here) and the multi-key call [Aquifer.getAll] issues — a retryable transport
     * failure re-runs the whole batch with backoff (omitted keys are definitive misses, never
     * retried).
     */
    public fun batchFetcher(fetch: suspend (keys: Set<K>) -> Map<K, V>) {
        batchFetcher = fetch
        // The last batchFetcher call fully defines batching: clear any coalescing a prior
        // call to the overload below may have set, so the plain form means "no coalescing".
        coalesceWindow = Duration.ZERO
        maxBatchSize = Int.MAX_VALUE
    }

    /**
     * A [batchFetcher] that additionally **auto-coalesces** individual `get`/`stream`/
     * `prefetch` fetches landing within [coalesceWindow] of each other into one [fetch] call
     * (DataLoader-style) — unchanged call sites, fewer round-trips. The batch dispatches when
     * the window elapses or once [maxBatchSize] distinct keys accumulate. [Aquifer.getAll]
     * always dispatches its own keys immediately, regardless of the window.
     *
     * Each coalesced single-key fetch is still retried by the store's [retry] policy,
     * re-entering the next window; the multi-key call [Aquifer.getAll] issues is retried too —
     * a retryable transport failure re-runs the whole batch with backoff.
     *
     * @param coalesceWindow how long to gather keys before dispatching a batch; must be
     *   positive and finite (use the single-argument [batchFetcher] for no coalescing).
     * @param maxBatchSize dispatch early once this many distinct keys accumulate; must be ≥ 1.
     */
    public fun batchFetcher(
        coalesceWindow: Duration,
        maxBatchSize: Int = Int.MAX_VALUE,
        fetch: suspend (keys: Set<K>) -> Map<K, V>,
    ) {
        require(coalesceWindow.isPositive() && coalesceWindow.isFinite()) {
            "coalesceWindow must be positive and finite, was $coalesceWindow"
        }
        require(maxBatchSize >= 1) { "maxBatchSize must be at least 1, was $maxBatchSize" }
        batchFetcher = fetch
        this.coalesceWindow = coalesceWindow
        this.maxBatchSize = maxBatchSize
    }

    /**
     * A [batchFetcher] that is also **conditional** — the batch mirror of [conditionalFetcher],
     * so ETag/304 revalidation composes with batching. Given the keys that need loading each
     * mapped to their cached [validator][PersistedEntry.validator] (`null` when nothing usable
     * is cached), it returns a [FetchResult] per key: [FetchResult.Fresh] with a new value (and
     * optional new validator), or [FetchResult.NotModified] to keep the cached value and re-age
     * it — the payload never crosses the network.
     *
     * ```
     * conditionalBatchFetcher { validators ->          // Map<ArticleId, String?>  (id -> ETag)
     *     api.fetchArticles(validators).mapValues { (_, r) ->
     *         if (r.status == 304) FetchResult.NotModified
     *         else FetchResult.Fresh(r.body, validator = r.etag)
     *     }
     * }
     * ```
     *
     * A key absent from the returned map fails only that key ([BatchKeyMissingException]); a
     * throwing fetcher fails the whole batch (and is retried by the store [retry] policy, like
     * [batchFetcher]). [FetchResult.NotModified] for a key with no cached validator is a contract
     * violation and fails that key. [Aquifer.getAll]/[Aquifer.streamMany]/[Aquifer.prefetchAll]
     * dispatch one call through it; an individual `get`/`stream`/`prefetch` uses it as a batch of
     * one. Configure exactly one of [fetcher], [conditionalFetcher], [batchFetcher], or
     * [conditionalBatchFetcher]; the auto-coalescing window is [batchFetcher]-only.
     */
    public fun conditionalBatchFetcher(fetch: suspend (validators: Map<K, String?>) -> Map<K, FetchResult<V>>) {
        conditionalBatchFetcher = fetch
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

    /**
     * Enables negative caching — failed fetches are remembered per key for a short window,
     * during which strategy-driven refetches are suppressed; see [NegativeCacheConfig].
     * Disabled unless this is called.
     */
    public fun negativeCache(configure: NegativeCacheConfig.() -> Unit) {
        negativeCacheEnabled = true
        negativeCache.configure()
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
        val plain = fetcher
        val conditional = conditionalFetcher
        val batch = batchFetcher
        val conditionalBatch = conditionalBatchFetcher
        require(listOfNotNull(plain, conditional, batch, conditionalBatch).size <= 1) {
            "Configure at most one of fetcher { }, conditionalFetcher { }, batchFetcher { }, " +
                "or conditionalBatchFetcher { }"
        }
        val fetch: suspend (key: K, validator: String?) -> FetchResult<V> = when {
            conditional != null -> conditional
            plain != null -> { key, _ -> FetchResult.Fresh(plain(key)) }
            // A single-key read over a batch fetcher is just a batch of one.
            batch != null -> { key, _ -> FetchResult.Fresh(batch(setOf(key))[key].orKeyMissing(key)) }
            // Same, conditionally: pass the key's validator through and return its FetchResult.
            conditionalBatch != null -> { key, validator ->
                conditionalBatch(mapOf(key to validator))[key].orKeyMissing(key)
            }
            else -> throw IllegalArgumentException(
                "aquifer { } requires a fetcher { }, conditionalFetcher { }, batchFetcher { }, " +
                    "or conditionalBatchFetcher { }",
            )
        }
        val negative = if (negativeCacheEnabled) {
            require(negativeCache.maxTimeToLive >= negativeCache.timeToLive) {
                "negativeCache.maxTimeToLive (${negativeCache.maxTimeToLive}) must be >= " +
                    "timeToLive (${negativeCache.timeToLive})"
            }
            NegativeCachePolicy(negativeCache)
        } else {
            null
        }
        return RealAquifer(
            fetcher = fetch,
            conditional = conditional != null || conditionalBatch != null,
            batchFetcher = batch,
            conditionalBatchFetcher = conditionalBatch,
            coalesceWindow = coalesceWindow,
            maxBatchSize = maxBatchSize,
            negativeCache = negative,
            timeToLive = freshness.timeToLive,
            ttlJitter = freshness.ttlJitter,
            maxEntries = memoryCache.maxEntries,
            clock = clock,
            parentScope = scope,
            persistence = persistence,
            retry = RetryPolicy(retry),
            listener = events,
        )
    }

    /** A batch result for [key], or a per-key [BatchKeyMissingException] when the batch omitted it. */
    private fun <R> R?.orKeyMissing(key: K): R = this ?: throw BatchKeyMissingException(key)
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

    /**
     * Fraction in `[0, 1]` by which each entry's *effective* time-to-live is
     * deterministically shortened, spreading the expiries of entries that were fetched
     * together so they don't all revalidate at once — the request-stampede mirror of retry
     * jitter. Each entry's factor derives from its key and its write timestamp (so
     * same-millisecond bursts still spread): stable across checks (an entry never flickers
     * between fresh and stale), and across restarts when the key's `hashCode` is
     * value-based and stable — data classes, strings, primitives; an identity-hashed key
     * re-rolls its factor on restart, which merely re-spreads its expiry.
     * [timeToLive] stays the hard upper bound; an entry's effective TTL falls in
     * `(timeToLive × (1 − ttlJitter), timeToLive]` — the lower bound is exclusive because
     * the per-entry fraction is drawn from `[0, 1)`. Per-call `maxAge` overrides are never
     * jittered (an explicit caller bar). 0 (the default) disables jitter.
     */
    public var ttlJitter: Double = 0.0
        set(value) {
            require(value in 0.0..1.0) { "ttlJitter must be within 0.0..1.0, was $value" }
            field = value
        }
}
