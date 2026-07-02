package io.github.quasarapps.aquifer

import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Negative caching: remembering terminal fetch failures per key so a failing endpoint is not
 * hammered by every new subscriber. Disabled unless [AquiferBuilder.negativeCache] is called:
 *
 * ```
 * val users = aquifer<UserId, User> {
 *     fetcher { api.fetchUser(it) }
 *     negativeCache {
 *         timeToLive = 30.seconds       // remember failures this long
 *         backoffMultiplier = 2.0       // consecutive failures stretch the window
 *         maxTimeToLive = 5.minutes     // never longer than this
 *     }
 * }
 * ```
 *
 * While a key's failure memory is live, strategy-driven fetches ([Freshness.CacheFirst],
 * [Freshness.StaleWhileRevalidate], [Freshness.NetworkFirst], [Aquifer.revalidateActive],
 * and new stream subscriptions) are suppressed: reads serve the cached value when one exists
 * (stale-if-error, but without re-asking the network) and otherwise fail fast with the
 * *remembered* error — the original exception instance, rethrown. [Freshness.NetworkOnly]
 * (and [Aquifer.fresh]) deliberately bypass the memory: an explicit demand for the network
 * is honoured, and its outcome refreshes the memory either way. A successful fetch,
 * [Aquifer.put], or [Aquifer.invalidate] clears the key's memory immediately; suppressed
 * reads are reported via [AquiferEvents.onFetchSuppressed].
 *
 * Consecutive failures stretch the window: the n-th straight failure is remembered for
 * `timeToLive × backoffMultiplier^(n−1)`, capped at [maxTimeToLive] — backoff memory in the
 * same spirit as retry backoff, but across fetch cycles instead of within one. "Consecutive"
 * means without an intervening success or mutation: window expiry re-allows fetching but
 * does not reset the streak, so a chronically failing key keeps its stretched window even
 * between spaced-out probes.
 *
 * Memory footprint: one small record per failing key, removed on success or on mutation of
 * the key. A key that fails once and never succeeds again keeps its record (the same
 * unbounded-growth class as tracked issue #13).
 */
@AquiferDsl
public class NegativeCacheConfig internal constructor() {

    /**
     * How long a terminal fetch failure suppresses new strategy-driven fetches of its key.
     * Must be positive and finite. Defaults to 30 seconds.
     */
    public var timeToLive: Duration = 30.seconds
        set(value) {
            require(value.isPositive() && value.isFinite()) {
                "timeToLive must be positive and finite, was $value"
            }
            field = value
        }

    /**
     * Factor stretching the suppression window after each *consecutive* failure of a key
     * (consecutive = without an intervening success or mutation). Must be ≥ 1. The default
     * 1.0 keeps the window constant.
     */
    public var backoffMultiplier: Double = 1.0
        set(value) {
            require(value >= 1.0) { "backoffMultiplier must be >= 1.0, was $value" }
            field = value
        }

    /**
     * Upper bound for the stretched window. Must be positive, finite, and at least
     * [timeToLive] (validated when the store is built). Defaults to 5 minutes.
     */
    public var maxTimeToLive: Duration = 5.minutes
        set(value) {
            require(value.isPositive() && value.isFinite()) {
                "maxTimeToLive must be positive and finite, was $value"
            }
            field = value
        }

    /**
     * Maximum number of failure records kept at once. The failure memory is otherwise unbounded
     * (a record per failing key, cleared only on that key's success or mutation), so a wide space
     * of one-time failures — e.g. a search store hitting transient errors on distinct queries —
     * would grow it without limit. When this cap is exceeded a new failure evicts the
     * least-recently-consulted record; evicting a record only re-permits a fetch of that key (it
     * carries no cached value), so the sole cost is losing that key's stretched backoff window —
     * a fetch it re-permits is still fenced exactly like any other. Must be positive. Defaults to
     * 512, matching the memory cache's default.
     */
    public var maxEntries: Int = 512
        set(value) {
            require(value > 0) { "maxEntries must be positive, was $value" }
            field = value
        }
}
