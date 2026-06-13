package io.github.quasarapps.aquifer

import kotlin.time.Duration

/**
 * Observer of a store's internal activity, for logging, metrics, and debugging — registered
 * via [AquiferBuilder.events]:
 *
 * ```
 * events(object : AquiferEvents<UserId> {
 *     override fun onFetchFailed(key: UserId, error: Throwable, attempts: Int) {
 *         Timber.w(error, "user fetch failed after %d attempts: %s", attempts, key)
 *     }
 * })
 * ```
 *
 * All methods have no-op defaults — override what you need. Callbacks are invoked inline on
 * the store's dispatcher and must be fast and non-blocking. A callback that throws never
 * disturbs the engine: the exception is swallowed.
 */
public interface AquiferEvents<K : Any> {

    /** A shared fetch actually started for [key] (one call per single-flight cycle). */
    public fun onFetchStarted(key: K) {}

    /** The fetch for [key] succeeded after [duration] (wall-clock, including retries). */
    public fun onFetchSucceeded(key: K, duration: Duration) {}

    /**
     * Attempt [attempt] for [key] failed with [error]; the next attempt runs after [nextDelay].
     * Only called when a retry will actually happen — terminal failures go to [onFetchFailed].
     */
    public fun onFetchRetried(key: K, attempt: Int, error: Throwable, nextDelay: Duration) {}

    /** The fetch for [key] failed for good after [attempts] attempt(s). */
    public fun onFetchFailed(key: K, error: Throwable, attempts: Int) {}

    /**
     * A strategy-driven fetch of [key] was suppressed by the negative cache: a recent
     * failure ([error]) is still inside its suppression window, with [remaining] left.
     * Reported once per suppressed read, so a hot screen may report it repeatedly.
     * Only emitted when [AquiferBuilder.negativeCache] is configured.
     */
    public fun onFetchSuppressed(key: K, error: Throwable, remaining: Duration) {}

    /**
     * The best-effort write-through to the [SourceOfTruth] failed after a successful fetch
     * of [key]. The fetched value was still served and cached in memory; only persistence
     * is out of date.
     */
    public fun onPersistenceWriteFailed(key: K, error: Throwable) {}

    /**
     * A [Aquifer.revalidateOn] subscription hit [error]. Two cases share this hook:
     *
     * - The *trigger flow itself* threw: that subscription has ended (the store keeps
     *   working) — re-attach a new trigger if revalidation should continue.
     * - A single *revalidation sweep* threw (for example a failing storage read while
     *   checking an active key): the subscription stays alive and future trigger emissions
     *   still revalidate.
     *
     * Without this hook either failure would be invisible.
     */
    public fun onRevalidationTriggerFailed(error: Throwable) {}
}
