package io.github.quasarapps.aquifer

import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Retry behaviour for failed fetches, configured via [AquiferBuilder.retry]:
 *
 * ```
 * val users = aquifer<UserId, User> {
 *     fetcher { api.fetchUser(it) }
 *     retry {
 *         maxAttempts = 3
 *         initialDelay = 250.milliseconds
 *         retryOn = { it is IOException }
 *     }
 * }
 * ```
 *
 * Delays grow exponentially — `initialDelay`, then `initialDelay * multiplier`, and so on —
 * capped at [maxDelay]. [jitter] then randomly *shortens* each delay by up to that fraction,
 * de-synchronising clients that fail in lockstep without ever exceeding the configured cap.
 *
 * Retrying happens inside the shared single-flight fetch: observers see one `Loading` state
 * for the whole cycle and one `Failure` if every attempt fails. Individual attempts are
 * reported to [AquiferEvents.onFetchRetried]. Cancellation is never retried.
 */
@AquiferDsl
public class RetryConfig internal constructor() {

    /**
     * Total number of attempts, including the first one. The default of 1 means failures are
     * not retried. Must be at least 1.
     */
    public var maxAttempts: Int = 1
        set(value) {
            require(value >= 1) { "maxAttempts must be at least 1, was $value" }
            field = value
        }

    /** Delay before the first retry. Must be positive and finite. Defaults to 250 ms. */
    public var initialDelay: Duration = 250.milliseconds
        set(value) {
            require(value.isPositive() && value.isFinite()) {
                "initialDelay must be positive and finite, was $value"
            }
            field = value
        }

    /** Upper bound for any single delay. Must be positive and finite. Defaults to 30 s. */
    public var maxDelay: Duration = 30.seconds
        set(value) {
            require(value.isPositive() && value.isFinite()) {
                "maxDelay must be positive and finite, was $value"
            }
            field = value
        }

    /** Factor applied to the delay after each failed attempt. Must be ≥ 1. Defaults to 2. */
    public var multiplier: Double = 2.0
        set(value) {
            require(value >= 1.0) { "multiplier must be >= 1.0, was $value" }
            field = value
        }

    /**
     * Fraction (0..1) by which each delay is randomly shortened. 0 disables jitter; the
     * default 0.5 draws each delay uniformly from the upper half of its exponential value.
     */
    public var jitter: Double = 0.5
        set(value) {
            require(value in 0.0..1.0) { "jitter must be within 0.0..1.0, was $value" }
            field = value
        }

    /**
     * Decides whether a failure is worth retrying — return `true` to retry. Defaults to
     * retrying everything. A predicate that itself throws is treated as `false`.
     * Cancellation is never passed to this predicate.
     */
    public var retryOn: (Throwable) -> Boolean = { true }
}
