package io.github.quasarapps.aquifer.internal

import io.github.quasarapps.aquifer.RetryConfig
import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.Duration

/** Immutable, testable snapshot of a [RetryConfig] that computes per-attempt delays. */
@Suppress("LongParameterList") // Mirrors RetryConfig's knobs one-to-one; bundling would just duplicate it.
internal class RetryPolicy(
    private val maxAttempts: Int,
    private val initialDelay: Duration,
    private val maxDelay: Duration,
    private val multiplier: Double,
    private val jitter: Double,
    private val retryOn: (Throwable) -> Boolean,
    private val random: Random = Random.Default,
) {

    constructor(config: RetryConfig) : this(
        maxAttempts = config.maxAttempts,
        initialDelay = config.initialDelay,
        maxDelay = config.maxDelay,
        multiplier = config.multiplier,
        jitter = config.jitter,
        retryOn = config.retryOn,
    )

    /**
     * Returns the delay to wait after failed attempt number [attempt] (1-based), or `null`
     * when no further attempt should be made — either because attempts are exhausted or
     * [failure] is not retryable. A throwing predicate counts as not retryable.
     */
    fun delayAfter(attempt: Int, failure: Throwable): Duration? {
        if (attempt >= maxAttempts) return null
        val retryable = runCatching { retryOn(failure) }.getOrDefault(false)
        if (!retryable) return null
        val base = (initialDelay * multiplier.pow(attempt - 1)).coerceAtMost(maxDelay)
        // Jitter only ever shortens the delay, so maxDelay remains a hard cap.
        return base * (1.0 - jitter * random.nextDouble())
    }

    companion object {
        /** Policy matching the default [RetryConfig]: a single attempt, no retries. */
        val NONE = RetryPolicy(RetryConfig())
    }
}
