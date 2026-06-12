package io.github.quasarapps.aquifer.internal

import io.github.quasarapps.aquifer.NegativeCacheConfig
import kotlin.time.Duration

/** Resolved negative-caching parameters; built only when the feature is enabled. */
internal class NegativeCachePolicy(config: NegativeCacheConfig) {

    private val timeToLive = config.timeToLive
    private val multiplier = config.backoffMultiplier
    private val maxTimeToLive = config.maxTimeToLive

    /**
     * Suppression window for the [consecutiveFailures]-th straight failure of a key:
     * `timeToLive × multiplier^(n−1)`, capped at [maxTimeToLive]. Computed by repeated
     * multiplication with an early exit at the cap, so large counts cannot overflow.
     */
    fun windowFor(consecutiveFailures: Int): Duration {
        // Constant-time fast path: a flat multiplier never grows the window, and a long
        // streak (callers guarantee >= 1) must not cost O(streak) per recorded failure.
        if (multiplier == 1.0) return timeToLive.coerceAtMost(maxTimeToLive)
        var window = timeToLive
        repeat(consecutiveFailures - 1) {
            if (window >= maxTimeToLive) return maxTimeToLive
            window = window * multiplier
        }
        return window.coerceAtMost(maxTimeToLive)
    }
}
