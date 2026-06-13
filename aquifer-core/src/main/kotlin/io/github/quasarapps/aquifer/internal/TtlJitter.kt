package io.github.quasarapps.aquifer.internal

/**
 * Deterministic per-entry jitter for time-to-live decisions.
 *
 * Entries fetched together (a list screen warming 50 keys in one frame) would otherwise go
 * stale together and stampede the backend with 50 simultaneous revalidations. Scaling each
 * entry's effective TTL by a factor derived *from its own write timestamp* spreads those
 * expiries — and because the factor is a pure function of the timestamp, the same entry
 * always gets the same horizon: no flickering between fresh and stale across checks, and
 * the same verdict even across process restarts (the timestamp is what persists).
 *
 * Like retry jitter, this only ever *shortens*: the configured `timeToLive` remains the
 * hard upper bound on freshness.
 */
internal object TtlJitter {

    // SplitMix64's finalizer: a well-studied bijective avalanche of the seed bits, so
    // consecutive millisecond timestamps yield decorrelated fractions.
    private const val MIX_1 = -0x61c8864680b583ebL // 0x9E3779B97F4A7C15
    private const val MIX_2 = -0x40a7b892e31b1a47L // 0xBF58476D1CE4E5B9
    private const val MIX_3 = -0x6b2fb644ecceee15L // 0x94D049BB133111EB
    private const val SHIFT_1 = 30
    private const val SHIFT_2 = 27
    private const val SHIFT_3 = 31
    private const val FRACTION_BITS = 53
    private const val FRACTION_SHIFT = 64 - FRACTION_BITS

    /** Uniform fraction in `[0, 1)` derived deterministically from [seed]. */
    fun fractionFor(seed: Long): Double {
        var z = seed + MIX_1
        z = (z xor (z ushr SHIFT_1)) * MIX_2
        z = (z xor (z ushr SHIFT_2)) * MIX_3
        z = z xor (z ushr SHIFT_3)
        // Top 53 bits -> the full precision of a Double in [0, 1).
        return (z ushr FRACTION_SHIFT) / (1L shl FRACTION_BITS).toDouble()
    }
}
