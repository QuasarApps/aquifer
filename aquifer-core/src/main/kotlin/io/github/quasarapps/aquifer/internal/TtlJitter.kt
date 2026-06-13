package io.github.quasarapps.aquifer.internal

/**
 * Deterministic per-entry jitter for time-to-live decisions.
 *
 * Entries fetched together (a list screen warming 50 keys in one frame) would otherwise go
 * stale together and stampede the backend with 50 simultaneous revalidations. Scaling each
 * entry's effective TTL by a factor derived from *its key and its write timestamp* spreads
 * those expiries — the key's hash keeps same-millisecond co-writes apart (bursty commits
 * land on the same tick routinely), and because the factor is a pure function of the two,
 * the same entry always gets the same horizon: no flickering between fresh and stale
 * across checks. The verdict also survives process restarts *when the key's `hashCode` is
 * value-based and stable* (data classes, strings, primitives — the norm); an
 * identity-hashed key re-rolls its factor on restart, which merely re-spreads its expiry.
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
    private const val KEY_HASH_SHIFT = 32

    /** Uniform fraction in `[0, 1)` derived deterministically from an entry's identity. */
    fun fractionFor(writtenAtMillis: Long, keyHash: Int): Double {
        // The hash rides the high bits so millisecond increments in the low bits can't
        // mask it; the finalizer below avalanches both into every output bit.
        var z = (writtenAtMillis xor (keyHash.toLong() shl KEY_HASH_SHIFT)) + MIX_1
        z = (z xor (z ushr SHIFT_1)) * MIX_2
        z = (z xor (z ushr SHIFT_2)) * MIX_3
        z = z xor (z ushr SHIFT_3)
        // Top 53 bits -> the full precision of a Double in [0, 1).
        return (z ushr FRACTION_SHIFT) / (1L shl FRACTION_BITS).toDouble()
    }
}
