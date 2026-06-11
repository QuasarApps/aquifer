package io.github.quasarapps.aquifer

/**
 * Source of wall-clock time used for cache timestamps and time-to-live checks.
 *
 * The default is [SYSTEM]. Substitute a fake in tests to control staleness deterministically,
 * or provide a server-synchronised clock when device time cannot be trusted:
 *
 * ```
 * val store = aquifer<String, User> {
 *     fetcher { api.fetchUser(it) }
 *     clock { syncedTime.currentMillis() }
 * }
 * ```
 */
public fun interface WallClock {

    /** Current time in milliseconds since the Unix epoch. */
    public fun nowMillis(): Long

    public companion object {
        /** A [WallClock] backed by [System.currentTimeMillis]. */
        public val SYSTEM: WallClock = WallClock { System.currentTimeMillis() }
    }
}
