package io.github.quasarapps.aquifer.test

import io.github.quasarapps.aquifer.WallClock
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration

/**
 * A manually advanced [WallClock] for deterministic time-to-live tests — the time half of
 * Aquifer's own dependency-injected determinism.
 *
 * Inject it into a real store and step it forward to drive staleness exactly when you choose,
 * instead of sleeping:
 *
 * ```
 * val clock = FakeClock()
 * val users = aquifer<String, User> {
 *     scope(backgroundScope)
 *     clock(clock)
 *     fetcher { api.user(it) }
 *     freshness { timeToLive = 5.minutes }
 * }
 * users.get("ada")          // fetched and cached
 * clock.advanceBy(6.minutes) // now stale
 * users.get("ada")          // refetches
 * ```
 */
public class FakeClock(initialMillis: Long = 0L) : WallClock {

    private val now = AtomicLong(initialMillis)

    override fun nowMillis(): Long = now.get()

    /** Advances the clock by [duration]; truncated to whole milliseconds, like the engine's bar. */
    public fun advanceBy(duration: Duration) {
        now.addAndGet(duration.inWholeMilliseconds)
    }
}
