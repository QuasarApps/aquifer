package io.github.quasarapps.aquifer

import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration

/** A manually advanced [WallClock] for deterministic time-to-live tests. */
class FakeClock(initialMillis: Long = 0L) : WallClock {

    private val now = AtomicLong(initialMillis)

    override fun nowMillis(): Long = now.get()

    fun advanceBy(duration: Duration) {
        now.addAndGet(duration.inWholeMilliseconds)
    }
}
