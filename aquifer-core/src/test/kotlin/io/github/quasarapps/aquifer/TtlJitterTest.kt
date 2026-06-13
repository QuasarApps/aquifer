package io.github.quasarapps.aquifer

import io.github.quasarapps.aquifer.internal.TtlJitter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes

/** TTL jitter: deterministic per-entry expiry spread, with `timeToLive` as the hard cap. */
class TtlJitterTest {

    @Test
    fun `fractions are uniform-ish, stable, and inside the unit interval`() {
        val keyHash = "k".hashCode()
        val timestamps = (0L..1000L).toList() // truly consecutive milliseconds
        val fractions = timestamps.map { TtlJitter.fractionFor(it, keyHash) }

        fractions.forEach { fraction ->
            assertTrue(fraction >= 0.0 && fraction < 1.0, "fraction $fraction outside [0, 1)")
        }
        // Determinism: the same entry identity always yields the same factor.
        timestamps.forEach {
            assertEquals(TtlJitter.fractionFor(it, keyHash), TtlJitter.fractionFor(it, keyHash))
        }
        // Spread: consecutive-millisecond writes must decorrelate, or co-fetched entries
        // would still expire together. A quarter of the unit interval is a generous floor.
        assertTrue(fractions.max() - fractions.min() > 0.25, "fractions barely spread")
    }

    @Test
    fun `same-millisecond writes of different keys still spread`() {
        // A bursty commit lands many keys on one clock tick — the exact stampede this
        // feature exists to prevent, so the key must contribute to the factor.
        val tick = 1_718_000_000_000
        val fractions = (0..100).map { TtlJitter.fractionFor(tick, "key-$it".hashCode()) }

        assertTrue(fractions.max() - fractions.min() > 0.25, "same-tick fractions barely spread")
    }

    @Test
    fun `the configured ttl stays the hard upper bound`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness {
                timeToLive = 10.minutes
                ttlJitter = 0.5
            }
        }
        store.put("k", 100)
        clock.advanceBy(10.minutes) // at the full TTL: stale for every possible factor

        assertEquals(1, store.get("k"))
        assertEquals(1, calls)
    }

    @Test
    fun `entries are never stale before the jitter floor`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness {
                timeToLive = 10.minutes
                ttlJitter = 0.5
            }
        }
        store.put("k", 100)
        clock.advanceBy(4.minutes) // under the 5-minute floor (ttl × (1 − jitter))

        assertEquals(100, store.get("k"))
        settle()
        assertEquals(0, calls)
    }

    @Test
    fun `the staleness verdict is stable across repeated reads`() = runTest {
        val clock = FakeClock()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { -1 }
            freshness {
                timeToLive = 10.minutes
                ttlJitter = 0.5
            }
        }
        store.put("k", 100)
        clock.advanceBy(4.minutes) // inside the floor: fresh under any factor

        // The verdict is a pure function of the write timestamp: repeated checks at the
        // same instant agree, so the entry cannot flicker between fresh and stale.
        repeat(3) { assertEquals(100, store.get("k", Freshness.CacheFirst)) }
        settle()
        assertEquals(100, store.get("k", Freshness.CacheOnly))
    }

    @Test
    fun `per-call maxAge is never jittered`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness {
                timeToLive = 10.minutes
                ttlJitter = 1.0 // maximal jitter on the store TTL...
            }
        }
        store.put("k", 100)
        clock.advanceBy(4.minutes)

        // ...but the caller's explicit 5-minute bar is exact: 4 minutes old serves.
        assertEquals(100, store.get("k", maxAge = 5.minutes))
        settle()
        assertEquals(0, calls)
    }

    @Test
    fun `jitter must be a fraction`() {
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, Int> {
                fetcher { 1 }
                freshness { ttlJitter = -0.1 }
            }
        }
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, Int> {
                fetcher { 1 }
                freshness { ttlJitter = 1.1 }
            }
        }
    }
}
