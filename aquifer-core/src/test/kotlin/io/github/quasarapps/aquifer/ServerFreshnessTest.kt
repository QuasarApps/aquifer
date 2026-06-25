package io.github.quasarapps.aquifer

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Server-declared freshness via [FetchResult.Fresh.freshFor]: authoritative over the store-wide
 * `timeToLive`, below a per-call `maxAge`, never jittered, and (with a persisting store) honored
 * across the entry's lifetime. The precedence is `maxAge > server freshFor > builder timeToLive`.
 */
class ServerFreshnessTest {

    @Test
    fun `server freshFor expires an entry the store TTL would keep fresh`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            conditionalFetcher { _, _ -> FetchResult.Fresh(++calls, validator = "v", freshFor = 1.minutes) }
            freshness { timeToLive = 1.hours } // store would keep it fresh for an hour
        }

        assertEquals(1, store.get("k")) // fetch, cached with a 1-minute server lifetime
        clock.advanceBy(30.seconds)
        assertEquals(1, store.get("k")) // within the server lifetime: served, no refetch
        assertEquals(1, calls)
        clock.advanceBy(40.seconds) // 70s old > 60s server lifetime: stale despite the 1h store TTL
        assertEquals(2, store.get("k"))
        assertEquals(2, calls)
    }

    @Test
    fun `a tighter per-call maxAge overrides a longer server freshFor`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            conditionalFetcher { _, _ -> FetchResult.Fresh(++calls, validator = "v", freshFor = 60.minutes) }
        }

        assertEquals(1, store.get("k")) // cached with a 60-minute server lifetime
        clock.advanceBy(2.minutes)
        assertEquals(2, store.get("k", maxAge = 1.minutes)) // caller's 1m bar beats the server's 60m
        assertEquals(2, calls)
    }

    @Test
    fun `a looser per-call maxAge serves an entry the server freshFor considers stale`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            conditionalFetcher { _, _ -> FetchResult.Fresh(++calls, validator = "v", freshFor = 1.minutes) }
        }

        assertEquals(1, store.get("k")) // 1-minute server lifetime
        clock.advanceBy(5.minutes) // server-stale
        assertEquals(1, store.get("k", maxAge = 10.minutes)) // caller's 10m bar serves it anyway
        assertEquals(1, calls)
    }

    @Test
    fun `server freshFor is never jittered`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            conditionalFetcher { _, _ -> FetchResult.Fresh(++calls, validator = "v", freshFor = 10.minutes) }
            // High jitter would shorten a builder-TTL horizon by up to 90%; it must not touch freshFor.
            freshness {
                timeToLive = 1.hours
                ttlJitter = 0.9
            }
        }

        assertEquals(1, store.get("k"))
        clock.advanceBy(9.minutes) // still inside the un-jittered 10-minute server lifetime
        assertEquals(1, store.get("k"))
        assertEquals(1, calls)
    }

    @Test
    fun `a server freshFor of ZERO marks the entry immediately stale`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            conditionalFetcher { _, _ -> FetchResult.Fresh(++calls, validator = "v", freshFor = Duration.ZERO) }
            freshness { timeToLive = 1.hours }
        }

        assertEquals(1, store.get("k")) // fetched, cached with a ZERO lifetime
        assertEquals(2, store.get("k")) // immediately stale on the next read, even on the same tick
        assertEquals(2, calls)
    }

    @Test
    fun `a null server freshFor falls through to the store TTL unchanged`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            conditionalFetcher { _, _ -> FetchResult.Fresh(++calls, validator = "v") } // no server opinion
            freshness { timeToLive = 10.minutes }
        }

        assertEquals(1, store.get("k"))
        clock.advanceBy(5.minutes)
        assertEquals(1, store.get("k")) // within the store TTL: served
        clock.advanceBy(6.minutes) // 11m > 10m store TTL: stale
        assertEquals(2, store.get("k"))
        assertEquals(2, calls)
    }

    @Test
    fun `a 304 carries the prior server freshFor forward and re-ages it`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            conditionalFetcher { _, validator ->
                calls++
                if (validator == null) {
                    FetchResult.Fresh(1, validator = "v", freshFor = 1.minutes)
                } else {
                    FetchResult.NotModified
                }
            }
            freshness { timeToLive = Duration.INFINITE }
        }

        assertEquals(1, store.get("k")) // Fresh: 60s server lifetime, validator "v"
        clock.advanceBy(70.seconds) // server-stale
        assertEquals(1, store.get("k")) // revalidate -> NotModified -> re-aged, keeps the 60s lifetime
        assertEquals(2, calls)
        clock.advanceBy(30.seconds) // 30s since the re-age: within the carried-forward 60s
        assertEquals(1, store.get("k"))
        assertEquals(2, calls) // not refetched -> the server lifetime survived the 304
        clock.advanceBy(40.seconds) // 70s since the re-age: stale again
        assertEquals(1, store.get("k"))
        assertEquals(3, calls)
    }
}
