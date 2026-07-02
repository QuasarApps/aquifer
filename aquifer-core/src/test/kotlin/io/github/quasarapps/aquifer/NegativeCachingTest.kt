package io.github.quasarapps.aquifer

import app.cash.turbine.test
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.time.Duration
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Negative caching: failures are remembered per key and suppress strategy-driven refetches. */
class NegativeCachingTest {

    private class Boom(val n: Int) : RuntimeException("boom-$n")

    @Test
    fun `a remembered failure suppresses refetches until the window passes`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { throw Boom(++calls) }
            negativeCache { timeToLive = 30.seconds }
        }

        val first = assertFailsWith<Boom> { store.get("k") } // real fetch, remembered
        val replay = assertFailsWith<Boom> { store.get("k") } // suppressed: rethrown, no fetch
        assertEquals(1, calls)
        assertSame(first, replay, "the original exception instance is rethrown")

        clock.advanceBy(31.seconds)
        assertFailsWith<Boom> { store.get("k") } // window over: the endpoint is asked again
        assertEquals(2, calls)
    }

    @Test
    fun `a stale value is served during suppression without re-fetching`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher {
                calls++
                throw Boom(calls)
            }
            freshness { timeToLive = 1.minutes }
            negativeCache { timeToLive = 30.seconds }
        }
        store.put("k", 7)
        clock.advanceBy(5.minutes) // stale by store TTL

        assertEquals(7, store.get("k")) // stale-if-error: fetch fails, stale served
        settle()
        assertEquals(1, calls)

        assertEquals(7, store.get("k")) // suppressed: stale served with no network attempt
        assertEquals(7, store.get("k", Freshness.StaleWhileRevalidate))
        assertEquals(7, store.get("k", Freshness.NetworkFirst))
        settle()
        assertEquals(1, calls)
    }

    @Test
    fun `new stream subscribers see the remembered failure without a fetch`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { throw Boom(++calls) }
            negativeCache { timeToLive = 30.seconds }
        }
        assertFailsWith<Boom> { store.get("k") }

        store.stream("k").test {
            // Primed straight to the remembered failure: no Loading, no second fetch.
            val state = assertIs<DataState.Failure<Int>>(awaitItem())
            assertEquals("boom-1", state.error.message)
            assertNull(state.value)
            settle()
            expectNoEvents()
        }
        assertEquals(1, calls)
    }

    @Test
    fun `consecutive failures stretch the window by the backoff multiplier`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { throw Boom(++calls) }
            negativeCache {
                timeToLive = 10.seconds
                backoffMultiplier = 2.0
                maxTimeToLive = 1.minutes
            }
        }

        assertFailsWith<Boom> { store.get("k") } // failure 1: window 10s
        clock.advanceBy(11.seconds)
        assertFailsWith<Boom> { store.get("k") } // failure 2: window 20s
        assertEquals(2, calls)

        clock.advanceBy(11.seconds) // 11s into a 20s window: still suppressed
        assertFailsWith<Boom> { store.get("k") }
        assertEquals(2, calls)

        clock.advanceBy(10.seconds) // 21s: window over
        assertFailsWith<Boom> { store.get("k") }
        assertEquals(3, calls)
    }

    @Test
    fun `a success resets the consecutive-failure count`() = runTest {
        val clock = FakeClock()
        var calls = 0
        var fail = true
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher {
                calls++
                if (fail) throw Boom(calls) else calls
            }
            freshness { timeToLive = 1.seconds } // lets later reads refetch without mutating
            negativeCache {
                timeToLive = 10.seconds
                backoffMultiplier = 2.0
            }
        }

        assertFailsWith<Boom> { store.get("k") } // failure 1: window 10s
        clock.advanceBy(11.seconds)
        assertFailsWith<Boom> { store.get("k") } // failure 2: window 20s
        clock.advanceBy(21.seconds)

        fail = false
        assertEquals(3, store.get("k")) // success alone — no invalidate — clears the streak

        clock.advanceBy(2.seconds) // cached 3 is now stale; the next read refetches
        fail = true
        assertEquals(3, store.get("k")) // failure again: stale-if-error, streak restarted at 1
        assertEquals(4, calls)

        clock.advanceBy(11.seconds) // 11s > the 10s first-failure window, but < a 40s streak-3 one
        assertEquals(3, store.get("k"))
        assertEquals(5, calls, "a reset streak means the window is 10s again, not 40s")
    }

    @Test
    fun `invalidate clears the suppression`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { throw Boom(++calls) }
            negativeCache { timeToLive = 30.seconds }
        }
        assertFailsWith<Boom> { store.get("k") }

        store.invalidate("k") // "the data changed, forget the failure too"

        assertFailsWith<Boom> { store.get("k") }
        assertEquals(2, calls, "post-invalidation read should ask the endpoint again")
    }

    @Test
    fun `network only bypasses the negative cache`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { throw Boom(++calls) }
            negativeCache { timeToLive = 30.seconds }
        }
        assertFailsWith<Boom> { store.get("k") }

        assertFailsWith<Boom> { store.fresh("k") } // explicit demand: goes to the network
        assertEquals(2, calls)
    }

    @Test
    fun `revalidateActive skips suppressed keys`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { throw Boom(++calls) }
            negativeCache { timeToLive = 30.seconds }
        }

        store.stream("k").test {
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals("boom-1", (awaitItem() as DataState.Failure).error.message)

            store.revalidateActive() // key is stale-and-missing but suppressed: no sweep fetch
            settle()
            expectNoEvents()
        }
        assertEquals(1, calls)
    }

    @Test
    fun `suppressed reads are reported through events`() = runTest {
        val clock = FakeClock()
        val suppressions = mutableListOf<Pair<String, Duration>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { throw Boom(1) }
            negativeCache { timeToLive = 30.seconds }
            events(object : AquiferEvents<String> {
                override fun onFetchSuppressed(key: String, error: Throwable, remaining: Duration) {
                    suppressions += key to remaining
                }
            })
        }
        assertFailsWith<Boom> { store.get("k") }
        clock.advanceBy(10.seconds)
        assertFailsWith<Boom> { store.get("k") }

        assertEquals(listOf("k" to 20.seconds), suppressions)
    }

    @Test
    fun `sub-millisecond windows still suppress until the next clock tick`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { throw Boom(++calls) }
            negativeCache { timeToLive = 500.microseconds }
        }

        assertFailsWith<Boom> { store.get("k") }
        assertFailsWith<Boom> { store.get("k") } // same tick: deadline rounded up, suppressed
        assertEquals(1, calls)

        clock.advanceBy(1.milliseconds)
        assertFailsWith<Boom> { store.get("k") }
        assertEquals(2, calls)
    }

    @Test
    fun `without the config failures are never remembered`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { throw Boom(++calls) }
        }
        assertFailsWith<Boom> { store.get("k") }
        assertFailsWith<Boom> { store.get("k") }
        assertEquals(2, calls)
    }

    @Test
    fun `configuration is validated`() {
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, Int> {
                fetcher { 1 }
                negativeCache { timeToLive = Duration.ZERO }
            }
        }
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, Int> {
                fetcher { 1 }
                negativeCache { backoffMultiplier = 0.5 }
            }
        }
        // Cross-field: the cap may not undercut the base window.
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, Int> {
                fetcher { 1 }
                negativeCache {
                    timeToLive = 10.minutes
                    maxTimeToLive = 1.minutes
                }
            }
        }
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, Int> {
                fetcher { 1 }
                negativeCache { maxEntries = 0 }
            }
        }
    }

    @Test
    fun `maxEntries bounds the failure memory, evicting the least-recently-used record`() = runTest {
        val fetched = mutableListOf<String>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { key ->
                fetched += key
                throw Boom(fetched.size)
            }
            // Long window so nothing expires mid-test; cap of 2 forces LRU eviction at the 3rd key.
            negativeCache {
                timeToLive = 30.seconds
                maxEntries = 2
            }
        }

        // Fail three distinct keys. Recording k3 pushes the map to 3 > cap, evicting the LRU
        // record — k1, the least-recently-inserted (no read reorders an absent key before this).
        assertFailsWith<Boom> { store.get("k1") }
        assertFailsWith<Boom> { store.get("k2") }
        assertFailsWith<Boom> { store.get("k3") }
        assertEquals(listOf("k1", "k2", "k3"), fetched)

        // k2 and k3 are still remembered: suppressed, rethrown, no new fetch.
        assertFailsWith<Boom> { store.get("k2") }
        assertFailsWith<Boom> { store.get("k3") }
        assertEquals(listOf("k1", "k2", "k3"), fetched, "still-remembered keys are suppressed, not re-fetched")

        // k1's record was evicted, so its fetch is re-permitted. Eviction only re-permits a fetch;
        // a negative record carries no value, so this can never resurrect stale data.
        assertFailsWith<Boom> { store.get("k1") }
        assertEquals(listOf("k1", "k2", "k3", "k1"), fetched, "the evicted key fetches again")
    }
}
