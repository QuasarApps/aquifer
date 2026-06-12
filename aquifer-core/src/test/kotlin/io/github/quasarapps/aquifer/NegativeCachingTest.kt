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
            negativeCache {
                timeToLive = 10.seconds
                backoffMultiplier = 2.0
            }
        }

        assertFailsWith<Boom> { store.get("k") } // failure 1
        clock.advanceBy(11.seconds)
        assertFailsWith<Boom> { store.get("k") } // failure 2: window now 20s
        clock.advanceBy(21.seconds)

        fail = false
        assertEquals(3, store.fresh("k")) // success clears the memory entirely
        store.invalidate("k")

        fail = true
        assertFailsWith<Boom> { store.get("k") } // fails again: count restarted...
        clock.advanceBy(11.seconds) // ...so the window is 10s again, not 40s
        assertFailsWith<Boom> { store.get("k") }
        assertEquals(5, calls)
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
    }
}
