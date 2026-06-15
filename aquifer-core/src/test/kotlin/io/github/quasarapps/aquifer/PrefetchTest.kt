package io.github.quasarapps.aquifer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** `prefetch`: fire-and-forget warmup that honours freshness, dedups, and never throws. */
class PrefetchTest {

    @Test
    fun `prefetch warms a missing key for the next read`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { ++calls }
        }

        store.prefetch("k") // returns immediately
        settle() // let the fire-and-forget fetch land
        assertEquals(1, calls)

        // The warmed value is served from cache — no second fetch.
        assertEquals(1, store.get("k"))
        assertEquals(1, calls)
    }

    @Test
    fun `prefetch does not fetch when the cached entry is still fresh`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { ++calls }
            freshness { timeToLive = 10.minutes }
        }
        store.put("k", 100)

        store.prefetch("k") // CacheFirst sees a fresh entry: nothing to do
        settle()
        assertEquals(0, calls)
    }

    @Test
    fun `prefetch of a fresh entry does not consult the negative cache`() = runTest {
        val clock = FakeClock()
        val suppressions = mutableListOf<String>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { throw IllegalStateException("boom") }
            freshness { timeToLive = 10.minutes }
            negativeCache {
                timeToLive = 30.minutes // outlives the store TTL below
                maxTimeToLive = 30.minutes
            }
            events(object : AquiferEvents<String> {
                override fun onFetchSuppressed(key: String, error: Throwable, remaining: Duration) {
                    suppressions += key
                }
            })
        }
        store.put("k", 100) // a fresh cached entry...
        // ...with a live suppression record alongside it: a NetworkOnly fetch fails (recording
        // the failure) without consulting suppression or clearing the cached value.
        assertFailsWith<IllegalStateException> { store.get("k", Freshness.NetworkOnly) }
        assertEquals(emptyList(), suppressions)

        store.prefetch("k") // CacheFirst + fresh entry ⇒ no fetch wanted
        settle()
        assertEquals(emptyList(), suppressions, "a no-op prefetch must not consult the negative cache")

        // The record really was live: once the entry goes stale, a read IS suppressed.
        clock.advanceBy(11.minutes)
        assertEquals(100, store.get("k")) // stale-if-error: served from cache, fetch suppressed
        assertEquals(listOf("k"), suppressions)
    }

    @Test
    fun `prefetch refetches a stale entry`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness { timeToLive = 1.minutes }
        }
        store.put("k", 100)
        clock.advanceBy(5.minutes) // stale

        store.prefetch("k")
        settle()
        assertEquals(1, calls)
    }

    @Test
    fun `prefetch shares a single in-flight fetch with a concurrent get`() = runTest {
        val gate = CompletableDeferred<Unit>()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher {
                calls++
                gate.await()
                42
            }
        }

        store.prefetch("k") // starts the fetch
        settle() // let it register in-flight and reach the gate
        val read = async { store.get("k", Freshness.NetworkFirst) } // joins the same fetch
        settle()
        gate.complete(Unit)

        assertEquals(42, read.await())
        assertEquals(1, calls, "prefetch and get shared one fetch")
    }

    @Test
    fun `prefetch with NetworkFirst fetches even when the cached entry is fresh`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { ++calls }
            freshness { timeToLive = 10.minutes }
        }
        store.put("k", 100) // fresh — but NetworkFirst always fetches (no cache read needed)

        store.prefetch("k", Freshness.NetworkFirst)
        settle()
        assertEquals(1, calls)
    }

    @Test
    fun `prefetch with CacheOnly never fetches`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { ++calls }
        }

        store.prefetch("k", Freshness.CacheOnly)
        settle()
        assertEquals(0, calls)
    }

    @Test
    fun `a prefetch failure is not thrown and is reported through events`() = runTest {
        val failures = mutableListOf<Throwable>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { throw IllegalStateException("boom") }
            events(object : AquiferEvents<String> {
                override fun onFetchFailed(key: String, error: Throwable, attempts: Int) {
                    failures += error
                }
            })
        }

        store.prefetch("k") // must not throw
        settle()

        assertEquals(1, failures.size)
        assertEquals("boom", failures.single().message)
    }

    @Test
    fun `prefetch stands down while a key is negative-cached`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { throw IllegalStateException("boom-${++calls}") }
            negativeCache { timeToLive = 30.seconds }
        }
        assertFailsWith<IllegalStateException> { store.get("k") } // failure 1, remembered
        assertEquals(1, calls)

        store.prefetch("k") // suppressed: no fetch
        settle()
        assertEquals(1, calls)
    }

    @Test
    fun `prefetch with NetworkOnly bypasses the negative cache`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { throw IllegalStateException("boom-${++calls}") }
            negativeCache { timeToLive = 30.seconds }
        }
        assertFailsWith<IllegalStateException> { store.get("k") } // failure 1, remembered
        assertEquals(1, calls)

        store.prefetch("k", Freshness.NetworkOnly) // explicit demand: bypasses suppression
        settle()
        assertEquals(2, calls)
    }

    @Test
    fun `prefetch on a closed store throws`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 1 }
        }
        store.close()

        assertFailsWith<IllegalStateException> { store.prefetch("k") }
    }
}
