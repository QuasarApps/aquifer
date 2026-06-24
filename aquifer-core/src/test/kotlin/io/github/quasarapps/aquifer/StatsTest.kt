package io.github.quasarapps.aquifer

import app.cash.turbine.test
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.minutes

/** `stats()`: non-suspending hit/miss/eviction/in-flight counters. */
class StatsTest {

    @Test
    fun `a fresh store reports empty stats`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
        }
        assertEquals(CacheStats.EMPTY, store.stats())
    }

    @Test
    fun `CacheFirst counts a miss then a hit`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 1 }
        }

        store.get("a") // miss: nothing cached -> fetch
        store.get("a") // hit: served from memory

        val stats = store.stats()
        assertEquals(1, stats.hits)
        assertEquals(1, stats.misses)
        assertEquals(2, stats.reads)
        assertEquals(0.5, stats.hitRate)
    }

    @Test
    fun `CacheOnly counts a hit when cached and a miss when absent`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 1 }
        }
        store.put("a", 1) // a put doesn't read, so it isn't counted

        assertEquals(1, store.get("a", Freshness.CacheOnly)) // hit
        assertFailsWith<CacheMissException> { store.get("x", Freshness.CacheOnly) } // miss

        assertEquals(1, store.stats().hits)
        assertEquals(1, store.stats().misses)
    }

    @Test
    fun `network-priority reads are always misses`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 1 }
        }
        store.put("a", 1)

        store.get("a", Freshness.NetworkFirst) // miss despite being cached
        store.fresh("a") // NetworkOnly: miss

        assertEquals(CacheStats(hits = 0, misses = 2, evictions = 0, inFlight = 0), store.stats())
    }

    @Test
    fun `StaleWhileRevalidate serving a stale value counts as a hit`() = runTest {
        val clock = FakeClock()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { 1 }
            freshness { timeToLive = 1.minutes }
        }
        store.put("a", 1)
        clock.advanceBy(2.minutes) // now stale

        store.get("a", Freshness.StaleWhileRevalidate) // serves stale -> hit, revalidates in bg

        assertEquals(1, store.stats().hits)
        assertEquals(0, store.stats().misses)
    }

    @Test
    fun `getAll counts a hit per cached key and a miss per fetched key`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 0 }
        }
        store.putAll(mapOf("a" to 1, "b" to 2))

        store.getAll(setOf("a", "b", "c")) // a,b hits; c miss

        assertEquals(2, store.stats().hits)
        assertEquals(1, store.stats().misses)
    }

    @Test
    fun `getAll counts a stale SWR key as a miss but a fresh one as a hit`() = runTest {
        // Unlike get/stream, getAll awaits the SWR refresh of a stale key, so it's served via a
        // fetch (a miss) — only a still-fresh SWR key is served from cache (a hit).
        val clock = FakeClock()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { 0 }
            freshness { timeToLive = 1.minutes }
        }
        store.put("stale", 1) // written at t0
        clock.advanceBy(2.minutes) // "stale" is now past its TTL
        store.put("fresh", 2) // written fresh at t0 + 2m

        store.getAll(setOf("fresh", "stale"), Freshness.StaleWhileRevalidate)

        assertEquals(1, store.stats().hits) // "fresh"
        assertEquals(1, store.stats().misses) // "stale" — awaited refresh
    }

    @Test
    fun `a stream counts its initial read`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 9 }
        }
        store.put("a", 1)

        store.stream("a").test { // cached -> hit
            assertEquals(1, awaitItem().value)
            cancelAndIgnoreRemainingEvents()
        }
        store.stream("b").test { // missing -> miss (Loading then fetched Content)
            awaitItem()
            assertEquals(9, awaitItem().value)
            cancelAndIgnoreRemainingEvents()
        }

        assertEquals(1, store.stats().hits)
        assertEquals(1, store.stats().misses)
    }

    @Test
    fun `evictions are counted as the LRU drops entries`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            memoryCache { maxEntries = 1 }
            fetcher { -1 }
        }

        store.put("a", 1)
        store.put("b", 2) // evicts a
        store.put("c", 3) // evicts b

        assertEquals(2, store.stats().evictions)
        assertEquals(setOf("c"), store.snapshot())
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `inFlight reflects fetches currently running`() = runTest {
        val gate = CompletableDeferred<Unit>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher {
                gate.await()
                1
            }
        }

        val read = launch { store.get("a") }
        advanceUntilIdle() // let the fetch start and park on the gate
        assertEquals(1, store.stats().inFlight)

        gate.complete(Unit)
        read.join()
        assertEquals(0, store.stats().inFlight)
    }

    @Test
    fun `stats is callable on a closed store`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 1 }
        }
        store.get("a")
        store.close()

        assertEquals(1, store.stats().misses) // the read-only peek stays callable, like snapshot()
    }
}
