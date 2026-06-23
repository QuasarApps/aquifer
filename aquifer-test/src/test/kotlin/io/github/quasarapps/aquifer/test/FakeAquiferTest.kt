package io.github.quasarapps.aquifer.test

import app.cash.turbine.test
import io.github.quasarapps.aquifer.CacheMissException
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.Freshness
import io.github.quasarapps.aquifer.Origin
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class FakeAquiferTest {

    @Test
    fun `seeded entries are served without fetching`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            seed("a" to 1, "b" to 2)
            fetcher { -1 }
        }

        assertEquals(1, store.get("a"))
        assertEquals(2, store.get("b"))
        assertEquals(0, store.fetchCount(), "seeds are a warm start, not fetches")
    }

    @Test
    fun `get fetches a scripted value once, then serves it from cache`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            returns("a", 42)
        }

        assertEquals(42, store.get("a")) // CacheFirst miss -> fetch
        assertEquals(42, store.get("a")) // now cached -> no fetch
        assertEquals(1, store.fetchCount("a"))
    }

    @Test
    fun `the fallback fetcher handles unscripted keys`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            returns("scripted", 1)
            fetcher { key -> key.length }
        }

        assertEquals(1, store.get("scripted"))
        assertEquals(5, store.get("abcde")) // via the fallback fetcher
        assertEquals(listOf("scripted", "abcde"), store.fetchedKeys())
    }

    @Test
    fun `a scripted failure is thrown and counted`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            failsWith("a", IOException("boom"))
        }

        assertFailsWith<IOException> { store.get("a") }
        assertEquals(1, store.fetchCount("a"))
    }

    @Test
    fun `an unscripted key with no fetcher fails loudly`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {}

        assertFailsWith<IllegalStateException> { store.get("missing") }
    }

    @Test
    fun `NetworkOnly always fetches, even when cached`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            seed("a" to 1)
            returns("a", 2)
        }

        assertEquals(2, store.fresh("a")) // NetworkOnly: re-fetches despite the seed
        assertEquals(1, store.fetchCount("a"))
    }

    @Test
    fun `NetworkFirst falls back to the cached value when the fetch fails`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            seed("a" to 1)
            failsWith("a", IOException("offline"))
        }

        assertEquals(1, store.get("a", Freshness.NetworkFirst)) // fetch fails -> stale fallback
        assertEquals(1, store.fetchCount("a"))
    }

    @Test
    fun `getAll returns the resolved subset and omits per-key failures`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            returns("a", 1)
            failsWith("b", IOException("boom"))
            returns("c", 3)
        }

        assertEquals(mapOf("a" to 1, "c" to 3), store.getAll(setOf("a", "b", "c")))
        assertEquals(1, store.fetchCount("b")) // the failing key was still attempted
    }

    @OptIn(ExperimentalCoroutinesApi::class) // TestScope.testScheduler.currentTime
    @Test
    fun `scripted delays elapse in virtual time`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            returns("a", 1)
            delays("a", 5.seconds)
        }

        store.get("a")
        assertEquals(5.seconds.inWholeMilliseconds, testScheduler.currentTime)
    }

    @Test
    fun `responses can be re-scripted between calls`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            failsWith("a", IOException("first attempt"))
        }

        assertFailsWith<IOException> { store.fresh("a") }
        store.returns("a", 99) // recover
        assertEquals(99, store.fresh("a"))
        assertEquals(2, store.fetchCount("a"))
    }

    @Test
    fun `writes and invalidation drive the cache and snapshot`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) { fetcher { -1 } }

        store.putAll(mapOf("a" to 1, "b" to 2))
        store.put("c", 3)
        assertEquals(setOf("a", "b", "c"), store.snapshot())

        store.invalidateWhere { it == "a" }
        assertEquals(setOf("b", "c"), store.snapshot())

        store.invalidateAll()
        assertEquals(emptySet(), store.snapshot())
    }

    @Test
    fun `a seeded stream emits Content without fetching`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            seed("a" to 1)
            fetcher { -1 }
        }

        store.stream("a").test {
            assertEquals(DataState.Content(1, Origin.MEMORY, isStale = false), awaitItem())
        }
        assertEquals(0, store.fetchCount("a"))
    }

    @Test
    fun `a stream of a missing key fetches once, emitting Loading then Content`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            returns("a", 7)
        }

        store.stream("a").test {
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Content(7, Origin.MEMORY, isStale = false), awaitItem())
        }
        assertEquals(1, store.fetchCount("a"))
    }

    @Test
    fun `a stream fetch failure surfaces as Failure`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            failsWith("a", IOException("down"))
        }

        store.stream("a").test {
            assertEquals(DataState.Loading(null), awaitItem())
            assertIs<DataState.Failure<Int>>(awaitItem())
        }
    }

    @Test
    fun `a stream over a cached key projects it without fetching, even for NetworkOnly`() = runTest {
        // Streams fetch only on a miss; network-priority force-refetch is a one-shot-read behavior,
        // so a stream never surfaces a stale cached value via Loading/Failure.
        val store = fakeAquifer<String, Int>(backgroundScope) {
            seed("a" to 1)
            returns("a", 2)
        }

        store.stream("a", Freshness.NetworkOnly).test {
            assertEquals(DataState.Content(1, Origin.MEMORY, isStale = false), awaitItem())
        }
        assertEquals(0, store.fetchCount("a"))
    }

    @Test
    fun `a cache-only stream of a missing key is Empty`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) { fetcher { -1 } }

        store.stream("x", Freshness.CacheOnly).test {
            assertEquals(DataState.Empty, awaitItem())
        }
        assertEquals(0, store.fetchCount(), "a cache-only stream never fetches")
    }

    @Test
    fun `prefetch with NetworkFirst refetches an already-cached key, like get`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            seed("a" to 1)
            returns("a", 2)
        }

        store.prefetch("a", Freshness.NetworkFirst) // network-priority: fetches despite the cache
        settle()

        assertEquals(1, store.fetchCount("a"))
        assertEquals(2, store.get("a", Freshness.CacheOnly)) // the prefetch updated the cache
    }

    @Test
    fun `a negative or non-finite scripted delay is rejected`() = runTest {
        assertFailsWith<IllegalArgumentException> {
            fakeAquifer<String, Int>(backgroundScope) { delays("a", -(1.seconds)) }
        }
        assertFailsWith<IllegalArgumentException> {
            fakeAquifer<String, Int>(backgroundScope) { fetchDelay = Duration.INFINITE }
        }
    }

    @Test
    fun `prefetch warms the cache on the scope`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) {
            returns("a", 1)
        }

        store.prefetch("a")
        settle() // let the fire-and-forget fetch run

        assertEquals(1, store.fetchCount("a"))
        assertEquals(1, store.get("a", Freshness.CacheOnly)) // served from the warmed cache
    }

    @Test
    fun `a closed store throws on every member except snapshot`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) { seed("a" to 1) }
        store.close()

        assertFailsWith<IllegalStateException> { store.get("a") }
        assertFailsWith<IllegalStateException> { store.put("a", 2) }
        assertFailsWith<IllegalStateException> { store.invalidateAll() }
        assertFailsWith<IllegalStateException> { store.stream("a") } // throws at call time, not on collect
        assertFailsWith<IllegalStateException> { store.streamMany(emptySet()) }
        assertEquals(setOf("a"), store.snapshot()) // the read-only peek stays callable
    }

    @Test
    fun `CacheOnly get throws on a miss`() = runTest {
        val store = fakeAquifer<String, Int>(backgroundScope) { fetcher { -1 } }

        assertFailsWith<CacheMissException> { store.get("x", Freshness.CacheOnly) }
        assertEquals(0, store.fetchCount())
    }
}
