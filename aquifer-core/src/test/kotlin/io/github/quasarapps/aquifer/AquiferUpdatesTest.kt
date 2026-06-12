package io.github.quasarapps.aquifer

import app.cash.turbine.test
import app.cash.turbine.testIn
import app.cash.turbine.turbineScope
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.minutes

class AquiferUpdatesTest {

    @Test
    fun `puts are broadcast to every active stream of the key`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
            freshness { timeToLive = 5.minutes }
        }
        store.put("k", "original")

        turbineScope {
            val first = store.stream("k").testIn(backgroundScope)
            val second = store.stream("k").testIn(backgroundScope)
            assertEquals(DataState.Content("original", Origin.MEMORY, isStale = false), first.awaitItem())
            assertEquals(DataState.Content("original", Origin.MEMORY, isStale = false), second.awaitItem())

            store.put("k", "edited")

            assertEquals(DataState.Content("edited", Origin.LOCAL, isStale = false), first.awaitItem())
            assertEquals(DataState.Content("edited", Origin.LOCAL, isStale = false), second.awaitItem())

            first.cancelAndIgnoreRemainingEvents()
            second.cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `puts do not leak into streams of other keys`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched-$it" }
        }

        store.stream("a").test {
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Content("fetched-a", Origin.FETCHER, isStale = false), awaitItem())

            store.put("b", "unrelated")
            expectNoEvents()
        }
    }

    @Test
    fun `invalidation makes active streams refetch automatically`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { ++calls }
        }

        store.stream("k").test {
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Content(1, Origin.FETCHER, isStale = false), awaitItem())

            store.invalidate("k")

            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Content(2, Origin.FETCHER, isStale = false), awaitItem())
        }
        assertEquals(2, calls)
    }

    @Test
    fun `invalidate all makes every active stream refetch its own key`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { key -> "fetched-$key" }
        }
        store.put("a", "seed-a")
        store.put("b", "seed-b")

        turbineScope {
            val a = store.stream("a").testIn(backgroundScope)
            val b = store.stream("b").testIn(backgroundScope)
            assertEquals(DataState.Content("seed-a", Origin.MEMORY, isStale = false), a.awaitItem())
            assertEquals(DataState.Content("seed-b", Origin.MEMORY, isStale = false), b.awaitItem())

            store.invalidateAll()

            assertEquals(DataState.Loading(null), a.awaitItem())
            assertEquals(DataState.Content("fetched-a", Origin.FETCHER, isStale = false), a.awaitItem())
            assertEquals(DataState.Loading(null), b.awaitItem())
            assertEquals(DataState.Content("fetched-b", Origin.FETCHER, isStale = false), b.awaitItem())

            a.cancelAndIgnoreRemainingEvents()
            b.cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `cache only streams observe invalidation as empty, never a refetch`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { ++calls }
        }
        store.put("k", 100)

        store.stream("k", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(100, Origin.MEMORY, isStale = false), awaitItem())

            store.invalidate("k")
            assertEquals(DataState.Empty, awaitItem())
            settle()
            expectNoEvents()
        }
        assertEquals(0, calls)
    }
}
