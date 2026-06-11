package io.github.quasarapps.aquifer

import app.cash.turbine.test
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.minutes

class AquiferStreamTest {

    @Test
    fun `empty cache emits loading then fetched content`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
        }

        store.stream("k").test {
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Content("fetched", Origin.FETCHER, isStale = false), awaitItem())
        }
    }

    @Test
    fun `fresh cache emits content without fetching`() = runTest {
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                calls++
                "fetched"
            }
            freshness { timeToLive = 5.minutes }
        }
        store.put("k", "cached")

        store.stream("k").test {
            assertEquals(DataState.Content("cached", Origin.MEMORY, isStale = false), awaitItem())
            expectNoEvents()
        }
        assertEquals(0, calls)
    }

    @Test
    fun `stale cache emits stale content then loading then revalidated content`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness { timeToLive = 1.minutes }
        }
        store.put("k", 100)
        clock.advanceBy(2.minutes)

        store.stream("k").test {
            assertEquals(DataState.Content(100, Origin.MEMORY, isStale = true), awaitItem())
            assertEquals(DataState.Loading(100), awaitItem())
            assertEquals(DataState.Content(1, Origin.FETCHER, isStale = false), awaitItem())
        }
    }

    @Test
    fun `fetch failure emits failure that retains the last known value`() = runTest {
        val clock = FakeClock()
        val boom = RuntimeException("network down")
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { throw boom }
            freshness { timeToLive = 1.minutes }
        }
        store.put("k", 100)
        clock.advanceBy(2.minutes)

        store.stream("k").test {
            assertEquals(DataState.Content(100, Origin.MEMORY, isStale = true), awaitItem())
            assertEquals(DataState.Loading(100), awaitItem())
            assertEquals(DataState.Failure(boom, 100), awaitItem())
        }
    }

    @Test
    fun `fetch failure on an empty cache emits failure with no value`() = runTest {
        val boom = RuntimeException("network down")
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { throw boom }
        }

        store.stream("k").test {
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Failure(boom, null), awaitItem())
        }
    }

    @Test
    fun `network only stream ignores the cached value`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
        }
        store.put("k", "cached")

        store.stream("k", Freshness.NetworkOnly).test {
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Content("fetched", Origin.FETCHER, isStale = false), awaitItem())
        }
    }

    @Test
    fun `cache only stream emits cache miss failure then observes later updates`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
        }

        store.stream("k", Freshness.CacheOnly).test {
            val miss = awaitItem()
            assertIs<DataState.Failure<String>>(miss)
            assertIs<CacheMissException>(miss.error)
            assertNull(miss.value)

            // A different caller fetches the key; the passive stream observes the result.
            store.get("k")
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Content("fetched", Origin.FETCHER, isStale = false), awaitItem())
        }
    }

    @Test
    fun `consecutive equal states are deduplicated`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
        }
        store.put("k", "same")

        store.stream("k").test {
            assertEquals(DataState.Content("same", Origin.MEMORY, isStale = false), awaitItem())

            // Differs from the snapshot only by origin, so it is emitted.
            store.put("k", "same")
            assertEquals(DataState.Content("same", Origin.LOCAL, isStale = false), awaitItem())

            // Identical to the previous state, so it is swallowed.
            store.put("k", "same")
            expectNoEvents()

            store.put("k", "different")
            assertEquals(DataState.Content("different", Origin.LOCAL, isStale = false), awaitItem())
        }
    }
}
