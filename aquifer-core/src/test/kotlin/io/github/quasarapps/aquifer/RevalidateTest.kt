package io.github.quasarapps.aquifer

import app.cash.turbine.test
import app.cash.turbine.testIn
import app.cash.turbine.turbineScope
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.minutes

class RevalidateTest {

    @Test
    fun `revalidate active refreshes stale keys with active streams`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness { timeToLive = 1.minutes }
        }
        store.put("k", 100)

        store.stream("k").test {
            assertEquals(DataState.Content(100, Origin.MEMORY, isStale = false), awaitItem())

            clock.advanceBy(10.minutes) // The entry is now stale; "connectivity returns".
            store.revalidateActive()

            assertEquals(DataState.Loading(100), awaitItem())
            assertEquals(DataState.Content(1, Origin.FETCHER, isStale = false), awaitItem())
        }
        assertEquals(1, calls)
    }

    @Test
    fun `fresh entries are not refetched`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness { timeToLive = 10.minutes }
        }
        store.put("k", 100)

        store.stream("k").test {
            assertEquals(DataState.Content(100, Origin.MEMORY, isStale = false), awaitItem())

            store.revalidateActive()
            settle()
            expectNoEvents()
        }
        assertEquals(0, calls)
    }

    @Test
    fun `keys without active streams are not refetched`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness { timeToLive = 1.minutes }
        }

        store.get("k") // One-shot read; no stream stays active.
        assertEquals(1, calls)
        clock.advanceBy(10.minutes)

        store.revalidateActive()
        settle()

        assertEquals(1, calls)
    }

    @Test
    fun `cache only streams do not make a key active`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness { timeToLive = 1.minutes }
        }
        store.put("k", 100)
        clock.advanceBy(10.minutes)

        store.stream("k", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(100, Origin.MEMORY, isStale = true), awaitItem())

            store.revalidateActive()
            settle()
            expectNoEvents()
        }
        assertEquals(0, calls)
    }

    @Test
    fun `a cancelled stream no longer keeps its key active`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness { timeToLive = 1.minutes }
        }
        store.put("k", 100)

        turbineScope {
            val stream = store.stream("k").testIn(backgroundScope)
            assertEquals(DataState.Content(100, Origin.MEMORY, isStale = false), stream.awaitItem())
            stream.cancelAndIgnoreRemainingEvents()
        }
        settle() // Let the cancellation unwind and unregister the key.

        clock.advanceBy(10.minutes)
        store.revalidateActive()
        settle()

        assertEquals(0, calls)
    }

    @Test
    fun `revalidateOn triggers revalidation on every emission`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness { timeToLive = 1.minutes }
        }
        val connectivityRestored = MutableSharedFlow<Unit>()
        store.revalidateOn(connectivityRestored)
        store.put("k", 100)

        store.stream("k").test {
            assertEquals(DataState.Content(100, Origin.MEMORY, isStale = false), awaitItem())

            clock.advanceBy(10.minutes)
            connectivityRestored.emit(Unit)

            assertEquals(DataState.Loading(100), awaitItem())
            assertEquals(DataState.Content(1, Origin.FETCHER, isStale = false), awaitItem())

            // A second reconnect while everything is fresh does nothing.
            connectivityRestored.emit(Unit)
            settle()
            expectNoEvents()
        }
        assertEquals(1, calls)
    }

    @Test
    fun `multiple streams of one key trigger a single shared refresh`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness { timeToLive = 1.minutes }
        }
        store.put("k", 100)

        turbineScope {
            val first = store.stream("k").testIn(backgroundScope)
            val second = store.stream("k").testIn(backgroundScope)
            first.awaitItem()
            second.awaitItem()

            clock.advanceBy(10.minutes)
            store.revalidateActive()

            assertEquals(DataState.Loading(100), first.awaitItem())
            assertEquals(DataState.Content(1, Origin.FETCHER, isStale = false), first.awaitItem())
            assertEquals(DataState.Loading(100), second.awaitItem())
            assertEquals(DataState.Content(1, Origin.FETCHER, isStale = false), second.awaitItem())

            first.cancelAndIgnoreRemainingEvents()
            second.cancelAndIgnoreRemainingEvents()
        }
        assertEquals(1, calls)
    }
}
