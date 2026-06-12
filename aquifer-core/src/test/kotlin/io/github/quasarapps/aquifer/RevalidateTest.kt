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
    fun `a throwing trigger is contained and reported, not crashing the process`() = runTest {
        val failures = mutableListOf<Throwable>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 1 }
            events(object : AquiferEvents<String> {
                override fun onRevalidationTriggerFailed(error: Throwable) {
                    failures += error
                }
            })
        }

        store.revalidateOn(kotlinx.coroutines.flow.flow<Unit> { error("broken trigger") })
        settle()

        // Without containment the exception escapes the supervisor as an uncaught error
        // (which runTest would surface as a test failure). The store keeps working:
        assertEquals(1, store.get("k"))
        assertEquals("broken trigger", failures.single().message)
    }

    @Test
    fun `a failing revalidation sweep does not end the trigger subscription`() = runTest {
        val failures = mutableListOf<Throwable>()
        var failReads = false
        val disk = object : SourceOfTruth<String, Int> {
            override suspend fun read(key: String): PersistedEntry<Int>? =
                if (failReads) throw java.io.IOException("disk hiccup") else null

            override suspend fun write(key: String, entry: PersistedEntry<Int>) = Unit
            override suspend fun delete(key: String) = Unit
            override suspend fun deleteAll() = Unit
        }
        val clock = FakeClock()
        var fetches = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { key -> if (key == "k") ++fetches else -1 }
            freshness { timeToLive = 1.minutes }
            memoryCache { maxEntries = 1 }
            persistence(disk)
            events(object : AquiferEvents<String> {
                override fun onRevalidationTriggerFailed(error: Throwable) {
                    failures += error
                }
            })
        }
        val trigger = MutableSharedFlow<Unit>()
        store.revalidateOn(trigger)

        store.stream("k").test {
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Content(1, Origin.FETCHER, isStale = false), awaitItem())

            store.get("evictor") // pushes "k" out of the size-1 memory cache
            clock.advanceBy(10.minutes) // "k" is stale; the sweep must consult storage

            failReads = true
            trigger.emit(Unit) // this sweep dies on the failing disk read…
            settle()
            assertEquals("disk hiccup", failures.single().message)
            expectNoEvents()

            failReads = false
            trigger.emit(Unit) // …but the subscription survives and the next sweep works.
            assertEquals(DataState.Loading(1), awaitItem())
            assertEquals(DataState.Content(2, Origin.FETCHER, isStale = false), awaitItem())
        }
        assertEquals(2, fetches)
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
