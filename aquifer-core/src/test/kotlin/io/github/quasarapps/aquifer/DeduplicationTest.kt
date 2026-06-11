package io.github.quasarapps.aquifer

import app.cash.turbine.testIn
import app.cash.turbine.turbineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals

class DeduplicationTest {

    @Test
    fun `concurrent gets for the same key share a single fetch`() = runTest {
        val calls = AtomicInteger()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                calls.incrementAndGet()
                delay(100)
                "value"
            }
        }

        val results = (1..25).map { async { store.get("k") } }.awaitAll()

        assertEquals(List(25) { "value" }, results)
        assertEquals(1, calls.get())
    }

    @Test
    fun `concurrent gets for distinct keys do not block each other`() = runTest {
        val calls = AtomicInteger()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { key ->
                calls.incrementAndGet()
                delay(100)
                "value-$key"
            }
        }

        val results = listOf("a", "b", "c").map { key -> async { store.get(key) } }.awaitAll()

        assertEquals(listOf("value-a", "value-b", "value-c"), results)
        assertEquals(3, calls.get())
    }

    @Test
    fun `concurrent streams of the same key share a single fetch`() = runTest {
        val calls = AtomicInteger()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                calls.incrementAndGet()
                delay(100)
                "value"
            }
        }

        turbineScope {
            val first = store.stream("k").testIn(backgroundScope)
            val second = store.stream("k").testIn(backgroundScope)

            assertEquals(DataState.Loading(null), first.awaitItem())
            assertEquals(DataState.Content("value", Origin.FETCHER, isStale = false), first.awaitItem())
            assertEquals(DataState.Loading(null), second.awaitItem())
            assertEquals(DataState.Content("value", Origin.FETCHER, isStale = false), second.awaitItem())
            assertEquals(1, calls.get())

            first.cancelAndIgnoreRemainingEvents()
            second.cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `a get issued while a stream's fetch is in flight joins that fetch`() = runTest {
        val calls = AtomicInteger()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                calls.incrementAndGet()
                delay(100)
                "value"
            }
        }

        turbineScope {
            val stream = store.stream("k").testIn(backgroundScope)
            assertEquals(DataState.Loading(null), stream.awaitItem())

            assertEquals("value", store.get("k"))
            assertEquals(DataState.Content("value", Origin.FETCHER, isStale = false), stream.awaitItem())
            assertEquals(1, calls.get())

            stream.cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `a new fetch starts after the previous one completes`() = runTest {
        val calls = AtomicInteger()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { calls.incrementAndGet() }
        }

        assertEquals(1, store.fresh("k"))
        assertEquals(2, store.fresh("k"))
        assertEquals(2, calls.get())
    }
}
