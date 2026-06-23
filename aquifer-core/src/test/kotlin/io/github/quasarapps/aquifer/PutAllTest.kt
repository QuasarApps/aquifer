package io.github.quasarapps.aquifer

import app.cash.turbine.test
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** `putAll`: bulk local write — the write-side mirror of getAll, one fenced commit per batch. */
class PutAllTest {

    @Test
    fun `putAll writes every entry as fresh, served from cache without fetching`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher {
                calls++
                -1
            }
        }

        store.putAll(mapOf("a" to 1, "bb" to 2))

        assertEquals(1, store.get("a"))
        assertEquals(2, store.get("bb"))
        assertEquals(0, calls, "putAll seeds the cache; reads never fetch")
    }

    @Test
    fun `putAll notifies an active stream of the written key`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 0 }
        }
        store.put("a", 1)

        store.stream("a", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(1, Origin.MEMORY, isStale = false), awaitItem())
            store.putAll(mapOf("a" to 100, "b" to 200))
            assertEquals(DataState.Content(100, Origin.LOCAL, isStale = false), awaitItem())
        }
    }

    @Test
    fun `putAll fences an in-flight fetch so its result cannot overwrite the written value`() = runTest {
        val gate = CompletableDeferred<Unit>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher {
                gate.await()
                -1 // would land in the cache if not fenced
            }
        }

        val read = async { store.get("a", Freshness.NetworkOnly) }
        settle() // the fetch is registered in-flight, suspended at the gate
        store.putAll(mapOf("a" to 99)) // fences the in-flight fetch
        gate.complete(Unit)
        assertEquals(-1, read.await()) // the fetch still resolves to its own value for its caller...

        // ...but its commit was fenced: the cache keeps the putAll value, not the fetched -1.
        assertEquals(99, store.get("a", Freshness.CacheOnly))
    }

    @Test
    fun `putAll writes through to persistence`() = runTest {
        val disk = InMemorySourceOfTruth<String, Int>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { 0 }
        }

        store.putAll(mapOf("a" to 1, "bb" to 2))

        assertEquals(1, disk.read("a")?.value)
        assertEquals(2, disk.read("bb")?.value)
    }

    @Test
    fun `putAll on an empty map is a no-op and emits nothing`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 0 }
        }
        store.put("a", 1)

        store.stream("a", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(1, Origin.MEMORY, isStale = false), awaitItem())
            store.putAll(emptyMap())
            expectNoEvents()
        }
    }

    @Test
    fun `putAll on a closed store throws`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 0 }
        }
        store.close()

        assertFailsWith<IllegalStateException> { store.putAll(mapOf("a" to 1)) }
    }
}
