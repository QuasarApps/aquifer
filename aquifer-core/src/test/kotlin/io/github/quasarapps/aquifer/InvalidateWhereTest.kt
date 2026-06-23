package io.github.quasarapps.aquifer

import app.cash.turbine.test
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds

/**
 * `invalidateWhere`: predicate/bulk invalidation — the middle ground between the surgical
 * `invalidate(key)` and the nuclear `invalidateAll()`, fencing each matched key like `invalidate`.
 */
class InvalidateWhereTest {

    @Test
    fun `invalidateWhere drops only the matching keys and keeps the rest cached`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
        }
        store.putAll(mapOf("tenant:a" to 1, "tenant:b" to 2, "other:c" to 3))

        store.invalidateWhere { it.startsWith("tenant:") }

        assertFailsWith<CacheMissException> { store.get("tenant:a", Freshness.CacheOnly) }
        assertFailsWith<CacheMissException> { store.get("tenant:b", Freshness.CacheOnly) }
        assertEquals(3, store.get("other:c", Freshness.CacheOnly)) // untouched, still served
    }

    @Test
    fun `a cache-only stream of a matched key observes the deletion as empty`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
        }
        store.put("k", 7)

        store.stream("k", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(7, Origin.MEMORY, isStale = false), awaitItem())
            store.invalidateWhere { it == "k" }
            assertEquals(DataState.Empty, awaitItem())
        }
    }

    @Test
    fun `a non-matching key's stream is undisturbed`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
        }
        store.put("keep", 1)
        store.put("drop", 2)

        store.stream("keep", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(1, Origin.MEMORY, isStale = false), awaitItem())
            store.invalidateWhere { it == "drop" }
            settle()
            expectNoEvents() // only "drop" was touched
        }
    }

    @Test
    fun `invalidateWhere fences an in-flight fetch for a matched key`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                delay(100)
                "stale" // would land in the cache if not fenced
            }
        }

        val pending = async { store.fresh("k") }
        settle() // the fetch is registered in-flight, suspended in the fetcher

        store.invalidateWhere { it == "k" }

        assertEquals("stale", pending.await()) // the fetch still resolves for its caller...
        // ...but its commit was fenced: nothing landed in the cache.
        assertFailsWith<CacheMissException> { store.get("k", Freshness.CacheOnly) }
    }

    @Test
    fun `invalidateWhere deletes persisted entries for matching keys and keeps the rest`() = runTest {
        val disk = InMemorySourceOfTruth<String, Int>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { -1 }
        }
        store.putAll(mapOf("a" to 1, "b" to 2))

        store.invalidateWhere { it == "a" }

        assertNull(disk.storage["a"])
        assertEquals(2, disk.storage["b"]?.value)
    }

    @Test
    fun `invalidateWhere reaches a persisted key already evicted from memory`() = runTest {
        val disk = InMemorySourceOfTruth<String, Int>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            memoryCache { maxEntries = 1 }
            persistence(disk)
            fetcher { -1 }
        }
        store.put("a", 1)
        store.put("b", 2) // evicts "a" from memory; it survives on disk and in keyEpochs

        store.invalidateWhere { it == "a" }

        assertNull(disk.storage["a"]) // reached via the write-epoch record, not memory
        assertEquals(2, disk.storage["b"]?.value)
    }

    @Test
    fun `invalidateWhere clears negative-cache suppression for matched keys`() = runTest {
        var attempts = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            negativeCache { timeToLive = 30.seconds }
            fetcher {
                attempts++
                error("boom")
            }
        }
        assertFailsWith<IllegalStateException> { store.get("k") } // records a failure, suppresses
        assertEquals(1, attempts)

        store.invalidateWhere { it == "k" } // "the data changed, forget the failure too"

        assertFailsWith<IllegalStateException> { store.get("k") } // not suppressed: fetches again
        assertEquals(2, attempts)
    }

    @Test
    fun `invalidateWhere whose persistence delete fails drops nothing from memory and emits nothing`() = runTest {
        // Delete-all-first: this store throws when deleting "b". The visible state (memory +
        // Invalidated events) must stay untouched — no key dropped, no broadcast — even though
        // a delete already ran for an earlier key (the documented non-transactional caveat).
        val disk = object : SourceOfTruth<String, Int> {
            val storage = mutableMapOf<String, PersistedEntry<Int>>()
            override suspend fun read(key: String): PersistedEntry<Int>? = storage[key]
            override suspend fun write(key: String, entry: PersistedEntry<Int>) {
                storage[key] = entry
            }

            override suspend fun delete(key: String) {
                if (key == "b") throw IOException("disk down")
                storage.remove(key)
            }

            override suspend fun deleteAll() = storage.clear()
        }
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { -1 }
        }
        store.putAll(mapOf("a" to 1, "b" to 2))

        store.stream("a", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(1, Origin.MEMORY, isStale = false), awaitItem())
            assertFailsWith<IOException> { store.invalidateWhere { it == "a" || it == "b" } }
            expectNoEvents() // the delete threw before any in-memory drop: "a" is not evicted
        }
        assertEquals(1, store.get("a", Freshness.CacheOnly)) // still resident
    }

    @Test
    fun `invalidateWhere with no matches is a no-op and emits nothing`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
        }
        store.put("k", 1)

        store.stream("k", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(1, Origin.MEMORY, isStale = false), awaitItem())
            store.invalidateWhere { false }
            settle()
            expectNoEvents()
        }
    }

    @Test
    fun `invalidateWhere on a closed store throws`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
        }
        store.close()

        assertFailsWith<IllegalStateException> { store.invalidateWhere { true } }
    }
}
