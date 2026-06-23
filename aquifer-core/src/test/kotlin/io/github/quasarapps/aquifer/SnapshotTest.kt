package io.github.quasarapps.aquifer

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** `snapshot()`: a non-suspending peek at the keys resident in memory — never fetches, never I/O. */
class SnapshotTest {

    @Test
    fun `a fresh store has an empty snapshot`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
        }
        assertEquals(emptySet(), store.snapshot())
    }

    @Test
    fun `snapshot reflects the resident keys and their count`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
        }
        store.putAll(mapOf("a" to 1, "b" to 2))
        store.put("c", 3)

        assertEquals(setOf("a", "b", "c"), store.snapshot())
        assertEquals(3, store.snapshot().size)
    }

    @Test
    fun `snapshot drops invalidated keys`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
        }
        store.putAll(mapOf("a" to 1, "b" to 2))

        store.invalidate("a")
        assertEquals(setOf("b"), store.snapshot())

        store.invalidateAll()
        assertEquals(emptySet(), store.snapshot())
    }

    @Test
    fun `snapshot reflects LRU eviction`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            memoryCache { maxEntries = 2 }
            fetcher { -1 }
        }
        store.put("a", 1)
        store.put("b", 2)
        store.put("c", 3) // evicts "a", the least recently used

        assertEquals(setOf("b", "c"), store.snapshot())
    }

    @Test
    fun `snapshot lists only memory-resident keys, not persisted-only ones`() = runTest {
        val disk = InMemorySourceOfTruth<String, Int>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            memoryCache { maxEntries = 1 }
            persistence(disk)
            fetcher { -1 }
        }
        store.put("a", 1)
        store.put("b", 2) // evicts "a" from memory; it survives on disk

        assertEquals(setOf("b"), store.snapshot()) // "a" is persisted but not resident
        assertEquals(1, disk.storage["a"]?.value) // ...still on disk, just not listed
    }

    @Test
    fun `snapshot never fetches`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher {
                calls++
                -1
            }
        }
        store.put("a", 1)

        repeat(3) { store.snapshot() }

        assertTrue(store.snapshot().contains("a"))
        assertEquals(0, calls, "snapshot is a pure memory peek; it never triggers a fetch")
    }

    @Test
    fun `snapshot returns a stable copy, not a live view`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
        }
        store.put("a", 1)

        val snap = store.snapshot()
        store.put("b", 2) // mutating the cache afterwards must not change the returned set

        assertEquals(setOf("a"), snap)
        assertEquals(setOf("a", "b"), store.snapshot())
    }

    @Test
    fun `snapshot is callable on a closed store and does not throw`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
        }
        store.put("a", 1)
        store.close()

        // Unlike get/stream/invalidate, the debug peek stays callable after close.
        assertFalse(store.snapshot().isEmpty())
        assertEquals(setOf("a"), store.snapshot())
    }
}
