package io.github.quasarapps.aquifer

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * The engine collapses its already-batched persistence paths onto the bulk [SourceOfTruth] SPI:
 * [Aquifer.putAll] calls [SourceOfTruth.writeAll] once and [Aquifer.invalidateWhere] calls
 * [SourceOfTruth.deleteMany] once, so a store that overrides them does a single batched
 * round-trip instead of N. A store that overrides neither still works through the per-key default.
 */
class BulkSourceOfTruthTest {

    /** Records whether the bulk or per-key methods were taken, on top of an in-memory map. */
    private class RecordingSourceOfTruth<K : Any, V : Any> : SourceOfTruth<K, V> {
        val storage = linkedMapOf<K, PersistedEntry<V>>()
        var writeAllCalls = 0
        var deleteManyCalls = 0
        var singleWrites = 0
        var singleDeletes = 0

        override suspend fun read(key: K): PersistedEntry<V>? = storage[key]

        override suspend fun write(key: K, entry: PersistedEntry<V>) {
            singleWrites++
            storage[key] = entry
        }

        override suspend fun delete(key: K) {
            singleDeletes++
            storage.remove(key)
        }

        override suspend fun deleteAll() = storage.clear()

        override suspend fun writeAll(entries: Map<K, PersistedEntry<V>>) {
            writeAllCalls++
            storage.putAll(entries)
        }

        override suspend fun deleteMany(keys: Collection<K>) {
            deleteManyCalls++
            keys.forEach { storage.remove(it) }
        }
    }

    @Test
    fun `putAll routes through writeAll in one batched call`() = runTest {
        val disk = RecordingSourceOfTruth<String, Int>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { 0 }
        }

        store.putAll(mapOf("a" to 1, "b" to 2, "c" to 3))

        assertEquals(1, disk.writeAllCalls, "one batched write, not three")
        assertEquals(0, disk.singleWrites)
        assertEquals(3, disk.storage.size)
    }

    @Test
    fun `invalidateWhere routes through deleteMany in one batched call`() = runTest {
        val disk = RecordingSourceOfTruth<String, Int>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { 0 }
        }
        store.putAll(mapOf("a" to 1, "b" to 2, "keep" to 3))

        store.invalidateWhere { it != "keep" }

        assertEquals(1, disk.deleteManyCalls, "one batched delete, not two")
        assertEquals(0, disk.singleDeletes)
        assertEquals(3, disk.storage["keep"]?.value)
        assertNull(disk.storage["a"])
        assertNull(disk.storage["b"])
    }

    @Test
    fun `a store that overrides neither bulk method still batches via the per-key default`() = runTest {
        // InMemorySourceOfTruth implements only read/write/delete/deleteAll: the default writeAll /
        // deleteMany loop those, so existing custom stores keep working unchanged.
        val disk = InMemorySourceOfTruth<String, Int>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { 0 }
        }

        store.putAll(mapOf("a" to 1, "b" to 2))
        assertEquals(1, disk.storage["a"]?.value)
        assertEquals(2, disk.storage["b"]?.value)

        store.invalidateWhere { it == "a" }
        assertNull(disk.storage["a"])
        assertEquals(2, disk.storage["b"]?.value)
    }
}
