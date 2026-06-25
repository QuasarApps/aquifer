package io.github.quasarapps.aquifer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.time.Duration

/**
 * The engine collapses its already-batched persistence paths onto the bulk [SourceOfTruth] SPI:
 * [Aquifer.putAll] calls [SourceOfTruth.writeAll], [Aquifer.invalidateWhere] calls
 * [SourceOfTruth.deleteMany], and the multi-key read paths ([Aquifer.getAll]/[Aquifer.streamMany])
 * call [SourceOfTruth.readAll] — each once, so a store that overrides them does a single batched
 * round-trip instead of N. A store that overrides none still works through the per-key default.
 */
class BulkSourceOfTruthTest {

    /** Records whether the bulk or per-key methods were taken, on top of an in-memory map. */
    private class RecordingSourceOfTruth<K : Any, V : Any> : SourceOfTruth<K, V> {
        val storage = linkedMapOf<K, PersistedEntry<V>>()
        var writeAllCalls = 0
        var deleteManyCalls = 0
        var readAllCalls = 0
        var singleWrites = 0
        var singleDeletes = 0
        var singleReads = 0

        override suspend fun read(key: K): PersistedEntry<V>? {
            singleReads++
            return storage[key]
        }

        override suspend fun write(key: K, entry: PersistedEntry<V>) {
            singleWrites++
            storage[key] = entry
        }

        override suspend fun delete(key: K) {
            singleDeletes++
            storage.remove(key)
        }

        override suspend fun deleteAll() = storage.clear()

        override suspend fun readAll(keys: Collection<K>): Map<K, PersistedEntry<V>> {
            readAllCalls++
            val result = LinkedHashMap<K, PersistedEntry<V>>()
            for (key in keys) storage[key]?.let { result[key] = it }
            return result
        }

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

    @Test
    fun `getAll routes its persistence reads through readAll in one batched call`() = runTest {
        val disk = RecordingSourceOfTruth<String, Int>()
        // Seed the store directly so nothing is warm in memory: getAll must read from persistence.
        disk.storage["a"] = PersistedEntry(1, writtenAtMillis = 0)
        disk.storage["b"] = PersistedEntry(2, writtenAtMillis = 0)
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { error("should be served from disk") }
            freshness { timeToLive = Duration.INFINITE }
        }

        val result = store.getAll(setOf("a", "b"), Freshness.CacheFirst)

        assertEquals(mapOf("a" to 1, "b" to 2), result)
        assertEquals(1, disk.readAllCalls, "one batched read, not two")
        assertEquals(0, disk.singleReads)
    }

    @Test
    fun `getAll through a store that does not override readAll still serves via the per-key default`() = runTest {
        val disk = InMemorySourceOfTruth<String, Int>()
        disk.storage["a"] = PersistedEntry(1, writtenAtMillis = 0)
        disk.storage["b"] = PersistedEntry(2, writtenAtMillis = 0)
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { error("should be served from disk") }
            freshness { timeToLive = Duration.INFINITE }
        }

        assertEquals(mapOf("a" to 1, "b" to 2), store.getAll(setOf("a", "b"), Freshness.CacheFirst))
    }

    @Test
    fun `a batched read fenced by a concurrent invalidate does not resurrect the deleted entry`() = runTest {
        val release = CompletableDeferred<Unit>()
        val disk = object : SourceOfTruth<String, Int> {
            val storage = linkedMapOf("k" to PersistedEntry(1, writtenAtMillis = 0))
            override suspend fun read(key: String) = storage[key]
            override suspend fun write(key: String, entry: PersistedEntry<Int>) {
                storage[key] = entry
            }

            override suspend fun delete(key: String) {
                storage.remove(key)
            }

            override suspend fun deleteAll() = storage.clear()

            override suspend fun readAll(keys: Collection<String>): Map<String, PersistedEntry<Int>> {
                // Snapshot before the gate, so the value returned is the *pre-invalidate* one — the
                // exact race load()/loadAll fence against: a read in flight across an invalidate.
                val snapshot = keys.mapNotNull { key -> storage[key]?.let { key to it } }.toMap()
                release.await()
                return snapshot
            }
        }
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { error("CacheOnly never fetches") }
            freshness { timeToLive = Duration.INFINITE }
        }

        val reading = async { store.getAll(setOf("k"), Freshness.CacheOnly) }
        settle() // getAll is suspended inside readAll, having already snapshotted "k"
        store.invalidate("k") // bumps the epoch and deletes "k" from disk
        release.complete(Unit)

        assertEquals(emptyMap(), reading.await()) // fenced: the stale disk read is dropped, not served
        assertFalse("k" in store.snapshot(), "the deleted entry was not hydrated back into memory")
    }
}
