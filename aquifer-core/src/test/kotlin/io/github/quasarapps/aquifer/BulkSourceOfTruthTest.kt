package io.github.quasarapps.aquifer

import app.cash.turbine.test
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

/**
 * The engine collapses its already-batched persistence paths onto the bulk [SourceOfTruth] SPI:
 * [Aquifer.putAll] calls [SourceOfTruth.writeAll], [Aquifer.invalidateWhere] calls
 * [SourceOfTruth.deleteMany], and the multi-key read paths
 * ([Aquifer.getAll]/[Aquifer.streamMany]/[Aquifer.prefetchAll]) call [SourceOfTruth.readAll] — each
 * once, so a store that overrides them does a single batched round-trip instead of N. A store that
 * overrides none still works through the per-key default.
 */
class BulkSourceOfTruthTest {

    /** Records whether the bulk or per-key methods were taken, on top of an in-memory map. */
    private class RecordingSourceOfTruth<K : Any, V : Any> : SourceOfTruth<K, V> {
        val storage = linkedMapOf<K, PersistedEntry<V>>()
        var writeAllCalls = 0
        var deleteManyCalls = 0
        var readAllCalls = 0
        var lastReadAllRequest: List<K> = emptyList()
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
            lastReadAllRequest = keys.toList()
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

    /**
     * A store whose [SourceOfTruth.readAll] snapshots the requested values, then suspends on
     * [release] before returning them — so a test can land a mutation *during* a batched read and
     * verify loadAll's fence against the pre-mutation snapshot it then sees.
     */
    private fun gatedReadAllDisk(
        release: CompletableDeferred<Unit>,
        seed: Map<String, PersistedEntry<Int>>,
    ): SourceOfTruth<String, Int> = object : SourceOfTruth<String, Int> {
        val storage = LinkedHashMap(seed)
        override suspend fun read(key: String) = storage[key]
        override suspend fun write(key: String, entry: PersistedEntry<Int>) {
            storage[key] = entry
        }

        override suspend fun delete(key: String) {
            storage.remove(key)
        }

        override suspend fun deleteAll() = storage.clear()

        override suspend fun readAll(keys: Collection<String>): Map<String, PersistedEntry<Int>> {
            val snapshot = keys.mapNotNull { key -> storage[key]?.let { key to it } }.toMap()
            release.await()
            return snapshot // the *pre-mutation* values, the exact race loadAll fences against
        }
    }

    @Test
    fun `a batched read fenced by a concurrent invalidate does not resurrect the deleted entry`() = runTest {
        val release = CompletableDeferred<Unit>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(gatedReadAllDisk(release, mapOf("k" to PersistedEntry(1, writtenAtMillis = 0))))
            fetcher { error("CacheOnly never fetches") }
            freshness { timeToLive = Duration.INFINITE }
        }

        val reading = async { store.getAll(setOf("k"), Freshness.CacheOnly) }
        settle() // getAll is suspended inside readAll, having already snapshotted "k"
        store.invalidate("k") // bumps the key epoch and deletes "k" from disk
        release.complete(Unit)

        assertEquals(emptyMap(), reading.await()) // fenced: the stale disk read is dropped, not served
        assertFalse("k" in store.snapshot(), "the deleted entry was not hydrated back into memory")
    }

    @Test
    fun `a batched read fenced by a concurrent invalidateAll does not resurrect the deleted entry`() = runTest {
        // The globalEpoch half of the fence (invalidateAll/logout), distinct from invalidate's
        // per-key epoch bump above — both halves must hold, exactly as the single-key suite checks.
        val release = CompletableDeferred<Unit>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(gatedReadAllDisk(release, mapOf("k" to PersistedEntry(1, writtenAtMillis = 0))))
            fetcher { error("CacheOnly never fetches") }
            freshness { timeToLive = Duration.INFINITE }
        }

        val reading = async { store.getAll(setOf("k"), Freshness.CacheOnly) }
        settle() // suspended inside readAll, having snapshotted "k"
        store.invalidateAll() // bumps globalEpoch and clears every key epoch + the disk
        release.complete(Unit)

        assertEquals(emptyMap(), reading.await())
        assertFalse("k" in store.snapshot())
    }

    @Test
    fun `a put racing a batched read wins over the stale disk snapshot`() = runTest {
        // loadAll's under-lock memory re-read: a put that lands during the batched read must not be
        // clobbered by the older disk snapshot the read then returns.
        val release = CompletableDeferred<Unit>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(gatedReadAllDisk(release, mapOf("k" to PersistedEntry(1, writtenAtMillis = 0))))
            fetcher { error("served from cache after the put") }
            freshness { timeToLive = Duration.INFINITE }
        }

        val reading = async { store.getAll(setOf("k"), Freshness.CacheFirst) }
        settle() // suspended inside readAll, having snapshotted the stale "k" = 1
        store.put("k", 2) // fresher value lands in memory while the disk read is in flight
        release.complete(Unit)

        assertEquals(mapOf("k" to 2), reading.await()) // the put wins, not the stale disk 1
        assertEquals(2, store.get("k", Freshness.CacheOnly)) // and memory holds the put value
    }

    // --- Cross-key fence independence: a mutation on one key of a batch must not disturb a sibling.
    // loadAll fences each key against its own captured epoch, so the un-mutated sibling is unaffected.

    @Test
    fun `in a two-key batch a put on one key leaves the sibling's disk hydration intact`() = runTest {
        val release = CompletableDeferred<Unit>()
        val seed = mapOf(
            "a" to PersistedEntry(1, writtenAtMillis = 0),
            "b" to PersistedEntry(2, writtenAtMillis = 0),
        )
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(gatedReadAllDisk(release, seed))
            fetcher { error("served from cache") }
            freshness { timeToLive = Duration.INFINITE }
        }

        val reading = async { store.getAll(setOf("a", "b"), Freshness.CacheFirst) }
        settle() // suspended inside readAll, having snapshotted a=1, b=2
        store.put("a", 99) // only "a"'s epoch moves; "b"'s is untouched
        release.complete(Unit)

        // "a" reflects the put (its under-lock memory re-read wins); "b" still hydrates from disk.
        assertEquals(mapOf("a" to 99, "b" to 2), reading.await())
        assertEquals(99, store.get("a", Freshness.CacheOnly))
        assertEquals(2, store.get("b", Freshness.CacheOnly))
    }

    @Test
    fun `in a two-key batch an invalidate on one key does not drop the sibling`() = runTest {
        val release = CompletableDeferred<Unit>()
        val seed = mapOf(
            "a" to PersistedEntry(1, writtenAtMillis = 0),
            "b" to PersistedEntry(2, writtenAtMillis = 0),
        )
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(gatedReadAllDisk(release, seed))
            fetcher { error("CacheOnly never fetches") }
            freshness { timeToLive = Duration.INFINITE }
        }

        val reading = async { store.getAll(setOf("a", "b"), Freshness.CacheOnly) }
        settle() // suspended inside readAll, having snapshotted a=1, b=2
        store.invalidate("a") // deletes "a" and bumps only "a"'s epoch
        release.complete(Unit)

        // "a" is fenced out (not resurrected); "b" is independent and still hydrates.
        assertEquals(mapOf("b" to 2), reading.await())
        assertFalse("a" in store.snapshot())
        assertEquals(2, store.get("b", Freshness.CacheOnly))
    }

    // --- Every multi-key read path batches through readAll; the always-fetch paths skip the read.

    @Test
    fun `prefetchAll batches its persistence reads through readAll in one call`() = runTest {
        val disk = RecordingSourceOfTruth<String, Int>()
        disk.storage["a"] = PersistedEntry(1, writtenAtMillis = 0)
        disk.storage["b"] = PersistedEntry(2, writtenAtMillis = 0)
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { error("entries are fresh; nothing to fetch") }
            freshness { timeToLive = Duration.INFINITE }
        }

        store.prefetchAll(setOf("a", "b"), Freshness.CacheFirst)
        settle()

        assertEquals(1, disk.readAllCalls, "the shared keysNeedingFetch read is one batched call")
        assertEquals(0, disk.singleReads)
    }

    @Test
    fun `streamMany batches its pre-fetch persistence reads through readAll in one call`() = runTest {
        val disk = RecordingSourceOfTruth<String, Int>()
        disk.storage["a"] = PersistedEntry(1, writtenAtMillis = 0)
        disk.storage["b"] = PersistedEntry(2, writtenAtMillis = 0)
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { error("entries are fresh; nothing to fetch") }
            freshness { timeToLive = Duration.INFINITE }
        }

        store.streamMany(setOf("a", "b")).test {
            var latest = awaitItem()
            while (latest.values.any { it !is DataState.Content }) latest = awaitItem()
            assertEquals(mapOf("a" to 1, "b" to 2), latest.mapValues { it.value.value })
            cancelAndIgnoreRemainingEvents()
        }

        assertEquals(1, disk.readAllCalls, "the pre-fetch trigger reads the batch once; streams then prime from memory")
        assertEquals(0, disk.singleReads)
    }

    @Test
    fun `getAll with NetworkOnly skips the persistence read entirely`() = runTest {
        val disk = RecordingSourceOfTruth<String, Int>()
        // Even a populated cache is bypassed: NetworkOnly is an explicit demand for the network.
        disk.storage["a"] = PersistedEntry(99, writtenAtMillis = 0)
        disk.storage["bb"] = PersistedEntry(99, writtenAtMillis = 0)
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { it.length }
            freshness { timeToLive = Duration.INFINITE }
        }

        val result = store.getAll(setOf("a", "bb"), Freshness.NetworkOnly)

        assertEquals(mapOf("a" to 1, "bb" to 2), result) // the fetched lengths, not the cached 99s
        assertEquals(0, disk.readAllCalls, "NetworkOnly never reads persistence")
        assertEquals(0, disk.singleReads)
    }

    @Test
    fun `a conditional batch gathers its cached validators without per-key reads`() = runTest {
        val clock = FakeClock()
        val disk = RecordingSourceOfTruth<String, Int>()
        disk.storage["a"] = PersistedEntry(1, writtenAtMillis = 0, validator = "etag-a")
        disk.storage["b"] = PersistedEntry(2, writtenAtMillis = 0, validator = "etag-b")
        val seen = mutableListOf<Map<String, String?>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            persistence(disk)
            freshness { timeToLive = 5.minutes }
            conditionalBatchFetcher { validators ->
                seen += validators
                validators.keys.associateWith<String, FetchResult<Int>> { FetchResult.NotModified }
            }
        }
        clock.advanceBy(10.minutes) // the seeded entries are now stale, so getAll revalidates

        val result = store.getAll(setOf("a", "b"))

        assertEquals(mapOf("a" to 1, "b" to 2), result) // 304: served from cache, re-aged
        assertEquals(
            mapOf("a" to "etag-a", "b" to "etag-b"),
            seen.single(),
            "the validator gather batches the read, never falling back to per-key read",
        )
        assertEquals(0, disk.singleReads)
    }

    // --- loadAll's epochs split: warm keys skip the read; an unrequested key returned is ignored.

    @Test
    fun `loadAll excludes warm-in-memory keys from the readAll argument`() = runTest {
        val disk = RecordingSourceOfTruth<String, Int>()
        disk.storage["disk"] = PersistedEntry(2, writtenAtMillis = 0)
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { it.length } // only the absent key reaches the network
            freshness { timeToLive = Duration.INFINITE }
        }
        store.put("warm", 1) // now warm in memory

        val result = store.getAll(setOf("warm", "disk", "absent"), Freshness.CacheFirst)

        assertEquals(1, disk.readAllCalls, "one batched read for the memory-miss keys")
        assertEquals(0, disk.singleReads)
        assertEquals(
            setOf("disk", "absent"),
            disk.lastReadAllRequest.toSet(),
            "the warm key is served from memory and never handed to readAll",
        )
        assertEquals(mapOf("warm" to 1, "disk" to 2, "absent" to 6), result)
    }

    @Test
    fun `loadAll ignores an entry the store returns for a key it never requested`() = runTest {
        // A misbehaving override that returns an extra, unrequested entry. loadAll must neither
        // serve it nor hydrate it into memory (the epochs[key] ?: continue guard).
        val rogue = object : SourceOfTruth<String, Int> {
            val storage = linkedMapOf<String, PersistedEntry<Int>>()
            override suspend fun read(key: String): PersistedEntry<Int>? = storage[key]
            override suspend fun write(key: String, entry: PersistedEntry<Int>) {
                storage[key] = entry
            }

            override suspend fun delete(key: String) {
                storage.remove(key)
            }

            override suspend fun deleteAll() = storage.clear()

            override suspend fun readAll(keys: Collection<String>): Map<String, PersistedEntry<Int>> =
                mapOf(
                    "asked" to PersistedEntry(1, writtenAtMillis = 0),
                    "unrequested" to PersistedEntry(999, writtenAtMillis = 0),
                )
        }
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(rogue)
            fetcher { error("CacheOnly never fetches") }
            freshness { timeToLive = Duration.INFINITE }
        }

        val result = store.getAll(setOf("asked"), Freshness.CacheOnly)

        assertEquals(mapOf("asked" to 1), result, "only the requested key is served")
        assertFalse("unrequested" in store.snapshot(), "the unrequested entry is not hydrated into memory")
    }
}
