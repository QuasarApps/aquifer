package io.github.quasarapps.aquifer

import app.cash.turbine.test
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.time.Duration

/**
 * The opt-in key-enumeration capability: a [SourceOfTruth] that overrides [SourceOfTruth.keys]
 * lets [Aquifer.invalidateWhere] reach persisted keys this process never tracked, making the
 * predicate disk-wide. A store that keeps the `null` default (e.g. the JSON file store) limits the
 * reach to in-process keys, exactly as before. Every matched key — including disk-only ones — is
 * fenced, so a read already in flight cannot resurrect a deleted entry.
 */
class KeyEnumerationTest {

    /** An enumerable in-memory store; [readGate], when set, suspends each [read] mid-call. */
    private class EnumerableSourceOfTruth<K : Any, V : Any>(
        private val readGate: CompletableDeferred<Unit>? = null,
    ) : SourceOfTruth<K, V> {
        val storage = linkedMapOf<K, PersistedEntry<V>>()
        var keysCalls = 0

        // Every delete this store receives, in order. invalidateWhere drives deletion through the
        // default deleteMany, which loops delete() once per key in its deduplicated `matched` set —
        // the same set that drives the per-key Invalidated broadcast 1:1 (RealAquifer.invalidateWhere).
        // So a key's occurrence count here is a non-stream proxy for how many Invalidated events it
        // produced, which distinctUntilChanged() would otherwise hide on a watching stream.
        //
        // This is a *structural* equivalence (both loops walk `matched`), not a pinned invariant: it
        // would miss a stray emit rooted outside `matched`. The only fully-direct guard would assert
        // on the raw Invalidated events, which needs the onInvalidated callback this suite defers.
        //
        // DO NOT override deleteMany on this double. The real stores batch it, but overriding it here
        // would stop routing through delete(), leaving deletedKeys silently empty while emits stay
        // correct — the guard would quietly stop measuring.
        val deletedKeys = mutableListOf<K>()

        override suspend fun read(key: K): PersistedEntry<V>? {
            val snapshot = storage[key]
            readGate?.await() // suspend after snapshotting, so a test can race a mutation in
            return snapshot
        }

        override suspend fun write(key: K, entry: PersistedEntry<V>) {
            storage[key] = entry
        }

        override suspend fun delete(key: K) {
            deletedKeys += key
            storage.remove(key)
        }

        override suspend fun deleteAll() = storage.clear()

        override suspend fun keys(): Set<K> {
            keysCalls++
            return LinkedHashSet(storage.keys)
        }
    }

    @Test
    fun `a store that does not override keys cannot enumerate`() = runTest {
        val disk = InMemorySourceOfTruth<String, Int>()
        disk.storage["a"] = PersistedEntry(1, writtenAtMillis = 0)

        assertNull(disk.keys(), "the default opts out of enumeration")
        assertNull(disk.keysWhere { true }, "keysWhere inherits the opt-out default")
    }

    @Test
    fun `keysWhere defaults to filtering the enumerated keys`() = runTest {
        val disk = EnumerableSourceOfTruth<String, Int>()
        disk.storage["tenant:a"] = PersistedEntry(1, writtenAtMillis = 0)
        disk.storage["tenant:b"] = PersistedEntry(2, writtenAtMillis = 0)
        disk.storage["other:c"] = PersistedEntry(3, writtenAtMillis = 0)

        assertEquals(setOf("tenant:a", "tenant:b", "other:c"), disk.keys())
        assertEquals(setOf("tenant:a", "tenant:b"), disk.keysWhere { it.startsWith("tenant:") })
    }

    @Test
    fun `invalidateWhere reaches a persisted key the engine never loaded when the store enumerates`() = runTest {
        val disk = EnumerableSourceOfTruth<String, Int>()
        // Seed straight into the store, so the engine keeps no in-process trace of "cold".
        disk.storage["cold"] = PersistedEntry(1, writtenAtMillis = 0)
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { -1 }
        }

        store.invalidateWhere { it == "cold" }

        assertNull(disk.storage["cold"], "enumeration made the predicate disk-wide")
        assertEquals(1, disk.keysCalls, "the engine enumerated the store once")
    }

    @Test
    fun `invalidateWhere cannot reach an untracked persisted key when the store does not enumerate`() = runTest {
        val disk = InMemorySourceOfTruth<String, Int>() // keeps the null default: opts out
        disk.storage["cold"] = PersistedEntry(1, writtenAtMillis = 0)
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { -1 }
        }

        store.invalidateWhere { it == "cold" }

        assertEquals(1, disk.storage["cold"]?.value, "no enumeration: an untracked key stays (use invalidateAll)")
    }

    @Test
    fun `disk-wide invalidateWhere drops both a resident and a cold persisted key in one pass`() = runTest {
        val disk = EnumerableSourceOfTruth<String, Int>()
        disk.storage["cold"] = PersistedEntry(1, writtenAtMillis = 0) // never loaded this run
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { -1 }
        }
        store.put("warm", 2) // resident in memory and on disk

        store.invalidateWhere { it == "cold" || it == "warm" }

        assertNull(disk.storage["cold"])
        assertNull(disk.storage["warm"])
        assertFailsWith<CacheMissException> { store.get("warm", Freshness.CacheOnly) }
        assertEquals(1, disk.keysCalls, "one enumeration covers the whole predicate")
    }

    @Test
    fun `a cold key fenced by disk-wide invalidateWhere is not resurrected by a racing read`() = runTest {
        val readGate = CompletableDeferred<Unit>()
        val disk = EnumerableSourceOfTruth<String, Int>(readGate = readGate)
        disk.storage["cold"] = PersistedEntry(1, writtenAtMillis = 0)
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { error("CacheOnly never fetches") }
        }

        // A read of "cold" snapshots the disk value, then suspends mid-read. getAll (not get) so a
        // fenced miss is an omitted key, not a thrown CacheMissException that would fail the async.
        val reading = async { store.getAll(setOf("cold"), Freshness.CacheOnly) }
        settle()
        // invalidateWhere enumerates, deletes "cold" from disk, and fences it — all while the read
        // is parked on its snapshot (the read happens outside commitGuard, so the commit proceeds).
        store.invalidateWhere { it == "cold" }
        readGate.complete(Unit)

        // The read resumes with the stale snapshot but is fenced: it never hydrates, so it misses.
        assertEquals(emptyMap(), reading.await())
        assertFalse("cold" in store.snapshot(), "the cold key was not resurrected into memory")
    }

    @Test
    fun `invalidateWhere via disk enumeration fires an Invalidated event to a watching stream`() = runTest {
        // Verify the Invalidated event actually reaches a CacheOnly stream when the key is matched.
        // Watching the stream hydrates "cold" into memory, so this drop is in fact driven by the
        // in-process path; keysCalls == 1 proves invalidateWhere still *invokes* disk enumeration,
        // not that enumeration caused this drop. The causal "enumeration reaches an untracked key"
        // proof lives in the sibling tests above, which never stream the key.
        val disk = EnumerableSourceOfTruth<String, Int>()
        disk.storage["cold"] = PersistedEntry(1, writtenAtMillis = 0)
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { error("CacheOnly never fetches") }
            freshness { timeToLive = Duration.INFINITE }
        }

        store.stream("cold", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(1, Origin.PERSISTENCE, isStale = false), awaitItem())
            store.invalidateWhere { it == "cold" }
            assertEquals(DataState.Empty, awaitItem())
        }
        assertEquals(1, disk.keysCalls, "invalidateWhere still invokes disk enumeration")
    }

    @Test
    fun `invalidateWhere emits exactly one Invalidated event when a key is both resident and enumerated`() = runTest {
        // A key present in both inProcess (memory/keyEpochs) and in the enumerable store's keysWhere
        // result must be deduplicated — the LinkedHashSet.apply { addAll(persisted) } path — so
        // exactly one fence, one delete and one Invalidated event happen, not two.
        //
        // A watching stream cannot prove exactly-once: distinctUntilChanged() collapses two
        // consecutive DataState.Empty emissions into one, so a double fence/emit would be invisible.
        // The non-stream guard is disk.deletedKeys: deleteMany drives one delete() per deduplicated
        // matched key, 1:1 with the Invalidated broadcast, so a single "warm" delete proves a single
        // Invalidated event even though the stream can only show one Empty either way.
        val disk = EnumerableSourceOfTruth<String, Int>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { error("CacheOnly never fetches") }
            freshness { timeToLive = Duration.INFINITE }
        }
        store.put("warm", 99) // resident in memory AND written through to disk

        store.stream("warm", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(99, Origin.MEMORY, isStale = false), awaitItem())
            store.invalidateWhere { it == "warm" }
            assertEquals(DataState.Empty, awaitItem())
            settle()
            expectNoEvents() // no spurious follow-up emission on the stream
        }
        assertNull(disk.storage["warm"])
        assertEquals(listOf("warm"), disk.deletedKeys, "the dedup'd key was deleted once, so one Invalidated event")
    }

    @Test
    fun `a cold key fenced by disk-wide invalidateWhere is not resurrected by a racing single-key get`() = runTest {
        // The single-key load()/get() fence is a separate implementation from loadAll()'s; this is
        // the sibling of the getAll case above, racing it against a disk-wide invalidateWhere.
        val readGate = CompletableDeferred<Unit>()
        val disk = EnumerableSourceOfTruth<String, Int>(readGate = readGate)
        disk.storage["cold"] = PersistedEntry(1, writtenAtMillis = 0)
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { error("CacheOnly never fetches") }
        }

        // runCatching so the fenced miss (a thrown CacheMissException) doesn't fail the async itself.
        val reading = async { runCatching { store.get("cold", Freshness.CacheOnly) } }
        settle()
        store.invalidateWhere { it == "cold" }
        readGate.complete(Unit)

        assertIs<CacheMissException>(reading.await().exceptionOrNull(), "the fenced single-key read misses")
        assertFalse("cold" in store.snapshot())
    }
}
