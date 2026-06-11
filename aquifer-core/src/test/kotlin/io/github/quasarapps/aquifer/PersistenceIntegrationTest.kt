package io.github.quasarapps.aquifer

import app.cash.turbine.test
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.minutes

/** In-memory [SourceOfTruth] that records operations for assertions. */
private class FakeSourceOfTruth<K : Any, V : Any> : SourceOfTruth<K, V> {

    val storage = mutableMapOf<K, PersistedEntry<V>>()
    var reads = 0
    var writes = 0
    var failNextWrite: Throwable? = null

    override suspend fun read(key: K): PersistedEntry<V>? {
        reads++
        return storage[key]
    }

    override suspend fun write(key: K, entry: PersistedEntry<V>) {
        failNextWrite?.let { failure ->
            failNextWrite = null
            throw failure
        }
        writes++
        storage[key] = entry
    }

    override suspend fun delete(key: K) {
        storage.remove(key)
    }

    override suspend fun deleteAll() {
        storage.clear()
    }
}

class PersistenceIntegrationTest {

    @Test
    fun `a fresh persisted entry is served without fetching`() = runTest {
        val disk = FakeSourceOfTruth<String, String>()
        disk.storage["k"] = PersistedEntry("from-disk", writtenAtMillis = 0)
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            clock(FakeClock())
            fetcher {
                calls++
                "fetched"
            }
            freshness { timeToLive = 5.minutes }
            persistence(disk)
        }

        assertEquals("from-disk", store.get("k"))
        assertEquals(0, calls)
    }

    @Test
    fun `cold start stream emits persisted content with persistence origin`() = runTest {
        val disk = FakeSourceOfTruth<String, String>()
        disk.storage["k"] = PersistedEntry("from-disk", writtenAtMillis = 0)
        val store = aquifer<String, String> {
            scope(backgroundScope)
            clock(FakeClock())
            fetcher { "fetched" }
            freshness { timeToLive = 5.minutes }
            persistence(disk)
        }

        store.stream("k").test {
            assertEquals(DataState.Content("from-disk", Origin.PERSISTENCE, isStale = false), awaitItem())
            expectNoEvents()
        }
    }

    @Test
    fun `a stale persisted entry is served then revalidated and rewritten`() = runTest {
        val clock = FakeClock()
        val disk = FakeSourceOfTruth<String, String>()
        disk.storage["k"] = PersistedEntry("stale-disk", writtenAtMillis = 0)
        val store = aquifer<String, String> {
            scope(backgroundScope)
            clock(clock)
            fetcher { "fetched" }
            freshness { timeToLive = 1.minutes }
            persistence(disk)
        }
        clock.advanceBy(10.minutes)

        store.stream("k").test {
            assertEquals(DataState.Content("stale-disk", Origin.PERSISTENCE, isStale = true), awaitItem())
            assertEquals(DataState.Loading("stale-disk"), awaitItem())
            assertEquals(DataState.Content("fetched", Origin.FETCHER, isStale = false), awaitItem())
        }

        settle()
        assertEquals("fetched", disk.storage["k"]?.value)
        assertEquals(clock.nowMillis(), disk.storage["k"]?.writtenAtMillis)
    }

    @Test
    fun `persisted timestamps drive staleness across process restarts`() = runTest {
        val clock = FakeClock()
        val disk = FakeSourceOfTruth<String, String>()
        // Written 10 minutes "before this process started".
        clock.advanceBy(10.minutes)
        disk.storage["k"] = PersistedEntry("old", writtenAtMillis = 0)
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            clock(clock)
            fetcher {
                calls++
                "fetched"
            }
            freshness { timeToLive = 1.minutes }
            persistence(disk)
        }

        // CacheFirst sees a stale entry and refetches rather than serving 10-minute-old data.
        assertEquals("fetched", store.get("k"))
        assertEquals(1, calls)
    }

    @Test
    fun `memory eviction falls back to persistence instead of the network`() = runTest {
        val disk = FakeSourceOfTruth<String, String>()
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { key ->
                calls++
                "fetched-$key"
            }
            memoryCache { maxEntries = 1 }
            persistence(disk)
        }

        store.get("a")
        store.get("b") // Evicts "a" from memory; persistence still holds it.
        assertEquals(2, calls)

        assertEquals("fetched-a", store.get("a"))
        assertEquals(2, calls)
    }

    @Test
    fun `successful fetches write through to persistence`() = runTest {
        val disk = FakeSourceOfTruth<String, String>()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
            persistence(disk)
        }

        store.get("k")
        settle()

        assertEquals("fetched", disk.storage["k"]?.value)
    }

    @Test
    fun `put writes through to persistence`() = runTest {
        val clock = FakeClock(initialMillis = 1234)
        val disk = FakeSourceOfTruth<String, String>()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            clock(clock)
            fetcher { "fetched" }
            persistence(disk)
        }

        store.put("k", "local-edit")

        assertEquals(PersistedEntry("local-edit", 1234), disk.storage["k"])
    }

    @Test
    fun `put propagates persistence failures and leaves no partial state`() = runTest {
        val disk = FakeSourceOfTruth<String, String>()
        val boom = RuntimeException("disk full")
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
            persistence(disk)
        }

        disk.failNextWrite = boom
        val failure = assertFailsWith<RuntimeException> { store.put("k", "lost") }
        assertEquals("disk full", failure.message)

        // Neither memory nor persistence saw the write.
        assertFailsWith<CacheMissException> { store.get("k", Freshness.CacheOnly) }
        assertEquals(null, disk.storage["k"])
    }

    @Test
    fun `a persistence write failure does not fail the fetch`() = runTest {
        val disk = FakeSourceOfTruth<String, String>()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
            persistence(disk)
        }

        disk.failNextWrite = RuntimeException("disk full")
        assertEquals("fetched", store.get("k"))
        settle()

        // The value is served and cached in memory even though the disk write failed.
        assertEquals("fetched", store.get("k", Freshness.CacheOnly))
        assertEquals(null, disk.storage["k"])
    }

    @Test
    fun `invalidate deletes the persisted entry`() = runTest {
        val disk = FakeSourceOfTruth<String, String>()
        disk.storage["k"] = PersistedEntry("persisted", writtenAtMillis = 0)
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
            persistence(disk)
        }

        store.invalidate("k")

        assertEquals(null, disk.storage["k"])
    }

    @Test
    fun `invalidate all clears persistence`() = runTest {
        val disk = FakeSourceOfTruth<String, String>()
        disk.storage["a"] = PersistedEntry("a", writtenAtMillis = 0)
        disk.storage["b"] = PersistedEntry("b", writtenAtMillis = 0)
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
            persistence(disk)
        }

        store.invalidateAll()

        assertEquals(emptyMap(), disk.storage)
    }

    @Test
    fun `network only get bypasses persistence entirely`() = runTest {
        val disk = FakeSourceOfTruth<String, String>()
        disk.storage["k"] = PersistedEntry("from-disk", writtenAtMillis = 0)
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
            persistence(disk)
        }

        assertEquals("fetched", store.fresh("k"))

        assertEquals(0, disk.reads)
    }

    @Test
    fun `network only stream bypasses persistence entirely`() = runTest {
        val disk = FakeSourceOfTruth<String, String>()
        disk.storage["k"] = PersistedEntry("from-disk", writtenAtMillis = 0)
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
            persistence(disk)
        }

        store.stream("k", Freshness.NetworkOnly).test {
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Content("fetched", Origin.FETCHER, isStale = false), awaitItem())
        }

        assertEquals(0, disk.reads)
    }

    @Test
    fun `memory hits do not touch persistence`() = runTest {
        val disk = FakeSourceOfTruth<String, String>()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
            persistence(disk)
        }

        store.get("k")
        settle()
        val readsAfterFirstGet = disk.reads

        repeat(5) { store.get("k") }
        assertEquals(readsAfterFirstGet, disk.reads)
    }
}
