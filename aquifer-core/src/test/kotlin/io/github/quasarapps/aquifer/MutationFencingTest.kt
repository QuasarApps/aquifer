package io.github.quasarapps.aquifer

import app.cash.turbine.test
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull

/**
 * Mutations must fence off fetches that were already in flight when the mutation happened:
 * their responses describe a state of the world the caller just declared wrong (logout,
 * local edit), and committing them would resurrect deleted data or clobber newer writes.
 */
class MutationFencingTest {

    @Test
    fun `invalidateAll during an in-flight fetch does not resurrect the data`() = runTest {
        val disk = InMemorySourceOfTruth<String, String>()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                delay(100)
                "old-user-data"
            }
            persistence(disk)
        }

        val pending = async { store.fresh("k") }
        settle() // the fetch is in flight

        store.invalidateAll() // logout-style reset

        // The caller that asked before the reset still gets its answer…
        assertEquals("old-user-data", pending.await())
        // …but nothing lands back in memory or on disk.
        assertFailsWith<CacheMissException> { store.get("k", Freshness.CacheOnly) }
        assertNull(disk.storage["k"])
    }

    @Test
    fun `invalidate during an in-flight fetch does not resurrect the data`() = runTest {
        val disk = InMemorySourceOfTruth<String, String>()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                delay(100)
                "stale"
            }
            persistence(disk)
        }

        val pending = async { store.fresh("k") }
        settle()

        store.invalidate("k")

        assertEquals("stale", pending.await())
        assertFailsWith<CacheMissException> { store.get("k", Freshness.CacheOnly) }
        assertNull(disk.storage["k"])
    }

    @Test
    fun `a local put wins over a fetch that was already in flight`() = runTest {
        val disk = InMemorySourceOfTruth<String, String>()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                delay(100)
                "stale-server-copy"
            }
            persistence(disk)
        }

        val pending = async { store.fresh("k") }
        settle() // the response now in flight predates the edit below

        store.put("k", "local-edit")

        assertEquals("stale-server-copy", pending.await())
        assertEquals("local-edit", store.get("k", Freshness.CacheOnly))
        assertEquals("local-edit", disk.storage["k"]?.value)
    }

    @Test
    fun `streams never observe a fenced-off fetch result`() = runTest {
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                val attempt = ++calls
                delay(100)
                "v$attempt"
            }
        }

        store.stream("k").test {
            assertEquals(DataState.Loading(null), awaitItem()) // fetch 1 in flight

            store.invalidate("k")

            // The stream's automatic refetch is a genuinely new request (fetch 2), and the
            // fenced-off fetch 1 result is never emitted.
            assertEquals(DataState.Content("v2", Origin.FETCHER, isStale = false), awaitItem())
            expectNoEvents()
        }
        assertEquals(2, calls)
    }

    /** Capture-then-suspend read: resolves its result, then waits at the gate before returning. */
    private class GatedDisk : SourceOfTruth<String, String> {
        val storage = mutableMapOf("k" to PersistedEntry("old-user-data", 0L))
        val gate = kotlinx.coroutines.CompletableDeferred<Unit>()

        override suspend fun read(key: String): PersistedEntry<String>? {
            val result = storage[key]
            gate.await()
            return result
        }

        override suspend fun write(key: String, entry: PersistedEntry<String>) {
            storage[key] = entry
        }

        override suspend fun delete(key: String) {
            storage.remove(key)
        }

        override suspend fun deleteAll() = storage.clear()
    }

    @Test
    fun `a persistence read suspended across invalidateAll cannot resurrect the data`() = runTest {
        val disk = GatedDisk()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
            persistence(disk)
        }

        // Hydration begins and suspends inside the storage read…
        val reader = async { runCatching { store.get("k", Freshness.CacheOnly) } }
        settle()

        store.invalidateAll() // …logout happens…
        disk.gate.complete(Unit) // …and the read resumes with the pre-logout entry.
        settle()

        // The resumed hydration must not commit the deleted entry back into memory.
        assertFailsWith<CacheMissException> { store.get("k", Freshness.CacheOnly) }
        // The suspended reader itself sees a miss too.
        assertIs<CacheMissException>(reader.await().exceptionOrNull())
    }

    @Test
    fun `a persistence read suspended across invalidate cannot resurrect the data`() = runTest {
        val disk = GatedDisk()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
            persistence(disk)
        }

        val reader = async { runCatching { store.get("k", Freshness.CacheOnly) } }
        settle()

        store.invalidate("k")
        disk.gate.complete(Unit)
        settle()

        assertFailsWith<CacheMissException> { store.get("k", Freshness.CacheOnly) }
        assertIs<CacheMissException>(reader.await().exceptionOrNull())
    }

    @Test
    fun `a fetch commit during a suspended persistence read is not clobbered by the older snapshot`() = runTest {
        val disk = GatedDisk() // holds "old-user-data" for "k"
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "network-new" }
            persistence(disk)
        }

        // Hydration captures the old disk entry, then suspends…
        val reader = async { runCatching { store.get("k", Freshness.CacheOnly) } }
        settle()

        // …a fetch lands the fresher value while the read is still suspended…
        assertEquals("network-new", store.fresh("k"))

        disk.gate.complete(Unit) // …and the resumed hydration must not overwrite it.
        settle()

        assertEquals("network-new", store.get("k", Freshness.CacheOnly))
        // The suspended reader also receives the fresher value, not its stale snapshot.
        assertEquals("network-new", reader.await().getOrNull())
    }

    @Test
    fun `a fetch started after the mutation commits normally`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 1 }
        }

        store.put("k", 100)
        store.invalidate("k")

        assertEquals(1, store.fresh("k"))
        assertEquals(1, store.get("k", Freshness.CacheOnly))
    }
}
