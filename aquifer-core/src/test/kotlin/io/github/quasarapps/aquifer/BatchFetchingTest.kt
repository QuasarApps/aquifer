package io.github.quasarapps.aquifer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes

/** Batched fetching: `batchFetcher` + `getAll` collapse N keys into one backend call. */
class BatchFetchingTest {

    @Test
    fun `getAll batches all missing keys into one backend call`() = runTest {
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys ->
                batches += keys
                keys.associateWith { it.length }
            }
        }

        assertEquals(
            mapOf("a" to 1, "bb" to 2, "ccc" to 3),
            store.getAll(setOf("a", "bb", "ccc")),
        )
        assertEquals(listOf(setOf("a", "bb", "ccc")), batches, "one call for all three keys")
    }

    @Test
    fun `getAll serves fresh cached keys and batches only the rest`() = runTest {
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys ->
                batches += keys
                keys.associateWith { it.length }
            }
        }
        store.put("a", 100) // fresh (default TTL is infinite)

        assertEquals(mapOf("a" to 100, "bb" to 2), store.getAll(setOf("a", "bb")))
        assertEquals(listOf(setOf("bb")), batches, "the fresh key is not refetched")
    }

    @Test
    fun `getAll omits keys the batch fetcher leaves out and reports them`() = runTest {
        val failures = mutableMapOf<String, Throwable>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys -> keys.filter { it != "miss" }.associateWith { it.length } }
            events(object : AquiferEvents<String> {
                override fun onFetchFailed(key: String, error: Throwable, attempts: Int) {
                    failures[key] = error
                }
            })
        }

        assertEquals(mapOf("a" to 1), store.getAll(setOf("a", "miss")))
        assertIs<BatchKeyMissingException>(failures["miss"])
        assertNull(failures["a"], "the present key succeeded")
    }

    @Test
    fun `a throwing batch fetcher fails every key without throwing to getAll`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { throw IOException("backend down") }
        }

        assertEquals(emptyMap(), store.getAll(setOf("a", "b")))
    }

    @Test
    fun `getAll joins an in-flight single fetch instead of re-requesting`() = runTest {
        val gate = CompletableDeferred<Unit>()
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys ->
                batches += keys
                gate.await()
                keys.associateWith { it.length }
            }
        }

        val single = async { store.get("a") } // batch of one, gated
        settle() // registers inFlight["a"], suspends in the batch call
        val all = async { store.getAll(setOf("a", "b")) } // "a" joins; only "b" is batched
        settle()
        gate.complete(Unit)

        assertEquals(1, single.await())
        assertEquals(mapOf("a" to 1, "b" to 1), all.await())
        assertEquals(listOf(setOf("a"), setOf("b")), batches, "\"a\" was not requested twice")
    }

    @Test
    fun `getAll with CacheOnly returns only the cached subset and never fetches`() = runTest {
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys ->
                batches += keys
                keys.associateWith { it.length }
            }
        }
        store.put("a", 100)

        assertEquals(mapOf("a" to 100), store.getAll(setOf("a", "b"), Freshness.CacheOnly))
        assertEquals(emptyList(), batches)
    }

    @Test
    fun `getAll with NetworkOnly batches even cached keys`() = runTest {
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys ->
                batches += keys
                keys.associateWith { it.length }
            }
        }
        store.put("a", 100)

        assertEquals(mapOf("a" to 1), store.getAll(setOf("a"), Freshness.NetworkOnly))
        assertEquals(listOf(setOf("a")), batches, "NetworkOnly ignores the cached value")
    }

    @Test
    fun `a single get over a batch fetcher works as a batch of one`() = runTest {
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys ->
                batches += keys
                keys.associateWith { it.length }
            }
        }

        assertEquals(3, store.get("abc"))
        assertEquals(listOf(setOf("abc")), batches)
    }

    @Test
    fun `getAll falls back to individual fetches without a batch fetcher`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher {
                calls++
                it.length
            }
        }

        assertEquals(mapOf("a" to 1, "bb" to 2), store.getAll(setOf("a", "bb")))
        assertEquals(2, calls, "a plain fetcher resolves keys individually")
    }

    @Test
    fun `getAll serves stale values when the batch fetch fails`() = runTest {
        val clock = FakeClock()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            freshness { timeToLive = 1.minutes }
            batchFetcher { throw IOException("down") }
        }
        store.put("a", 100)
        clock.advanceBy(5.minutes) // stale

        // CacheFirst: stale ⇒ fetch ⇒ fails ⇒ stale-if-error falls back to the cached value.
        assertEquals(mapOf("a" to 100), store.getAll(setOf("a")))
    }

    @Test
    fun `getAll on an empty key set never calls the fetcher`() = runTest {
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys ->
                batches += keys
                keys.associateWith { it.length }
            }
        }

        assertEquals(emptyMap(), store.getAll(emptySet()))
        assertEquals(emptyList(), batches)
    }

    @Test
    fun `the retry policy wraps a single fetch but not the multi-key batch call`() = runTest {
        var singleAttempts = 0
        var batchAttempts = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            retry {
                maxAttempts = 3
                initialDelay = 1.milliseconds
            }
            batchFetcher { keys ->
                if (keys.size == 1) singleAttempts++ else batchAttempts++
                throw IOException("down")
            }
        }

        // A single get is a batch of one: the retry policy applies (3 attempts).
        assertFailsWith<IOException> { store.get("a") }
        assertEquals(3, singleAttempts)

        // The multi-key batch call is not retried in this release: one attempt, all keys drop.
        assertEquals(emptyMap(), store.getAll(setOf("x", "y")))
        assertEquals(1, batchAttempts)
    }

    @Test
    fun `configuring a batch fetcher alongside another fetcher is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, Int> {
                fetcher { 1 }
                batchFetcher { keys -> keys.associateWith { 1 } }
            }
        }
    }

    @Test
    fun `getAll on a closed store throws`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys -> keys.associateWith { it.length } }
        }
        store.close()

        assertFailsWith<IllegalStateException> { store.getAll(setOf("a")) }
    }
}
