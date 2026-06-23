package io.github.quasarapps.aquifer

import app.cash.turbine.ReceiveTurbine
import app.cash.turbine.test
import kotlinx.coroutines.test.runTest
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** `streamMany`: a combined `Map<K, DataState>` flow whose initial fetches are batched. */
class StreamManyTest {

    /** Awaits combined emissions until every member key has resolved to [DataState.Content]. */
    private suspend fun ReceiveTurbine<Map<String, DataState<Int>>>.awaitAllContent(): Map<String, DataState<Int>> {
        var latest = awaitItem()
        while (latest.values.any { it !is DataState.Content }) latest = awaitItem()
        return latest
    }

    @Test
    fun `streamMany batches all missing member keys into one backend call`() = runTest {
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys ->
                batches += keys
                keys.associateWith { it.length }
            }
        }

        store.streamMany(setOf("a", "bb", "ccc")).test {
            awaitAllContent()
            cancelAndIgnoreRemainingEvents()
        }
        assertEquals(listOf(setOf("a", "bb", "ccc")), batches, "one batch call for all three keys")
    }

    @Test
    fun `streamMany emits a combined map keyed by every member`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys -> keys.associateWith { it.length } }
        }

        store.streamMany(setOf("a", "bb")).test {
            val resolved = awaitAllContent()
            assertEquals(
                mapOf(
                    "a" to DataState.Content(1, Origin.FETCHER, isStale = false),
                    "bb" to DataState.Content(2, Origin.FETCHER, isStale = false),
                ),
                resolved,
            )
            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `streamMany serves a fresh cached key and batches only the missing ones`() = runTest {
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            freshness { timeToLive = 10.minutes }
            batchFetcher { keys ->
                batches += keys
                keys.associateWith { it.length }
            }
        }
        store.put("a", 100) // fresh

        store.streamMany(setOf("a", "bb")).test {
            val resolved = awaitAllContent()
            assertEquals(100, resolved["a"]?.value)
            assertEquals(2, resolved["bb"]?.value)
            cancelAndIgnoreRemainingEvents()
        }
        assertEquals(listOf(setOf("bb")), batches, "the fresh key is not batched")
    }

    @Test
    fun `streamMany re-emits when one member key is written`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys -> keys.associateWith { it.length } }
        }

        store.streamMany(setOf("a", "bb")).test {
            awaitAllContent()

            store.put("a", 100)
            var updated = awaitItem()
            while (updated["a"]?.value != 100) updated = awaitItem()
            assertEquals(100, updated["a"]?.value)
            assertEquals(2, updated["bb"]?.value, "the other key is untouched")
            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `streamMany without a batch fetcher fetches each key individually`() = runTest {
        val fetched = mutableListOf<String>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { key ->
                fetched += key
                key.length
            }
        }

        store.streamMany(setOf("a", "bb")).test {
            val resolved = awaitAllContent()
            assertEquals(mapOf("a" to 1, "bb" to 2), resolved.mapValues { it.value.value })
            cancelAndIgnoreRemainingEvents()
        }
        assertEquals(setOf("a", "bb"), fetched.toSet())
    }

    @Test
    fun `streamMany on an empty key set emits a single empty map`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys -> keys.associateWith { it.length } }
        }

        store.streamMany(emptySet()).test {
            assertEquals(emptyMap(), awaitItem())
            awaitComplete()
        }
    }

    @Test
    fun `streamMany on a closed store throws`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys -> keys.associateWith { it.length } }
        }
        store.close()

        assertFailsWith<IllegalStateException> { store.streamMany(setOf("a")) }
    }

    @Test
    fun `streamMany reports a suppressed key's onFetchSuppressed exactly once`() = runTest {
        val suppressions = mutableListOf<String>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { throw IOException("down") }
            negativeCache { timeToLive = 30.seconds }
            events(object : AquiferEvents<String> {
                override fun onFetchSuppressed(key: String, error: Throwable, remaining: Duration) {
                    suppressions += key
                }
            })
        }
        // Fail "a" once (a batch of one) so it becomes negative-cached.
        assertFailsWith<IOException> { store.get("a") }
        suppressions.clear()

        store.streamMany(setOf("a")).test {
            awaitItem() // the suppressed, valueless key surfaces as Failure
            cancelAndIgnoreRemainingEvents()
        }
        // The pre-batch gate stays silent (reportSuppression = false); only the stream prime reports.
        assertEquals(listOf("a"), suppressions, "exactly one onFetchSuppressed, not one per decision pass")
    }
}
