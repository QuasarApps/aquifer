package io.github.quasarapps.aquifer

import app.cash.turbine.test
import kotlinx.coroutines.test.runTest
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes

/** Conditional batch fetching: ETag/304 revalidation composed with batched fetching (RFC #29). */
class ConditionalBatchFetchingTest {

    @Test
    fun `getAll passes each key's validator and stores fresh results`() = runTest {
        val seen = mutableListOf<Map<String, String?>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            conditionalBatchFetcher { validators ->
                seen += validators
                validators.keys.associateWith { FetchResult.Fresh(it.length, validator = "etag-$it") }
            }
        }

        assertEquals(mapOf("a" to 1, "bb" to 2), store.getAll(setOf("a", "bb")))
        assertEquals(
            listOf(mapOf<String, String?>("a" to null, "bb" to null)),
            seen,
            "first call: nothing cached, no validators",
        )

        // A second pass carries the stored validators (NetworkOnly forces the re-request).
        assertEquals(mapOf("a" to 1, "bb" to 2), store.getAll(setOf("a", "bb"), Freshness.NetworkOnly))
        assertEquals(mapOf("a" to "etag-a", "bb" to "etag-bb"), seen[1])
    }

    @Test
    fun `getAll NotModified re-ages cached entries without re-downloading`() = runTest {
        val clock = FakeClock()
        var freshCalls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            freshness { timeToLive = 5.minutes }
            conditionalBatchFetcher { validators ->
                validators.mapValues { (key, validator) ->
                    if (validator == null) {
                        freshCalls++
                        FetchResult.Fresh(key.length, "etag")
                    } else {
                        FetchResult.NotModified
                    }
                }
            }
        }

        assertEquals(mapOf("a" to 1, "bb" to 2), store.getAll(setOf("a", "bb"))) // misses -> Fresh
        assertEquals(2, freshCalls)
        clock.advanceBy(10.minutes) // both stale

        // Stale getAll revalidates: both answer 304, served from cache and re-aged.
        assertEquals(mapOf("a" to 1, "bb" to 2), store.getAll(setOf("a", "bb")))
        assertEquals(2, freshCalls, "NotModified did not re-download")

        // The 304s re-aged the entries: within TTL again, no fetch.
        clock.advanceBy(2.minutes)
        assertEquals(mapOf("a" to 1, "bb" to 2), store.getAll(setOf("a", "bb")))
        assertEquals(2, freshCalls)
    }

    @Test
    fun `a single get over a conditional batch fetcher is a batch of one`() = runTest {
        val seen = mutableListOf<Map<String, String?>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            conditionalBatchFetcher { validators ->
                seen += validators
                validators.keys.associateWith { FetchResult.Fresh(it.length, "etag") }
            }
        }

        assertEquals(3, store.get("abc"))
        assertEquals(listOf(mapOf<String, String?>("abc" to null)), seen)
    }

    @Test
    fun `NotModified reaches a stream as fresh content, not a new value`() = runTest {
        val clock = FakeClock()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            freshness { timeToLive = 5.minutes }
            conditionalBatchFetcher { validators ->
                validators.mapValues { (key, validator) ->
                    if (validator == null) FetchResult.Fresh(key.length, "etag") else FetchResult.NotModified
                }
            }
        }
        assertEquals(1, store.get("a"))
        clock.advanceBy(10.minutes)

        store.stream("a").test {
            assertEquals(DataState.Content(1, Origin.MEMORY, isStale = true), awaitItem())
            assertEquals(DataState.Loading(1), awaitItem())
            assertEquals(DataState.Content(1, Origin.FETCHER, isStale = false), awaitItem())
        }
    }

    @Test
    fun `streamMany over a conditional batch batches the initial fetch into one call`() = runTest {
        val seen = mutableListOf<Map<String, String?>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            conditionalBatchFetcher { validators ->
                seen += validators
                validators.keys.associateWith { FetchResult.Fresh(it.length, "etag-$it") }
            }
        }

        store.streamMany(setOf("a", "bb")).test {
            var latest = awaitItem()
            while (latest.values.any { it !is DataState.Content }) latest = awaitItem()
            assertEquals(mapOf("a" to 1, "bb" to 2), latest.mapValues { it.value.value })
            cancelAndIgnoreRemainingEvents()
        }
        assertEquals(
            listOf(mapOf<String, String?>("a" to null, "bb" to null)),
            seen,
            "one conditional batch call for both keys",
        )
    }

    @Test
    fun `prefetchAll over a conditional batch warms keys in one call`() = runTest {
        val seen = mutableListOf<Map<String, String?>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            conditionalBatchFetcher { validators ->
                seen += validators
                validators.keys.associateWith { FetchResult.Fresh(it.length, "etag") }
            }
        }

        store.prefetchAll(setOf("a", "bb"))
        settle()
        assertEquals(listOf(mapOf<String, String?>("a" to null, "bb" to null)), seen)
        assertEquals(mapOf("a" to 1, "bb" to 2), store.getAll(setOf("a", "bb")))
    }

    @Test
    fun `a key omitted from the conditional batch fails only that key`() = runTest {
        val failures = mutableMapOf<String, Throwable>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            conditionalBatchFetcher { validators ->
                validators.keys.filter { it != "miss" }.associateWith { FetchResult.Fresh(it.length) }
            }
            events(object : AquiferEvents<String> {
                override fun onFetchFailed(key: String, error: Throwable, attempts: Int) {
                    failures[key] = error
                }
            })
        }

        assertEquals(mapOf("a" to 1), store.getAll(setOf("a", "miss")))
        assertIs<BatchKeyMissingException>(failures["miss"])
    }

    @Test
    fun `NotModified for a key with no cached validator fails that key`() = runTest {
        val failures = mutableMapOf<String, Throwable>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            // Wrong: answers 304 though nothing is cached to be "not modified" against.
            conditionalBatchFetcher { validators ->
                validators.keys.associateWith<String, FetchResult<Int>> { FetchResult.NotModified }
            }
            events(object : AquiferEvents<String> {
                override fun onFetchFailed(key: String, error: Throwable, attempts: Int) {
                    failures[key] = error
                }
            })
        }

        assertEquals(emptyMap(), store.getAll(setOf("a")))
        assertIs<IllegalStateException>(failures["a"])
    }

    @Test
    fun `the retry policy wraps the conditional batch call`() = runTest {
        var attempts = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            retry {
                maxAttempts = 3
                initialDelay = 1.milliseconds
            }
            conditionalBatchFetcher {
                attempts++
                throw IOException("down")
            }
        }

        assertEquals(emptyMap(), store.getAll(setOf("a", "b")))
        assertEquals(3, attempts, "whole-batch retry applies to the conditional batch too")
    }

    @Test
    fun `configuring a conditional batch fetcher alongside another fetcher is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, Int> {
                fetcher { 1 }
                conditionalBatchFetcher { validators -> validators.keys.associateWith { FetchResult.Fresh(1) } }
            }
        }
    }
}
