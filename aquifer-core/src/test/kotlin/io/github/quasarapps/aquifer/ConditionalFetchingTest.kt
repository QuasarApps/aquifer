package io.github.quasarapps.aquifer

import app.cash.turbine.test
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.minutes

/** Conditional fetching: validators round-trip and `NotModified` re-ages the cached entry. */
class ConditionalFetchingTest {

    @Test
    fun `the first fetch gets no validator and a fresh result stores one`() = runTest {
        val seen = mutableListOf<String?>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            conditionalFetcher { _, validator ->
                seen += validator
                FetchResult.Fresh(seen.size, validator = "etag-${seen.size}")
            }
        }

        assertEquals(1, store.fresh("k"))
        assertEquals(2, store.fresh("k"))

        // First fetch: nothing cached, no validator. Second: the stored "etag-1".
        assertEquals(listOf(null, "etag-1"), seen)
    }

    @Test
    fun `not modified re-ages the entry instead of re-downloading`() = runTest {
        val clock = FakeClock()
        var fetches = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            freshness { timeToLive = 5.minutes }
            conditionalFetcher { _, validator ->
                fetches++
                if (validator == null) FetchResult.Fresh(42, "etag") else FetchResult.NotModified
            }
        }

        assertEquals(42, store.get("k")) // miss -> Fresh(42, "etag")
        clock.advanceBy(10.minutes) // stale by TTL

        // Stale read revalidates: the fetcher answers NotModified against "etag".
        assertEquals(42, store.get("k"))
        assertEquals(2, fetches)

        // The 304 re-aged the entry: within TTL again, no third fetch.
        clock.advanceBy(2.minutes)
        assertEquals(42, store.get("k"))
        assertEquals(2, fetches)
    }

    @Test
    fun `not modified reaches streams as fresh content, not a new value`() = runTest {
        val clock = FakeClock()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            freshness { timeToLive = 5.minutes }
            conditionalFetcher { _, validator ->
                if (validator == null) FetchResult.Fresh(42, "etag") else FetchResult.NotModified
            }
        }
        assertEquals(42, store.get("k"))
        clock.advanceBy(10.minutes)

        store.stream("k").test {
            // Stale snapshot, then the revalidation that comes back 304:
            assertEquals(DataState.Content(42, Origin.MEMORY, isStale = true), awaitItem())
            assertEquals(DataState.Loading(42), awaitItem())
            assertEquals(DataState.Content(42, Origin.FETCHER, isStale = false), awaitItem())
        }
    }

    @Test
    fun `the validator survives persistence and process restarts`() = runTest {
        val disk = InMemorySourceOfTruth<String, Int>()
        val first = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            conditionalFetcher { _, _ -> FetchResult.Fresh(1, validator = "etag-1") }
        }
        assertEquals(1, first.fresh("k"))
        first.close()

        var receivedValidator: String? = null
        val reborn = aquifer<String, Int> {
            scope(backgroundScope)
            persistence(disk)
            conditionalFetcher { _, validator ->
                receivedValidator = validator
                FetchResult.NotModified
            }
        }
        assertEquals(1, reborn.fresh("k"))
        assertEquals("etag-1", receivedValidator)
    }

    @Test
    fun `not modified without a cached entry fails the fetch`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            conditionalFetcher { _, _ -> FetchResult.NotModified }
        }

        assertFailsWith<IllegalStateException> { store.get("k") }
    }

    @Test
    fun `a local put clears the validator`() = runTest {
        var seen: String? = "sentinel"
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            conditionalFetcher { _, validator ->
                seen = validator
                FetchResult.Fresh(9)
            }
        }
        store.put("k", 5) // local writes carry no validator

        assertEquals(9, store.fresh("k"))
        assertNull(seen)
    }

    @Test
    fun `configuring both fetchers is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, Int> {
                fetcher { 1 }
                conditionalFetcher { _, _ -> FetchResult.Fresh(1) }
            }
        }
    }
}
