package io.github.quasarapps.aquifer

import app.cash.turbine.test
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame

/**
 * `DataState.Empty` semantics (RFC #23): cache-only streams observe deletion and misses as
 * an affirmative empty state; fetch-capable streams keep communicating through their refetch.
 */
class EmptyStateTest {

    @Test
    fun `a cache-only stream observes invalidate as empty`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 1 }
        }
        store.put("k", 7)

        store.stream("k", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(7, Origin.MEMORY, isStale = false), awaitItem())

            store.invalidate("k")
            assertEquals(DataState.Empty, awaitItem())

            // The slot recovers: a later write is observed normally.
            store.put("k", 8)
            assertEquals(DataState.Content(8, Origin.LOCAL, isStale = false), awaitItem())
        }
    }

    @Test
    fun `a cache-only stream observes invalidateAll as empty`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 1 }
        }
        store.put("k", 7)

        store.stream("k", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(7, Origin.MEMORY, isStale = false), awaitItem())

            store.invalidateAll() // logout-style reset
            assertEquals(DataState.Empty, awaitItem())
        }
    }

    @Test
    fun `repeated invalidations deduplicate to a single empty`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 1 }
        }
        store.put("k", 7)

        store.stream("k", Freshness.CacheOnly).test {
            assertEquals(DataState.Content(7, Origin.MEMORY, isStale = false), awaitItem())

            store.invalidate("k")
            store.invalidate("k")
            store.invalidateAll()
            assertEquals(DataState.Empty, awaitItem())
            settle()
            expectNoEvents()
        }
    }

    @Test
    fun `fetch-capable streams observe invalidation as a refetch, never empty`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { 42 }
        }
        store.put("k", 7)

        store.stream("k").test {
            assertEquals(DataState.Content(7, Origin.MEMORY, isStale = false), awaitItem())

            store.invalidate("k")
            // The deletion is visible as the refetch's Loading with a null value — no Empty.
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Content(42, Origin.FETCHER, isStale = false), awaitItem())
        }
    }

    @Test
    fun `empty carries no value and the extensions treat it accordingly`() {
        val empty: DataState<Int> = DataState.Empty // covariant: Empty fits any value type

        assertNull(empty.value)
        assertFalse(empty.isLoading)
        assertFailsWith<NoSuchElementException> { empty.valueOrThrow() }
        assertSame(DataState.Empty, empty.map { "$it!" })

        var touched = false
        empty
            .onContent { touched = true }
            .onFailure { touched = true }
        assertFalse(touched)
    }
}
