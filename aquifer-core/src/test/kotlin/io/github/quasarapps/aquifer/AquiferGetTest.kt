package io.github.quasarapps.aquifer

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.minutes

class AquiferGetTest {

    @Test
    fun `cache miss fetches and caches the value`() = runTest {
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { key ->
                calls++
                "$key-$calls"
            }
        }

        assertEquals("user-1", store.get("user"))
        assertEquals("user-1", store.get("user"))
        assertEquals(1, calls)
    }

    @Test
    fun `distinct keys fetch independently`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { key -> "value-of-$key" }
        }

        assertEquals("value-of-a", store.get("a"))
        assertEquals("value-of-b", store.get("b"))
    }

    @Test
    fun `cache first refetches once the entry exceeds its time to live`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness { timeToLive = 1.minutes }
        }

        assertEquals(1, store.get("k"))
        clock.advanceBy(1.minutes)
        assertEquals(2, store.get("k"))
        assertEquals(2, calls)
    }

    @Test
    fun `cache first serves a stale value when the refetch fails`() = runTest {
        val clock = FakeClock()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            clock(clock)
            fetcher { error("network down") }
            freshness { timeToLive = 1.minutes }
        }

        store.put("k", "stale-but-usable")
        clock.advanceBy(2.minutes)

        assertEquals("stale-but-usable", store.get("k"))
    }

    @Test
    fun `stale while revalidate returns the stale value immediately and refreshes in the background`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock(clock)
            fetcher { ++calls }
            freshness { timeToLive = 1.minutes }
        }

        store.put("k", 100)
        clock.advanceBy(2.minutes)

        assertEquals(100, store.get("k", Freshness.StaleWhileRevalidate))
        settle()

        assertEquals(1, calls)
        assertEquals(1, store.get("k", Freshness.CacheOnly))
    }

    @Test
    fun `network first fetches even when a fresh value is cached`() = runTest {
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { ++calls }
        }

        store.put("k", 100)

        assertEquals(1, store.get("k", Freshness.NetworkFirst))
        assertEquals(1, calls)
    }

    @Test
    fun `network first falls back to the cached value when the fetch fails`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { error("network down") }
        }

        store.put("k", "cached")

        assertEquals("cached", store.get("k", Freshness.NetworkFirst))
    }

    @Test
    fun `network only propagates fetch failures`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { throw IllegalStateException("boom") }
        }

        store.put("k", "cached")

        val failure = assertFailsWith<IllegalStateException> { store.fresh("k") }
        assertEquals("boom", failure.message)
    }

    @Test
    fun `cache only throws on a cache miss`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "never used" }
        }

        val failure = assertFailsWith<CacheMissException> { store.get("k", Freshness.CacheOnly) }
        assertIs<AquiferException>(failure)
    }

    @Test
    fun `cache only serves stale entries without fetching`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            clock(clock)
            fetcher {
                calls++
                "fetched"
            }
            freshness { timeToLive = 1.minutes }
        }

        store.put("k", "old")
        clock.advanceBy(10.minutes)

        assertEquals("old", store.get("k", Freshness.CacheOnly))
        settle()
        assertEquals(0, calls)
    }

    @Test
    fun `entry written via put is fresh from the moment of the write`() = runTest {
        val clock = FakeClock()
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            clock(clock)
            fetcher {
                calls++
                "fetched"
            }
            freshness { timeToLive = 5.minutes }
        }

        store.put("k", "local")
        clock.advanceBy(2.minutes)

        assertEquals("local", store.get("k"))
        assertEquals(0, calls)
    }
}
