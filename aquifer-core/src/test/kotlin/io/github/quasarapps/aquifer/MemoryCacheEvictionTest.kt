package io.github.quasarapps.aquifer

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class MemoryCacheEvictionTest {

    @Test
    fun `exceeding max entries evicts the least recently used key`() = runTest {
        val fetched = mutableListOf<String>()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            memoryCache { maxEntries = 2 }
            fetcher { key ->
                fetched += key
                "value-$key"
            }
        }

        store.get("a")
        store.get("b")
        store.get("c") // Evicts "a", the least recently used.

        store.get("b") // Still cached.
        store.get("a") // Was evicted, so this fetches again.

        assertEquals(listOf("a", "b", "c", "a"), fetched)
    }

    @Test
    fun `reading a key protects it from eviction`() = runTest {
        val fetched = mutableListOf<String>()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            memoryCache { maxEntries = 2 }
            fetcher { key ->
                fetched += key
                "value-$key"
            }
        }

        store.get("a")
        store.get("b")
        store.get("a") // Marks "a" as recently used.
        store.get("c") // Evicts "b" instead of "a".

        store.get("a")
        assertEquals(listOf("a", "b", "c"), fetched)

        store.get("b")
        assertEquals(listOf("a", "b", "c", "b"), fetched)
    }
}
